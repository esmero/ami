<?php

namespace Drupal\ami\Plugin\ImporterAdapter;

use Drupal\ami\Plugin\ImporterAdapterInterface as ImporterPluginAdapterInterface;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Messenger\MessengerInterface;
use Drupal\Core\Messenger\MessengerTrait;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\StreamWrapper\StreamWrapperManagerInterface;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use GuzzleHttp\ClientInterface;
use Solarium\Core\Query\AbstractQuery as SolariumAbstractQuery;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\ami\AmiUtilityService;
use Solarium\Core\Client\Adapter\Curl as SolariumCurl;
use Solarium\Client as SolariumClient;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Drupal\file\Entity\File;

/**
 * ADO importer from a remote Google Spreadsheet.
 *
 * @ImporterAdapter(
 *   id = "solr",
 *   label = @Translation("Islandora 7 Solr Importer"),
 *   remote = true,
 *   batch = true
 * )
 */
class SolrImporter extends SpreadsheetImporter {

  use StringTranslationTrait;
  use MessengerTrait;

  const SOLR_FIELD_SUFFIX = ['_ms', '_mdt', '_s', '_dt', '_t', '_mt', '_mlt'];

  const MULTICHILDREN_CMODELS = [
    'info:fedora/islandora:compoundCModel',
    'info:fedora/islandora:bookCModel',
    'info:fedora/islandora:newspaperIssueCModel',
    'info:fedora/islandora:newspaperCModel',
    'info:fedora/islandora:manuscriptCModel',
  ];

  const FILE_COLUMNS = [
    'documents',
    'images',
    'texts',
    'videos',
    'audios',
    'models',
    'applications',
  ];

  const BATCH_INCREMENTS = 500;

  /**
   * @var \Drupal\Core\StreamWrapper\StreamWrapperManagerInterface
   */
  protected StreamWrapperManagerInterface $streamWrapperManager;

  /**
   * @var \GuzzleHttp\ClientInterface
   */
  protected $httpClient;

  /**
   * Solr Importer constructor.
   *
   * @param array $configuration
   * @param $plugin_id
   * @param $plugin_definition
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface $entityTypeManager
   * @param \GuzzleHttp\ClientInterface $httpClient
   * @param \Drupal\Core\StreamWrapper\StreamWrapperManagerInterface $streamWrapperManager
   * @param \Drupal\ami\AmiUtilityService $ami_utility
   */
  public function __construct(array $configuration, $plugin_id, $plugin_definition, EntityTypeManagerInterface $entityTypeManager, ClientInterface $httpClient, StreamWrapperManagerInterface $streamWrapperManager, AmiUtilityService $ami_utility) {
    parent::__construct($configuration, $plugin_id, $plugin_definition,
      $entityTypeManager, $streamWrapperManager, $ami_utility);
    $this->streamWrapperManager = $streamWrapperManager;
    $this->httpClient = $httpClient;
  }

  /**
   * {@inheritdoc}
   */
  public static function create(ContainerInterface $container, array $configuration, $plugin_id, $plugin_definition) {
    return new static(
      $configuration,
      $plugin_id,
      $plugin_definition,
      $container->get('entity_type.manager'),
      $container->get('http_client'),
      $container->get('stream_wrapper_manager'),
      $container->get('ami.utility')
    );
  }

  /**
   * {@inheritdoc}
   */
  public function interactiveForm(array $parents, FormStateInterface $form_state): array {
    // None of the interactive Form elements should be persisted as Config elements
    // Here.
    // Maybe we should have some annotation that says which ones for other plugins?
    //$form = parent::interactiveForm($parents,$form_state);
    $form['op'] = [
      '#type' => 'select',
      '#title' => $this->t('Operation'),
      '#options' => [
        'create' => 'Create New ADOs',
      ],
      '#description' => $this->t('The desired Operation'),
      '#required' => TRUE,
      '#default_value' => $form_state->getValue(array_merge($parents, ['op'])),
      '#empty_option' => $this->t('- Please select an Operation -'),
    ];
    $form['ready'] = [
      '#type' => 'value',
      '#default_value' => $form_state->getValue(array_merge($parents,
        ['ready']), FALSE),
    ];

    $rows_form_state  = $form_state->get('rows');
    $rows = $form_state->getValue(array_merge($parents,
      ['solarium_config', 'rows']), 0);
    if ($rows_form_state !== NULL) {
      $rows = $rows_form_state;
    }
    if (empty($rows)) {
      $form_state->setValue(['pluginconfig','ready'], FALSE);
    }

    /* Calculate Max timeout based on PHP Settings */
    $max_time = (int) ini_get('max_execution_time') * 0.75;
    if ($max_time === 0) {
      $max_time = 60;
    }
    $max_time = floor($max_time);

    $form['solarium_config'] = [
      '#prefix' => '<div id="ami-solrapi">',
      '#suffix' => '</div>',
      '#tree' => TRUE,
      '#type' => 'fieldset',
      '#title' => 'Solr Server Configuration',
      '#element_validate' => [[get_class($this), 'validateSolrConfig']],
      'islandora_collection' => [
        '#type' => 'textarea',
        '#required' => TRUE,
        '#title' => $this->t('PID(s) of the Islandora Collection(s) you want to fetch members for or Individual PIDs (non collections)'),
        '#description' => $this->t('Example: islandora:root. If multiple use one PID per line.'),
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_config', 'islandora_collection'])),
      ],
      'scheme' => [
        '#type' => 'radios',
        '#required' => TRUE,
        '#title' => $this->t('The Scheme (protocol) of your Solr Server'),
        '#options' => [
          'http' => 'HTTP',
          'https' => 'HTTPS'
        ],
        '#description' => $this->t('If you port is 443 most likely your scheme is going to be HTTPS.'),
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_config', 'schema'])),
      ],
      'host' => [
        '#type' => 'textfield',
        '#required' => TRUE,
        '#title' => $this->t('Host of your Solr Server'),
        '#description' => $this->t('Example: repositorydomain.org'),
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_config', 'host'])),
      ],
      'port' => [
        '#type' => 'textfield',
        '#required' => TRUE,
        '#title' => $this->t('Port'),
        '#description' => $this->t('Example: 8080'),
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_config', 'port'])),
      ],
      'path' => [
        '#type' => 'textfield',
        '#required' => TRUE,
        '#title' => $this->t('Path'),
        '#description' => $this->t('example: /'),
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_config', 'path'])),
      ],
      'timeout' => [
        '#type' => 'number',
        '#required' => TRUE,
        '#min' => 5,
        '#max' => $max_time,
        '#title' => $this->t('Remote Solr Server time out in seconds.'),
        '#description' => $this->t('The default is 5 seconds. The maximum allowed value your server can manage is @max. seconds',[
          '@max' => $max_time
        ]),
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_config', 'timeout']),5),
      ],
      'type' => [
        '#type' => 'select',
        '#required' => TRUE,
        '#options' => [
          'single' => 'Single Solr Server',
          'cloud' => 'Solr Cloud Ensemble',
        ],
        '#title' => $this->t('Type of Solr deployment'),
        '#description' => $this->t('Most typical one is Single Server.'),
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_config', 'type'])),
      ],
      'core' => [
        '#type' => 'textfield',
        '#title' => $this->t('Core'),
        '#description' => $this->t('example: islandora'),
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_config', 'core'])),
        '#states' => [
          'visible' => [
            ':input[name*="type"]' => ['value' => 'single'],
          ],
          'required' => [
            ':input[name*="type"]' => ['value' => 'single'],
          ]
        ]
      ],
      'collection' => [
        '#type' => 'textfield',
        '#title' => $this->t('Collection'),
        '#description' => $this->t('example: collection1'),
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_config', 'collection'])),
        '#states' => [
          'visible' => [
            ':input[name*="type"]' => ['value' => 'cloud'],
          ],
          'required' => [
            ':input[name*="type"]' => ['value' => 'cloud'],
          ]
        ]
      ],
      'start' => [
        '#type' => 'number',
        '#min' => 0,
        '#max' => 65535,
        '#title' => $this->t('Starting Row'),
        '#description' => $this->t('Initial Row to fetch. This is an offset. Defaults to 0'),
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_config', 'start']), 0),
      ],
      'rows' => [
        '#type' => 'number',
        '#min' => 0,
        '#max' => 65535,
        '#title' => $this->t('Number of Rows for Top Objects'),
        '#description' => $this->t('Total number of Rows to Fetch for top Objects. Settings this to empty or null will prefill with the real Number Rows found by the Solr Query invoked. If you set this number higher than the actual results we will only fetch what can be fetched.</br><em>Note:</em>Children objects are not being counted for and will add to your total spreadsheet row count if found.'),
        '#default_value' => $rows,
      ],
    ];
    /**
     * @TODO let's tell the user why it is not ready?
     * Also todo, move mappings already done between children/parents/children
     * That way previous decisions on any of each sides will stick on the other
     */
    $cmodels = $form_state->get('facet_cmodel') ?? $form_state->getValue(array_merge($parents,
        ['solarium_mapping', 'cmodel_mapping']), []);
    $cmodels_children = $form_state->get('facet_cmodel_children') ?? $form_state->getValue(array_merge($parents,
        ['solarium_mapping', 'cmodel_children'], []));

    // New. Give the user choices for fetching more than just OBJ.
    $datastreams = $form_state->get('facet_datastreams') ?? $form_state->getValue(array_merge($parents,
        ['solarium_mapping', 'datastreams_mapping'], []));

    if (empty($cmodels) || empty($datastreams)) {
      $form_state->setValue(['pluginconfig','ready'], FALSE);
    }


    if (empty($cmodels_children) && $form_state->getValue(array_merge($parents,
        ['solarium_mapping', 'collapse']), FALSE)) {
      $form_state->setValue(['pluginconfig','ready'], FALSE);
    }

    $default_parent = $form_state->getValue(array_merge($parents,
      ['solarium_mapping', 'parent_ado']), NULL);
    if ($default_parent) {
      try {
        $default_parent = $this->entityTypeManager->getStorage('node')
          ->load($default_parent);
      }
      catch (\Exception $e) {
        $this->messenger()->addError('Could not load Parent ADO with message @e'. [
            '@e' => $e->getMessage(),
          ]);
      }
    }

    $types = $this->AmiUtilityService->getWebformOptions();
    $cmodels_source = $cmodels ? array_combine(array_keys($cmodels),
      array_keys($cmodels)) : [];
    $cmodels_source_children = $cmodels_children ? array_combine(array_keys($cmodels_children),
      array_keys($cmodels_children)) : [];


    $datastreams_source = $datastreams ? array_combine(array_keys($datastreams),
      array_keys($datastreams)) : [];
    unset($datastreams_source['OBJ']);


    $cmodel_values = $form_state->getValue(array_merge($parents,
      ['solarium_mapping', 'cmodel_mapping'], []));
    $cmodel_values_children = $form_state->getValue(array_merge($parents,
      ['solarium_mapping', 'cmodel_children'], []));
    /* Doing this for Alliomeria, so mapping is passed from children/tops/back */
    $cmodel_values_combined = $cmodel_values + $cmodel_values_children;

    $datastreams_values_combined =  $form_state->getValue(array_merge($parents,
      ['solarium_mapping', 'datastreams_mapping'], []));
    $datastreams_how =  $form_state->getValue(array_merge($parents,
      ['solarium_mapping', 'datastreams_how'], []));


    $form['solarium_mapping'] = [
      '#tree' => TRUE,
      '#prefix' => '<div id="ami-solrmapping">',
      '#suffix' => '</div>',
      '#tree' => TRUE,
      '#type' => 'fieldset',
      '#title' => 'Islandora Mappings',
      'collapse' => [
        '#type' => 'checkbox',
        '#title' => $this->t('Collapse Multi Children Objects'),
        '#description' => $this->t('This will collapse Children Datastreams into a single ADO with many attached files. e.g Book Pages will be fetched but also the Top Level PDF if present.'),
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_mapping', 'collapse'])),
      ],
      'parent_ado' => [
        '#type' => 'entity_autocomplete',
        '#title' => $this->t('ADO to be used as Parent'),
        '#description' => $this->t('The ADO used as parent of the Imported Objects'),
        '#target_type' => 'node',
        '#maxlength' => 1024,
        '#default_value' => $default_parent,
        '#selection_handler' => 'default:nodewithstrawberry',
      ],
      'datastreams_mapping' => [
        '#access' => !empty($cmodels),
        '#type' => 'checkboxes',
        '#title' => $this->t('Additional Datastreams to fetch'),
        '#required' => FALSE,
        '#description_display' => 'before',
        '#description' => $this->t('Additional Datastreams to fetch. <em>OBJ</em> datastream will always be fetched. Not all datastreams listed here might be present once your data is fetched.'),
        '#default_value' => $datastreams_values_combined ?? [],
        '#options' => $datastreams_source ?? [],
      ],
      'datastreams_how' => [
        '#access' => !empty($cmodels),
        '#type' => 'select',
        '#title' => $this->t('Where extra datastreams should go'),
        '#required' => FALSE,
        '#description_display' => 'before',
        '#description' => $this->t('Datastreams to fetch. <em>OBJ</em> datastream will always be fetched. Not all datastreams listed here might be present once your data is fetched.'),
        '#default_value' => 'standard',
        '#default_value' => $datastreams_how,
        '#options' => [
          'standard' => $this->t('organize by mime type. e.g TRANSCRIPT will go into the "texts" column'),
          'islandora' => $this->t('Each in a separate column based on the datastream name, TRANSCRIPT will go into the "transcripts" column')
          ],
        '#states' => [
          'visible' => [
            ':input[name^="pluginconfig[solarium_mapping][datastreams_mapping]"]' => [['checked' => TRUE]],
          ]
        ],
      ],
      'cmodel_mapping' => [
        '#access' => !empty($cmodels),
        '#type' => 'webform_mapping',
        '#title' => $this->t('Required ADO mappings.'),
        '#format' => 'list',
        '#required' => TRUE,
        '#description_display' => 'before',
        '#description' => $this->t('Your Islandora Content Models to ADO types mapping, eg: for <em>info:fedora/islandora:sp_large_image_cmodel</em> you may want to use <em>Photograph</em>.'),
        '#empty_option' => $this->t('- Please Map your Islandora CMODELs to ADO types -'),
        '#empty_value' => NULL,
        '#default_value' => $cmodel_values_combined,
        '#source' => $cmodels_source,
        '#source__title' => $this->t('CMODELs found'),
        '#destination__title' => $this->t('ADO Types'),
        '#destination' => $types,
        '#destination__type' => 'webform_select_other',
      ],
      'cmodel_children' => [
        '#type' => 'webform_mapping',
        '#title' => $this->t('ADO mappings for Child Objects.'),
        '#description_display' => 'before',
        '#description' => $this->t('Your Islandora Content Models to ADO types mapping for possible Children, eg: for <em>info:fedora/islandora:sp_large_image_cmodel</em> you may want to use <em>Photograph</em>.'),
        '#empty_option' => $this->t('- Please Map your Islandora CMODELs to ADO types -'),
        '#empty_value' => NULL,
        '#default_value' => $cmodel_values_combined,
        '#source' => $cmodels_source_children,
        '#source__title' => $this->t('Other CMODELs'),
        '#destination__title' => $this->t('ADO Types'),
        '#destination' => $types,
        '#destination__type' => 'webform_select_other',
        '#states' => [
          'visible' => [
            ':input[name="pluginconfig[solarium_mapping][collapse]"]' => ['checked' => FALSE],
          ],
          'required' => [
            ':input[name="pluginconfig[solarium_mapping][collapse]"]' => ['checked' => FALSE],
          ]
        ]
      ],
      'server_domain' => [
        '#access' => !empty($cmodels),
        '#required' => TRUE,
        '#type' => 'url',
        '#title' => $this->t('Repository Domain'),
        '#description' => t('Where we can find your datastreams, example: https://repositorydomain.org'),
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_mapping', 'server_domain'])),
      ]
    ];

    return $form;
  }

  public static function validateSolrConfig($element, FormStateInterface $form_state) {

    if (isset($form_state->getTriggeringElement()['#name']) && $form_state->getTriggeringElement()['#name'] == 'prev') {
      return;
    }

    $config = $form_state->getValue($element['#parents']);

    if ((empty($config['host']) || empty($config['path']))) {
      $form_state->setError($element,
        t('Please fill Solr Server Config.'));
    }
    $config['port'] = !empty($config['port']) ? (int)$config['port'] : NULL;

    $solr_config = [
      'endpoint' => [
        'amiremote' => [
          'host' => $config['host'],
          'scheme' => $config['scheme'] ?? 'http',
          'port' => $config['port'],
          'path' => $config['path'],
        ],
      ],
    ];
    if ($config['type'] == 'single') {
      $solr_config['endpoint']['amiremote']['core'] = $config['core'];
      if (empty($config['core'])) {
        $form_state->setError($element['core'],
          t('Please Setup your Solr Core.'));
      }
    }
    else {
      // Solr Cloud uses collection instead of core
      $solr_config['endpoint']['amiremote']['collection'] = $config['collection'];
      if (empty($config['collection'])) {
        $form_state->setError($element['collection'],
          t('Please Setup your Solr Cloud Collection.'));
      }
    }

    $adapter = new SolariumCurl(); // or any other adapter implementing AdapterInterface
    $adapter->setTimeout($config['timeout'] ?? 5);
    $eventDispatcher = new EventDispatcher();

    $client = new SolariumClient($adapter, $eventDispatcher, $solr_config);
    $ping = $client->createPing();

    // execute the ping query
    try {
      $result = $client->ping($ping);
    } catch (\Exception $e) {
      $form_state->setError($element,
        t('Ups. We could not contact your server. Check if your settings are correct and/or firewalls are open for this IP address. Remote error is @e', [
          '@e' => $e->getMessage()
        ]));
    }
  }

  /**
   * {@inheritdoc}
   */
  public function getInfo(array $config, FormStateInterface $form_state, $page = 0, $per_page = 20): array {
    $filename_columns = [];
    $datastreams_how = $config['solarium_mapping']['datastreams_how'] ?? 'standard';
    $datastreams_additional = $config['solarium_mapping']['datastreams_mapping'] ?? [];
    $datastreams_additional = array_filter($datastreams_additional);
    if ($datastreams_how !== 'standard' && !empty($datastreams_additional)) {
      foreach ($datastreams_additional as $datastreams_additional) {
        $filename_columns[] = strtolower($datastreams_additional).'s';
      }
    }
    $filename_columns = array_unique(array_merge(static::FILE_COLUMNS,$filename_columns));

    $solr_config = [
      'endpoint' => [
        'amiremote' => [
          'host' => $config['solarium_config']['host'],
          'scheme' => $config['solarium_config']['scheme'],
          'port' => $config['solarium_config']['port'],
          'path' => $config['solarium_config']['path'],
        ],
      ],
    ];
    if ($config['solarium_config']['type'] == 'single') {
      $solr_config['endpoint']['amiremote']['core'] = $config['solarium_config']['core'];
    }
    else {
      // Solr Cloud uses collection instead of core
      $solr_config['endpoint']['amiremote']['collection'] = $config['solarium_config']['collection'];
    }
    $input = $config['solarium_config']['islandora_collection'];

    $adapter = new SolariumCurl(); // or any other adapter implementing AdapterInterface
    $adapter->setTimeout($config['solarium_config']['timeout'] ?? 5);
    $eventDispatcher = new EventDispatcher();

    $client = new SolariumClient($adapter, $eventDispatcher, $solr_config);
    $ping = $client->createPing();
    $tabdata = ['headers' => [], 'data' => [], 'totalrows' => 0];

    // execute the ping query
    try {
      $result = $client->ping($ping);
      $ping_sucessful = $result->getData();
    } catch (\Exception $e) {
      $form_state->setError($element,
        $this->t('Ups. We could not contact your server. Check if your settings,ports,core,etc are correct and/or firewalls are open for this IP address. The error thrown is: @e',
        [
          '@e' => $e->getMessage()
        ]));
      $form_state->setValue(['pluginconfig','ready'], FALSE);
      return $tabdata;
    }

    if ($ping_sucessful) {
      try {
        $query = $client->createSelect();
        $helper = $query->getHelper();
        /* @TODO we should save the config this way too */
        $pids = array_map('trim', explode("\n", $input));
        $pids = array_filter($pids);
        foreach ($pids as &$pid) {
          $pid = $helper->escapePhrase('info:fedora/' . $pid);
        }
        $escaped = implode(' OR ', $pids);
        $escaped = '('. $escaped .')';

        $query->setQuery('*:*');
        $query->createFilterQuery('collection_members')->setQuery('RELS_EXT_isMemberOfCollection_uri_s:' . $escaped . ' OR RELS_EXT_isMemberOf_uri_s:' . $escaped);
        // PLEASE REMOVE Collection Objects that ARE ALSO part of a compound. WE DO NOT WANT THOSE
        $query->createFilterQuery('notconstituent')->setQuery('-RELS_EXT_isConstituentOf_uri_ms:[ * TO * ]');
        /*
         * This is a good option if Solr data is homogenenous
         * Maybe on a next iteration?
        $groupComponent = $query->getGrouping();
        $groupComponent->addField('RELS_EXT_hasModel_uri_s');
        // maximum number of items per group
        $groupComponent->setLimit(3);
        // get a group count
        $groupComponent->setNumberOfGroups(true); */

        $facetSet = $query->getFacetSet();
        $facet = $facetSet->createFacetField('cmodel')
          ->setField('RELS_EXT_hasModel_uri_s');
        $facet2 = $facetSet->createFacetField('dsid')
          ->setField('fedora_datastreams_ms');
        /* Work around only needed for a few very strange custom islandoras */
        $facetSet->setLimit(500);
        $rows = (int) $config['solarium_config']['rows'] > 0 && (int) $config['solarium_config']['rows'] <= 100  ? $config['solarium_config']['rows'] : 100;

        $query->setResponseWriter('csv');
        $query->setStart(0)->setRows(0);

        $query->setFields([
          'PID',
          'fgs_label_s',
          'fedora_datastream_latest_*_MIMETYPE_ms',
          'mods_*',
          'RELS_EXT_hasModel_uri_s',
          'RELS_EXT_isM*',
          'RELS_EXT_isC*',
          'RELS_EXT_isP*',
          'fgs_createdDate_dt',
          'fgs_ModifiedDate_dt',
          'fedora_datastreams_ms'
        ]);
        $resultset = $client->select($query);
        $allheaders = $resultset->getResponse()->getBody();
        $allheaders_array = str_getcsv($allheaders);
        $allheaders_array = array_map([$this, 'multipleToSingleFieldName'], $allheaders_array);
        // Remove this non sense of mods_*_authority_marcrelator
        array_filter($allheaders_array, function ($value) {
          return $this->endsWith($value, 'authority_marcrelator');
        });

        // Reuse the query now
        $query->setResponseWriter(SolariumAbstractQuery::WT_JSON);
        $query->setStart($config['solarium_config']['start'] ?? 0)->setRows($rows);
        $resultset = $client->select($query);
        // display the total number of documents found by solr
        $facet = $resultset->getFacetSet()->getFacet('cmodel');
        $cmodel = [];
        $collapse_children =  $config['solarium_mapping']['collapse'] ?? FALSE;
        foreach ($facet as $value => $count) {
          if ($count) {
            $cmodel[$value] = $value;
          }
          else{
            $cmodel_children[$value] = $value;
          }
        }

        $facet2 = $resultset->getFacetSet()->getFacet('dsid');
        foreach ($facet2 as $value => $count) {
          $datastreams[$value] = $value;
        }

        // Set extracted Facets in a temp value
        $form_state->set('facet_cmodel', $cmodel);
        $form_state->set('facet_cmodel_children', $cmodel_children);
        $form_state->set('facet_datastreams', $datastreams);
        // Unset the passed ones so we do not carry those over

        $resultset_iterator = $resultset->getIterator();
        // Empty value? just return
        if (($resultset_iterator == NULL) || empty($resultset_iterator)) {
          $this->messenger()->addMessage(
            t('Nothing to read, check your Solr Query Arguments'),
            MessengerInterface::TYPE_ERROR
          );
          $form_state->setValue(['pluginconfig','ready'], FALSE);
          return $tabdata;
        }
      } catch (\Exception $e) {
        $this->messenger()->addMessage(
          t('Solr Error: @e', ['@e' => $e->getMessage()]),
          MessengerInterface::TYPE_ERROR
        );
        $form_state->setValue(['pluginconfig','ready'], FALSE);
        return $tabdata;
      }
      $table = [];
      // Add 'type' because we provide it always.

      $headers['type'] = 'type';
      $headers['ismemberof'] = 'ismemberof';
      $headers['ispartof'] = 'ispartof';
      $allheaders_array = array_map('strtolower', $allheaders_array);
      foreach ($allheaders_array as $headerkey) {
        $headers[$headerkey] = $headerkey;
      }
      $highestRow = 0;

      for ($resultset_iterator->rewind(); $resultset_iterator->valid(); $resultset_iterator->next()) {
        try {
          $highestRow = $resultset_iterator->key() + 1;
          $document = $resultset_iterator->current();
          foreach ($document as $field => $value) {
            $fieldname = $field;
            $fieldname = $this->multipleToSingleFieldName($field);
            // Exclude this non-sense fields
            if (strpos($field, '_roleTerm_', 0) !== FALSE) {
              continue;
            }
            if ($this->endsWith($fieldname, 'authority_marcrelator')) {
              continue;
            }
            $headers[$fieldname] = $fieldname;
            // this converts multi valued fields to a |@|  string
            $original_value = $sp_data[$resultset_iterator->key()][$fieldname] ?? NULL;
            $value = $this->concatValues((array) $value, $original_value);
            $sp_data[$resultset_iterator->key()][$fieldname] = $value;
          }
          // Let's add generic all columns needed for files.
          foreach ($filename_columns as $column) {
            $sp_data[$resultset_iterator->key()][$column] = '';
          }
          $sp_data[$resultset_iterator->key()]['type'] = $config['solarium_mapping']['cmodel_mapping'][$sp_data[$resultset_iterator->key()]['rels_ext_hasmodel_uri']] ?? 'Thing';
        }
        catch (\Exception $exception) {
          continue;
        }
      }
      // $headers = $allheaders_array;
      // Also add these base ones to the headers
      foreach ($filename_columns as $column) {
        $headers[$column] = $column;
      }


      if (($highestRow) >= 1) {
        // Returns Row Headers.
        foreach ($sp_data as $rowindex => $row) {
          $i = 0;
          $newrow = [];
          foreach ($headers as $field) {
            $newrow[$i] = $sp_data[$rowindex][$field] ?? '';
            $i++;
          }
          $table[$rowindex] = $newrow;
        }
      }

      $tabdata = [
        'headers' => array_keys($headers),
        'data' => $table,
        'totalrows' => $highestRow,
      ];
      //dpm($tabdata);

      // Check if we still have the same CMODELS after running this. In case something changed.
      if ((count(array_intersect_key($form_state->getValue(['pluginconfig', 'solarium_mapping','cmodel_mapping'], []), $cmodel)) == count($cmodel)) && count($cmodel) >= 1) {
        $form_state->setValue(['pluginconfig', 'ready'], TRUE);
      } else {
        $form_state->setValue(['pluginconfig', 'ready'], FALSE);
      }
      if (((count(array_intersect_key($form_state->getValue(['pluginconfig', 'solarium_mapping','cmodel_children'], []), $cmodel_children)) == count($cmodel_children))
          && count($cmodel_children) >= 1) || $form_state->getValue(['pluginconfig', 'solarium_mapping', 'collapse']) == TRUE) {
        $form_state->setValue(['pluginconfig', 'ready'], TRUE);
      }
      else {
        $form_state->setValue(['pluginconfig', 'ready'], FALSE);
      }



      if ((int) $form_state->getValue(['pluginconfig', 'solarium_config', 'rows'],0) == 0) {
        $user_input = $form_state->getUserInput();
        $user_input['pluginconfig']['solarium_config']['rows'] = $resultset->getNumFound();
        $form_state->setUserInput($user_input);
        $form_state->setValue(['pluginconfig', 'solarium_config', 'rows'],  $resultset->getNumFound());
        $form_state->setValue(['pluginconfig','ready'], FALSE);
        if ($resultset->getNumFound() == 0) {
          $this->messenger()->addMessage(
            t(
              'Your query did not match any ROWS in the remote server. Check your PIDs and server configuration and try again.'
            ),
            MessengerInterface::TYPE_ERROR
          );
        }
        else {
          $form_state->set('rows', $resultset->getNumFound());
        }
      }
      elseif ($resultset->getNumFound() == 0) {
        $user_input = $form_state->getUserInput();
        $form_state->setValue(['pluginconfig','ready'], FALSE);
        $form_state->setValue(['pluginconfig', 'solarium_config', 'rows'],  0);
        $form_state->set('rows', 0);
        $user_input['pluginconfig']['solarium_config']['rows'] = $resultset->getNumFound();
        $form_state->setUserInput($user_input);
        $this->messenger()->addMessage(
          t('Your query did not yield any results in the remote server. Check your Collection PIDs and/or server configuration and try again.'),
          MessengerInterface::TYPE_ERROR
        );
      }

    }
    else {
      $this->messenger()->addMessage(
        t('Your Solr Config did not work out. Sorry'),
        MessengerInterface::TYPE_ERROR
      );
      $form_state->setValue(['pluginconfig', 'ready'], FALSE);
    }
    return $tabdata;
  }

  /**
   * {@inheritdoc}
   */
  public function getData(array $config, $page = 0, $per_page = 20): array {

    $filename_columns = [];
    $datastreams_how = $config['solarium_mapping']['datastreams_how'] ?? 'standard';
    $datastreams_additional = $config['solarium_mapping']['datastreams_mapping'] ?? [];
    if ($datastreams_how !== 'standard' && !empty($datastreams_additional)) {
      foreach ($datastreams_additional as $datastreams_additional) {
        $filename_columns[strtolower($datastreams_additional).'s'] = '';
      }
    }
    $filename_columns = static::FILE_COLUMNS + $filename_columns;
    // IN this case $page really means $offset.
    $solr_config = [
      'endpoint' => [
        'amiremote' => [
          'host' => $config['solarium_config']['host'],
          'scheme' => $config['solarium_config']['scheme'] ?? 'http',
          'port' => $config['solarium_config']['port'],
          'path' => $config['solarium_config']['path'],
        ],
      ],
    ];
    $parent_ado =  $config['parent_ado_uuid'] ?? NULL;
    // Passed by batch operation
    $headers = $config['headers'] ?? [];
    $headers = array_combine($headers, $headers);
    $headersWithData = $config['headerswithdata'] ?? [];

    if ($config['solarium_config']['type'] == 'single') {
      $solr_config['endpoint']['amiremote']['core'] = $config['solarium_config']['core'];
    }
    else {
      // Solr Cloud uses collection instead of core
      $solr_config['endpoint']['amiremote']['collection'] = $config['solarium_config']['collection'];
    }

    $adapter = new SolariumCurl(); // or any other adapter implementing AdapterInterface
    $adapter->setTimeout($config['solarium_config']['timeout'] ?? 5);
    $eventDispatcher = new EventDispatcher();
    // This adds 'headerswithdata' so we can clean our mess up once finished
    $tabdata = ['headers' => [], 'data' => [], 'totalrows' => 0, 'totalfound' => 0, 'headerswithdata' => $headersWithData];
    $client = new SolariumClient($adapter, $eventDispatcher, $solr_config);
    $ping = $client->createPing();
    // execute the ping query
    try {
      $result = $client->ping($ping);
      $ping_sucessful = $result->getStatus() + 1;
    }
    catch (\Exception $e) {
      return $tabdata;
    }

    $collapse_children =  $config['solarium_mapping']['collapse'] ?? FALSE;

    // We are not paging here, we are using absolute starting values.
    $offset = $page;
    $per_page = $per_page > 0 ? $per_page : static::BATCH_INCREMENTS;

    $next_forced_offset = NULL;
    if ($ping_sucessful) {
      try {
        $query = $client->createSelect();
        // search input string, this value fails without escaping because of the double-quote
        $input = $config['solarium_config']['islandora_collection'];

        // in this case phrase escaping is used (most common) but you can also do term escaping, see the manual
        // also note that the same can be done using the placeholder syntax, see example 6.3
        $helper = $query->getHelper();
        $pids = array_map('trim', explode("\n", $input));
        $pids = array_filter($pids);
        foreach ($pids as &$pid) {
          $pid = $helper->escapePhrase('info:fedora/' . $pid);
        }
        $escaped = implode(' OR ', $pids);
        $escaped = '('. $escaped .')';
        $query->setQuery('*:*');

        $query->createFilterQuery('collection_members')->setQuery('RELS_EXT_isMemberOfCollection_uri_s:' . $escaped . ' OR RELS_EXT_isMemberOf_uri_s:' . $escaped);
        // PLEASE REMOVE Collection Objects that ARE ALSO part of a compound. WE DO NOT WANT THOSE
        $query->createFilterQuery('notconstituent')->setQuery('-RELS_EXT_isConstituentOf_uri_ms:[ * TO * ]');
        $query->addSort('PID', 'asc');
        // New thing here
        // If we are in the mode of getting a lot of children
        // And ending with little top objects, e.g i request 100, return 3 and the rest are children
        // I can here, fictiously based on the last time that happened
        // Reduce the number of top objects to make all faster
        // Once we return to a state where the number of objects we get
        // is e.g 75% or more of what is requested i restore this value
        // But i can not change it on the caller (the batch)
        // because it would also reduce the number of children?
        // Nah, not really.
        $query->setStart($offset)->setRows($per_page);
        $query->setFields([
          'PID',
          'fgs_label_s',
          'fedora_datastream_latest_*_MIMETYPE_ms',
          'mods_*',
          'RELS_EXT_hasModel_uri_s',
          'RELS_EXT_isM*',
          'RELS_EXT_isC*',
          'RELS_EXT_isP*',
          'fgs_createdDate_dt',
          'fgs_ModifiedDate_dt',
          'fedora_datastreams_ms'
        ]);

        $resultset = $client->select($query);
        // display the total number of documents found by solr
        $resultset_iterator = $resultset->getIterator();
        // Empty value? just return
        if (($resultset_iterator == NULL) || empty($resultset_iterator)) {
          $this->messenger()->addMessage(
            t('Nothing to read, check your Solr Query Arguments'),
            MessengerInterface::TYPE_ERROR
          );
          return $tabdata;
        }
      } catch (\Exception $e) {
        $this->messenger()->addMessage(
          t('Solr Error: @e', ['@e' => $e->getMessage()]),
          MessengerInterface::TYPE_ERROR
        );
        return $tabdata;
      }
      $table = [];
      foreach ($filename_columns as $column) {
        $headers[$column] = $headersWithData[$column] = $column;
      }
      // Ensure the basic fields for this data
      $headers['type'] =  $headersWithData['type'] = 'type';
      $headers['ismemberof'] = $headersWithData['ismemberof'] = 'ismemberof';
      $headers['ispartof'] = $headersWithData['ispartof'] = 'ispartof';
      $headersWithData['node_uuid'] = 'node_uuid';

      $highestRow = 0;
      $pids_to_fetch = [];
      $previous_index = $config['prev_index'] ?? 0;

      for ($resultset_iterator->rewind(); $resultset_iterator->valid(); $resultset_iterator->next()) {
        try {
          $highestRow = $resultset_iterator->key() + 1;
          $document = $resultset_iterator->current();
          foreach ($document as $field => $value) {
            // Exclude this non-sense fields
            if (strpos($field, '_roleTerm_', 0) !== FALSE) {
              continue;
            }
            $fieldname = $this->multipleToSingleFieldName($field);
            $headers[$fieldname] = $fieldname;
            $original_value = $sp_data[$resultset_iterator->key()][$fieldname] ?? NULL;
            $value = $this->concatValues((array) $value, $original_value);
            $sp_data[$resultset_iterator->key()][$fieldname] = $value;
          }
          // Let's add generic all columns needed for files.
          foreach ($filename_columns as $column) {
            $sp_data[$resultset_iterator->key()][$column] = '';
          }
          if ($parent_ado) {
            $sp_data[$resultset_iterator->key()]['ismemberof'] = $parent_ado;
          }
          $sp_data[$resultset_iterator->key()]['type'] = $config['solarium_mapping']['cmodel_mapping'][$sp_data[$resultset_iterator->key()]['rels_ext_hasmodel_uri']] ?? 'Thing';

          $datastreams = $this->buildDatastreamURL($config, $document);
          if (is_array($datastreams) && count($datastreams)) {
            foreach ($datastreams as $datastream_key => $datastream_urls) {
              $sp_data[$resultset_iterator->key()][$datastream_key] = implode(
                ";", $datastream_urls
              );
            }
          }
          // Fetch Children
          if (in_array($sp_data[$resultset_iterator->key()]['rels_ext_hasmodel_uri'], static::MULTICHILDREN_CMODELS)) {
            $pids_to_fetch[$resultset_iterator->key()] = $sp_data[$resultset_iterator->key()]['pid'];
          }
          // If deep traverse is enabled we should also mark collections here?
          // Call getData recursevely?

        }
        catch (\Exception $exception) {
          continue;
        }
      }

      if (($highestRow) >= 1) {
        $childrenoffset = 0;
        // There is a chance that not all Rows have the same fields.
        // $sp_data will contain here max static::BATCH_INCREMENTS;
        foreach ($sp_data as $rowindex => $row) {
          $i = 0;
          $newrow = [];
          $realrowindex = $rowindex + $childrenoffset + $previous_index;
          foreach ($headers as $field) {
            // We keep track of empty headers here.
            if (!empty($sp_data[$rowindex][$field])) {
              $headersWithData[$field] = $field;
            }
            $newrow[$i] = $sp_data[$rowindex][$field] ?? '';
            $i++;
          }

          $table[$realrowindex] = $newrow;
          if (isset($pids_to_fetch[$rowindex])) {
            // We will let all children to be harvested
            // Even if they surpass the given static::BATCH_INCREMENTS;
            // But we will return and stop getting more children if so
            $children_data = $this->getDataChildren($config, $client, $pids_to_fetch[$rowindex]);
            if (count($children_data)) {
              foreach ($children_data as $childrenindex => $childrenrow) {
                $j = 0;
                $newrow = [];
                if (!$collapse_children) {
                  foreach ($headers as $field) {
                    if (!empty($children_data[$childrenindex][$field])) {
                      $headersWithData[$field] = $field;
                    }
                    if ($field == 'ispartof') {
                      // Arrays. Headers is 0, first row is 1, but in CSV
                      // Headers is 1. You got this.
                      $newrow[$j] = $realrowindex + 2;
                    }
                    else {
                      $newrow[$j] = $children_data[$childrenindex][$field] ?? '';
                    }
                    $j++;
                  }
                  $table[$realrowindex + $childrenindex + 1] = $newrow;
                }
                else {
                  // Take the Children Datastreams and add them to the parent's respective Columns
                  // Easiest route is to iterate over all columns and join then with current column using a ';'
                  foreach ($headers as $field) {
                    if (in_array($field, $filename_columns)) {
                      // For each File source Column, take all datastreams in children
                      // And glue them to the existing ones via an ';'
                      if (!empty($children_data[$childrenindex][$field])) {
                        $table[$realrowindex][$j] = empty($table[$realrowindex][$j]) ? $children_data[$childrenindex][$field] : $table[$realrowindex][$j] . ';' . $children_data[$childrenindex][$field];
                      }
                      // @TODO. Should we also keep children metadata somewhere?
                    }
                    $j++;
                  }
                }
              }
              // Only add an offset if we are not collapsing
              $childrenoffset = !$collapse_children ? $childrenoffset + count($children_data) : $childrenoffset;
            }
          }
          // Instead of checking against the actual requested $per_page (which can be low) because it might have
          // been reduced by our attempt to process/get less top objects when many children are present
          // We will still try to get many children here. If not possible all good. No forced offset in the next
          // pass.
          if (count($table) >= static::BATCH_INCREMENTS) {
            // This will forcely reduce the expected nextoffset to the actual top object position
            // We managed to fetch. Will of course re-do the original query
            // And get some of the same Top PIDs but will also
            // Avoid memory running out (hopefully)
            $next_forced_offset = $offset + $rowindex + 1;
            $highestRow = $rowindex + 1;
            break 1;
          }
        }
      }

      $tabdata = [
        'headers' => array_keys($headers),
        'data' => $table,
        'totalrows' => $highestRow,
        'totalfound' => count($table),
        'headerswithdata' => $headersWithData,
        'nextforcedoffset' => $next_forced_offset
      ];
    }
    else {
      $this->messenger()->addMessage(
        t('Your Solr Config did not work out. Sorry. Check your settings or if your remote server is having issues.'),
        MessengerInterface::TYPE_ERROR
      );
    }
    return $tabdata;
  }

  /**
   * Fetches Children Objects from Solr and returns an array
   *
   * @param array $config
   * @param \Solarium\Client $client
   * @param string $input
   *
   * @return array
   */
  protected function getDataChildren(array $config, SolariumClient $client, string $input):array {


    // FILENAME COLUMNS
    // if $config['solarium_mapping']['datastreams_how'] is not 'standard'
    // we need to add the ones selected
    $filename_columns = [];
    $datastreams_how = $config['solarium_mapping']['datastreams_how'] ?? 'standard';
    $datastreams_additional = $config['solarium_mapping']['datastreams_mapping'] ?? [];
    $datastreams_additional = array_filter($datastreams_additional);
    if ($datastreams_how !== 'standard' && !empty($datastreams_additional)) {
      foreach ($datastreams_additional as $datastreams_additional) {
        $filename_columns[] = strtolower($datastreams_additional).'s';
      }
    }
    $filename_columns = array_unique(array_merge(static::FILE_COLUMNS,$filename_columns));

    $sp_data = [];
    $query = $client->createSelect();
    $helper = $query->getHelper();
    $escaped = $helper->escapePhrase('info:fedora/' . $input);
    $escaped_pid = str_replace(':', '_', $input);
    $query->addSort("RELS_EXT_isSequenceNumberOf{$escaped_pid}_literal_intDerivedFromString_l", 'asc');
    $query->addSort("RELS_EXT_isPageNumber_literal_intDerivedFromString_l", 'asc');
    $query->createFilterQuery('constituent')->setQuery('RELS_EXT_isConstituentOf_uri_ms:'.$escaped .' OR RELS_EXT_isPageOf_uri_ms:'.$escaped .' OR RELS_EXT_isMemberOf_uri_ms:'.$escaped );
    $query->setQuery('*:*');
    $query->setStart(0)->setRows(5000);
    $query->setFields([
      'PID',
      'fgs_label_s',
      'fedora_datastream_latest_*_MIMETYPE_ms',
      'mods_*',
      'RELS_EXT_hasModel_uri_s',
      'RELS_EXT_isM*',
      'RELS_EXT_isC*',
      'RELS_EXT_isP*',
      'fgs_createdDate_dt',
      'fgs_ModifiedDate_dt',
      'fedora_datastreams_ms'
    ]);

    $resultset = $client->select($query);
    if ($resultset->getNumFound() == 0) {
      return [];
    }

    $resultset_iterator = $resultset->getIterator();
    // Empty value? just return
    if (($resultset_iterator == NULL) || empty($resultset_iterator)) {
      $this->messenger()->addMessage(
        t('Nothing to read, check your Solr Query Arguments'),
        MessengerInterface::TYPE_ERROR
      );
      return [];
    }

    for ($resultset_iterator->rewind(); $resultset_iterator->valid(); $resultset_iterator->next()) {
      try {
        $highestRow = $resultset_iterator->key() + 1;
        $document = $resultset_iterator->current();
        foreach ($document as $field => $value) {
          $fieldname = $this->multipleToSingleFieldName($field);
          // Exclude this non-sense fields
          if (strpos($fieldname, '_roleTerm_', 0) !== FALSE) {
            continue;
          }
          if ($this->endsWith($fieldname, 'authority_marcrelator')) {
            continue;
          }

          $headers[$fieldname] = $fieldname;
          $original_value = $sp_data[$resultset_iterator->key()][$fieldname] ?? NULL;
          $value = $this->concatValues((array) $value, $original_value);
          $sp_data[$resultset_iterator->key()][$fieldname] = $value;
        }
        // Let's add generic all columns needed for files.
        // Let's add generic all columns needed for files.
        foreach ($filename_columns as $column) {
          $sp_data[$resultset_iterator->key()][$column] = '';
        }
        // if $config['solarium_mapping']['datastreams_how'] is not 'standard'
        // we need to add the ones selected
        $datastreams_how = $config['solarium_mapping']['datastreams_how'] ?? 'standard';
        $datastreams_additional = $config['solarium_mapping']['datastreams_mapping'] ?? [];
        if ($datastreams_how !== 'standard' && !empty($datastreams_additional)) {
            foreach ($datastreams_additional as $datastreams_additional) {
              $sp_data[$resultset_iterator->key()][strtolower($datastreams_additional).'s'] = '';
            }
        }



        // Try with both main mapping and children mapping
        $type = $config['solarium_mapping']['cmodel_children'][$sp_data[$resultset_iterator->key()]['rels_ext_hasmodel_uri']] ?? NULL;
        $type2 = $type ?? ($config['solarium_mapping']['cmodel_mapping'][$sp_data[$resultset_iterator->key()]['rels_ext_hasmodel_uri']] ?? NULL);
        $sp_data[$resultset_iterator->key()]['type'] = $type2 ?? 'Thing';
        // Get me the datastream
        $datastreams = $this->buildDatastreamURL($config, $document);
        if (is_array($datastreams) && count($datastreams)) {
          foreach ($datastreams as $datastream_key => $datastream_urls) {
            $sp_data[$resultset_iterator->key()][$datastream_key] = implode(
              ";", $datastream_urls
            );
          }
        }
      } catch (\Exception $exception) {
        // @TODO log the error here.
        continue;
      }
    }
    return $sp_data;
  }

  /**
   * @param array $config
   * @param \Solarium\QueryType\Select\Result\Document $document
   *
   * @return array|string[]
   */
  protected function buildDatastreamURL(array $config, \Solarium\QueryType\Select\Result\Document $document):array {
    $datastreams = [];
    $additional_datastreams = $config['solarium_mapping']['datastreams_mapping'] ?? [];
    $additional_datastreams = array_filter($additional_datastreams);
    $how = $config['solarium_mapping']['datastreams_how'] ?? 'standard';

    // First OBJ
    $pdf_fetched = FALSE;
    if (!empty($document->fedora_datastream_latest_OBJ_MIMETYPE_ms)) {
      // Calculate the destination json key
      $dsid = 'OBJ';
      $mime = $document->fedora_datastream_latest_OBJ_MIMETYPE_ms[0];
      $as_file_type = explode('/', $mime);
      $as_file_type = count($as_file_type) == 2 ? $as_file_type[0] : 'document';
      $as_file_type = ($as_file_type != 'application') ? $as_file_type
        : 'document';
      $url = rtrim($config['solarium_mapping']['server_domain'], '/')
        . '/islandora/object/' . urlencode($document->PID)
        . "/datastream/{$dsid}/download";
      // We add an 's' at the end to match our typical file bearing CSV header naming.
      $datastreams[$as_file_type . 's'][] = $url;
    }
    elseif (!empty($document->fedora_datastream_latest_PDF_MIMETYPE_ms)) {
      $pdf_fetched = TRUE;
      $dsid = 'PDF';
      $mime = $document->fedora_datastream_latest_PDF_MIMETYPE_ms[0];
      $as_file_type = 'document';
      $url = rtrim($config['solarium_mapping']['server_domain'], '/')
        . '/islandora/object/' . urlencode($document->PID)
        . "/datastream/{$dsid}/download";
      // We add an 's' at the end to match our typical file bearing CSV header naming.
      $datastreams[$as_file_type . 's'][] = $url;
    }

    if ($pdf_fetched) {
      // If we already fetched the PDF no need to get it again if added as part of
      // the extras.
      unset($additional_datastreams['PDF']);
    }

    foreach ($additional_datastreams as $additional_datastream) {
      $property = "fedora_datastream_latest_{$additional_datastream}_MIMETYPE_ms";
      if (!empty($document->{$property} && is_array($document->{$property}))) {
        // Calculate the destination json key
        $dsid = $additional_datastream;
        // Allow here things to go into application, e.g XMLs.
        $url = rtrim($config['solarium_mapping']['server_domain'], '/')
          . '/islandora/object/' . urlencode($document->PID)
          . "/datastream/{$dsid}/download";
        if ($how == 'standard') {
          $mime = $document->{$property}[0];
          if ($mime == 'application/pdf' ) {
            $as_file_type = 'document';
          }
          elseif ($mime == 'text/xml') {
            $as_file_type = 'application';
          }
          else {
            $as_file_type = explode('/', $mime);
            $as_file_type = count($as_file_type) == 2 ? $as_file_type[0]
              : 'document';
          }

          // We add an 's' at the end to match our typical file bearing CSV header naming.
          $datastreams[$as_file_type . 's'][] = $url;
        }
        else {
          $datastreams[strtolower($additional_datastream) . 's'][] = $url;
        }
      }
    }
    return $datastreams;
  }

  /**
   * This function normalizes field names to join _ms, _s etc without prefixes.
   *
   * Also lower cases every field name.
   * @param $field
   *
   * @return false|string
   */
  protected function multipleToSingleFieldName($field) {
    // this converts multi valued fields to a comma-separated string
    foreach (static::SOLR_FIELD_SUFFIX as $suffix) {
      $suffix_offset = strpos($field , $suffix , strlen($field) - strlen($suffix) -1);
      if ($suffix_offset!== false) {
        $field = substr($field, 0, $suffix_offset);
        break 1;
      }
    }
    return strtolower($field);
  }


  /**
   * Checks if string ends in another string. PHP 7.
   *
   * @param $haystack
   * @param $needle
   *
   * @return bool
   */
  protected function endsWith($haystack, $needle) {
    $length = strlen($needle);
    return $length > 0 ? substr($haystack, -$length) === $needle : true;
  }


  /**
   * Implodes array and concatenates to existing string using common delimiter.
   *
   * Will also remove whitespaces from start/end of each value.
   *
   * @param array $value
   * @param string|null $oldvalue
   *
   * @return string
   */
  protected function concatValues(array $value, string $original_value = NULL): string {
    if (!empty($original_value)) {
      $original_value = explode('|@|', $original_value) ?? [];
      $value = array_unique(array_merge($original_value, $value));
    }
    $value = array_map('trim', $value);
    return implode('|@|', array_unique($value));
  }


  /**
   * {@inheritdoc}
   */
  public function getBatch(FormStateInterface $form_state, array $config, \stdClass $amisetdata) {
    $batch = [
      'title' => $this->t('Batch fetching from Solr'),
      'operations' => [],
      'finished' => '\Drupal\ami\Plugin\ImporterAdapter\SolrImporter::finishfetchFromSolr',
      'progress_message' => t('Processing Set @current of @total.'),
    ];
    $parent_ado_uuid = NULL;
    $file = $this->entityTypeManager->getStorage('file')->load($amisetdata->csv);
    if (!empty($config['solarium_mapping']['parent_ado']) && is_scalar($config['solarium_mapping']['parent_ado'])) {
      $parent_ado = $this->entityTypeManager->getStorage('node')->load($config['solarium_mapping']['parent_ado']);
      $parent_ado_uuid = $parent_ado->uuid();
    }
    $config['parent_ado_uuid'] = $parent_ado_uuid;
    $batch['operations'][] = [
      '\Drupal\ami\Plugin\ImporterAdapter\SolrImporter::fetchBatch',
      [$config, $this, $file, $amisetdata],
    ];
    return $batch;
  }

  /**
   * {@inheritdoc}
   */
  public static function fetchBatch(array $config, ImporterPluginAdapterInterface $plugin_instance, File $file, \stdClass $amisetdata, array &$context):void {

    $rows = $config['solarium_config']['rows'] ?? 500;
    $offset = $config['solarium_config']['start'] ?? 0;
    $increment = static::BATCH_INCREMENTS;
    if (!isset($context['sandbox']['progress'])) {
      $context['sandbox']['progress'] = 0;
      $context['sandbox']['totalfound'] = 0;
    }

    if (!array_key_exists('max',
        $context['sandbox']) || $context['sandbox']['max'] < $rows) {
      $context['sandbox']['max'] = $rows;
    }
    if (!array_key_exists('prev_index',
      $context['sandbox'])) {
      $context['sandbox']['prev_index'] = 0;
    }
    $context['finished'] = 0;
    try {
      // Increment constantly by static::BATCH_INCREMENTS except when what is left < static::BATCH_INCREMENTS

      $next_increment = ($context['sandbox']['progress'] + $increment > $rows)
        ? ($rows - $context['sandbox']['progress']) : $increment;
      // Or if the requested number is under a 75%. If so reduce the request
      // This means we are getting lots of children.
      // Once this returns to a higher threashold keep incrementing normally
      // This helps with over processing of parents when we are only be able to fetch a few
      if (isset($context['sandbox']['nextforcedoffset'])
        && $context['sandbox']['nextforcedoffset'] !== NULL
        && $context['sandbox']['progress'] > 0
        && $next_increment > 0
      ) {
        $ratio = ($context['results']['processed']['total_rows']
          / $next_increment);
        if ($ratio < 0.75) {
          $next_increment_smaller = ceil($increment * $ratio);
          $next_increment = $next_increment_smaller > 0
            ? $next_increment_smaller + 1 : $next_increment;
        }
      }

      $nextoffset = $context['sandbox']['progress'] + $offset;
      $nextoffset = isset($context['sandbox']['nextforcedoffset'])
      && $context['sandbox']['nextforcedoffset'] !== NULL
        ? $context['sandbox']['nextforcedoffset'] : $nextoffset;

      if ($context['sandbox']['progress'] == 0) {
        $title = t(
          'Attempting to fetch first %progress of <b>%count</b> Objects.',
          [
            '%count'    => $rows,
            '%progress' => $context['sandbox']['progress'] + $next_increment,
          ]
        );
      }
      else {
        $progress = $context['sandbox']['progress'] + $next_increment;
        $progress = ($progress >= $rows) ? $rows : $progress;
        $title = t(
          'Fetching %progress of <b>%count</b> top Objects with <b>%total</b> total rows retrieved so far.',
          [
            '%count'    => $rows,
            '%progress' => $progress,
            '%total'    => $context['sandbox']['totalfound']
          ]
        );
      }

      $context['message'] = $title;
      // WE keep track in the AMI set Config of the previous total rows
      // Because Children will offset all the results significantly
      // And we pass that data into the ::getData to offset the next set of
      // Parents/Children.
      $config['prev_index'] = $context['sandbox']['prev_index'];
      // Pass the headers into the config so we have a unified/normalized version
      // And not the mess each doc returns
      $config['headers'] = !empty($amisetdata->column_keys) ? $amisetdata->column_keys : (!empty($config['headers']) ? $config['headers'] : []);
      $config['headerswithdata'] = $context['results']['processed']['headerswithdata'] ?? [];

      $data = $plugin_instance->getData($config, $nextoffset,
        $next_increment);

      if (isset($data['nextforcedoffset']) && $data['nextforcedoffset'] !== NULL) {
        $context['sandbox']['nextforcedoffset'] = $data['nextforcedoffset'];
      }
      else {
        $context['sandbox']['nextforcedoffset'] = NULL;
      }
      if ($data['totalrows'] == 0) {
        $context['finished'] = 1;
      }
      else {
        $context['sandbox']['prev_index'] = $context['sandbox']['prev_index'] + $data['totalfound'];
        $append_headers = $context['sandbox']['progress'] == 0 ? TRUE : FALSE;
        // New here or @TODO. We should append to CSV sooner instead of moving data around
        // Why? A single Solr query data structure with children might fill up
        // The PHP memory
        // $amisetdata->adomapping['uuid']['uuid'] for this case will always be node_uuid and autouuid always TRUE.
        // This is the form settings. Wonder if for safety i should just still fix the values here?
        \Drupal::service('ami.utility')->csv_append($data, $file, $amisetdata->adomapping['uuid']['uuid'], $append_headers, TRUE, $amisetdata->adomapping['autouuid'] ?? FALSE);
        $context['sandbox']['progress'] = $context['sandbox']['progress'] + $data['totalrows'];
        // Update context
        $context['sandbox']['totalfound'] = $context['sandbox']['totalfound'] + $data['totalfound'];
        $context['results']['processed']['fileuuid'] = $file->uuid();
        $context['results']['processed']['headers'] = $data['headers'];
        $context['results']['processed']['total_rows'] = $data['totalrows'] ?? 0;
        $context['results']['processed']['headerswithdata'] = $data['headerswithdata'] ?? [];
        $context['finished'] = $context['sandbox']['progress'] / $context['sandbox']['max'];
      }
    } catch (\Exception $e) {
      // In case of any other kind of exception, log it and leave the item
      // in the queue to be processed again later.
      watchdog_exception('ami', $e);
      $context['results']['errors'][] = $e->getMessage();
      $context['finished'] = 1;
    }
  }

  public static function finishfetchFromSolr($success, $results, $operations) {

    $allheaders = $results['processed']['headers'] ?? [];

    $data['headers'] = array_values($allheaders);
    $data['total_rows'] = $results['processed']['total_rows'] ?? 0;
    // Clean the CSV removing empty headers!
    $file_uuid = $results['processed']['fileuuid'] ?? NULL;
    if ($file_uuid) {
      $file = \Drupal::service('entity.repository')->loadEntityByUuid(
        'file', $file_uuid
      );
      if ($file) {
        \Drupal::service('ami.utility')->csv_clean(
          $file, $results['processed']['headerswithdata']
        );
      }
    }
    else {
      $data['results']['errors'][] = t('Error. We could not save your remote data to CSV!');
    }
    \Drupal::service('tempstore.private')->get('ami_multistep_data')->set('batch_finished', $data);
  }

  public function provideTypes(array $config, array $data): array {
    $keys = $config['solarium_mapping']['cmodel_mapping'] ?? [];
    // Remove children types if collapse is enabled
    if ($config['solarium_mapping']['collapse'] == 0) {
      $keys_children = $config['solarium_mapping']['cmodel_children'] ?? [];
    }
    else {
      $keys_children = [];
    }
    $keys = array_unique(array_merge(array_values($keys), array_values($keys_children)));
    unset($keys_children);
    return $keys;
  }

  public function provideKeys(array $config, array $data): array {

    if (count($data) > 0) {
      $filename_columns = [];
      $datastreams_how = $config['solarium_mapping']['datastreams_how'] ?? 'standard';
      $datastreams_additional = $config['solarium_mapping']['datastreams_mapping'] ?? [];
      $datastreams_additional = array_filter($datastreams_additional);
      if ($datastreams_how !== 'standard' && !empty($datastreams_additional)) {
        foreach ($datastreams_additional as $datastreams_additional) {
          $filename_columns[] = strtolower($datastreams_additional).'s';
        }
      }
      $filename_columns = array_unique(array_merge(static::FILE_COLUMNS,$filename_columns));
      $columns = array_merge(['type','node_uuid','ismemberof','ispartof','fgs_label','mods_titleinfo_title'], $filename_columns);
      return $columns;
    }
    return [];
  }
}

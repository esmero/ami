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
use Hoa\Math\Sampler\Random;
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
 *   label = @Translation("Solr Importer"),
 *   remote = true,
 *   batch = true
 * )
 */
class SolrImporter extends SpreadsheetImporter {

  use StringTranslationTrait;
  use MessengerTrait;

  const BATCH_INCREMENTS = 200;
  const SOLR_CONFIG = [
    'endpoint' => [
      'localhost' => [
        'host' => 'dcmny.org',
        'port' => 8080,
        'path' => '/',
        'core' => 'metrocore1',
        // For Solr Cloud you need to provide a collection instead of core:
        // 'collection' => 'techproducts',
      ]
    ]
  ];


  /**
   * @var \Drupal\Core\StreamWrapper\StreamWrapperManagerInterface
   */
  protected $streamWrapperManager;

  /**
   * @var \GuzzleHttp\ClientInterface
   */
  protected $httpClient;

  /**
   * GoogleSheetImporter constructor.
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
  public function interactiveForm(array $parents = [], FormStateInterface $form_state): array {
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


    $rows  = $form_state->get('rows');
    $rows = $rows ?? $form_state->getValue(array_merge($parents,
           ['solarium_config', 'rows']), 0);
    if (empty($rows)) {
      $form_state->setValue(['pluginconfig','ready'], FALSE);
    }


    $form['solarium_config'] = [
      '#prefix' => '<div id="ami-solrapi">',
      '#suffix' => '</div>',
      '#tree' => TRUE,
      '#type' => 'fieldset',
      '#element_validate' => [[get_class($this), 'validateSolrConfig']],
      'islandora_collection' => [
        '#type' => 'textfield',
        '#required' => TRUE,
        '#title' => $this->t('PID of the Islandora Collection Members you want to fetch'),
        '#description' => $this->t('Example: islandora:root'),
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_config', 'islandora_collection'])),
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
      'collapse' => [
        '#type' => 'checkbox',
        '#title' => $this->t('Collapse Multi Children Objects'),
        '#description' => $this->t('This will collapse Children Datastreams into a single ADO with many attached files.'),
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_config', 'collapse'])),
      ],
      'start' => [
        '#type' => 'number',
        '#min' => 0,
        '#max' => 65535,
        '#title' => $this->t('Starting Row'),
        '#description' => $this->t('Initial Row to fetch. This is an offset. Defaults to 0'),
        '#default_value' => $form_state->getValue(array_merge($parents,
        ['solarium_config', 'start'])),
      ],
      'rows' => [
        '#type' => 'number',
        '#min' => 0,
        '#max' => 65535,
        '#title' => $this->t('Number of Rows'),
        '#description' => $this->t('Total number of Rows to Fetch. Settings this to empty or null will prefill with the real Number Rows found by the Solr Query invoked. If you set this number higher than the actual results we will only fetch what can be fetched'),
        '#default_value' => $rows,
      ],
    ];

    $cmodels = $form_state->get('facet_cmodel') ?? $form_state->getValue(array_merge($parents,
        ['solarium_mapping', 'cmodel_mapping'], []));
    if (empty($cmodels)) {
      $form_state->setValue(['pluginconfig','ready'], FALSE);
    }
    $form['solarium_mapping'] = [
      'cmodel_mapping' => [
        '#access' => !empty($cmodels),
        '#type' => 'webform_mapping',
        '#title' => $this->t('Required ADO mappings.'),
        '#format' => 'list',
        '#required' => TRUE,
        '#description_display' => 'before',
        '#description' => 'Your Islandora Content Models to ADO types mapping, eg: for <em>info:fedora/islandora:sp_large_image_cmodel</em> you may want to use <em>Photograph</em>.',
        '#empty_option' => $this->t('- Please Map your Islandora CMODELs to ADO types -'),
        '#empty_value' => NULL,
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_mapping', 'cmodel_mapping'], [])),
        '#source' => $cmodels ? array_combine(array_keys($cmodels),
          array_keys($cmodels)) : [],
        '#source__title' => $this->t('CMODELs found'),
        '#destination__title' => $this->t('ADO Types'),
        '#destination' => $this->AmiUtilityService->getWebformOptions(),
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
    $config = $form_state->getValue($element['#parents']);
    $solr_config = [
      'endpoint' => [
        'amiremote' => [
          'host' => $config['host'],
          'port' => $config['port'],
          'path' => $config['path'],
        ],
      ],
    ];
    if ($config['type'] == 'single') {
      $solr_config['endpoint']['amiremote']['core'] = $config['core'];
    }
    else {
      // Solr Cloud uses collection instead of core
      $solr_config['endpoint']['amiremote']['collection'] = $config['collection'];
    }

    $adapter = new SolariumCurl(); // or any other adapter implementing AdapterInterface
    $eventDispatcher = new EventDispatcher();

    $client = new SolariumClient($adapter, $eventDispatcher, $solr_config);
    $ping = $client->createPing();

    // execute the ping query
    try {
      $result = $client->ping($ping);
      error_log('good! Ping -> Pong');
    } catch (\Exception $e) {
      $form_state->setError($element,
        t('Ups. We could not contact your server. Check if your settings are correct and/or firewalls are open for this IP address.'));
    }

    /* if ($config['rows'] == 0) {
      $form_state->setErrorByName('pluginconfig][solarium_config][rows',
        t('You need to fetch at least one row.'));
    }*/
  }

  /**
   * {@inheritdoc}
   */
  public function getInfo(array $config, FormStateInterface $form_state, $page = 0, $per_page = 20): array {
    $solr_config = [
      'endpoint' => [
        'amiremote' => [
          'host' => $config['solarium_config']['host'],
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
    $input = 'info:fedora/' . $config['solarium_config']['islandora_collection'];

    $adapter = new SolariumCurl(); // or any other adapter implementing AdapterInterface
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
        $this->t('Ups. We could not contact your server. Check if your settings are correct and/or firewalls are open for this IP address.'));
        $form_state->setValue(['pluginconfig','ready'], FALSE);
        return $tabdata;
    }

    if ($ping_sucessful) {
      try {
        $query = $client->createSelect();
        $helper = $query->getHelper();
        $query->setQuery('RELS_EXT_isMemberOfCollection_uri_s:' . $helper->escapePhrase($input));

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
        $rows = (int) $config['solarium_config']['rows'] > 0 && (int) $config['solarium_config']['rows'] <= 100  ? $config['solarium_config']['rows'] : 100;
        $query->setStart($config['solarium_config']['start'] ?? 0)->setRows($rows);
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
        $facet = $resultset->getFacetSet()->getFacet('cmodel');
        $cmodel = [];
        foreach ($facet as $value => $count) {
          if ($count) {
            $cmodel[$value] = $value;
          }
        }
        // Set extracted CMODELS in a temp value
        $form_state->set('facet_cmodel', $cmodel);
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
      $headers = [];
      $maxRow = 0;
      $highestRow = 0;

      for ($resultset_iterator->rewind(); $resultset_iterator->valid(); $resultset_iterator->next()) {
        try {
          $highestRow = $resultset_iterator->key() + 1;
          $document = $resultset_iterator->current();
          foreach ($document as $field => $value) {
            // this converts multi valued fields to a comma-separated string
            $headers[$field] = $field;
            if (is_array($value)) {
              // Check if there is also a _s key for the same _ms
              $clean_field = substr($field, 0, -2);
              $single_value = $document[$clean_field . 's'] ?? NULL;
              if ($single_value) {
                $fieldsToDelete[] = $clean_field . 's';
                $value[] = is_array($single_value) ? $single_value[0] : $single_value ;
              }
              $value = implode(', ', array_unique($value));
            }
            $sp_data[$resultset_iterator->key()][$field] = $value;
          }
          // Let's add generic all columns needed for files.

          $sp_data[$resultset_iterator->key()]['documents'] = '';
          $sp_data[$resultset_iterator->key()]['images'] = '';
          $sp_data[$resultset_iterator->key()]['videos'] = '';
          $sp_data[$resultset_iterator->key()]['audios'] = '';
          $sp_data[$resultset_iterator->key()]['models'] = '';
          $sp_data[$resultset_iterator->key()]['texts'] = '';
        }
        catch (\Exception $exception) {
          continue;
        }
      }
      // Also add these base ones to the headers
      $headers['documents'] = 'documents';
      $headers['images'] = 'images';
      $headers['videos'] = 'videos';
      $headers['audios'] = 'audios';
      $headers['models'] = 'models';
      $headers['texts'] = 'texts';

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

      // Check if we still have the same CMODELS after running this. In case something changed.
       if ((count(array_intersect_key($form_state->getValue(['pluginconfig', 'solarium_mapping','cmodel_mapping'], []), $cmodel)) == count($cmodel)) && count($cmodel) >= 1) {
        $form_state->setValue(['pluginconfig', 'ready'], TRUE);
      } else {
        $form_state->setValue(['pluginconfig', 'ready'], FALSE);
      }
     if ((int) $form_state->getValue(['pluginconfig', 'solarium_config', 'rows'],0) == 0) {
        $user_input = $form_state->getUserInput();
        $user_input['pluginconfig']['solarium_config']['rows'] = $resultset->getNumFound();
        $form_state->setUserInput($user_input);
        $form_state->setValue(['pluginconfig', 'solarium_config', 'rows'],  $resultset->getNumFound());
        $form_state->setValue(['pluginconfig','ready'], FALSE);
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
   // IN this case $page really means $offset.

    $solr_config = [
      'endpoint' => [
        'amiremote' => [
          'host' => $config['solarium_config']['host'],
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

    $adapter = new SolariumCurl(); // or any other adapter implementing AdapterInterface
    $eventDispatcher = new EventDispatcher();
    $tabdata = ['headers' => [], 'data' => [], 'totalrows' => 0, 'totalfound' => 0];
    $client = new SolariumClient($adapter, $eventDispatcher, $solr_config);
    $ping = $client->createPing();
    $ping_sucessful = 0;
    // execute the ping query
    try {
      $result = $client->ping($ping);
      $ping_sucessful = $result->getData();
    } catch (\Exception $e) {
      return $tabdata;
    }

    // We are not pagin here, we are using absolute starting values.
    $offset = $page;
    $per_page = $per_page > 0 ? $per_page : static::BATCH_INCREMENTS;

    if ($ping_sucessful) {
      try {
        $query = $client->createSelect();

        // search input string, this value fails without escaping because of the double-quote
        $input = 'info:fedora/' . $config['solarium_config']['islandora_collection'];

        // in this case phrase escaping is used (most common) but you can also do term escaping, see the manual
        // also note that the same can be done using the placeholder syntax, see example 6.3
        $helper = $query->getHelper();
        $query->setQuery('RELS_EXT_isMemberOfCollection_uri_s:' . $helper->escapePhrase($input));
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

        error_log($client->createRequest($query)->getUri());
        $resultset = $client->select($query);
        // display the total number of documents found by solr
        error_log('NumFound: ' . $resultset->getNumFound());

        $resultset_iterator = $resultset->getIterator();
        // Empty value? just return
        if (($resultset_iterator == NULL) || empty($resultset_iterator)) {
          $this->messenger()->addMessage(
            t('Nothing to read, check your Solr Query Arguments'),
            MessengerInterface::TYPE_ERROR
          );
          error_log('returning');
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
      $headers = [];
      $maxRow = 0;
      $highestRow = 0;

      for ($resultset_iterator->rewind(); $resultset_iterator->valid(); $resultset_iterator->next()) {
        try {
          $highestRow = $resultset_iterator->key() + 1;
          $document = $resultset_iterator->current();
          foreach ($document as $field => $value) {
            // this converts multi valued fields to a comma-separated string
            $headers[$field] = $field;
            if (is_array($value)) {
              // Check if there is also a _s key for the same _ms
              $clean_field = substr($field, 0, -2);
              $single_value = $document[$clean_field . 's'] ?? NULL;
              if ($single_value) {
                $fieldsToDelete[] = $clean_field . 's';
                $value[] = is_array($single_value) ? $single_value[0] : $single_value ;
              }
              $value = implode(', ', array_unique($value));
            }
            $sp_data[$resultset_iterator->key()][$field] = $value;
          }
        } catch (\Exception $exception) {
          continue;
        }
      }
      // Remove Single fields we also got as
      /* foreach ($fieldsToDelete as $fieldtodelete) {
        unset($headers[$fieldtodelete]);
      }*/
      // Reorder.
      //$headers = array_values($headers);

      if (($highestRow) >= 1) {
        // Returns Row Headers.

        $maxRow = 1; // at least until here.
        // There is a chance that not all Rows have the same fields.
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
        'totalfound' => $resultset->getNumFound(),
      ];
    }
    else {
      $this->messenger()->addMessage(
        t('Your Solr Config did not work out. Sorry'),
        MessengerInterface::TYPE_ERROR
      );
    }
    return $tabdata;
  }

  /**
   * {@inheritdoc}
   */
  public function getBatch(FormStateInterface $form_state, array $config, \stdClass $amisetdata) {
    $batch = [
      'title' => $this->t('Batch fetching from Solr'),
      'operations' => [],
      'finished' => [
        '\Drupal\ami\Plugin\ImporterAdapter\SolrImporter::finishfetchFromSolr'
      ],
      'progress_message' => t('Processing Set @current of @total.'),
    ];

    $file =  $this->entityTypeManager->getStorage('file')->load($amisetdata->csv);

    $batch['operations'][] = [
      '\Drupal\ami\Plugin\ImporterAdapter\SolrImporter::fetchBatch',
      [$config, $this, $file, $amisetdata],
    ];
    return $batch;
  }

  /**
   * {@inheritdoc}
   */
  public static function fetchBatch(array $config, ImporterPluginAdapterInterface $plugin_instace, File $file, \stdClass $amisetdata, array &$context):void {

    $rows = $config['solarium_config']['rows'] ?? 500;
    $offset = $config['solarium_config']['start'] ?? 0;
    $increment = static::BATCH_INCREMENTS;
    if (!isset($context['sandbox']['progress'])) {
      $context['sandbox']['progress'] = 0;
    }

    if (!array_key_exists('max',
        $context['sandbox']) || $context['sandbox']['max'] < $rows) {
      $context['sandbox']['max'] = $rows;
    }
    $context['finished'] = 0;
    try {
      $title = t('Processing %progress of <b>%count</b>', [
        '%count' => $rows,
        '%progress' => $context['sandbox']['progress'] + $increment
      ]);
      $context['message'] = $title;
      $data = $plugin_instace->getData($config, $context['sandbox']['progress'] + $offset,
        $increment);
      if ($data['totalrows'] == 0) {
        $context['finished'] = 1;
      }
      else {
        \Drupal::service('ami.utility')->csv_append($data, $file, $amisetdata->adomapping['uuid']['uuid']);
        $context['sandbox']['progress'] = $context['sandbox']['progress'] + $data['totalrows'];
        // Update context
        error_log($context['sandbox']['progress']);
        $context['results']['processed'][] = $data;
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
   error_log(print_r($results));
   dpm($results);
   }

}

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
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\ami\AmiUtilityService;
use Solarium\Core\Client\Adapter\Curl as SolariumCurl;
use Solarium\Client as SolariumClient;
use Symfony\Component\EventDispatcher\EventDispatcher;

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
        '#description' => 'Example: islandora:root',
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_config', 'islandora_collection'])),
      ],
      'host' => [
        '#type' => 'textfield',
        '#required' => TRUE,
        '#title' => $this->t('Host of your Solr Server'),
        '#description' => 'Example: https://repositorydomain.org',
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_config', 'host'])),
      ],
      'port' => [
        '#type' => 'textfield',
        '#required' => TRUE,
        '#title' => $this->t('Port'),
        '#description' => t('Example: 8080'),
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_config', 'port'])),
        //'#element_validate' => [[get_class($this), 'validatePort']],
      ],
      'path' => [
        '#type' => 'textfield',
        '#required' => TRUE,
        '#title' => $this->t('Path'),
        '#description' => t('example: /'),
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_config', 'path'])),
        //'#element_validate' => [[get_class($this), 'validatePort']],
      ],
      'type' => [
        '#type' => 'select',
        '#required' => TRUE,
        '#options' => [
          'single' => 'Single Solr Server',
          'cloud' => 'Solr Cloud Ensemble',
        ],
        '#title' => $this->t('Type of Solr deployment'),
        '#description' => t('Most typical one is Single Server.'),
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_config', 'type'])),
      ],
      'core' => [
        '#type' => 'textfield',
        '#title' => $this->t('Core'),
        '#description' => t('example: islandora'),
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
        '#description' => t('example: collection1'),
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
    ];


    $cmodels = $form_state->get('facet_cmodel');
    $form['solarium_mapping'] = [
      'cmodel_mapping' => [
        '#type' => 'webform_mapping',
        '#title' => $this->t('Required ADO mappings'),
        '#format' => 'list',
        '#description_display' => 'before',
        '#empty_option' => $this->t('- Please Map your Islandora CMODELs to ADO types -'),
        '#empty_value' => NULL,
        '#default_value' => $form_state->getValue(array_merge($parents,
          ['solarium_mapping', 'cmodel_mapping'], [])),
        '#source' => $cmodels ? array_combine(array_keys($cmodels),
          array_keys($cmodels)) : [],
        '#source__title' => $this->t('Base ADO mappings'),
        '#destination__title' => $this->t('columns'),
        '#destination' => [
          'Book' => 'Book',
          'Photograph' => 'Photograph',
          'Article' => 'Article',
        ]
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
  }

  /**
   * {@inheritdoc}
   */
  public function getData(array $config, $page = 0, $per_page = 20): array {
    error_log('running getData');

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

    $client = new SolariumClient($adapter, $eventDispatcher, $solr_config);
    $ping = $client->createPing();
    $ping_sucessful = 0;
    // execute the ping query
    try {
      $result = $client->ping($ping);
      $ping_sucessful = $result->getData();
    } catch (\Exception $e) {
      dpm($e->getMessage());
      error_log('error pinging');
    }

    $tabdata = ['headers' => [], 'data' => [], 'totalrows' => 0, 'totalfound' => 0];


    // Load the account
    $offset = $per_page > 0 ? $page * $per_page : $page * 100;
    $per_page = $per_page > 0 ? $per_page : 100;
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
          'mods_*',
          'RELS_EXT_hasModel_uri_s',
          'RELS_EXT_is*',
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
      dpm($tabdata);
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
  public function getInfo(array $config, FormStateInterface $form_state, $page = 0, $per_page = 20): array {
    error_log('running getInfo');
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

    $client = new SolariumClient($adapter, $eventDispatcher, $solr_config);
    $ping = $client->createPing();
    $ping_sucessful = 0;
    // execute the ping query
    try {
      $result = $client->ping($ping);
      $ping_sucessful = $result->getData();
    } catch (\Exception $e) {
      dpm($e->getMessage());
      error_log('error pinging');
    }

    $tabdata = ['headers' => [], 'data' => [], 'totalrows' => 0];

    if ($ping_sucessful) {
      try {
        $query = $client->createSelect();

        // search input string, this value fails without escaping because of the double-quote
        $input = 'info:fedora/' . $config['solarium_config']['islandora_collection'];

        // in this case phrase escaping is used (most common) but you can also do term escaping, see the manual
        // also note that the same can be done using the placeholder syntax, see example 6.3
        $helper = $query->getHelper();
        $query->setQuery('RELS_EXT_isMemberOfCollection_uri_s:' . $helper->escapePhrase($input));

        /*
         * This is a good option if Solr data is homogene
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
        $query->setStart(0)->setRows(100);
        $query->setFields([
          'PID',
          'fgs_label_s',
          'mods_*',
          'RELS_EXT_hasModel_uri_s',
          'RELS_EXT_is*',
          'fgs_createdDate_dt',
          'fgs_ModifiedDate_dt',
          'fedora_datastreams_ms'
        ]);
        $resultset = $client->select($query);
        // display the total number of documents found by solr
        error_log('NumFound: ' . $resultset->getNumFound());
        $facet = $resultset->getFacetSet()->getFacet('cmodel');
        $cmodel = [];
        foreach ($facet as $value => $count) {
          if ($count) {
            $cmodel[$value] = $value;
          }
        }
        $form_state->set('facet_cmodel', $cmodel);
        $facet = $resultset->getFacetSet()->getFacet('dsid');
        foreach ($facet as $value => $count) {
        }

        $resultset_iterator = $resultset->getIterator();
        // Empty value? just return
        if (($resultset_iterator == NULL) || empty($resultset_iterator)) {
          $this->messenger()->addMessage(
            t('Nothing to read, check your Solr Query Arguments'),
            MessengerInterface::TYPE_ERROR
          );
          $form_state->setValue(['pluginconfig']['ready'], FALSE);
          return $tabdata;
        }
      } catch (\Exception $e) {
        $this->messenger()->addMessage(
          t('Solr Error: @e', ['@e' => $e->getMessage()]),
          MessengerInterface::TYPE_ERROR
        );
        $form_state->setValue(['pluginconfig', 'ready'], FALSE);
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
      ];
      if (count($config['solarium_mapping']['cmodel_mapping']) == count($cmodel) && count($cmodel) >= 1) {
        $form_state->setValue(['pluginconfig', 'ready'], TRUE);
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


  /*
 * Process queue(s) with batch.
 *
 * @param \Drupal\Core\Form\FormStateInterface $form_state
 * @param $queue
 */
  public function getBatch(FormStateInterface $form_state, $config) {
    $batch = [
      'title' => $this->t('Batch fetching from Solr'),
      'operations' => [],
      'finished' => [
        '\Drupal\ami\Plugin\ImporterAdapter\SolrImporter::finishfetchFromSolr'
      ],
      'progress_message' => t('Processing Set @current of @total.'),
    ];
    $batch['operations'][] = [
      '\Drupal\ami\Plugin\ImporterAdapter\SolrImporter::fetchBatch',
      [$config, $this],
    ];
    return $batch;
  }

  /**
   *  Batch processes data fetch
   *
   * @param array $config
   *
   * @param \Drupal\ami\Plugin\ImporterAdapterInterface $plugin_instace
   * @param array $context
   */
  public static function fetchBatch(array $config, ImporterPluginAdapterInterface $plugin_instace, array &$context):void {
    error_log('fetching batch');
    $rows = $config['solarium_config']['rows'] ?? NULL;
    $offset = $config['solarium_config']['start'] ?? 0;
    $increment = 100;
    if (!isset($context['sandbox']['progress'])) {
      $context['sandbox']['progress'] = 0;
    }

    if (!array_key_exists('max',
        $context['sandbox']) || $context['sandbox']['max'] < $rows) {
      $context['sandbox']['max'] = $rows;
    }
    $context['finished'] = 0;

    try {
      $title = t('Processing <b>%count</b>', [
        '%count' => $rows,
      ]);
      $context['message'] = $title;
      $data = $plugin_instace->getData($config, $context['sandbox']['progress'],
        100);
      dpm($data);
      $context['sandbox']['progress'] = $context['sandbox']['progress'] + count($data['totalrows']);

      // Update context
      $context['results']['processed'][] = $data;
      $context['finished'] = $context['sandbox']['progress'] >= $context['sandbox']['max'] ? 1 : 0;


    } catch (\Exception $e) {
      // In case of any other kind of exception, log it and leave the item
      // in the queue to be processed again later.
      watchdog_exception('ami', $e);
      $context['results']['errors'][] = $e->getMessage();
      $context['finished'] = 1;
    }

  }

   public static function finishfetchFromSolr($success, $results, $operations) {
   dpm($results);
   dpm($operations);
   }

}

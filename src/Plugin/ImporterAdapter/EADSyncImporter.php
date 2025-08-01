<?php

namespace Drupal\ami\Plugin\ImporterAdapter;

use Drupal\ami\AmiUtilityService;
use Drupal\ami\Plugin\ImporterAdapterInterface as ImporterPluginAdapterInterface;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Messenger\MessengerTrait;
use Drupal\Core\StreamWrapper\StreamWrapperManagerInterface;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\ami\Plugin\ImporterAdapterBase;
use Drupal\Core\TempStore\PrivateTempStore;
use GuzzleHttp\ClientInterface;
use PhpOffice\PhpSpreadsheet\IOFactory;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\file\Entity\File;
use Drupal\Core\Utility\Error;
use Drupal\ami\simpleXMLtoArrayEAD;
use Ramsey\Uuid\Uuid;

/**
 * ADO importer from EAD XMLs with SYNC .
 *
 * @ImporterAdapter(
 *   id = "ead_sync",
 *   label = @Translation("EAD Syncronizer from XML files"),
 *   remote = false,
 *   batch = true,
 * )
 */
class EADSyncImporter extends SpreadsheetImporter {


  use StringTranslationTrait;
  use MessengerTrait;

  use StringTranslationTrait;

  const FILE_COLUMNS = [
    'document',
    'dsc_csv'
  ];

  const BATCH_INCREMENTS = 1;

  /**
   * @var \Drupal\Core\StreamWrapper\StreamWrapperManagerInterface
   */
  protected StreamWrapperManagerInterface $streamWrapperManager;

  /**
   * EAD Sync Importer constructor.
   *
   * @param array $configuration
   * @param $plugin_id
   * @param $plugin_definition
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface $entityTypeManager
   * @param \Drupal\Core\StreamWrapper\StreamWrapperManagerInterface $streamWrapperManager
   * @param \Drupal\ami\AmiUtilityService $ami_utility
   */
  public function __construct(array $configuration, $plugin_id, $plugin_definition, EntityTypeManagerInterface $entityTypeManager, StreamWrapperManagerInterface $streamWrapperManager, AmiUtilityService $ami_utility) {
    parent::__construct($configuration, $plugin_id, $plugin_definition,
      $entityTypeManager, $streamWrapperManager, $ami_utility);
    $this->streamWrapperManager = $streamWrapperManager;
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
      $container->get('stream_wrapper_manager'),
      $container->get('ami.utility')
    );
  }

  /**
   * {@inheritdoc}
   */
  public function interactiveForm(array $parents, FormStateInterface $form_state):array {
    $form = [];
    $form['op'] = [
      '#type' => 'select',
      '#title' => $this->t('Operation'),
      '#options' => [
        'sync' => 'Sync ADOs',
      ],
      '#description' => $this->t('The desired Operation. This plugin can only perform Sync. New ADOs will be created, existing ones updated, removed Children (containers) of existing marked for removal.'),
      '#required' => TRUE,
      '#default_value' => $form_state->getValue(array_merge($parents, ['op'])),
      '#empty_option' => $this->t('- Please select an Operation -'),
    ];
    $uuid_seed = $form_state->getValue(array_merge($parents,
      ['eadsync_config', 'uuidV5seed'])) ?? '';
    if (is_scalar($uuid_seed)) {
      $uuid_seed = trim($uuid_seed);
    }
    $form['eadsync_config'] = [
      '#prefix' => '<div id="ami-eadsync">',
      '#suffix' => '</div>',
      '#tree' => TRUE,
      '#type' => 'fieldset',
      '#title' => 'EAD Sync Configuration',
      'uuidV5seed' => [
        '#type' => 'textfield',
        '#maxlength' => 36,
        '#required' => TRUE,
        '#title' => $this->t('A UUIDv4 to be used as Seed to generate consistent UUIDv5 for Each ADO, Components (top) and Containers (Children), across multiple Ingest/Update operations.'),
        '#description' => $this->t('This value needs to be the same everytime a sync Operation runs to ensure EAD XML processed and their containers preserve their UUIDs and can be identified and marked for Update or deletion'),
        '#default_value' => $uuid_seed,
        '#element_validate' => [[static::class,'EADSyncUuidValidation']]
      ],
    ];
    return $form;
  }

  public static function EADSyncUuidValidation(&$element, FormStateInterface $form_state, $complete_form) {
    $uuid = $element['#value'];
   if (!Uuid::isValid($uuid)) {
      $form_state->setError($element, t('UUIDv4 is invalid'));
    }
  }

  /**
   * {@inheritdoc}
   */
  public function getData(array $config,  $page = 0, $per_page = 20): array {
    // $config['xml_file'] will contain the filename we want
    // $config['zip_file'] will contain the original ZIP file ... sadly we need to reload it here.
    // $config['eadsync_config']['uuidV5seed'] needs to contain the seed used to consistently
    // to reload afterward?(or here?) all CSV files and attach them to the existing ZIP.
    // Bundle config
    // $config['
    // generate the same UUIDs using the Archivespace ID(s).

    // Note. Every XML Might generate a different list of Headers.
    // Means we need to return the $data with their own headers
    // Fetch will write it to temporary ->key DB storage
    // and then the finalizer will have to normalize everything

    $seed_uuid_for_uuidv5 = $config['eadsync_config']['uuidV5seed'];
    $config_bundle['component'] = $config['eadsync_config']['bundle_component'];
    $config_bundle['container'] = $config['eadsync_config']['bundle_container'];
    $property_path_split_component = explode(':', $config_bundle['component']);
    $property_path_split_container = explode(':', $config_bundle['container']);
    $temp_store_id = $config['temp_store_id'];

    $data = [];
    // We don't use an offset here.No $page nor $per_page
    $tabdata = [
      'headers' => [],
      'data' => $data,
      'totalrows' => 0,
      'errors' => []
    ];

    // Here it goes bananas.
    // First load the ZIP file to extract one of the XMLs
    /* @var File $file */
    $zip_file = $this->entityTypeManager->getStorage('file')
      ->load($config['zip_file']);
    if (!$zip_file) {
      $this->messenger()->addMessage(
        $this->t(
          'Could not load the file. Please check your Drupal logs or contact your Repository Admin'
        )
      );
      return $tabdata;
    }

    $resulting_row = [];
    $resulting_row_clean = [];
    $file_name_without_extension = basename($config['xml_file'] ?? '', '.xml');
    // Remove ; from file name;
    $file_name_without_extension = str_replace(";","-", $file_name_without_extension);
    // Don't process DOT files.

    $data_from_new_xml = $this->AmiUtilityService->getZipFileContent($zip_file, $config['xml_file']);
    // process for the first XML
    $new_data = $this->processCSVfromXML($file_name_without_extension, $data_from_new_xml, $seed_uuid_for_uuidv5);
    //  $tabdata = ['data_with_headers' => [], 'children_data_with_headers' => [], 'errors' => []];
    $original_value = NULL;
    $original_xml_file_id = NULL;
    if (isset($new_data['data_with_headers']) && is_array($new_data['data_with_headers']) && !empty($new_data['data_with_headers'])) {
      // We have a single ROW PER XML, so not $new_data['data_with_headers'][0]['node_uuid']
      $new_uuid = $new_data['data_with_headers']['node_uuid'] ?? NULL;
      if ($new_uuid) {
        $existing = $this->entityTypeManager->getStorage('node')
          ->loadByProperties(['uuid' => $new_uuid]);
        $existing = reset($existing);
        if ($existing) {
          $new_data['data_with_headers']['ami_sync_op'] = "update";
          // @TODO make this configurable.
          // This allows us not to pass an offset if the SBF is multivalued.
          // WE do not do this, Why would you want that? Who knows but possible.
          $field_name_offset = $property_path_split_component[2] ?? 0;
          $field_name = $property_path_split_component[1];
          /** @var \Drupal\Core\Field\FieldItemInterface $field */
          $field = $existing->get($field_name);
          /** @var \Drupal\strawberryfield\Field\StrawberryFieldItemList $field */
          if (!$field->isEmpty()) {
            /** @var $field \Drupal\Core\Field\FieldItemList */
            foreach ($field->getIterator() as $delta => $itemfield) {
              /** @var \Drupal\strawberryfield\Plugin\Field\FieldType\StrawberryFieldItem $itemfield */
              if ($field_name_offset == $delta) {
                $original_value = $itemfield->provideDecoded(TRUE);
                break;
              }
            }
          }
          // So opinionated. Only way with crazy EADs.
          if ($original_value && isset($original_value["collection_id"])) {
            foreach ($original_value["as:document"] ?? [] as $possible_xml) {
              if (($possible_xml["dr:mimetype"] ?? NULL) == "application/xml") {
                $original_xml_file_id = $possible_xml["dr:fid"] ?? NULL;
                break;
              }
            }
            if ($original_xml_file_id) {
              /** @var File $original_xml_file */
              $original_xml_file = $this->entityTypeManager->getStorage('file')
                ->load(
                  $original_xml_file_id
                );
              if ($original_xml_file) {
                $data_from_original_xml = @file_get_contents($original_xml_file->getFileUri());
                if ($data_from_original_xml) {
                  $original_data = $this->processCSVfromXML($file_name_without_extension, $data_from_original_xml, $seed_uuid_for_uuidv5);
                  // We need to compare UUIDs only of containers. Ones that exist ONLY in the original data and not in the new processed data need to be marked for deletion
                  $new_uuids = [];
                  $new_containers_headers = [];
                  // Edge case. Original data has children, but new 0.
                  // So we need to use either headers.
                  if (isset($new_data['children_data_with_headers']) && is_array($new_data['children_data_with_headers'])) {
                    $new_containers_headers = array_keys($new_data['children_data_with_headers'][0] ?? []);
                    foreach ($new_data['children_data_with_headers'] as &$child_row) {
                      $new_uuids[] = $child_row['node_uuid'];
                      $child_row['ami_sync_op'] = "";
                      // Here we check if the to-be-created ADO (container) exists or not.
                      $existing_child = $this->entityTypeManager->getStorage('node')
                        ->loadByProperties(['uuid' => $child_row['node_uuid'] ?? '']);
                      if ($existing_child) {
                        $child_row['ami_sync_op'] = "update";
                      }
                      else {
                        $child_row['ami_sync_op'] = "create";
                      }
                      // Just in case we make these required, but then the list only contains
                      // ispartof (all series?). This restores them. We add also document/dsc_csv because
                      // we might by mistake map also those for the container part.
                      // Very opinionanted
                      $child_row['ispartof'] = $child_row['ispartof'] ?? '';
                      $child_row['ismemberof'] = $child_row['ismemberof'] ?? '';
                      $child_row['iscontainedby'] = $child_row['iscontainedby'] ?? '';
                      $child_row['document'] = $child_row['document'] ?? '';
                      $child_row['dsc_csv'] = $child_row['dsc_csv'] ?? '';
                      // We only need this one once, since ::processCSVfromXML already returns normalized ROWS.
                      $new_containers_headers = array_keys($child_row);
                    }
                  }
                  // either new or old.
                  $new_containers_headers = $new_containers_headers ?? array_keys($original_data['children_data_with_headers'][0] ?? []);
                  foreach (($original_data['children_data_with_headers'] ?? []) as $original_child_row) {
                    if (!in_array($original_child_row['node_uuid'], $new_uuids)) {
                      $new_row = array_fill_keys($new_containers_headers, NULL);
                      $new_row['node_uuid'] = $original_child_row['node_uuid'];
                      $new_row['label'] = $original_child_row['label'];
                      $new_row['type'] = $original_child_row['type'];
                      $new_row['ami_sync_op'] = "delete";
                      // These can't be null bc we might have just a "delete" one and we need to be sure we always preserve them
                      // For the AMI set/mapping to validate. Thank you @alliomera!
                      $new_row['ispartof'] = $original_child_row['ispartof'] ?? '';
                      $new_row['ismemberof'] = $original_child_row['ismemberof'] ?? '';
                      $new_row['iscontainedby'] = $original_child_row['iscontainedby'] ?? '';
                      $new_row['document'] = $original_child_row['document'] ?? '';
                      $new_row['dsc_csv'] = $original_child_row['dsc_csv'] ?? '';
                      $new_data['children_data_with_headers'][] = $new_row;
                    }
                  }
                }
              }
            }
          }
        }
        else {
          $new_data['data_with_headers']['ami_sync_op'] = "create";
        }
      }
    }
    unset($original_data);
    // Here we need to write the CSV file back to ZIP file.
    $file_child = NULL;
    $file_child_id = NULL;
    // Let's add forced the CSV colum to hold it. Might not have a value
    // if there were none, and no new ones are added neither
    if (count($new_data['children_data_with_headers'] ?? [])) {
      // we take the first one for the headers
      $csv_header_array = array_keys($new_data['children_data_with_headers'][0] ?? []);
      $file_name = $file_name_without_extension . '.csv';
      // append to the actual data bc we have children to output.
      $new_data['data_with_headers']['dsc_csv'] = $file_name;
      $file_child_id = $this->AmiUtilityService->csv_touch($file_name, 'ami_sync/'.$temp_store_id, TRUE);
      $file_child = $file_child_id ? $this->entityTypeManager->getStorage('file')->load(
        $file_child_id) : NULL;
      if ($file_child) {
        $child_data['data'] = $new_data['children_data_with_headers'];
        unset($new_data['children_data_with_headers']);
        $child_data['headers'] = $csv_header_array;
        $this->AmiUtilityService->csv_append($child_data, $file_child, 'node_uuid', TRUE, FALSE);
      }
    }

    // This is just the Top EAD data.
    $tabdata = ['headers' => array_keys($new_data['data_with_headers'] ?? []), 'data' => [$new_data['data_with_headers']], 'child_csv_id' => $file_child_id, 'totalrows' => 1, 'errors' => []];
    return $tabdata;
  }

  public function getInfo(array $config, FormStateInterface $form_state, $page = 0, $per_page = 20): array {
    // Fixed getInfo.
    $headers['type'] = 'type';
    $headers['ismemberof'] = 'ismemberof';
    $headers['ispartof'] = 'ispartof';
    $headers['iscontainedby'] = 'iscontainedby';
    $headers['node_uuid'] = 'node_uuid';
    $headers['label'] = 'label';
    $headers['document'] = 'document';
    $headers['dsc_csv'] = 'dsc_csv';
    $data = array_values($headers);
    $tabdata = ['headers' => array_keys($headers), 'data' => array_values($headers), 'totalrows' => 1];
    return $tabdata;
  }

  public function provideTypes(array $config, array $data): array
  {
    // These are our discussed types. No flexibility here.
    return [
      'ArchiveComponent' => 'ArchiveComponent',
      'ArchiveContainer' =>  'ArchiveContainer',
    ];
  }

  /**
   * Shutdown that "should" clean temp file if one was generated
   */
  public function shutdown() {
    // on PHP-FPM there will be never output of this one..
    if ($this->tempFile !== NULL) {
      $this->AmiUtilityService->cleanUpTemp($this->tempFile);
    }
  }

  public function stepFormAlter(&$form, FormStateInterface $form_state, PrivateTempStore $store, $step): void
  {
    if ($step == 3) {
      $form['ingestsetup']['globalmapping'] = [
        '#type' => 'select',
        '#title' => $this->t('Select the data transformation approach'),
        '#default_value' => 'custom',
        '#options' => ['custom' => 'Custom (Expert Mode)'],
        '#description' => $this->t('How your source data will be transformed into EADs Metadata.'),
        '#required' => TRUE,
      ];
      foreach ($form['ingestsetup']['custommapping'] ?? [] as $key => &$settings) {
        if (strpos($key,'#') !== 0 && is_array($settings)) {
          if ($settings['metadata']['#default_value'] ?? NULL) {
            $form['ingestsetup']['custommapping'][$key]['metadata']['#default_value'] = 'template';
            $form['ingestsetup']['custommapping'][$key]['metadata']['#options'] = ['template' => 'Template'];
          }
        }
      }
    }
    if ($step == 4) {
      $current_options = $form['ingestsetup']['adomapping']['parents']['#options'];
      $current_options['iscontainedby'] = 'iscontainedby (used by CSV child container)';
      // We add this just in case the top level does not has it. NOTE the CSV needs to have it at the end.
      $current_options['ispartof'] = 'ispartof';
      unset($current_options['node_uuid']);
      if (empty($form['ingestsetup']['adomapping']['parents']['#default_value'])) {
        $form['ingestsetup']['adomapping']['parents']['#default_value'] = ['ispartof', 'iscontainedby'];
      }
      $form['ingestsetup']['adomapping']['parents']['#options'] = $current_options;
      $form['ingestsetup']['adomapping']['autouuid'] = [
        '#disabled' => TRUE,
        '#default_value' => FALSE,
      ];
      $form['ingestsetup']['adomapping']['uuid'] = [
        '#default_value' => 'node_uuid',
        '#disabled' => TRUE,
      ];
      foreach ($form['ingestsetup']['custommapping'] ?? [] as $key => &$settings) {
        if (strpos($key,'#') !== 0 && is_array($settings)) {
          if ($settings['metadata']['#default_value'] ?? NULL) {
            $form['ingestsetup']['custommapping'][$key]['metadata']['#default_value'] = 'template';
            $form['ingestsetup']['custommapping'][$key]['metadata']['#options'] = ['template' => 'Template'];
          }
        }
      }
    }
    $form = $form;
  }

  /**
   * @inheritDoc
   */
  public function alterStepStore(FormStateInterface $form_state, PrivateTempStore $store, int $step = 1): void {
    if ($step == 4) {
      $mapping = $store->get('mapping');
      // We only set this for ArchiveComponent, that way we won't have nested of nested CSVs (means the container CSV
      // won't have nested CSV again. We can. We won't
      // Diego wake up!
      if (isset($mapping['custommapping_settings']['ArchiveComponent'])) {
        // Needs to be an associative array bc we convert into object afterward and access the files_csv as property
        $mapping['custommapping_settings']['ArchiveComponent']['files_csv'] = ['dsc_csv' => 'dsc_csv'];
        // Will be used by \Drupal\ami\Plugin\QueueWorker\IngestADOQueueWorker::processItem
      }
      $store->set('mapping', $mapping);
    }
  }


  /**
   * {@inheritdoc}
   */
  public function getBatch(FormStateInterface $form_state, array $config, \stdClass $amisetdata) {
    $temp_uuid = Uuid::uuid4()->toString();
    $config['temp_store_id'] = $temp_uuid;
    $batch = [
      'title' => $this->t('Batch processing from XML files inside your attached ZIP file'),
      'operations' => [],
      'finished' => '\Drupal\ami\Plugin\ImporterAdapter\EADSyncImporter::finishfetchFromZip',
      'progress_message' => t('Processing Set @current of @total.'),
    ];
    $zipfile = $this->entityTypeManager->getStorage('file')->load($amisetdata->zip);
    if (!$zipfile) {
      // @TODO ERROR
      return FALSE;
    }
    $xml_files = $this->AmiUtilityService->listZipFileContent($zipfile, 'xml');
    $xml_files = array_filter($xml_files?? [], function ($filepath)  {
      // remove any dot files.
      return !str_starts_with(basename((string)($filepath ?? '')), '.');
    });
    // We need to re-index if not our increment/to file marker dies.
    $config['xml_files'] = array_values($xml_files ?? []);
    $file = $this->entityTypeManager->getStorage('file')->load($amisetdata->csv);
    if (!$file || empty($config['xml_files'])) {
      // @TODO ERROR
      return FALSE;
    }
    $batch['operations'][] = [
      '\Drupal\ami\Plugin\ImporterAdapter\EADSyncImporter::fetchBatch',
      [$config, $this, $file, $amisetdata],
    ];
    return $batch;
  }

  /**
   * @param array $config
   * @param \Drupal\ami\Plugin\ImporterAdapterInterface $plugin_instance
   * @param \Drupal\file\Entity\File $file
   *    This differs from the standard implementation where the $file is the CSV
   *    Here we pass the ZIP file around bc we need to extract the source XMLs
   * @param \stdClass $amisetdata
   * @param array $context
   *
   * @return void
   */
  public static function fetchBatch(array $config, ImporterPluginAdapterInterface $plugin_instance, File $file, \stdClass $amisetdata, array &$context):void {

    $increment = static::BATCH_INCREMENTS;
    $xml_files_count = count($config['xml_files']);
    $config['eadsync_config']['bundle_component'] = $amisetdata->mapping->custommapping_settings->ArchiveComponent->bundle ?? 'digital_object_collection:field_descriptive_metadata';
    $config['eadsync_config']['bundle_container'] = $amisetdata->mapping->custommapping_settings->ArchiveContainer->bundle ?? 'digital_object:field_descriptive_metadata';

    if (!isset($context['sandbox']['progress'])) {
      $context['sandbox']['progress'] = 0;
    }
    $context['results']['processed']['headerswithdata'] = $context['results']['processed']['headerswithdata'] ?? [];
    $context['results']['processed']['tempstore_ids'] =  $context['results']['processed']['tempstore_ids'] ?? [];

    if (!array_key_exists('max',
        $context['sandbox']) || $context['sandbox']['max'] < $xml_files_count) {
      $context['sandbox']['max'] = $xml_files_count;
    }
    if (!array_key_exists('prev_index',
      $context['sandbox'])) {
      $context['sandbox']['prev_index'] = 0;
    }
    $context['finished'] = $context['finished'] ?? 0;
    try {
      // Our increment here is always 1. EAD XML files can be huge.

      if ($context['sandbox']['progress'] == 0) {
        $title = t(
          'Attempting to process first XML of <b>%count</b> total.',
          [
            '%count'    => $xml_files_count,
            '%progress' => $context['sandbox']['progress'] + $increment,
          ]
        );
      }
      else {
        $title = t(
          'Processing %progress of <b>%count</b> XMLs so far.',
          [
            '%count'    => $xml_files_count,
            '%progress' =>  $context['sandbox']['progress'] + $increment,
          ]
        );
      }

      $context['message'] = $title;
      // WE keep track in the AMI set Config of the previous total rows
      // Because Children will offset all the results significantly
      // And we pass that data into the ::getData to offset the next set of
      // Parents/Children.
      // Pass the headers into the config, so we have a unified/normalized version
      // And not the mess each doc returns

      $context['finished'] = $context['sandbox']['progress'] / $context['sandbox']['max'];
      if ($context['finished'] !== 1) {
        $config['zip_file'] = $amisetdata->zip;
        if (isset($config['xml_files'][$context['sandbox']['progress']])) {
          $config['xml_file'] = $config['xml_files'][$context['sandbox']['progress']];
          $data = $plugin_instance->getData($config, 0,
            1);
          $append_headers = $context['sandbox']['progress'] == 0 ? TRUE : FALSE;
          $context['results']['processed']['fileuuid'] = $file->uuid();
          $context['results']['processed']['filezip'] = $amisetdata->zip ?? NULL;
          if (!empty($data['errors']) && is_array($data['errors'])) {
            foreach ($data['errors'] as $getdataerror) {
              $context['results']['errors'][] = $getdataerror;
            }
          }
          if (!empty($data['child_csv_id'])) {
            // If an XML generated a child CSV, the Drupal File ID will be accumulated here.
            // And then iterated and added to the original ZIP on ::finishfetchFromZip
            $context['results']['processed']['children_csv_ids'][] = $data['child_csv_id'];
          }

          $context['results']['processed']['headers'] = $data['headers'];
          $context['results']['processed']['headerswithdata'] = array_unique(array_merge($data['headers'],$context['results']['processed']['headerswithdata']));

          $file_csv_uuid = $context['results']['processed']['fileuuid'] ?? NULL;
          if (count($data['headers'] ?? [])) {
            // Only store if we really got data.
            $tempstore = \Drupal::service('tempstore.private')
              ->get('ami_multistep_batch_data');
            $temp_store_row_id = md5($config['temp_store_id'] . $config['xml_file']);
            $context['results']['processed']['tempstore_ids'][] = $temp_store_row_id;
            $tempstore->set($temp_store_row_id, $data['data']);
          }
          $context['sandbox']['progress']++;
        }
        else {
          // The progress key exceeds the number of files or we have a file missing. Mark this done.
          $context['finished'] == 1;
        }
      }
      // We might had already processed the last one. So we double check here. We can bail out sooner.
			$context['finished'] = $context['sandbox']['progress'] / $context['sandbox']['max'];
    } catch (\Exception $e) {
      // In case of any other kind of exception, log it
      $logger = \Drupal::logger('ami');
      Error::logException($logger, $e);
      $context['results']['errors'][] = $e->getMessage();
      $context['finished'] = 1;
    }
  }

  public static function finishfetchFromZip($success, $results, $operations) {

    $allheaders = $results['processed']['headerswithdata'] ?? [];
    $tempstore = \Drupal::service('tempstore.private')
      ->get('ami_multistep_batch_data');
    // Clean the CSV removing empty headers

    // Normalize based on the accumulated_keys!
    $template = array_fill_keys($allheaders, NULL);
    ksort($template, SORT_NATURAL);

    $file_csv_uuid = $results['processed']['fileuuid'] ?? NULL;
    if ($file_csv_uuid) {
      $file_csv = \Drupal::service('entity.repository')->loadEntityByUuid(
        'file', $file_csv_uuid
      );
      if ($file_csv) {
        // $append_headers only on the first row.
        $i = 0;
        foreach (($results['processed']['tempstore_ids'] ?? []) as $temp_id) {
          $data_rows = $tempstore->get($temp_id);
          $data = [];
          $data_rows = array_map(function($item) use ($template) {
            $toreturn = array_merge($template, (array) $item);
            ksort($toreturn, SORT_NATURAL);
            return $toreturn;
          }, $data_rows);
          $data['data'] = $data_rows;
          // $template bc we sorted.
          $data['headers'] =  array_keys($template);
          $append_headers = FALSE;
          if  ($i == 0) {
            $append_headers = TRUE;
          }
          \Drupal::service('ami.utility')->csv_append($data, $file_csv, 'node_uuid', $append_headers, TRUE, FALSE);
          $tempstore->delete($temp_id);
          $i++;
        }
        if ($results['processed']['filezip'] ?? NULL) {
          // Now deal with adding any/if any CSVs to the ZIP
          /* @var File $zip_file */
          $zip_file = \Drupal::entityTypeManager()->getStorage('file')
            ->load($results['processed']['filezip']);
          if ($zip_file) {
            $to_be_zipped = [];
            if (!empty($results['processed']['children_csv_ids']) && is_array($results['processed']['children_csv_ids'])) {
              foreach ($results['processed']['children_csv_ids'] as $csv_id) {
                $csv_file = \Drupal::entityTypeManager()
                  ->getStorage('file')
                  ->load($csv_id);
                if ($csv_file) {
                  $realpath = \Drupal::service('file_system')->realpath($csv_file->getFileUri());
                  if ($realpath) {
                    $to_be_zipped[] = ['path' => $realpath, 'dest' => basename($realpath)];
                  }
                }
              }
              // Will accumulated. Now add to the ZIP file.
              $success = \Drupal::service('ami.utility')->AddFilesToZip($zip_file, $to_be_zipped);
            }
          }
        }
      }
    }
    else {
      $data['results']['errors'][] = t('Error. We could not save your Process data from XML to CSV!');
    }
    \Drupal::service('tempstore.private')->get('ami_multistep_data')->set('batch_finished', $data);
		\Drupal::service('tempstore.private')->get('ami_multistep_data')->set('zip', NULL);
  }

  protected static function arrayToFlatJsonPropertyPathsXML(&$flat, array $sourcearray = [], $propertypath = '', $excludepaths = [], $excludekeys = [], $useNumericKeys = FALSE, $pastpost = NULL) {
    // Orangelist paths. Strip the last dot in case this was called recursively.
    if (!empty($excludepaths) && in_array(rtrim($propertypath,'.'), $excludepaths, true)) {
      return $flat;
    }

    foreach ($sourcearray as $key => $values) {
      if (in_array($key, $excludekeys, true)) {
        $flat[$key] = json_encode($values);
        continue;
      }

      // If a Key is an URL chances are we are dealing with many different ones
      // Also we want to build JSON Paths here, so replace with *
      // But PHP does not know anything about URIs... like URN...
      if(filter_var($key, FILTER_VALIDATE_URL) || \Drupal\strawberryfield\Tools\StrawberryfieldJsonHelper::validateURN($key)) {
        $pastpost_next = $pastpost;
        $path_key = "*";
      }
      elseif (is_integer($key)) {
        if ($useNumericKeys) {
          $path_key = $key;
        }
        else {
          // We will use $pos to keep track of the position of the value in case there are offsets or
          // Properties that only exist under a certain hierarchy but not in others.
          $pastpost_next = $pastpost ? $pastpost + $key : $key;
          $path_key = '[*]';
        }
      }
      else {
        $pastpost_next = $pastpost;
        $path_key = $key;
      }
      // I could break here instead of iterating further, but that could exclude sub properties not present
      // In the first element
      if (is_array($values)) {
        $flat = static::arrayToFlatJsonPropertyPathsXML($flat , $values,$propertypath.$path_key.'.', $excludepaths, $excludekeys, $useNumericKeys, $pastpost_next);
      }
      else {
        if ($pastpost || $pastpost_next) {
          for($i =0; $i<=$pastpost;$i++) {
            // SUPER POOR APPROACH. SHOULD WORK?
            if (!isset($flat[$propertypath . $path_key][$i])) {
              $flat[$propertypath . $path_key][$i] = "NOT PRESENT";
            }
          }
          $flat[$propertypath . $path_key][$pastpost] = $values;
        }
        else {
          $flat[$propertypath . $path_key][] = $values;
        }
      }
    }
    return $flat;
  }

  /**
   * @param array $array
   *     An Associative array coming, maybe, from a JSON string.
   * @param array $flat
   *     An, by reference, accumulator.
   * @param bool $jsonld
   *    If special JSONLD handling is desired.
   *
   * @return array
   *   Same as the accumulator but left there in case someone needs a return.
   */
  static function arrayToFlatCommonkeysWithParentEAD(
    array &$array,
    &$flat = [],
    $jsonld = TRUE,
    $parent = '',
    $greenlist = [],
    $orangelist = [],
    $level = 0,
  ) {
    if (($jsonld) && array_key_exists('@graph', $array)) {
      $array = $array['@graph'];
    }
    else {
      // @TODO We need to deal with the possibility of multiple @Contexts
      // Which could make a same $key mean different things.
      // In this case @context could or not exist.
      unset($array['@context']);
    }
    // Complex ISSUE April 2024. If we need to keep track of parent/child and nesting
    // And the XML itself did not have for what we want @id per element that define
    // We can't blindly assign ID's to anything bc we will end with
    // children that point to a grandchild/ID out of scope...
    // Based on how XML to JSON works, so far it seems it is only safe to apply an ID to numeric keys.
    foreach ($array as $key => &$value) {
      if (is_array($value)) {
        if (isset($value['@id'])) {
          $newparent = $value['@id'];
        }
        else {
          if (is_numeric(($key))) {
            $value['@id'] = md5(json_encode($value)) . '_' . $key .'_'.($parent ?? '');
            $newparent = $value['@id'];
          }
          else {
            $newparent = $parent;
          }
        }
        static::arrayToFlatCommonkeysWithParentEAD($value, $flat, $jsonld, $newparent, $greenlist, $orangelist, $level++);
        // Don't accumulate int keys. Makes no sense.
        if (isset($value[$key])) {
          unset($value[$key]);
        }
        if (is_string($key)) {
          if (!isset($flat[$key])) {
            $flat[$key] = [];
          }
          // Means we can't keep flattening
          if (is_array($value)) {
            if (!static::arrayIsMultiSimple($value)) {
              foreach ($value as &$item) {
                if (isset($item[$key])) {
                  unset($item[$key]);
                }
                // Only elements that have an actual @ID get a @parent
                if (isset($item['@id'])) {
                  $item['@parentid'] = $parent;
                }
              }
            }
            else {
              // Only elements that have an actual @ID get a @parent
              if (isset($value['@id'])) {
                $value['@parentid'] = $parent;
              }
            }
            // We could unset the current key from the child array
            // To avoid double nesting.
            // We want flat-flat, without repetition

            $flat[$key] = array_merge($flat[$key], $value);
          }
          else {
            $flat[$key][] = $value;
          }
        }
      }
      else {
        // Don't accumulate int keys. Makes no sense.
        if (is_string($key)) {
          $flat[$key][] = $value;
        }
      }
    }
    return $flat;
  }

  /**
   * @param $numeric_id
   * @param $parent_uuid
   *
   * @return array
   */
  static function jsonContainerADO($numeric_id, $parent_uuid): array {
    $container_csv = [];
    $container_csv['node_uuid'] = Uuid::uuid5($parent_uuid, $numeric_id['@id']);
    $container_csv['node_uuid'] = $container_csv['node_uuid']->toString();
    foreach ($numeric_id as $container_key => $key_value) {
      if ($container_key == "did") {
        foreach ($key_value[0] ?? [] as $did_key => $did_value) {
          if  ($did_key == "unittitle") {
            if (!isset($did_value[0]['@value'])) {
              foreach ($did_value[0] as $substructure) {
                if ($substructure[0]['@value'] ?? NULL) {
                  $container_csv['label'] = ($container_csv['label'] ?? '') . $substructure[0]['@value'];
                }
              }
            }
            else {
              $container_csv['label'] = $did_value[0]['@value'];
            }
          }
          $container_csv[$did_key] = is_array($did_value) ? json_encode($did_value, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_HEX_QUOT ,512) : $did_value;
        }
      }
      else {
        $container_csv[$container_key] = is_array($key_value) ? json_encode($key_value, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_HEX_QUOT ,512) : $key_value;
      }
    }
    return $container_csv;
  }

  static function sortByParent(array $source, $predicate): array {
    $children=[];
    $roots =[];
    // Key here is the original order.
    // Used in the parent relationship
    foreach($source as $key => $value) {
      if (isset($value[$predicate]) && (!empty($value[$predicate]) || $value[$predicate]!="")) {
        $children[$value[$predicate]][$key] = $value;
      }
      else {
        $roots[$key] = $value;
      }
    }
    // We can't sort numerically here. Nor sort at all.. but we can try to get
    // parents first, then children directly to those parents and then go on that way.
    $return = [];
    $i = 1;
    static::fetchChildren($roots, $children, $return, $i, $predicate);
    return $return;
  }

  static function fetchChildren(array $roots, array $children, array &$return = [], &$i = 1, $predicate = 'parent'): void {
    foreach ($roots as $key => $value) {
      $i++;
      // All Parents first
      $return[$i] = $value;
      $current_parent = $i;
      if (isset($children[$key])) {
        $source = $children[$key];
        unset($children[$key]);
        // All children of the Parents
        foreach($source as $key_internal => $internal_value) {
          if (isset($internal_value[$predicate]) && (!empty($internal_value[$predicate]) && isset($children[$key_internal]))) {
            // Means we have children for the current row
            $newroots = [];
            $internal_value[$predicate] = $current_parent;
            $newroots[$key_internal] = $internal_value;
            static::fetchChildren($newroots, $children, $return, $i, $predicate);
          }
          else {
            $i++;
            $internal_value[$predicate] = $current_parent;
            $return[$i] = $internal_value;
          }
        }
      }
    }
  }

  private function processCSVfromXML(string $file_name, string $data, string $seed_uuid_for_uuidv5) {
    $tabdata = [
      'data_with_headers' => [],
      'children_data_with_headers' => [],
      'errors' => []
    ];
    $internalErrors = libxml_use_internal_errors(TRUE);
    $csv_header = [];
    $container_csv = [];
    libxml_clear_errors();
    libxml_use_internal_errors($internalErrors);
    try {
      $simplexml = @simplexml_load_string($data);
    }
    catch (\Throwable $e) {
      $tabdata['errors'] = [$this->t('@file is not a valid XML', ['@file' => $file_name])];
      return $tabdata;
    }
    if (!$simplexml) {
      $tabdata['errors'] = [$this->t('@file is not a valid XML', ['@file' => $file_name])];
      return $tabdata;
    }

    $SimpleXMLtoArray = new simpleXMLtoArrayEAD($simplexml);
    $xmljsonarray = $SimpleXMLtoArray->xmlToArray();
    $ead_container_keys = ['c','c01','c02','c03', 'c04'];

    static::arrayToFlatJsonPropertyPathsXML($resulting_row, $xmljsonarray, '', [], $ead_container_keys, FALSE, FALSE );
    $accumulated_flat = [];
    static::arrayToFlatCommonkeysWithParentEAD($xmljsonarray,$accumulated_flat, FALSE, '', [], []);

    // This will remove nesting of containers. WE COULD TO THIS LATER, BUT SEEMS LIKE MEMORY IS AN ISSUE, SO DO IT RIGHT NOW
    $resulting_row['c'] = $accumulated_flat['c'] ?? [] ;
    $c01 =  $accumulated_flat['c01']  ?? [];
    $c02 = $accumulated_flat['c02'] ?? [];
    $c03 = $accumulated_flat['c03'] ?? [];
    $c04 = $accumulated_flat['c04'] ?? [];
    $previous_parent = NULL;
    $sequence = 1;
    $unitid = is_string($resulting_row['ead.archdesc.[*].did.[*].unitid.[*].@value']) ? $resulting_row['ead.archdesc.[*].did.[*].unitid.[*].@value'] : reset($resulting_row['ead.archdesc.[*].did.[*].unitid.[*].@value']);

    foreach ($c01 as &$value) {
      if ($value['@parentid'] != ($previous_parent ?? NULL)) {
        $sequence = 1;
        $previous_parent = $value['@parentid'];
      }
      else {
        $sequence = $sequence+1;
      }
      $value['sequence_id'] = $sequence;
    }
    $previous_parent = NULL;
    $sequence = 1;
    foreach ($c02 as &$value) {
      if ($value['@parentid'] != ($previous_parent ?? NULL)) {
        $sequence = 1;
        $previous_parent = $value['@parentid'];
      }
      else {
        $sequence = $sequence+1;
      }
      $value['sequence_id'] = $sequence;
    }
    $previous_parent = NULL;
    $sequence = 1;
    foreach ($c03 as &$value) {
      if ($value['@parentid'] != ($previous_parent ?? NULL)) {
        $sequence = 1;
        $previous_parent = $value['@parentid'];
      }
      else {
        $sequence = $sequence+1;
      }
      $value['sequence_id'] = $sequence;
    }
    $previous_parent = NULL;
    $sequence = 1;
    foreach ($c04 as &$value) {
      if ($value['@parentid'] != ($previous_parent ?? NULL)) {
        $sequence = 1;
        $previous_parent = $value['@parentid'];
      }
      else {
        $sequence = $sequence+1;
      }
      $value['sequence_id'] = $sequence;
    }
    $previous_parent = NULL;
    $sequence = 1;
    foreach ($resulting_row['c'] as &$value) {
      if ($value['@parentid'] != ($previous_parent ?? NULL)) {
        $sequence = 1;
        $previous_parent = $value['@parentid'];
      }
      else {
        $sequence = $sequence+1;
      }
      $value['sequence_id'] = $sequence;
    }
    $resulting_row['c'] = array_merge($resulting_row['c'], $c01, $c02, $c03,$c04);
    // We will use this as check when writing the CSV. If it does not match we alert/break.
    $number_of_containers = count( $resulting_row['c']);
    unset($c01, $c02, $c03, $c04);

    $resulting_row['document'] = $file_name.'.xml';
    if ($seed_uuid_for_uuidv5 && !empty($resulting_row['ead.archdesc.[*].did.[*].unitid.[*].@value'])) {
      $seed = '';
      try {
        $seed = is_string($resulting_row['ead.archdesc.[*].did.[*].unitid.[*].@value']) ? $resulting_row['ead.archdesc.[*].did.[*].unitid.[*].@value'] : reset($resulting_row['ead.archdesc.[*].did.[*].unitid.[*].@value']);
        $resulting_row['node_uuid'] = Uuid::uuid5($seed_uuid_for_uuidv5, $seed);
        $resulting_row['node_uuid'] = $resulting_row['node_uuid']->toString();
      }
      catch (\Exception $error) {
        $tabdata['errors'] = [$this->t('@UUID or @ID are not good for generating an UUIDV5 Identifier ', ['@UUID'=>$seed_uuid_for_uuidv5, '@ID' => $seed])];
        return $tabdata;
      }
      $resulting_row['node_uuidv5_namespace'] = $seed_uuid_for_uuidv5;
      // NOTE to myself. @TODO: check if the original SEED USED in any ingested Object is the same we are passing. If not bail out.
      $resulting_row['type'] = 'ArchiveComponent';
    }

    $resulting_row['label'] = is_array($resulting_row['ead.archdesc.[*].did.[*].unittitle.[*].@value']) ? reset($resulting_row['ead.archdesc.[*].did.[*].unittitle.[*].@value']) : $resulting_row['ead.archdesc.[*].did.[*].unittitle.[*].@value'];
    if (empty($resulting_row['label'])) {
      //@TODO ask Allison if we should clean here, right any emph found. Also, we should join label is multiple values (array)
      // as seen in the wild
      $resulting_row['label'] = "UNKNOWN TITLE for EAD";
    }
    // Can't be NULL bc we filter. But are needed even if empty bc we want to be sure
    // both children CSVs and Top CSV has all the needed arguments or the AMI set will fail.
    $resulting_row['ispartof'] = $resulting_row['ispartof'] ?? '';
    $resulting_row['ismemberof'] = $resulting_row['ismemberof'] ?? '';
    $resulting_row['iscontainedby'] = $resulting_row['iscontainedby'] ?? '';
    // This might change afterwards once original XMLs (if any) are processed and compared.
    $resulting_row['ami_sync_op'] = 'create';
    // Now transform $resulting_row['c'] into the proper expected structure
    // as Individual ADOs
    $k = 1;
    $numeric_ids = [];
    $numeric_ids_id = [];

    foreach ($resulting_row['c'] as $key => &$container) {
      if (is_array($container)) {
        foreach ($ead_container_keys as $nestedkey) {
          unset($container[$nestedkey]);
        }

        if (!isset($container["@id"])) {
          // @TODO: how do we log this? print_r('Container has no id?');
          $container["@id"] = 'automatic_id_' . $key;
        }
        // Take each "@id" and put it into a hash, so we have map for numeric IDs
        $numeric_ids[$container["@id"]] = $numeric_ids[$container["@id"]] ?? $container;
        $numeric_ids_id[$container["@id"]] = $numeric_ids_id[$container["@id"]] ?? $k++;
      }
    }
    $resulting_row["ap:importeddata"]["dsc_csv"]["format"] = "csv";

    foreach ($numeric_ids as $id => $numeric_id) {
      $container_csv = [];
      $container_csv = static::jsonContainerADO($numeric_id, $resulting_row['node_uuid']);
      // could be empty, it @ids were generated automatically and
      // the parent of a "series" points to the ID of the whole `dsc`, out of scope in our current
      // merged arrays of c, c01, c02, c03, etc.
      if (isset($numeric_id['@parentid']) && isset($numeric_ids_id[$numeric_id['@parentid']])) {
        $parent_id = !empty($numeric_id['@parentid']) ? (string)$numeric_ids_id[$numeric_id['@parentid']] : "";
      }
      else {
        $parent_id = "";
      }
      // @TODO. We should here make this the UUID? reflect on that? Why/benefits?
      $container_csv['iscontainedby'] = (string) $parent_id;
      $container_csv['type'] = 'ArchiveContainer';
      $container_csv['ispartof'] = (string)  $resulting_row['node_uuid'];
      $container_csv['ismemberof'] = '';
      // This might be updated to "update" once the parent method checks of there are existing ADOs with the same UUID.
      $container_csv['ami_sync_op'] = 'create';
      $resulting_row["ap:importeddata"]["dsc_csv"]["content"][(string)$numeric_ids_id[$id]] = $container_csv;
      // Now accumulate all keys of this container to build the CSV header
      $csv_header = array_unique(array_filter(array_merge($csv_header, array_keys($container_csv))));
    }


    if (!empty($resulting_row["ap:importeddata"]["dsc_csv"]["content"])) {
      $sort_predicate = 'iscontainedby';
      $container_csv_data = static::sortByParent($resulting_row["ap:importeddata"]["dsc_csv"]["content"], $sort_predicate);
      unset($resulting_row["ap:importeddata"]);
      if (count($csv_header)) {
        $csv_header_array = array_fill_keys($csv_header, NULL);
        foreach ($container_csv_data as $extra_csv_row) {
          $csv_header_combined = [];
          $csv_header_combined = array_merge($csv_header_array, $extra_csv_row);
          $tabdata['children_data_with_headers'][] = $csv_header_combined;
        }
        $resulting_row['dsc_csv'] = $file_name.'.csv';
      }
    }

    // New to avoid 14K CONTAINERS TO BE JSONENCODED IN A SINGLE COLUMN FEB/2024
    unset($resulting_row["ap:importeddata"]);
    foreach($ead_container_keys as $nestedkey) {
      unset($resulting_row[$nestedkey]);
    }
    unset($resulting_row["c"]);

    foreach($resulting_row as $key => $fullvalue) {
      // Don't treat empty strings as empty
      if (!empty($fullvalue) || $fullvalue === '') {
        $resulting_row_clean[$key] = is_array($fullvalue) ? json_encode($fullvalue,
          JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_HEX_QUOT,
          512) : $fullvalue;
      }
    }
    // Just in case restore these, since they are required.
    $resulting_row_clean['ispartof'] = $resulting_row_clean['ispartof'] ?? '';
    $resulting_row_clean['ismemberof'] = $resulting_row_clean['ismemberof'] ?? '';
    $resulting_row_clean['iscontainedby'] = $resulting_row_clean['iscontainedby'] ?? '';

    unset($resulting_row);
    unset($container_csv_data);
    $tabdata['data_with_headers'] = $resulting_row_clean;

    return $tabdata;
  }

  static function arrayIsMultiSimple(array $sourcearray =  []) {
    return !empty(array_filter(array_keys($sourcearray), 'is_string'));
  }


}

<?php

namespace Drupal\ami\Plugin\QueueWorker;

use Drupal\ami\AmiLoDService;
use Drupal\ami\AmiUtilityService;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Logger\LoggerChannelFactoryInterface;
use Drupal\Core\Messenger\MessengerInterface;
use Drupal\Core\Plugin\ContainerFactoryPluginInterface;
use Drupal\Core\Queue\QueueWorkerBase;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\strawberryfield\StrawberryfieldFilePersisterService;
use Drupal\strawberryfield\StrawberryfieldUtilityService;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Swaggest\JsonDiff\JsonDiff;
use Drupal\strawberryfield\Tools\StrawberryfieldJsonHelper;
use \Drupal\Core\TempStore\PrivateTempStoreFactory;

/**
 * Processes and Ingests each AMI Set CSV row.
 *
 * @QueueWorker(
 *   id = "ami_ingest_ado",
 *   title = @Translation("AMI Digital Object Ingester Queue Worker")
 * )
 */
class IngestADOQueueWorker extends QueueWorkerBase implements ContainerFactoryPluginInterface {

  use StringTranslationTrait;
  /**
   * Drupal\Core\Entity\EntityTypeManager definition.
   *
   * @var \Drupal\Core\Entity\EntityTypeManager
   */
  protected $entityTypeManager;

  /**
   * The logger factory.
   *
   * @var \Drupal\Core\Logger\LoggerChannelFactoryInterface
   */
  protected $loggerFactory;

  /**
   * The Strawberry Field Utility Service.
   *
   * @var \Drupal\strawberryfield\StrawberryfieldUtilityService
   */
  protected $strawberryfieldUtility;


  /**
   * @var \Drupal\ami\AmiUtilityService
   */
  protected $AmiUtilityService;

  /**
   * The messenger.
   *
   * @var \Drupal\Core\Messenger\MessengerInterface
   */
  protected $messenger;

  /**
   * @var \Drupal\ami\AmiLoDService
   */
  protected $AmiLoDService;

  /**
   * The Strawberryfield File Persister Service
   *
   * @var \Drupal\strawberryfield\StrawberryfieldFilePersisterService
   */
  protected $strawberryfilepersister;

  /**
   * @var \Drupal\user\PrivateTempStore
   */
  protected $store;

  /**
   * Constructor.
   *
   * @param array $configuration
   * @param string $plugin_id
   * @param mixed $plugin_definition
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface $entity_type_manager
   * @param \Drupal\Core\Logger\LoggerChannelFactoryInterface $logger_factory
   * @param \Drupal\strawberryfield\StrawberryfieldUtilityService $strawberryfield_utility_service
   * @param \Drupal\ami\AmiUtilityService $ami_utility
   * @param \Drupal\ami\AmiLoDService $ami_lod
   * @param \Drupal\Core\Messenger\MessengerInterface $messenger
   * @param \Drupal\strawberryfield\StrawberryfieldFilePersisterService $strawberry_filepersister
   * @param \Drupal\Core\TempStore\PrivateTempStoreFactory $temp_store_factory
   */
  public function __construct(
    array $configuration,
    $plugin_id,
    $plugin_definition,
    EntityTypeManagerInterface $entity_type_manager,
    LoggerChannelFactoryInterface $logger_factory,
    StrawberryfieldUtilityService $strawberryfield_utility_service,
    AmiUtilityService $ami_utility,
    AmiLoDService $ami_lod,
    MessengerInterface $messenger,
    StrawberryfieldFilePersisterService $strawberry_filepersister,
    PrivateTempStoreFactory $temp_store_factory

  ) {
    parent::__construct($configuration, $plugin_id, $plugin_definition);
    $this->entityTypeManager = $entity_type_manager;
    $this->loggerFactory = $logger_factory;
    $this->strawberryfieldUtility = $strawberryfield_utility_service;
    $this->AmiUtilityService = $ami_utility;
    $this->messenger = $messenger;
    $this->AmiLoDService = $ami_lod;
    $this->strawberryfilepersister = $strawberry_filepersister;
    $this->store = $temp_store_factory->get('ami_queue_worker_file');
  }

  /**
   * Implementation of the container interface to allow dependency injection.
   *
   * @param \Symfony\Component\DependencyInjection\ContainerInterface $container
   * @param array $configuration
   * @param string $plugin_id
   * @param mixed $plugin_definition
   *
   * @return static
   */
  public static function create(
    ContainerInterface $container,
    array $configuration,
    $plugin_id,
    $plugin_definition
  ) {
    return new static(
      empty($configuration) ? [] : $configuration,
      $plugin_id,
      $plugin_definition,
      $container->get('entity_type.manager'),
      $container->get('logger.factory'),
      $container->get('strawberryfield.utility'),
      $container->get('ami.utility'),
      $container->get('ami.lod'),
      $container->get('messenger'),
      $container->get('strawberryfield.file_persister'),
      $container->get('tempstore.private')
    );
  }

  /**
   * {@inheritdoc}
   */
  public function processItem($data) {
    /* Data info for an ADO has this structure
      $data->info = [
        'row' => The actual data
        'set_id' => The Set id
        'uid' => The User ID that processed the Set
        'set_url' => A direct URL to the set.
        'status' => Either a string (moderation state) or a 1/0 for published/unpublished if not moderated
        'op_secondary' => applies only to Update/Patch operations. Can be one of 'update','replace','append'
        'ops_safefiles' => Boolean, True if we will not allow files/mappings to be removed/we will keep them warm and safe
        'log_jsonpatch' => If for Update operations we will generate a single PER ADO Log with a full JSON Patch,
        'attempt' => The number of attempts to process. We always start with a 1
        'zip_file' => File ID of a zip file if a any
        'waiting_for_files' => will only exist and TRUE if we re-enqueued this ADO after figuring out we had too many Files.
        'queue_name' => because well ... we use Hydroponics too
        'force_file_queue' => defaults to false, will always treat files as separate queue items.
        'force_file_process' => defaults to false, will force all techmd and file fetching to happen from scratch instead of using cached versions.
        'manyfiles' => Number of files (passed by \Drupal\ami\Form\amiSetEntityProcessForm::submitForm) that will trigger queue processing for files,
        'ops_skip_onmissing_file' => Skips ADO operations if a passed/mapped file is not present,
        'ops_forcemanaged_destination_file' => Forces Archipelago to manage a files destination when the source matches the destination Schema (e.g S3),
      ];
    */
    /* Data info for a File has this structure
      $data->info = [
        'set_id' => The Set id
        'uid' => The User ID that processed the Set
        'uuid' => The uuid of the ADO that needs this file
        'filename' => The File name
        'file_column' => The File column where the file needs to be saved.
        'zip_file' => File ID of a zip file if a any,
        'processed_row' => Full metadata of the ADO holding the file processed and ready as an array
        'queue_name' => because well ... we use Hydroponics too
        'force_file_process' => defaults to false, will force all techmd and file fetching to happen from scratch instead of using cached versions.
        'reduced' => If reduced EXIF or not should be generated,
        'ops_forcemanaged_destination_file' => Forces Archipelago to manage a files destination when the source matches the destination Schema (e.g S3),
      ];
    */

    // This will simply go to an alternate processing on this same Queue Worker
    // Just for files.
    if (!empty($data->info['filename']) && !empty($data->info['file_column']) && !empty($data->info['processed_row'])) {
      $this->processFile($data);
      return;
    }

    // Before we do any processing. Check if Parent(s) exists?
    // If not, re-enqueue: we try twice only. Should we try more?
    $parent_nodes = [];
    if (isset($data->info['row']['parent']) && is_array($data->info['row']['parent'])) {
      $parents = $data->info['row']['parent'];
      $parents = array_filter($parents);
      foreach($parents as $parent_property => $parent_uuid) {
        $parent_uuids = (array) $parent_uuid;
        $existing = $this->entityTypeManager->getStorage('node')->loadByProperties(['uuid' => $parent_uuids]);
        if (count($existing) != count($parent_uuids)) {
          $this->messenger->addWarning($this->t('Sorry, we can not process ADO with @uuid from Set @setid yet, there are missing parents with UUID(s) @parent_uuids. We will retry.',[
            '@uuid' => $data->info['row']['uuid'],
            '@setid' => $data->info['set_id'],
            '@parent_uuids' => implode(',', $parent_uuids)
          ]));
          // Pushing to the end of the queue.
          $data->info['attempt']++;
          if ($data->info['attempt'] < 3) {
            \Drupal::queue($data->info['queue_name'])
              ->createItem($data);
            return;
          }
          else {
            $this->messenger->addWarning($this->t('Sorry, We tried twice to process ADO with @uuid from Set @setid yet, but you have missing parents. Please check your CSV file and make sure parents with an UUID are in your REPO first and that no other parent generated by the set itself is failing',[
              '@uuid' => $data->info['row']['uuid'],
              '@setid' => $data->info['set_id']
            ]));
            return;
            // We could enqueue in a "failed" queue?
          }
        }
        else {
          // Get the IDs!
          foreach($existing as $node) {
            $parent_nodes[$parent_property][] = (int) $node->id();
          }
        }
      }
    }

    $processed_metadata = NULL;

    $method = $data->mapping->globalmapping ?? "direct";
    if ($method == 'custom') {
      $method = $data->mapping->custommapping_settings->{$data->info['row']['type']}->metadata ?? "direct";
    }
    if ($method == 'template') {
      $processed_metadata = $this->AmiUtilityService->processMetadataDisplay($data);
      if (!$processed_metadata) {
        $this->messenger->addWarning($this->t('Sorry, we can not cast ADO with @uuid into proper Metadata. Check the Metadata Display Template used, your permissions and/or your data ROW in your CSV for set @setid.',[
          '@uuid' => $data->info['row']['uuid'],
          '@setid' => $data->info['set_id']
        ]));
        return;
      }
    }
    if ($method == "direct") {
      if (isset($data->info['row']['data']) && !is_array($data->info['row']['data'])) {
        $this->messenger->addWarning($this->t('Sorry, we can not cast ADO with @uuid directly into proper Metadata. Check your data ROW in your CSV for set @setid for invalid data.',[
          '@uuid' => $data->info['row']['uuid'] ?? "MISSING UUID",
          '@setid' => $data->info['set_id']
        ]));
        return;
      }
      elseif (!isset($data->info['row']['data'])) {
        $this->messenger->addWarning($this->t('Sorry, we can not cast an ADO directly into proper Metadata. Check your data ROW in your CSV for set @setid for invalid data.',
          [
            '@setid' => $data->info['set_id'],
          ]));
        return;
      }

      $processed_metadata = $this->AmiUtilityService->expandJson($data->info['row']['data']);
      $processed_metadata = !empty($processed_metadata) ? json_encode($processed_metadata) : NULL;
      $json_error = json_last_error();
      if ($json_error !== JSON_ERROR_NONE || !$processed_metadata) {
        $this->messenger->addWarning($this->t('Sorry, we can not cast ADO with @uuid directly into proper Metadata. Check your data ROW in your CSV for set @setid for invalid JSON data.',[
          '@uuid' => $data->info['row']['uuid'],
          '@setid' => $data->info['set_id']
        ]));
        return;
      }
    }

    // If at this stage $processed_metadata is empty or Null there was a wrong
    // Manual added wrong mapping or any other User input induced error
    // We do not process further
    // Maybe someone wants to ingest FILES only without any Metadata?
    // Not a good use case so let's stop that non sense here.

    if (empty($processed_metadata)) {
      $message = $this->t('Sorry, ADO with @uuid is empty or has wrong data/metadata. Check your data ROW in your CSV for set @setid or your Set Configuration for manually entered JSON that may break your setup.',[
        '@uuid' => $data->info['row']['uuid'],
        '@setid' => $data->info['set_id']
      ]);
      $this->messenger->addWarning($message);
      $this->loggerFactory->get('ami')->error($message);
      return;
    }

    $cleanvalues = [];
    // Now process Files and Nodes
    $ado_object = $data->adomapping->parents ?? NULL;

    if ($data->mapping->globalmapping == "custom") {
      $file_object = $data->mapping->custommapping_settings->{$data->info['row']['type']}->files ?? NULL;
    }
    else {
      $file_object = $data->mapping->globalmapping_settings->files ?? NULL;
    }

    $file_columns = [];
    $ado_columns = [];
    if ($file_object && is_object($file_object)) {
      $file_columns = array_values(get_object_vars($file_object));
    }

    if ($ado_object && is_object($ado_object)) {
      $ado_columns = array_values(get_object_vars($ado_object));
    }

    // deal with possible overrides from either Direct ingest of
    // A Smart twig template that adds extra mappings
    // This decode will always work because we already decoded and encoded again.
    $processed_metadata = json_decode($processed_metadata, TRUE);
    $processed_metadata['ap:entitymapping'] = isset($processed_metadata['ap:entitymapping']) && is_array($processed_metadata['ap:entitymapping']) ? $processed_metadata['ap:entitymapping'] : [];
    $custom_file_mapping = isset($processed_metadata['ap:entitymapping']['entity:file']) && is_array($processed_metadata['ap:entitymapping']['entity:file']) ? $processed_metadata['ap:entitymapping']['entity:file'] : [];
    $custom_node_mapping = isset($processed_metadata['ap:entitymapping']['entity:node']) && is_array($processed_metadata['ap:entitymapping']['entity:node']) ? $processed_metadata['ap:entitymapping']['entity:node'] : [];

    $entity_mapping_structure['entity:file'] = array_unique(array_merge($custom_file_mapping, $file_columns));
    $entity_mapping_structure['entity:node'] =  array_unique(array_merge($custom_node_mapping, $ado_columns));
    // Unset so we do not lose our merge after '+' both arrays
    unset($processed_metadata['ap:entitymapping']);

    $cleanvalues['ap:entitymapping'] = $entity_mapping_structure;
    $processed_metadata  = $processed_metadata + $cleanvalues;
    // Assign parents as NODE Ids.
    // @TODO if we decide to allow multiple parents this is a place that
    // Needs change.
    foreach ($parent_nodes as $parent_property => $node_ids) {
      $processed_metadata[$parent_property] = $node_ids;
    }
    $processed_files = 0;
    $process_files_via_queue = FALSE;

    // Now do heavy file lifting
    foreach($file_columns as $file_column) {
      // Why 5? one character + one dot + 3 for the extension
      if (isset($data->info['row']['data'][$file_column]) && strlen(trim($data->info['row']['data'][$file_column])) >= 5) {
        $filenames = trim($data->info['row']['data'][$file_column]);
        $filenames = explode(';', $filenames);
        // Clear first. Who knows whats in there. May be a file string that will eventually fail. We should not allow anything coming
        // From the template neither.
        // @TODO ask users.
        $processed_metadata[$file_column] = [];

        // Now the hard part. Do we have too many files?
        $file_limit = $data->info['manyfiles'] ?? 0;
        if (($data->info['force_file_queue'] ?? FALSE) || (($file_limit != 0) && (count($filenames) + $processed_files > $file_limit) && empty($data->info['waiting_for_files']))) {
          // We will add future files to the queue...
          // accumulating all the ones we need
          // and at the end
          // re-enqueue this little one
          $process_files_via_queue = TRUE;
        }
        foreach ($filenames as $filename) {
          $filename = trim($filename);
          if (empty($data->info['waiting_for_files'])) {
            // Always produce now the data structure so we can call processFile either via a queue or
            // via process file.
            $reduced = (count($filenames) + $processed_files >= $file_limit) && ($file_limit != 0);
            $data_file = new \stdClass();
            // Any changed data from a previous run, like SET ID, ZIP FILE, processed ROW
            // Should force us to regenerate the private storage. That is key here.
            // Why? We could have changed our processing strategy, the file column, etc!
            $data_file->info = [
              'zip_file' => $data->info['zip_file'],
              'set_id' => $data->info['set_id'],
              'uid' => $data->info['uid'],
              'processed_row' => $processed_metadata,
              'file_column' => $file_column,
              'filename' => $filename,
              'queue_name' => $data->info['queue_name'],
              'uuid' => $data->info['row']['uuid'],
              'force_file_process' => $data->info['force_file_process'],
              'reduced' => $reduced,
              'ops_forcemanaged_destination_file' => isset($data->info['ops_forcemanaged_destination_file']) ? $data->info['ops_forcemanaged_destination_file'] : TRUE,
            ];
            if ($process_files_via_queue) {
              // Enqueue file separately
              \Drupal::queue($data->info['queue_name'])
                ->createItem($data_file);
            }
            else {
              // Do the file processing syncroneusly
              $this->processFile($data_file);
            }
          }
          // If we were waiting for files OR never asked to process via queue do the same
          if (!empty($data->info['waiting_for_files']) || !$process_files_via_queue) {
            $processed_file_data = $this->store->get('set_' . $data->info['set_id'] . '-' . md5($filename));
            if (!empty($processed_file_data['as_data']) && !empty($processed_file_data['file_id'])) {
              // Last sanity check and make file temporary
              // TODO: remove on SBF 1.0.0 since we are going to also persist files where the source is
              // of streamwrapper temporary:// type
              /* @var \Drupal\file\FileInterface $file */
              $file = $this->entityTypeManager->getStorage('file')->load($processed_file_data['file_id']);
              if ($file) {
                $file->setTemporary();
                $file->save();
                $processed_metadata[$file_column][] = (int) $processed_file_data['file_id'];
                $processed_metadata = array_merge_recursive($processed_metadata,
                  (array) $processed_file_data['as_data']);
              }
              else {
                $this->messenger->addWarning($this->t('Sorry, for ADO with UUID:@uuid, File @filename at column @filecolumn could not be processed. Skipping. Please check your CSV for set @setid.',
                  [
                    '@uuid' => $data->info['row']['uuid'],
                    '@setid' => $data->info['set_id'],
                    '@filename' => $filename,
                    '@filecolumn' => $file_column,
                  ]));
              }
            }
            else {
              // Delete Cache if this if processed_file_data['as_data'] and file_id are empty.
              // Means something failed at \Drupal\ami\Plugin\QueueWorker\IngestADOQueueWorker::processFile
              $this->store->delete('set_' . $data->info['set_id'] . '-' . md5($filename));
              $this->messenger->addWarning($this->t('Sorry, for ADO with UUID:@uuid, File @filename at column @filecolumn was processed but something failed. Skipping. Please check your File Sources and CSV for set @setid.',
                [
                  '@uuid' => $data->info['row']['uuid'],
                  '@setid' => $data->info['set_id'],
                  '@filename' => $filename,
                  '@filecolumn' => $file_column,
                ]));
            }
          }
        }
      }
    }

    if ($process_files_via_queue && empty($data->info['waiting_for_files'])) {
      // If so we need to push this one to the end..
      // Reset the attempts
      $data->info['waiting_for_files'] = TRUE;
      $data->info['attempt'] = $data->info['attempt'] ? $data->info['attempt'] + 1 : 0;
      \Drupal::queue($data->info['queue_name'])
        ->createItem($data);
      return;
    }

    // Decode the JSON that was captured.
    $this->persistEntity($data, $processed_metadata);
  }


  /**
   * Quick helper is Remote or local helper
   *
   * @param $uri
   *
   * @return bool
   */
  private function isRemote($uri) {
    // WE do have a similar code in \Drupal\ami\AmiUtilityService::file_get
    // @TODO refactor to a single method.
    $parsed_url = parse_url($uri);
    $remote_schemes = ['http', 'https', 'feed'];
    $remote = FALSE;
    if (isset($parsed_url['scheme']) && in_array($parsed_url['scheme'], $remote_schemes)) {
      $remote = TRUE;
    }
    return $remote;
  }


  /**
   * Saves an ADO (NODE Entity).
   *
   * @param \stdClass $data
   * @param array $processed_metadata
   *
   * @throws \Drupal\Component\Plugin\Exception\InvalidPluginDefinitionException
   * @throws \Drupal\Component\Plugin\Exception\PluginNotFoundException
   */
  private function persistEntity(\stdClass $data, array $processed_metadata) {

    //OP can be one of
    /*
    'create' => 'Create New ADOs',
    'update' => 'Update existing ADOs',
    'patch' => 'Patch existing ADOs',
    'delete' => 'Delete existing ADOs',
    */
    $op = $data->pluginconfig->op;
    $op_secondary =  $data->info['op_secondary'] ?? NULL;
    /* OP Secondary for Update/Patch operations can be
    'update', 'replace' , 'append'
    */

    $ophuman = [
      'create' => 'created',
      'update' => 'updated',
      'patch' => 'patched',
    ];

    /** @var \Drupal\Core\Entity\ContentEntityInterface[] $existing */
    $existing = $this->entityTypeManager->getStorage('node')->loadByProperties(
      ['uuid' => $data->info['row']['uuid']]
    );

    if (count($existing) && $op == 'create') {
      $this->messenger->addError($this->t('Sorry, you requested an ADO with UUID @uuid to be created via Set @setid. But there is already one in your repo with that UUID. Skipping',[
        '@uuid' => $data->info['row']['uuid'],
        '@setid' => $data->info['set_id']
      ]));
      return;
    }
    elseif (!count($existing) && $op !== 'create') {
      $this->messenger->addError($this->t('Sorry, the ADO with UUID @uuid you requested to be @ophuman via Set @setid does not exist. Skipping',[
        '@uuid' => $data->info['row']['uuid'],
        '@setid' => $data->info['set_id'],
        '@ophuman' => $ophuman[$op],
      ]));
      return;
    }
    $account =  $data->info['uid'] == \Drupal::currentUser()->id() ? \Drupal::currentUser() : $this->entityTypeManager->getStorage('user')->load($data->info['uid']);

    if ($op !== 'create' && $account && $existing && count($existing) == 1) {
      $existing_object = reset($existing);
      if (!$existing_object->access('update', $account)) {
        $this->messenger->addError($this->t('Sorry you have no system permission to @ophuman ADO with UUID @uuid via Set @setid. Skipping',[
          '@uuid' => $data->info['row']['uuid'],
          '@setid' => $data->info['set_id'],
          '@ophuman' => $ophuman[$op],
        ]));
        return;
      }
    }

    if ($data->mapping->globalmapping == "custom") {
      $property_path = $data->mapping->custommapping_settings->{$data->info['row']['type']}->bundle ?? NULL;
    }
    else {
      $property_path = $data->mapping->globalmapping_settings->bundle ?? NULL;
    }

    $label_column = $data->adomapping->base->label ?? 'label';
    // Always (because of processed metadata via template) try to fetch again the mapped version
    $label = $processed_metadata[$label_column] ?? ($processed_metadata['label'] ?? NULL);
    $property_path_split = explode(':', $property_path);

    if (!$property_path_split || count($property_path_split) < 2 ) {
      $this->messenger->addError($this->t('Sorry, your Bundle/Fields set for the requested an ADO with @uuid on Set @setid are wrong. You may have made a larger change in your repo and deleted a Content Type. Aborting.',[
        '@uuid' => $data->info['row']['uuid'],
        '@setid' => $data->info['set_id']
      ]));
      return;
    }

    $bundle = $property_path_split[0];
    $field_name = $property_path_split[1];
    // @TODO make this configurable.
    // This would allows us to pass an offset if the SBF is multivalued.
    // WE do not do this, Why would you want that? Who knows but possible.
    // @see also \Drupal\ami\AmiUtilityService::processMetadataDisplay
    $field_name_offset = $property_path_split[2] ?? 0;
    // Fall back to not published in case no status was passed.
    $status = $data->info['status'][$bundle] ?? 0;
    // default Sortfile which will respect the ingest order. If there was already one set, preserve.
    $sort_files = isset($processed_metadata['ap:tasks']) && isset($processed_metadata['ap:tasks']['ap:sortfiles']) ?  $processed_metadata['ap:tasks']['ap:sortfiles'] : 'index';
    if (isset($processed_metadata['ap:tasks']) && is_array($processed_metadata['ap:tasks'])) {
      $processed_metadata['ap:tasks']['ap:sortfiles'] = $sort_files;
    }
    else {
      $processed_metadata['ap:tasks'] = [];
      $processed_metadata['ap:tasks']['ap:sortfiles'] = $sort_files;
    }

    // JSON_ENCODE AGAIN!
    $jsonstring = json_encode($processed_metadata, JSON_PRETTY_PRINT, 50);

    if ($jsonstring) {
      $nodeValues = [
        'uuid' =>  $data->info['row']['uuid'],
        'type' => $bundle,
        'title' => $label,
        'uid' =>  $data->info['uid'],
        $field_name => $jsonstring
      ];

      /** @var \Drupal\Core\Entity\EntityPublishedInterface $node */
      try {
        if ($op ==='create') {
          if ($status && is_string($status)) {
            // String here means we got moderation_status;
            $nodeValues['moderation_state'] = $status;
            $status = 0; // Let the Moderation Module set the right value
          }
          $node = $this->entityTypeManager->getStorage('node')
            ->create($nodeValues);
        }
        else {
          $vid = $this->entityTypeManager
            ->getStorage('node')
            ->getLatestRevisionId($existing_object->id());

          $node = $vid ? $this->entityTypeManager->getStorage('node')
            ->loadRevision($vid) : $existing[0];

          /** @var \Drupal\Core\Field\FieldItemInterface $field*/
          $field = $node->get($field_name);
          if ($status && is_string($status)) {
            $node->set('moderation_state', $status);
            $status = 0;
          }
          /** @var \Drupal\strawberryfield\Field\StrawberryFieldItemList $field */
          if (!$field->isEmpty()) {
            /** @var $field \Drupal\Core\Field\FieldItemList */
            foreach ($field->getIterator() as $delta => $itemfield) {
              /** @var \Drupal\strawberryfield\Plugin\Field\FieldType\StrawberryFieldItem $itemfield */
              if ($field_name_offset == $delta) {
                $original_value = $itemfield->provideDecoded(TRUE);
                // Now calculate what we need to do here regarding files/node mappings
                $original_file_mappings = $original_value['ap:entitymapping']['entity:file'] ?? [];
                $original_node_mappings = $original_value['ap:entitymapping']['entity:node'] ?? [];

                // This will preserve existing File Keys/values/mappings
                foreach ($original_file_mappings as $filekey) {
                  if (!in_array($filekey, $processed_metadata['ap:entitymapping']['entity:file'])) {
                    $processed_metadata[$filekey] = $original_value[$filekey] ?? [];
                    $processed_metadata['ap:entitymapping']['entity:file'][] = $filekey;
                  }
                  else {
                    // Means new processing is adding these and they are already in the mapping. Is safe Files enabled?
                    if ($data->info['ops_safefiles']) {
                      // We take the old file ids and merge the new ones. Nothing gets lost ever.
                      // If there were no new ones, or new ones were URLs and failed processing
                      // The $processed_metadata[$filekey] might be a string or empty!
                      $processed_metadata[$filekey] = array_merge((array) $original_value[$filekey] ?? [],  is_array($processed_metadata[$filekey]) ? $processed_metadata[$filekey] : []);
                    }
                  }
                }
                // This will preserve existing Node Keys/values/mappings
                foreach ($original_node_mappings as $nodekey) {
                  if (!in_array($nodekey, $processed_metadata['ap:entitymapping']['entity:node'])) {
                    $processed_metadata[$nodekey] = $original_value[$nodekey] ?? [];
                    $processed_metadata['ap:entitymapping']['entity:node'][] = $nodekey;
                  }
                  // No old Node-to-Node relationships is preserved if the processed data contains an already mapped key
                }
                // Really not needed?
                $processed_metadata['ap:entitymapping']['entity:node'] = array_unique($processed_metadata['ap:entitymapping']['entity:node'] ?? []);
                $processed_metadata['ap:entitymapping']['entity:file'] = array_unique($processed_metadata['ap:entitymapping']['entity:file'] ?? []);

                // Copy directly all as:mediatype into the child, the File Persistent Event will clean this up if redundant.
                // THIS MEANS WE DO NOT ALLOW AS:FILETYPES OVERRIDES!!! (SO HAPPY)
                // @TODO: DOCUMENT THIS!
                foreach(StrawberryfieldJsonHelper::AS_FILE_TYPE as $as_file_type) {
                  if (isset($original_value[$as_file_type])) {
                    $processed_metadata[$as_file_type] = $original_value[$as_file_type];
                  }
                }
                // Now deal with Update modes.
                if ($op_secondary == 'replace') {
                  $processed_metadata_keys = array_keys($processed_metadata);
                  // We want to avoid replacing File keys.if
                  if ($data->info['ops_safefiles']) {
                    // This will exclude all keys that contained files initially.
                    // Making touching/deleting files basically impossible.
                    $processed_metadata_keys = array_diff($processed_metadata_keys, $original_file_mappings);
                  }

                  foreach ($processed_metadata_keys as $processed_metadata_key) {
                    $original_value[$processed_metadata_key] = $processed_metadata[$processed_metadata_key];
                  }
                  $processed_metadata = $original_value;
                }


                if (isset($data->info['log_jsonpatch']) && $data->info['log_jsonpatch']) {
                  $this->patchJson($original_value ?? [], $processed_metadata ?? [], true);
                }

                if ($op_secondary == 'append') {
                  $processed_metadata_keys = array_keys($processed_metadata);
                  // We will exclude a bunch of keys from appending since
                  // we already did a smart-ish processing moving them to
                  // processed_metadata
                  /* @TODO remove this once PHP 7 is deprecated
                  // Symfony\Polyfill\Php80 alread implements somthing similar.
                   */
                  if (!function_exists('str_starts_with')) {
                    function str_starts_with($haystack, $needle) {
                      return (string)$needle !== '' && strncmp($haystack, $needle, strlen($needle)) === 0;
                    }
                  }
                  $contract_keys = array_filter($processed_metadata_keys, function ($value) {
                    return (str_starts_with($value, "as:") || str_starts_with($value, "ap:"));
                    }
                  );
                  $processed_metadata_keys = array_diff($processed_metadata_keys, $contract_keys);
                  foreach ($processed_metadata_keys as $processed_metadata_key) {
                    if (isset($original_value[$processed_metadata_key])) {
                      // Initialize this as if we were dealing with string/numbers and not arrays
                      // We check later if any/old/new are indeed arrays or not
                      $new = [$processed_metadata[$processed_metadata_key]];
                      $old = [$original_value[$processed_metadata_key]];
                      if (is_array($original_value[$processed_metadata_key])) {
                        // If we are dealing with an associative array we treat is an an object that might be
                        // 1 of many
                        if (StrawberryfieldJsonHelper::arrayIsMultiSimple($original_value[$processed_metadata_key])) {
                          $old[] = $original_value[$processed_metadata_key];
                        }
                        else {
                          $old = $original_value[$processed_metadata_key];
                        }
                      }
                      if (is_array($processed_metadata[$processed_metadata_key])) {
                        // If we are dealing with an associative array we treat is an an object that might be
                        // 1 of many
                        if (StrawberryfieldJsonHelper::arrayIsMultiSimple($processed_metadata[$processed_metadata_key])) {
                          $new[] = $processed_metadata[$processed_metadata_key];
                        }
                        else {
                          $new = $processed_metadata[$processed_metadata_key];
                        }
                      }
                      // This should help avoiding duplicates.
                      $original_value[$processed_metadata_key] = $old + $new;
                    }
                    else {
                      $original_value[$processed_metadata_key] = $processed_metadata[$processed_metadata_key];
                    }
                  }
                  // Copy over our contract keys.
                  foreach ($contract_keys as $contract_key) {
                    $original_value[$contract_key] = $processed_metadata[$contract_key];
                    // Keep memory footprint lower?
                    unset($processed_metadata[$contract_key]);
                  }
                  $processed_metadata = $original_value;
                }
                // @TODO. Log this?
                // $this->patchJson($original_value, $processed_metadata);

                $itemfield->setMainValueFromArray($processed_metadata);
                break;
              }
            }
          }
          else {
            // if the Saved one is empty use the new always.
            // Applies to Patch/Update.
            $field->setValue($jsonstring);
          }
          if ($node->getEntityType()->isRevisionable()) {
            // Forces a New Revision for Not-create Operations.
            $node->setNewRevision(TRUE);
            $node->setRevisionCreationTime(\Drupal::time()->getRequestTime());
            // Set data for the revision
            $node->setRevisionLogMessage('ADO modified via AMI Set ' . $data->info['set_id']);
            $node->setRevisionUserId($data->info['uid']);
          }
        }
        // In case $status was not moderated.
        if ($status) {
          $node->setPublished();
        }
        elseif (!isset($nodeValues['moderation_state'])) {
          // Only unpublish if not moderated.
          $node->setUnpublished();
        }
        $node->save();

        $link = $node->toUrl()->toString();
        $this->messenger->addStatus($this->t('ADO <a href=":link" target="_blank">%title</a> with UUID:@uuid on Set @setid was @ophuman!',[
          '@uuid' => $data->info['row']['uuid'],
          '@setid' => $data->info['set_id'],
          ':link' => $link,
          '%title' => $label,
          '@ophuman' => $ophuman[$op]
        ]));
      }
      catch (\Exception $exception) {
        $this->messenger->addError($this->t('Sorry we did all right but failed @ophuman the ADO with UUID @uuid on Set @setid. Something went wrong. Please check your Drupal Logs and notify your admin.',[
          '@uuid' => $data->info['row']['uuid'],
          '@setid' => $data->info['set_id'],
          '@ophuman' => $ophuman[$op],
        ]));
        return;
      }
    }
    else {
      $this->messenger->addError($this->t('Sorry we did all right but JSON resulting at the end is flawed and we could not @ophuman the ADO with UUID @uuid on Set @setid. This is quite strange. Please check your Drupal Logs and notify your admin.',[
        '@uuid' => $data->info['row']['uuid'],
        '@setid' => $data->info['set_id'],
        '@ophuman' => $ophuman[$op],
      ]));
      return;
    }
  }

  /**
   * Will return a Patched array using on original/new arrays.
   *
   * @param array $original
   * @param array $new
   *
   * @param bool $reverse
   *     If true we will generate an UNDO patch
   *
   * @return false|string
   */
  protected function patchJson(array $original, array $new, $reverse = FALSE) {
    // IMPORTANT:
    // We need to Object-it-fy here to make sure that
    // the patch generated is Object properties aware
    // Instead of an associative Array of arrays (which fails)
    $original = json_decode(json_encode($original));
    $new = json_decode(json_encode($new));
    //@TODO bring this back when we add extra patch update flags
    // JsonDiff can only work with Arrays that do not contain Objects as values.
    // @throws \Swaggest\JsonDiff\Exception
    try {
      if ($reverse) {
        $r = new JsonDiff(
          $original,
          $new,
          JsonDiff::SKIP_JSON_MERGE_PATCH + JsonDiff::COLLECT_MODIFIED_DIFF
        );
      }
      else {
        $r = new JsonDiff(
          $new,
          $original,
          JsonDiff::SKIP_JSON_MERGE_PATCH + JsonDiff::COLLECT_MODIFIED_DIFF
        );
      }
      $patch = $r->getPatch()->jsonSerialize();
    }
    catch (\Swaggest\JsonDiff\Exception $exception) {
      // We do not want to make ingesting slower. Just return [];
      return [];
    }
    return json_encode($patch ?? []);
  }

  /**
   * Processes a File and technical metadata to avoid congestion.
   *
   * @param mixed $data
   */
  protected function processFile($data) {

    // First check if we already have the info here, if so do nothing.
    $current_data_hash = md5(serialize($data));


    if (($data->info['force_file_process'] ?? FALSE) || empty($this->store->get('set_' . $data->info['set_id'] . '-' . md5($data->info['filename'])))) {
      $file = $this->AmiUtilityService->file_get(trim($data->info['filename']),
        $data->info['zip_file']);
      if ($file) {
        $force_destination = isset($data->info['ops_forcemanaged_destination_file']) ? (bool) $data->info['ops_forcemanaged_destination_file'] : TRUE;
        $reduced = $data->info['reduced'] ?? FALSE;
        $processedAsValuesForKey = $this->strawberryfilepersister
          ->generateAsFileStructure(
            [$file->id()],
            $data->info['file_column'],
            $data->info['processed_row'],
            $force_destination,
            $reduced
          );
        $data_to_store['as_data'] = $processedAsValuesForKey;
        $data_to_store['file_id'] = $file->id();
        $this->store->set('set_' . $data->info['set_id'] . '-' . md5($data->info['filename']),
          $data_to_store);
      }
      else {
        $this->messenger->addWarning($this->t('Sorry, we really tried to process File @filename from Set @setid yet. Giving up',
          [
            '@setid' => $data->info['set_id'],
            '@filename' => $data->info['filename']
          ]));
      }
    }
  }
}

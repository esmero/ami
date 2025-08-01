<?php

namespace Drupal\ami\Plugin\QueueWorker;

use Drupal\ami\AmiLoDService;
use Drupal\ami\AmiUtilityService;
use Drupal\ami\Entity\amiSetEntity;
use Drupal\Core\Datetime\DrupalDateTime;
use Drupal\file\FileInterface;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Logger\LoggerChannelFactoryInterface;
use Drupal\Core\Messenger\MessengerInterface;
use Drupal\Core\Plugin\ContainerFactoryPluginInterface;
use Drupal\Core\Queue\QueueWorkerBase;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\Core\StringTranslation\TranslatableMarkup;
use Drupal\strawberryfield\Event\StrawberryfieldFileEvent;
use Drupal\strawberryfield\StrawberryfieldEventType;
use Drupal\strawberryfield\StrawberryfieldFilePersisterService;
use Drupal\strawberryfield\StrawberryfieldUtilityService;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Monolog\Formatter\JsonFormatter;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Swaggest\JsonDiff\JsonDiff;
use Drupal\strawberryfield\Tools\StrawberryfieldJsonHelper;
use \Drupal\Core\TempStore\PrivateTempStoreFactory;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;
use Drupal\views_bulk_operations\Service\ViewsBulkOperationsActionManager;
use Drupal\views_bulk_operations\Service\ViewsBulkOperationsActionProcessorInterface;
use Drupal\Core\Session\AccountSwitcherInterface;
use Drupal\Core\Access\AccessResultReasonInterface;
use Drupal\Core\Action\ActionInterface;
use Drupal\Core\Entity\EntityStorageException;

/**
 * Processes, Ingests and Run Actions on each AMI Set CSV row.
 *
 * @QueueWorker(
 *   id = "ami_ingest_ado",
 *   title = @Translation("AMI Digital Object Ingester & Action Queue Worker")
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
   * @var \Drupal\Core\TempStore\PrivateTempStore
   */
  protected $store;

  /**
   * Private Store used to keep the Set Processing Status/count
   *
   * @var \Drupal\Core\TempStore\PrivateTempStore
   */
  protected $statusStore;


  /**
   * Private Store used to keep the Set Processing Status/count
   *
   * @var \Drupal\Core\TempStore\PrivateTempStore
   */
  protected $contextStore;

  /**
   * The AMI specific logger
   *
   * @var \Psr\Log\LoggerInterface
   */
  protected $logger;

  /**
   * Human language past tense for operations.
   *
   * @var array
   */
  protected CONST OP_HUMAN = [
    'create' => 'created',
    'update' => 'updated',
    'patch' => 'patched',
    'sync' => 'synced',
  ];

  /**
   * The event dispatcher.
   *
   * @var EventDispatcherInterface
   */
  protected EventDispatcherInterface $eventDispatcher;

  /**
   * @var ViewsBulkOperationsActionManager
   */
  protected ViewsBulkOperationsActionManager $actionManager;

  /**
   * @var ViewsBulkOperationsActionProcessorInterface
   */
  protected ViewsBulkOperationsActionProcessorInterface $actionProcessor;

  /**
   * @var \Drupal\Core\TempStore\PrivateTempStoreFactory
   */
  protected PrivateTempStoreFactory $privateStoreFactory;

  /**
   * The current_user service used by this plugin.
   *
   * @var \Drupal\Core\Session\AccountSwitcherInterface|null
   */
  protected $accountSwitcher;

  /**
   * Constructor.
   *
   * @param array $configuration
   * @param string $plugin_id
   * @param mixed $plugin_definition
   * @param EntityTypeManagerInterface $entity_type_manager
   * @param LoggerChannelFactoryInterface $logger_factory
   * @param StrawberryfieldUtilityService $strawberryfield_utility_service
   * @param AmiUtilityService $ami_utility
   * @param AmiLoDService $ami_lod
   * @param MessengerInterface $messenger
   * @param StrawberryfieldFilePersisterService $strawberry_filepersister
   * @param PrivateTempStoreFactory $temp_store_factory
   * @param EventDispatcherInterface $event_dispatcher
   * @param \Drupal\ami\Plugin\QueueWorker\ViewsBulkOperationsActionManager $actionManager
   * @param \Drupal\ami\Plugin\QueueWorker\ViewsBulkOperationsActionProcessorInterface $actionProcessor
   * @param \Drupal\ami\Plugin\QueueWorker\AccountSwitcherInterface $accountSwitcher
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
    PrivateTempStoreFactory $temp_store_factory,
    EventDispatcherInterface $event_dispatcher,
    ViewsBulkOperationsActionManager $actionManager,
    ViewsBulkOperationsActionProcessorInterface $actionProcessor,
    AccountSwitcherInterface $accountSwitcher,
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
    $this->statusStore = $temp_store_factory->get('ami_queue_status');
    $this->contextStore = $temp_store_factory->get('ami_action_context');
    $this->eventDispatcher = $event_dispatcher;
    $this->actionManager = $actionManager;
    $this->actionProcessor = $actionProcessor;
    $this->accountSwitcher = $accountSwitcher;
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
  public static function create(ContainerInterface $container, array $configuration, $plugin_id, $plugin_definition) {
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
      $container->get('tempstore.private'),
      $container->get('event_dispatcher'),
      $container->get('plugin.manager.views_bulk_operations_action'),
      $container->get('views_bulk_operations.processor'),
      $container->get('account_switcher')
    );
  }

  /**
   * {@inheritdoc}
   */
  public function processItem($data)
  {
    $log = new Logger('ami_file');
    $private_path = \Drupal::service('stream_wrapper_manager')->getViaUri('private://')->getDirectoryPath();
    $handler = new StreamHandler($private_path . '/ami/logs/set' . $data->info['set_id'] . '.log', Logger::DEBUG);
    $handler->setFormatter(new JsonFormatter());
    $log->pushHandler($handler);
    // This will add the File logger not replace the DB
    // We can not use addLogger because in a single PHP process multiple Queue items might be invoked
    // And loggers are additive. Means i can end with a few duplicated entries!
    // @TODO: i should inject this into the Containers but i wanted to keep
    // it simple for now.
    $this->loggerFactory->get('ami_file')->setLoggers([[$log]]);

    /* $data will  contain a pluginconfig Object with at least
        $data->pluginconfig->op;
        // op --> action is handled differently
    */
    /* Data info for an ADO has this structure
      $data->info = [
        'row' => The actual data
        'set_id' => The Set id
        'uid' => The User ID that processed the Set
        'set_url' => A direct URL to the set.
        'status' => Either a string (moderation state) or a 1/0 for published/unpublished if not moderated
        'status_keep' => If the current status should be kept or not. Only applies to existing ADOs via either Update or Sync Ops.
        'op_secondary' => applies only to Update/Patch operations. Can be one of 'update','replace','append'
        'ops_safefiles' => Boolean, True if we will not allow files/mappings to be removed/we will keep them warm and safe
        'log_jsonpatch' => If for Update operations we will generate a single PER ADO Log with a full JSON Patch,
        'attempt' => The number of attempts to process. We always start with a 1
        'zip_file' => Zip File/File Entity
        'waiting_for_files' => will only exist and TRUE if we re-enqueued this ADO after figuring out we had too many Files.
        'queue_name' => because well ... we use Hydroponics too
        'force_file_queue' => defaults to false, will always treat files as separate queue items.
        'force_file_process' => defaults to false, will force all techmd and file fetching to happen from scratch instead of using cached versions.
        'manyfiles' => Number of files (passed by \Drupal\ami\Form\amiSetEntityProcessForm::submitForm) that will trigger queue processing for files,
        'ops_skip_onmissing_file' => Skips ADO operations if a passed/mapped file is not present,
        'ops_forcemanaged_destination_file' => Forces Archipelago to manage a files destination when the source matches the destination Schema (e.g S3),
        'time_submitted' => Timestamp on when the queue was send. All Entries will share the same
      ];
    */
    /* Data info for a File has this structure
      $data->info = [
        'set_id' => The Set id
        'uid' => The User ID that processed the Set
        'uuid' => The uuid of the ADO that needs this file
        'attempt' => The number of attempts to process. We always start with a 1
        'filename' => The File name
        'file_column' => The File column where the file needs to be saved.
        'zip_file' => Zip File/File Entity,
        'processed_row' => Full metadata of the ADO holding the file processed and ready as an array
        'queue_name' => because well ... we use Hydroponics too
        'force_file_process' => defaults to false, will force all techmd and file fetching to happen from scratch instead of using cached versions.
        'reduced' => If reduced EXIF or not should be generated,
        'ops_forcemanaged_destination_file' => Forces Archipelago to manage a files destination when the source matches the destination Schema (e.g S3),
        'time_submitted' => Timestamp on when the queue was send. All Entries will share the same
      ];
    */

    // Actions will go their own way into the processAction() method.
    if ($data->pluginconfig->op === "action") {
      $message = $this->t('Attempting to process SET @setid with action @action for ADO UUIDs @uuids.',
        [
          '@setid' => $data->info['set_id'],
          '@action' => $data->info['action'],
          '@uuids' => implode(",", $data->info['uuids'] ?? []),
        ]);
      $this->loggerFactory->get('ami_file')->info($message, [
        'setid' => $data->info['set_id'] ?? NULL,
        'time_submitted' => $data->info['time_submitted'] ?? '',
      ]);

      if ($this->processAction($data) === FALSE) {
        $message = $this->t('Action Processing SET @setid with action @action for ADO UUIDs @uuids failed',
          [
            '@setid' => $data->info['set_id'],
            '@action' => $data->info['action'],
            '@uuids' => implode(",", $data->info['uuids'] ?? []),
          ]);
        $this->loggerFactory->get('ami_file')->warning($message, [
          'setid' => $data->info['set_id'] ?? NULL,
          'time_submitted' => $data->info['time_submitted'] ?? '',
        ]);
      }
      return;
    }


    // This will simply go to an alternate processing on this same Queue Worker
    // Just for files.
    if (!empty($data->info['filename']) && !empty($data->info['file_column']) && !empty($data->info['processed_row'])) {
      $this->processFile($data);
      return;
    }
    // CSV (nested ones) can not be processed as "pre-files", but still need to be processed as files.
    // There might be an edge case where the user decides that the CSV that generated the children
    // Should also be attached to the parent ADO.Still, we need to be sure the ADO itself was persisted
    // (either here or already existed!) before treating the CSV as a source for children objects.

    $file_csv_columns = [];
    if ($data->mapping->globalmapping == "custom") {
      $csv_file_object = $data->mapping->custommapping_settings->{$data->info['row']['type']}->files_csv ?? NULL;
    } else {
      $csv_file_object = $data->mapping->globalmapping_settings->files_csv ?? NULL;
    }

    if ($csv_file_object && is_object($csv_file_object)) {
      $file_csv_columns = array_values(get_object_vars($csv_file_object));
    }

    $persisted = FALSE;
    $should_process = TRUE;
    if ($this->canProcess($data) === FALSE) {
      return;
    } elseif ($this->canProcess($data) === NULL) {
      if (!empty($file_csv_columns)) {
        $should_process = FALSE;
      } else {
        return;
      }
    }
    if ($should_process) {
    // Before we do any processing. Check if Parent(s) exists?
    // If not, re-enqueue: we try twice only. Should we try more?
    $parent_nodes = [];
    if (isset($data->info['row']['parent']) && is_array($data->info['row']['parent'])) {
      $parents = $data->info['row']['parent'];
      $parents = array_filter($parents);
      foreach ($parents as $parent_property => $parent_uuid) {
        $parent_uuids = (array)$parent_uuid;
        // We should validate each member to be an UUID here (again). Just in case.
        $existing = $this->entityTypeManager->getStorage('node')->loadByProperties(['uuid' => $parent_uuids]);
        if (count($existing) != count($parent_uuids)) {
          $message = $this->t('Sorry, we can not process ADO with @uuid from Set @setid yet, there are missing parents with UUID(s) @parent_uuids. We will retry.', [
            '@uuid' => $data->info['row']['uuid'],
            '@setid' => $data->info['set_id'],
            '@parent_uuids' => implode(',', $parent_uuids)
          ]);
          $this->loggerFactory->get('ami_file')->warning($message, [
            'setid' => $data->info['set_id'] ?? NULL,
            'time_submitted' => $data->info['time_submitted'] ?? '',
          ]);

          // Pushing to the end of the queue.
          $data->info['attempt']++;
          if ($data->info['attempt'] < 3) {
            \Drupal::queue($data->info['queue_name'])
              ->createItem($data);
            return;
          } else {
            $message = $this->t('Sorry, We tried twice to process ADO with @uuid from Set @setid yet, but you have missing parents. Please check your CSV file and make sure parents with an UUID are in your REPO first and that no other parent generated by the set itself is failing', [
              '@uuid' => $data->info['row']['uuid'],
              '@setid' => $data->info['set_id']
            ]);
            $this->loggerFactory->get('ami_file')->error($message, [
              'setid' => $data->info['set_id'] ?? NULL,
              'time_submitted' => $data->info['time_submitted'] ?? '',
            ]);
            $this->setStatus(amiSetEntity::STATUS_PROCESSING_WITH_ERRORS, $data);
            return;
            // We could enqueue in a "failed" queue?
            // @TODO for 0.6.0: Or better. We could keep track of the dependency
            // and then afterwards update one and then the other
            // Why? Because if we have one object pointing to X
            // and the other pointing back the graph is not acyclic
            // but we could still via an update operation
            // Ingest without the relations both. Then update both once the
            // Ingest is ready IF both have IDs.
          }
        } else {
          // Get the IDs!
          foreach ($existing as $node) {
            $parent_nodes[$parent_property][] = (int)$node->id();
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
        $message = $this->t('Sorry, we can not cast ADO with @uuid into proper Metadata. Check the Metadata Display Template used, your permissions and/or your data ROW in your CSV for set @setid.', [
          '@uuid' => $data->info['row']['uuid'],
          '@setid' => $data->info['set_id']
        ]);
        $this->loggerFactory->get('ami_file')->error($message, [
          'setid' => $data->info['set_id'] ?? NULL,
          'time_submitted' => $data->info['time_submitted'] ?? '',
        ]);
        $this->setStatus(amiSetEntity::STATUS_PROCESSING_WITH_ERRORS, $data);
        return;
      }
    }
    if ($method == "direct") {
      if (isset($data->info['row']['data']) && !is_array($data->info['row']['data'])) {
        $message = $this->t('Sorry, we can not cast ADO with @uuid directly into proper Metadata. Check your data ROW in your CSV for set @setid for invalid data.', [
          '@uuid' => $data->info['row']['uuid'] ?? "MISSING UUID",
          '@setid' => $data->info['set_id']
        ]);

        $this->loggerFactory->get('ami_file')->error($message, [
          'setid' => $data->info['set_id'] ?? NULL,
          'time_submitted' => $data->info['time_submitted'] ?? '',
        ]);
        $this->setStatus(amiSetEntity::STATUS_PROCESSING_WITH_ERRORS, $data);
        return;
      } elseif (!isset($data->info['row']['data'])) {
        $message = $this->t('Sorry, we can not cast an ADO directly into proper Metadata. Check your data ROW in your CSV for set @setid for invalid data.',
          [
            '@setid' => $data->info['set_id'],
          ]);
        $this->loggerFactory->get('ami_file')->error($message, [
          'setid' => $data->info['set_id'] ?? NULL,
          'time_submitted' => $data->info['time_submitted'] ?? '',
        ]);
        $this->setStatus(amiSetEntity::STATUS_PROCESSING_WITH_ERRORS, $data);
        return;
      }

      $processed_metadata = $this->AmiUtilityService->expandJson($data->info['row']['data']);
      $processed_metadata = !empty($processed_metadata) ? json_encode($processed_metadata) : NULL;
      $json_error = json_last_error();
      if ($json_error !== JSON_ERROR_NONE || !$processed_metadata) {
        $message = $this->t('Sorry, we can not cast ADO with @uuid directly into proper Metadata. Check your data ROW in your CSV for set @setid for invalid JSON data.', [
          '@uuid' => $data->info['row']['uuid'],
          '@setid' => $data->info['set_id']
        ]);
        $this->loggerFactory->get('ami_file')->error($message, [
          'setid' => $data->info['set_id'] ?? NULL,
          'time_submitted' => $data->info['time_submitted'] ?? '',
        ]);
        $this->setStatus(amiSetEntity::STATUS_PROCESSING_WITH_ERRORS, $data);
        return;
      }
    }

    // If at this stage $processed_metadata is empty or Null there was a wrong
    // Manual added wrong mapping or any other User input induced error
    // We do not process further
    // Maybe someone wants to ingest FILES only without any Metadata?
    // Not a good use case so let's stop that nonsense here.

    if (empty($processed_metadata)) {
      $message = $this->t('Sorry, ADO with @uuid is empty or has wrong data/metadata. Check your data ROW in your CSV for set @setid or your Set Configuration for manually entered JSON that may break your setup.', [
        '@uuid' => $data->info['row']['uuid'],
        '@setid' => $data->info['set_id']
      ]);
      $this->loggerFactory->get('ami_file')->error($message, [
        'setid' => $data->info['set_id'] ?? NULL,
        'time_submitted' => $data->info['time_submitted'] ?? '',
      ]);
      $this->setStatus(amiSetEntity::STATUS_PROCESSING_WITH_ERRORS, $data);
      return;
    }

    $cleanvalues = [];
    // Now process Files and Nodes
    $ado_object = $data->adomapping->parents ?? NULL;

    if ($data->mapping->globalmapping == "custom") {
      $file_object = $data->mapping->custommapping_settings->{$data->info['row']['type']}->files ?? NULL;
    } else {
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
    $entity_mapping_structure['entity:node'] = array_unique(array_merge($custom_node_mapping, $ado_columns));
    // Unset so we do not lose our merge after '+' both arrays
    unset($processed_metadata['ap:entitymapping']);

    $cleanvalues['ap:entitymapping'] = $entity_mapping_structure;
    $processed_metadata = $processed_metadata + $cleanvalues;
    // Assign parents as NODE Ids.
    foreach ($parent_nodes as $parent_property => $node_ids) {
      $processed_metadata[$parent_property] = $node_ids;
    }
    $processed_files = 0;
    $process_files_via_queue = FALSE;

    // Now do heavy file lifting
    $allfiles = TRUE;
    foreach ($file_columns as $file_column) {
      // Why 5? one character + one dot + 3 for the extension
      if (isset($data->info['row']['data'][$file_column]) && strlen(trim($data->info['row']['data'][$file_column])) >= 5) {
        $filenames = trim($data->info['row']['data'][$file_column]);
        $filenames = array_map(function ($value) {
          $value = $value ?? '';
          return trim($value);
        }, explode(';', $filenames));

        // Someone can just pass a ; and we end with an empty wich means a while folder, remove the thing!
        $filenames = array_filter($filenames);
        // Clear first. Who knows whats in there. May be a file string that will eventually fail. We should not allow anything coming
        // From the template neither.
        // @TODO ask users. But this also means Templates can not PROVIDE FIXTURES/PREEXISTING FILES
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
              'attempt' => 1,
              'queue_name' => $data->info['queue_name'],
              'uuid' => $data->info['row']['uuid'],
              'force_file_process' => $data->info['force_file_process'],
              'reduced' => $reduced,
              'ops_forcemanaged_destination_file' => isset($data->info['ops_forcemanaged_destination_file']) ? $data->info['ops_forcemanaged_destination_file'] : TRUE,
              'time_submitted' => $data->info['time_submitted'],
            ];
            if ($process_files_via_queue) {
              // Enqueue file separately
              \Drupal::queue($data->info['queue_name'])
                ->createItem($data_file);
            } else {
              // Do the file processing syncroneusly
              $this->processFile($data_file);
            }
          }
          // If we were waiting for files OR never asked to process via queue do the same

          if (!empty($data->info['waiting_for_files']) || !$process_files_via_queue) {
            $zip_file_id = is_object($data->info['zip_file']) && $data->info['zip_file'] instanceof FileInterface ? (string)$data->info['zip_file']->id() : '0';
            // Matches same logic as in \Drupal\ami\Plugin\QueueWorker\IngestADOQueueWorker::processFile
            $private_temp_key = md5($file_column . '-' . $filename . '-' . $zip_file_id);

            $processed_file_data = $this->store->get('set_' . $data->info['set_id'] . '-' . $private_temp_key);
            if (!empty($processed_file_data['as_data']) && !empty($processed_file_data['file_id'])) {
              // Last sanity check and make file temporary
              // TODO: remove on SBF 1.0.0 since we are going to also persist files where the source is
              // of streamwrapper temporary:// type
              /* @var \Drupal\file\FileInterface $file */
              $file = $this->entityTypeManager->getStorage('file')->load($processed_file_data['file_id']);
              if ($file) {
                $file->setTemporary();
                $file->save();
                $processed_metadata[$file_column][] = (int)$processed_file_data['file_id'];
                $processed_metadata = array_merge_recursive($processed_metadata,
                  (array)$processed_file_data['as_data']);
              } else {
                $allfiles = FALSE;
                $message = $this->t('Sorry, for ADO with UUID:@uuid, File @filename at column @filecolumn could not be processed. Skipping. Please check your CSV for set @setid.',
                  [
                    '@uuid' => $data->info['row']['uuid'],
                    '@setid' => $data->info['set_id'],
                    '@filename' => $filename,
                    '@filecolumn' => $file_column,
                  ]);
                $this->loggerFactory->get('ami_file')->warning($message, [
                  'setid' => $data->info['set_id'] ?? NULL,
                  'time_submitted' => $data->info['time_submitted'] ?? '',
                ]);
              }
            } else {
              $allfiles = FALSE;
              // Delete Cache if this is processed_file_data['as_data'] and file_id are empty.
              // Means something failed at \Drupal\ami\Plugin\QueueWorker\IngestADOQueueWorker::processFile
              // NO need to report again since that was already reported.
              $this->store->delete('set_' . $data->info['set_id'] . '-' . $private_temp_key);
            }
          }
        }
      }
    }

    if ($process_files_via_queue && empty($data->info['waiting_for_files'])) {
      // If so we need to push this one to the end.
      // Reset the attempts
      $data->info['waiting_for_files'] = TRUE;
      $data->info['attempt'] = $data->info['attempt'] ? $data->info['attempt'] + 1 : 0;
      \Drupal::queue($data->info['queue_name'])
        ->createItem($data);
      return;
    }
    if (!empty($data->info['ops_skip_onmissing_file'] && $data->info['ops_skip_onmissing_file'] == TRUE && !$allfiles)) {
      $message = $this->t('Skipping ADO with UUID:@uuid because one or more files could not be processed and <em>Skip ADO processing on missing File</em> is enabled',
        [
          '@uuid' => $data->info['row']['uuid'],
          '@setid' => $data->info['set_id'],
        ]);
      $this->loggerFactory->get('ami_file')->warning($message, [
        'setid' => $data->info['set_id'] ?? NULL,
        'time_submitted' => $data->info['time_submitted'] ?? '',
      ]);
      $this->setStatus(amiSetEntity::STATUS_PROCESSING_WITH_ERRORS, $data);
      return;
    }
    // Only persist if we passed this.
    // True if all ok, to the best of our knowledge of course
    $persisted = $this->persistEntity($data, $processed_metadata);
  }
    // We only process a CSV column IF and only if the row that contains it generated an ADO
    // or the ADO was already there.
    if (($persisted || !$should_process ) && !empty($file_csv_columns)) {
      $current_uuid = $data->info['row']['uuid'] ?? NULL;
      $current_row_id = $data->info['row']['row_id'] ?? NULL;
      $data_csv = clone $data;
      unset($data_csv->info['row']);
      foreach ($file_csv_columns as $file_csv_column) {
        if (isset($data->info['row']['data'][$file_csv_column]) && strlen(trim($data->info['row']['data'][$file_csv_column])) >= 5) {
          $filenames = trim($data->info['row']['data'][$file_csv_column]);
          $filenames = array_map(function($value) {
            $value = $value ?? '';
            return trim($value);
          }, explode(';', $filenames));
          $filenames = array_filter($filenames);
          // We will keep the original row ID, so we can log it.
          $data_csv->info['row']['row_id'] = $current_row_id;
          foreach($filenames as $filename) {
            $data_csv->info['csv_filename'] = $filename;
            $csv_file = $this->processCSvFile($data_csv);
            if ($csv_file) {
              $data_csv->info['csv_file'] = $csv_file;
              // Push to the CSV  queue
              \Drupal::queue('ami_csv_ado')
                ->createItem($data_csv);
            }
          }
        }
      }
    }
    return;
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

    if (!$this->canProcess($data)) {
      return FALSE;
    }

    //OP can be one of:
    /*
    'create' => 'Create New ADOs',
    'update' => 'Update existing ADOs',
    'patch' => 'Patch existing ADOs',
    'delete' => 'Delete existing ADOs',
    'sync' => 'Sync' -> which will have op_secondary as the actual OP.
    */
    $op = $op_original = $data->pluginconfig->op;
    $op_secondary =  $data->info['op_secondary'] ?? NULL;
    $was_op_sync = FALSE;
    if ($op === 'sync') {
      if ($op_secondary == "create") {
        $op = "create";
        $op_secondary = NULL;
      }
      if ($op_secondary == "update") {
        $op = "update";
        // $op_secondary stays untouched.
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
    // SOME PEOPLE USING LABEL AS AN ARRAY? GOSH.
    if (is_array($label)) {
      $label = reset($label);
      $label = is_string($label) ?  $label : NULL;
    }
    elseif (is_object($label)) {
      // No guessing. People get your data right. We NULL-i-fy
      $label = NULL;
    }

    $property_path_split = explode(':', $property_path);

    if (!$property_path_split || count($property_path_split) < 2 ) {
      $message = $this->t('Sorry, your Bundle/Fields to Type mapping for the requested an ADO with @uuid on Set @setid are wrong. You might have an unmapped type, or might have made a larger change in your repo and deleted a Content Type. Aborting.',[
        '@uuid' => $data->info['row']['uuid'],
        '@setid' => $data->info['set_id']
      ]);
      $this->loggerFactory->get('ami_file')->error($message ,[
        'setid' => $data->info['set_id'] ?? NULL,
        'time_submitted' => $data->info['time_submitted'] ?? '',
      ]);
      $this->setStatus(amiSetEntity::STATUS_PROCESSING_WITH_ERRORS, $data);
      return FALSE;
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
    // This is tricky. String 1 v/s integer one
    if ($status == "1") {
      $status = 1;
    }
    if ($status == "0") {
      $status = 0;
    }
    $status_keep = $data->info['status_keep'] ?? FALSE;
    // default Sortfile which will respect the ingest order. If there was already one set, preserve.
    $sort_files = isset($processed_metadata['ap:tasks']) && isset($processed_metadata['ap:tasks']['ap:sortfiles']) ?  $processed_metadata['ap:tasks']['ap:sortfiles'] : 'index';
    // We can't blindly override ap:tasks if we are dealing with an update operation. So make an exception here for only create
    // And deal with the same for update but later
    if ($op ==='create') {
      if (isset($processed_metadata['ap:tasks']) && is_array($processed_metadata['ap:tasks'])) {
        $processed_metadata['ap:tasks']['ap:sortfiles'] = $sort_files;
      } else {
        $processed_metadata['ap:tasks'] = [];
        $processed_metadata['ap:tasks']['ap:sortfiles'] = $sort_files;
      }
    }

    // JSON_ENCODE AGAIN!
    $jsonstring = json_encode($processed_metadata, JSON_PRETTY_PRINT, 50);

    if ($jsonstring) {
      // Correct to send Label as NULL here if the user messed up..
      $nodeValues = [
        'uuid' =>  $data->info['row']['uuid'],
        'type' => $bundle,
        'title' => $label,
        'uid' =>  $data->info['uid'],
        $field_name => $jsonstring
      ];

      /** @var \Drupal\Core\Entity\EntityPublishedInterface $node */
      try {
        if ($op === 'create') {
          if ($status && is_string($status)) {
            // String here means we got moderation_status;
            $nodeValues['moderation_state'] = $status;
            $status = 0; // Let the Moderation Module set the right value
          }
          $node = $this->entityTypeManager->getStorage('node')
            ->create($nodeValues);
        }
        else {
          // This is duplication. We already load existing to check
          // If we can proceed in \Drupal\ami\Plugin\QueueWorker\IngestADOQueueWorker::canProcess
          // But it is cleaner to make that function boolean
          // And re useable than return existing from there.
          /** @var \Drupal\Core\Entity\ContentEntityInterface[] $existing */
          $existing = $this->entityTypeManager->getStorage('node')->loadByProperties(
            ['uuid' => $data->info['row']['uuid']]
          );
          // Ups ... should never happen but what if just after checking race/condition it is gone?
          $existing_object = reset($existing);

          $vid = $this->entityTypeManager
            ->getStorage('node')
            ->getLatestRevisionId($existing_object->id());

          $node = $vid ? $this->entityTypeManager->getStorage('node')
            ->loadRevision($vid) : $existing_object;

          /** @var \Drupal\Core\Field\FieldItemInterface $field*/
          $field = $node->get($field_name);
          // Ignore status for updates if status_keep == TRUE.
          if ($status && is_string($status) && $status_keep == FALSE) {
            $node->set('moderation_state', $status);
            $status = 0;
            error_log('moderated');
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
                    // Means new processing is adding these, and they are already in the mapping. Is safe Files enabled?
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
                  // New in 0.9.0. Also protect "label" and "type" those are always strings.
                  // We should never add to them.
                  $contract_keys = array_filter($processed_metadata_keys, function ($value) {
                    return (
                      str_starts_with($value, "as:") ||
                      str_starts_with($value, "ap:") ||
                      $value == "label" ||
                      $value == "type"
                    );
                  }
                  );
                  $processed_metadata_keys = array_diff($processed_metadata_keys, $contract_keys);
                  function _update_append_array_unique_multidimensional($input) {
                    $serialized = array_map('serialize', $input);
                    $unique = array_unique($serialized);
                    return array_intersect_key($input, $unique);
                  }

                  foreach ($processed_metadata_keys as $processed_metadata_key) {
                    if (isset($original_value[$processed_metadata_key])) {
                      // Only make array out of non arrays.
                      if (!is_array($processed_metadata[$processed_metadata_key])) {
                        $new = [$processed_metadata[$processed_metadata_key]];
                      }
                      else {
                        $new = $processed_metadata[$processed_metadata_key];
                      }
                      if (!is_array($original_value[$processed_metadata_key])) {
                        $old = [$original_value[$processed_metadata_key]];
                      }
                      else {
                        $old = $original_value[$processed_metadata_key];
                      }
                      if (!StrawberryfieldJsonHelper::arrayIsMultiSimple($old) && !StrawberryfieldJsonHelper::arrayIsMultiSimple($new)) {
                        // Both are numeric indexed arrays.
                        $original_value[$processed_metadata_key] = [...$old,...$new];
                        $original_value[$processed_metadata_key] = _update_append_array_unique_multidimensional($original_value[$processed_metadata_key]);
                      }
                      else if(StrawberryfieldJsonHelper::arrayIsMultiSimple($old) && StrawberryfieldJsonHelper::arrayIsMultiSimple($new)) {
                        $original_value[$processed_metadata_key] = $old + $new;
                      }
                      else if(StrawberryfieldJsonHelper::arrayIsMultiSimple($old) && !StrawberryfieldJsonHelper::arrayIsMultiSimple($new)) {
                        $original_value[$processed_metadata_key] = [...[$old],...$new];
                        $original_value[$processed_metadata_key] = _update_append_array_unique_multidimensional($original_value[$processed_metadata_key]);
                      }
                      else if(!StrawberryfieldJsonHelper::arrayIsMultiSimple($old) && StrawberryfieldJsonHelper::arrayIsMultiSimple($new)) {
                        $original_value[$processed_metadata_key] = [...$old,...[$new]];
                        $original_value[$processed_metadata_key] = _update_append_array_unique_multidimensional($original_value[$processed_metadata_key]);
                      }
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

                // Now deal again with ap:tasks only if the replace/append operation stripped the basics out

                if (isset($processed_metadata['ap:tasks']) && is_array($processed_metadata['ap:tasks'])) {
                  // basically reuse what is there (which at this stage will be a mix of original data and new or default to the $sort file defined before
                  $processed_metadata['ap:tasks']['ap:sortfiles'] = $processed_metadata['ap:tasks']['ap:sortfiles'] ?? $sort_files;
                } else {
                  // Only set if after all the options it was removed at all.
                  $processed_metadata['ap:tasks'] = [];
                  $processed_metadata['ap:tasks']['ap:sortfiles'] = $sort_files;
                }


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
          // Publish status keep is FALSE
          if ($op == 'update' && $status_keep == FALSE) {
            $node->setPublished();
            error_log('publishing');
          }
        }
        elseif (!isset($nodeValues['moderation_state'])) {
          // Only unpublish if not moderated and status keep is FALSE.
          if ($op == 'update' && $status_keep == FALSE) {
            $node->setUnpublished();
            error_log('unpublishing');
          }
        }
        $node->save();

        $link = $node->toUrl()->toString();
        $message = $this->t('ADO <a href=":link" target="_blank">%title</a> with UUID:@uuid on Set @setid was @ophuman!',[
          '@uuid' => $data->info['row']['uuid'],
          '@setid' => $data->info['set_id'],
          ':link' => $link,
          '%title' => $label ?? 'UNAMED ADO',
          '@ophuman' => $op == $op_original ? static::OP_HUMAN[$op] : static::OP_HUMAN[$op] . ' and ' . static::OP_HUMAN[$op_original]
        ]);
        $this->loggerFactory->get('ami_file')->info($message ,[
          'setid' => $data->info['set_id'] ?? NULL,
          'time_submitted' => $data->info['time_submitted'] ?? '',
        ]);
        $this->setStatus(amiSetEntity::STATUS_PROCESSING, $data);
        return TRUE;
      }
      catch (\Exception $exception) {
        $message = $this->t('Sorry we did all right but failed @ophuman the ADO with UUID @uuid on Set @setid. Something went wrong. Please check your Drupal Logs and notify your admin.',[
          '@uuid' => $data->info['row']['uuid'],
          '@setid' => $data->info['set_id'],
          '@ophuman' => $op == $op_original ? static::OP_HUMAN[$op] : static::OP_HUMAN[$op] . ' and ' . static::OP_HUMAN[$op_original]
        ]);
        $this->loggerFactory->get('ami_file')->error($message ,[
          'setid' => $data->info['set_id'] ?? NULL,
          'time_submitted' => $data->info['time_submitted'] ?? '',
        ]);
        $this->setStatus(amiSetEntity::STATUS_PROCESSING_WITH_ERRORS, $data);
        return FALSE;
      }
    }
    else {
      $message = $this->t('Sorry we did all right but JSON resulting at the end is flawed and we could not @ophuman the ADO with UUID @uuid on Set @setid. This is quite strange. Please check your Drupal Logs and notify your admin.',[
        '@uuid' => $data->info['row']['uuid'],
        '@setid' => $data->info['set_id'],
        '@ophuman' => $op == $op_original ? static::OP_HUMAN[$op] : static::OP_HUMAN[$op] . ' and ' . static::OP_HUMAN[$op_original]
      ]);
      $this->loggerFactory->get('ami_file')->info($message ,[
        'setid' => $data->info['set_id'] ?? NULL,
        'time_submitted' => $data->info['time_submitted'] ?? '',
      ]);
      $this->setStatus(amiSetEntity::STATUS_PROCESSING_WITH_ERRORS, $data);
      return FALSE;
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
      return '{}';
    }
    return json_encode($patch ?? []);
  }

  /**
   * Processes a File and technical metadata to avoid congestion.
   *
   * @param mixed $data
   */
  protected function processFile($data) {
    // What makes a Processed File Cache reusable?
    // If the filename (source) for this Set is the same
    // If the ZIP file has not changed (if any)
    // If the source Key IS the same (will affect the dr:for)
    // @NOTE: we could add tons of more logic here to avoid over processing but
    // given how many combinations of data might affect the output
    // plus the fact that extra logic will duplicate (and require maintenance)
    // at this level makes no sense. Safer to regenerate.
    $zip_file_id = is_object($data->info['zip_file']) && $data->info['zip_file'] instanceof FileInterface ? (string) $data->info['zip_file']->id() : '0';
    $private_temp_key = md5(($data->info['file_column'] ?? '') . '-' . ($data->info['filename'] ?? '') . '-' . $zip_file_id);

    $processed_file_data = $this->store->get('set_' . $data->info['set_id'] . '-' . $private_temp_key);
    // even if the cache is there, the file might have been gone because of a previous cron
    // run after someone deleted the Ingested, waited too long.
    // If that is the case reprocessing will be needed.
    if (!empty($processed_file_data['as_data']) && !empty($processed_file_data['file_id'])) {
      /* @var \Drupal\file\FileInterface $file */
      $file = $this->entityTypeManager->getStorage('file')->load(
        $processed_file_data['file_id']
      );
      if (!$file) {
        $data->info['force_file_process'] = TRUE;
      }
      elseif (file_exists($file->getFileUri()) == FALSE) {
        $data->info['force_file_process'] = TRUE;
      }
    }
    else {
      $data->info['force_file_process'] = TRUE;
    }

    // First check if we already have the info here and not forced to recreate, if so do nothing.
    if (($data->info['force_file_process'] ?? FALSE)) {
      $file = $this->AmiUtilityService->file_get(trim($data->info['filename']),
        $data->info['zip_file'], $data->info['force_file_process']);
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
        $this->store->set('set_' . $data->info['set_id'] . '-' . $private_temp_key,
          $data_to_store);
      }
      else {
        $message = $this->t('Sorry, we really tried to process File @filename from Set @setid yet. Giving up',
          [
            '@setid' => $data->info['set_id'],
            '@filename' => $data->info['filename']
          ]);
        $this->loggerFactory->get('ami_file')->warning($message ,[
          'setid' => $data->info['set_id'] ?? NULL,
          'time_submitted' => $data->info['time_submitted'] ?? '',
        ]);
      }
    }
  }

  /**
   * Processes a CSV File without technical metadata. This is just for the purpose of input for the CSV queue worker
   *
   * @param mixed $data
   */
  protected function processCSvFile($data): \Drupal\Core\Entity\EntityInterface|\Drupal\file\Entity\File|null {
    if (!($data->info['csv_filename'] ?? NULL)) {
      return NULL;
    }
    $file = $this->AmiUtilityService->file_get(trim($data->info['csv_filename']),
      $data->info['zip_file'] ?? NULL, TRUE);
    if ($file) {
      $event_type = StrawberryfieldEventType::TEMP_FILE_CREATION;
      $current_timestamp = (new DrupalDateTime())->getTimestamp();
      $event = new StrawberryfieldFileEvent($event_type, 'ami', $file->getFileUri(), $current_timestamp);
      // This will allow the extracted CSV from the zip to be composted, even if it was not a CSV.
      // IN a queue by \Drupal\strawberryfield\EventSubscriber\StrawberryfieldEventCompostBinSubscriber
      $this->eventDispatcher->dispatch($event, $event_type);
    }
    if ($file && $file->getMimeType() == 'text/csv') {
      return $file;
    }
    else {
      $message = $this->t('The referenced nested CSV @filename on row id @rowid from Set @setid could not be found or had the wrong format. Skipping',
        [
          '@setid' => $data->info['set_id'],
          '@filename' => $data->info['csv_filename'],
          '@rowid' => $data->info['row']['row_id'] ?? '0',
        ]);
      $this->loggerFactory->get('ami_file')->warning($message ,[
        'setid' => $data->info['set_id'] ?? NULL,
        'time_submitted' => $data->info['time_submitted'] ?? '',
      ]);
      return NULL;
    }
  }


  /**
   * Checks if processing can be done, so we can bail out sooner.
   *
   * @param $data
   *
   * @return bool|null
   *    FALSE if can not, TRUE if it can, NULL if it is "create" but already there allowing further processing if needed.
   * @throws \Drupal\Component\Plugin\Exception\InvalidPluginDefinitionException
   * @throws \Drupal\Component\Plugin\Exception\PluginNotFoundException
   */
  private function canProcess($data): bool|null
  {

    //OP can be one of
    /*
    'create' => 'Create New ADOs',
    'update' => 'Update existing ADOs',
    'patch' => 'Patch existing ADOs',
    'delete' => 'Delete existing ADOs',
    'sync' => 'Sync' with either create/update/delete as sub operation.
    */

    $op = $data->pluginconfig->op;
    if ($data->pluginconfig->op == 'sync') {
      // We relay on the CSV expander to set this correctly
      // We always default to create. Worst case scenario it will
      // fail bc we can't if already there
      $op = $data->info['op_secondary'] ?? 'create';
      $op = in_array($op, ['create','update','delete']) ? $op : NULL;
    }

    /** @var \Drupal\Core\Entity\ContentEntityInterface[] $existing */
    $existing = $this->entityTypeManager->getStorage('node')->loadByProperties(
      ['uuid' => $data->info['row']['uuid']]
    );

    if (count($existing) && $op === 'create') {
      $message = $this->t('Sorry, you requested an ADO with UUID @uuid to be created via Set @setid. But there is already one in your repo with that UUID. Skipping',[
        '@uuid' => $data->info['row']['uuid'],
        '@setid' => $data->info['set_id']
      ]);

      $this->loggerFactory->get('ami_file')->warning($message ,[
        'setid' => $data->info['set_id'] ?? NULL,
        'time_submitted' => $data->info['time_submitted'] ?? '',
      ]);
      $this->setStatus(amiSetEntity::STATUS_PROCESSING_WITH_ERRORS, $data);
      return NULL;
    }
    elseif (!count($existing) && $op !== 'create') {
      $message = $this->t('Sorry, the ADO with UUID @uuid you requested to be @ophuman via Set @setid does not exist. Skipping',[
        '@uuid' => $data->info['row']['uuid'],
        '@setid' => $data->info['set_id'],
        '@ophuman' => static::OP_HUMAN[$op],
      ]);
      $this->loggerFactory->get('ami_file')->warning($message ,[
        'setid' => $data->info['set_id'] ?? NULL,
        'time_submitted' => $data->info['time_submitted'] ?? '',
      ]);
      $this->setStatus(amiSetEntity::STATUS_PROCESSING_WITH_ERRORS, $data);
      return FALSE;
    }
    $account =  $data->info['uid'] == \Drupal::currentUser()->id() ? \Drupal::currentUser() : $this->entityTypeManager->getStorage('user')->load($data->info['uid']);

    if ($op !== 'create' && $account && $existing && count($existing) == 1) {
      $existing_object = reset($existing);
      if (!$existing_object->access('update', $account)) {
        $message = $this->t('Sorry you have no system permission to @ophuman ADO with UUID @uuid via Set @setid. Skipping',[
          '@uuid' => $data->info['row']['uuid'],
          '@setid' => $data->info['set_id'],
          '@ophuman' => static::OP_HUMAN[$op],
        ]);
        $this->loggerFactory->get('ami_file')->error($message ,[
          'setid' => $data->info['set_id'] ?? NULL,
          'time_submitted' => $data->info['time_submitted'] ?? '',
        ]);
        $this->setStatus(amiSetEntity::STATUS_PROCESSING_WITH_ERRORS, $data);
        return FALSE;
      }
    }
    return TRUE;
  }

  /**
   * Sets AMI set processing status
   *
   * @param string    $status
   * @param \stdClass $data
   */
  protected function setStatus(string $status, \stdClass $data) {
    try {
      $set_id = $data->info['set_id'];
      if (!empty($set_id)) {
        $processed_set_status = $this->statusStore->get('set_' . $set_id);
        $processed_set_status['processed'] = $processed_set_status['processed'] ?? 0;
        $processed_set_status['errored'] = $processed_set_status['errored'] ?? 0;
        $processed_set_status['total'] = $processed_set_status['total'] ?? 0;
        if ($status != amiSetEntity::STATUS_PROCESSING) {
          $processed_set_status['errored'] = $processed_set_status['errored']
            + 1;
        }
        else {
          $processed_set_status['processed'] = $processed_set_status['processed'] + 1;
        }
        $this->statusStore->set('set_' . $set_id, $processed_set_status);

        $sofar = $processed_set_status['processed'] + $processed_set_status['errored'];
        $finished = ($sofar >= $processed_set_status['total']);

        $ami_set = $this->entityTypeManager->getStorage('ami_set_entity')
          ->load($set_id);
        // Means the AMI set is gone.
        if (!$ami_set) {
          $message = $this->t('The original AMI Set ID @setid does not longer exist. Last known status was: @status ',[
            '@setid' => $data->info['set_id'],
            '@status' => $status ?? 'Unknown',
          ]);
          $this->loggerFactory->get('ami')->warning($message ,[
            'setid' => $data->info['set_id'] ?? NULL,
            'time_submitted' => $data->info['time_submitted'] ?? '',
          ]);
        }
        else {
          if ($ami_set->getStatus() != $status
            && $status != amiSetEntity::STATUS_PROCESSING
            && !$finished
          ) {
            $ami_set->setStatus(
              $status
            );
            $ami_set->save();
          }
          elseif ($finished) {
            if ($processed_set_status['errored'] == 0) {
              $ami_set->setStatus(
                amiSetEntity::STATUS_PROCESSED
              );
            }
            elseif ($processed_set_status['errored']
              == $processed_set_status['total']
            ) {
              $ami_set->setStatus(
                amiSetEntity::STATUS_FAILED
              );
            }
            else {
              $ami_set->setStatus(
                amiSetEntity::STATUS_PROCESSED_WITH_ERRORS
              );
            }
            $ami_set->save();
          }
        }
      }
    }
    catch (\Exception $exception)  {
      $message = $this->t('The original AMI Set ID @setid does not longer exist or some other uncaught error happened while setting the status: @e.',[
        '@setid' => $data->info['set_id'],
        '@e' => $exception->getMessage(),
      ]);
      // Makes no sense to log to the AMI file logger anymore?
      // Send to the global ami logger (DB)
      $this->loggerFactory->get('ami')->warning($message ,[
        'setid' => $data->info['set_id'] ?? NULL,
        'time_submitted' => $data->info['time_submitted'] ?? '',
      ]);
    }
  }

  /**
   *
   * Process Action Entries
   *
   * @param $data
   *
   * @return bool|null
   */
  private function processAction($data): bool|null {
    $success = FALSE;
    $existing = [];

    $account = $data->info['uid'] == \Drupal::currentUser()->id() ? \Drupal::currentUser() : $this->entityTypeManager->getStorage('user')->load($data->info['uid']);
    if ($account && !$account->isAnonymous()) {
      $this->accountSwitcher->switchTo($account);
    }
    else {
      $message = $this->t('Not so great. @action via Set @setid can not run as anonymous user. Bailing out.', [
        '@setid' => $data->info['set_id'],
        '@action' => $data->info['action'],
      ]);
      $this->loggerFactory->get('ami_file')->error($message, [
        'setid' => $data->info['set_id'] ?? NULL,
        'time_submitted' => $data->info['time_submitted'] ?? '',
      ]);
      return FALSE;
    }

    /** @var \Drupal\Core\Entity\ContentEntityInterface[] $existing */
    try {
      $existing = $this->entityTypeManager->getStorage('node')->loadByProperties(
        ['uuid' => $data->info['uuids']]
      );
    }
    catch (\Exception $e) {
      $message = $this->t('Error loading ADOs for @action via Set @setid. All ADOs in this queue item will be skipped ', [
        '@setid' => $data->info['set_id'],
        '@action' => $data->info['action'],
        '@error' => $e->getMessage(),
      ]);
      $this->loggerFactory->get('ami_file')->warning($message, [
        'setid' => $data->info['set_id'] ?? NULL,
        'time_submitted' => $data->info['time_submitted'] ?? '',
      ]);
    }

    // We need to log if number of requested to be deleted != number the user can delete.
    // Not a blocker to abort all actions but the user needs to know not all what was requested
    // Could be processed.
    // What if no a whole chunk is missing?
    // We still need to keep adding context to the action/storing it, etc. In
    // the hope other batches have enough data.

    // Each Action might have its own check/permission. But we know for sure delete requires `delete`
    if (($data->info['action'] ?? NULL) == 'delete') {
      if (count($existing)) {
        $access_type = "delete";
        foreach ($existing as $key => $existing_object) {
          if (!$existing_object->access($access_type, $account)) {
            $message = $this->t('Sorry you have no system permission to execute action @action on ADOs with UUID @uuid via Set @setid. Skipping', [
              '@uuid' => $existing_object->uuid(),
              '@setid' => $data->info['set_id'],
              '@action' => $data->info['action'],
            ]);
            $this->loggerFactory->get('ami_file')->error($message, [
              'setid' => $data->info['set_id'] ?? NULL,
              'time_submitted' => $data->info['time_submitted'] ?? '',
            ]);
            unset($existing[$key]);
            //$this->setStatus(amiSetEntity::STATUS_PROCESSING_WITH_ERRORS, $data);
          }
          else {
            $message = $this->t('You have permission to execute action @action on ADOs with UUID @uuid via Set @setid. Executed', [
              '@uuid' => $existing_object->uuid(),
              '@setid' => $data->info['set_id'],
              '@action' => $data->info['action'],
            ]);
            $this->loggerFactory->get('ami_file')->info($message, [
              'setid' => $data->info['set_id'] ?? NULL,
              'time_submitted' => $data->info['time_submitted'] ?? '',
            ]);
          }
        }
        $existing = array_filter($existing);
        try {
          if (count($existing)) {
            $this->entityTypeManager->getStorage('node')->delete($existing);
            $message = $this->t('Deleting Node IDs @node via Set @setid.', [
              '@node' => implode(",", array_keys($existing)),
              '@setid' => $data->info['set_id'],
              '@action' => $data->info['action'],
            ]);
            $this->loggerFactory->get('ami_file')->info($message, [
              'setid' => $data->info['set_id'] ?? NULL,
              'time_submitted' => $data->info['time_submitted'] ?? '',
            ]);
            $success = TRUE;
          }
        }
        catch (\Exception $e) {
          $message = $this->t('Error executing @action on ADOs via Set @setid.', [
            '@setid' => $data->info['set_id'],
            '@action' => $data->info['action'],
            '@error' => $e->getMessage(),
          ]);
          $this->loggerFactory->get('ami_file')->error($message, [
            'setid' => $data->info['set_id'] ?? NULL,
            'time_submitted' => $data->info['time_submitted'] ?? '',
          ]);
        }
      }
    }
    elseif (!empty($data->info['action'])) {
      try {
        // To make $context work we need to run this even if $existing is empty.
        $action_config = $data->info['action_config'] ?? [];
        /* @var $action ActionInterface */
        $action = $this->actionManager->createInstance($data->info['action'], $action_config);
        // Not all actions use context.
        $context = NULL;
        $context_keystore_id = NULL;
        $batch_size = count($data->info['uuids'] ?? []) ?? ($data->info['batch_size'] ?? 10);
        if ($action && \method_exists($action, 'setContext')) {
          $context_keystore_id = 'ami_action_' . $data->info['set_id'] . '_' . $data->info['time_submitted'];
          // We set current_batch 0 so we can increment upfront?
          // We also add a custom logger_channel wich
          $context = [
            'sandbox' => [
              'processed' => 0,
              'total' => 0,
              'page' => 0,
              'current_batch' => 1,
              'logger_channel' => 'ami_file'
            ],
            'results' => [
              'operations' => [],
            ],
          ];

          // Simulate a Batch Context on a Queue.
          $context = $this->contextStore
            ->get($context_keystore_id) ?? $context;
          if (empty($context['sandbox']['total'])) {
            $context['sandbox']['total'] = $data->info['batch_total'] ?? $batch_size;
            $context['sandbox']['batch_size'] = $batch_size;
            // Fake data. VBO is meant for Views!
            $context['view_id'] = 'ami_set';
            $context['display_id'] = $data->info['set_id'];
            $context['action_id'] = $data->info['action'];
            $context['action_config'] = $action_config;
            // Also Fake. used to fill with some-how useful data for actions that
            // Normally would run on a Batch/Interactive queue
            // Should we increment this on every batch? How large will be the key store?
            $context['list'] = array_combine($data->info['uuids'], $data->info['uuids']);
          }
          $action->setContext($context);
        }
        foreach ($existing as $key => $existing_object) {
          if (\method_exists($action, 'access')) {
            $accessResult = $action->access($existing_object, $account, TRUE);
            if ($accessResult->isAllowed() === FALSE) {
              $reason = '';
              if ($accessResult instanceof AccessResultReasonInterface) {
                $reason = $accessResult->getReason();
              }
              $message = $this->t('Sorry you have no system permission to execute action @action on ADOs with UUID @uuid via Set @setid. Skipping', [
                '@uuid' => $existing_object->uuid(),
                '@setid' => $data->info['set_id'],
                '@action' => $data->info['action'],
                '@reason' => $reason,
              ]);
              $this->loggerFactory->get('ami_file')->error($message, [
                'setid' => $data->info['set_id'] ?? NULL,
                'time_submitted' => $data->info['time_submitted'] ?? '',
              ]);
              unset($existing[$key]);
            }
          }
        }
        $existing = array_filter($existing);
        // Process Action on loaded entities.
        // If the action uses $context we need to run it anyways, even if
        // no entities were found or are accesible, or any logic that depends on
        // data actually existing will fail.
        if (count($existing) || ($context && $context_keystore_id) ) {
          $results = $action->executeMultiple($existing) ?? [];
          // Increment the current batch
          // Even if nothing was processed, or we did not have permissions we will increment the processed
          // by the original size. If not the Batch will basically never be assumed as DONE
          $results = array_filter($results ?? []);
          foreach ($results as $result) {
            // No idea why some results have the translation/others not
            // So .. we will cast to string
            if (is_object($result) && $result instanceof TranslatableMarkup) {
              $result = (string) $result;
              $this->loggerFactory->get('ami_file')->info($result, [
                'setid' => $data->info['set_id'] ?? NULL,
                'time_submitted' => $data->info['time_submitted'] ?? '',
              ]);
            }
          }
          $success = TRUE;
        }
        if ($context && $context_keystore_id) {
          $context['sandbox']['current_batch'] = ($context['sandbox']['current_batch'] ?? 1) + 1;
          $context['sandbox']['processed'] = $context['sandbox']['processed'] + $batch_size;
          $this->contextStore
            ->set($context_keystore_id, $context);
        }
      }
      catch (\Exception $e) {
        $message = $this->t('Sorry, the action @action configured to run on Set @setid does not exist. Skipping, with error @e', [
          '@setid' => $data->info['set_id'],
          '@action' => $data->info['action'],
          '@e' => $e->getMessage(),
        ]);
        $this->loggerFactory->get('ami_file')->error($message, [
          'setid' => $data->info['set_id'] ?? NULL,
          'time_submitted' => $data->info['time_submitted'] ?? '',
        ]);
        $success = FALSE;
      }
    }
    else {
      // Incomplete Queue worker entry.
      $success = FALSE;
    }
    $this->accountSwitcher->switchBack();
    return $success;
  }
}

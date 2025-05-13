<?php

namespace Drupal\ami\Plugin\QueueWorker;

use Drupal\ami\AmiLoDService;
use Drupal\ami\AmiUtilityService;
use Drupal\ami\Entity\amiSetEntity;
use Drupal\Core\Access\AccessResultReasonInterface;
use Drupal\Core\Action\ActionInterface;
use Drupal\Core\Datetime\DrupalDateTime;
use Drupal\Core\Entity\EntityStorageException;
use Drupal\facets\Exception\Exception;
use Drupal\file\FileInterface;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Logger\LoggerChannelFactoryInterface;
use Drupal\Core\Messenger\MessengerInterface;
use Drupal\Core\Plugin\ContainerFactoryPluginInterface;
use Drupal\Core\Queue\QueueWorkerBase;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\strawberryfield\Event\StrawberryfieldFileEvent;
use Drupal\strawberryfield\StrawberryfieldEventType;
use Drupal\strawberryfield\StrawberryfieldFilePersisterService;
use Drupal\strawberryfield\StrawberryfieldUtilityService;
use Drupal\views_bulk_operations\Service\ViewsBulkOperationsActionManager;
use Drupal\views_bulk_operations\Service\ViewsBulkOperationsActionProcessorInterface;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Monolog\Formatter\JsonFormatter;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Swaggest\JsonDiff\JsonDiff;
use Drupal\strawberryfield\Tools\StrawberryfieldJsonHelper;
use \Drupal\Core\TempStore\PrivateTempStoreFactory;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

/**
 * Processes and Ingests each AMI Set CSV row.
 *
 * @QueueWorker(
 *   id = "ami_action_ado",
 *   title = @Translation("AMI Digital Object Action Queue Worker")
 * )
 */
class ActionADOQueueWorker extends QueueWorkerBase implements ContainerFactoryPluginInterface {

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
   * The AMI specific logger
   *
   * @var \Psr\Log\LoggerInterface
   */
  protected $logger;

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
   * @param ViewsBulkOperationsActionManager $actionManager
   * @param ViewsBulkOperationsActionProcessorInterface $actionProcessor
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
    ViewsBulkOperationsActionProcessorInterface $actionProcessor
  ) {
    parent::__construct($configuration, $plugin_id, $plugin_definition);
    $this->entityTypeManager = $entity_type_manager;
    $this->loggerFactory = $logger_factory;
    $this->strawberryfieldUtility = $strawberryfield_utility_service;
    $this->AmiUtilityService = $ami_utility;
    $this->messenger = $messenger;
    $this->AmiLoDService = $ami_lod;
    $this->strawberryfilepersister = $strawberry_filepersister;
    $this->privateStoreFactory = $temp_store_factory;
    $this->store =  $this->privateStoreFactory->get('ami_queue_worker_file');
    $this->statusStore =  $this->privateStoreFactory->get('ami_queue_status');
    $this->eventDispatcher = $event_dispatcher;
    /** @var ViewsBulkOperationsActionManager $actionManager */
    $this->actionManager = $actionManager;
    /** @var ViewsBulkOperationsActionProcessorInterface $actionProcessor */
    $this->actionProcessor = $actionProcessor;
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
  public static function create(ContainerInterface $container, array $configuration, $plugin_id, $plugin_definition
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
      $container->get('tempstore.private'),
      $container->get('event_dispatcher'),
      $container->get('plugin.manager.views_bulk_operations_action'),
      $container->get('views_bulk_operations.processor')
    );
  }

  /**
   * {@inheritdoc}
   */
  public function processItem($data) {
    $log = new Logger('ami_file');
    $private_path = \Drupal::service('stream_wrapper_manager')
      ->getViaUri('private://')
      ->getDirectoryPath();
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
    */
    /* Data info for an ADO ACtion has this structure
       $data->info = [
              'uuids' => Array of multiple UUIDs, max count  == batch size
              'set_id' => The Set it
              'uid' => The user that should run the action (used for permissions)
              'action' => the action ID
              'action_config' => an array with extra configutation options for the action
              'set_url' => the SET URL
              'attempt' => 1, Number of attempts
              'queue_name' => "ami_action_ado" (fixed)
              'time_submitted' => whe  this was originally submitted (at its Root)
              'batch_size' => max Number of UUIDs/ADOS to process per Queue entry, defaults to 50,
            ];
    */
    if ($data->pluginconfig->op !== "action") {
      return;
    }

    $message = $this->t('Attempting to process SET @setid with action @action for @uuids.',
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
      $message = $this->t('Action Processing SET @setid with action @action for @uuids failed',
        [
          '@setid' => $data->info['set_id'],
          '@action' => $data->info['action'],
          '@uuids' => implode(",", $data->info['uuids'] ?? []),
        ]);
      $this->loggerFactory->get('ami_file')->warning($message, [
        'setid' => $data->info['set_id'] ?? NULL,
        'time_submitted' => $data->info['time_submitted'] ?? '',
      ]);
      return;
    }
  }

  private function processAction($data): bool|null {
    $success = FALSE;
    $existing = [];
    /** @var \Drupal\Core\Entity\ContentEntityInterface[] $existing */
    try {
      $existing = $this->entityTypeManager->getStorage('node')->loadByProperties(
        ['uuid' => $data->info['uuids']]
      );
    }
    catch (\Exception $e) {
      $message = $this->t('Error loading NODES for @action on ADOs via Set @setid.', [
        '@setid' => $data->info['set_id'],
        '@action' => $data->info['action'],
        '@error' => $e->getMessage(),
      ]);
      $this->loggerFactory->get('ami_file')->warning($message, [
        'setid' => $data->info['set_id'] ?? NULL,
        'time_submitted' => $data->info['time_submitted'] ?? '',
      ]);
      return FALSE;
    }

    // We need to log if number of requested to be deleted != number the user can delete.
    // Not a blocker to abort all actions but the user needs to know not all what was requested
    // Could be processed.
    $account = $data->info['uid'] == \Drupal::currentUser()->id() ? \Drupal::currentUser() : $this->entityTypeManager->getStorage('user')->load($data->info['uid']);
    if (count($existing) && $account) {
      // Each Action might have its own check/permission. But we know for sure delete requires `delete`
      if (($data->info['action'] ?? NULL) == 'delete') {
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
          $this->entityTypeManager->getStorage('node')->delete($existing);
          $message = $this->t('Deleting UUIDs @uuid via Set @setid.', [
            '@uuid' => $existing_object->uuid(),
            '@setid' => $data->info['set_id'],
            '@action' => $data->info['action'],
          ]);
          $this->loggerFactory->get('ami_file')->info($message, [
            'setid' => $data->info['set_id'] ?? NULL,
            'time_submitted' => $data->info['time_submitted'] ?? '',
          ]);
          $success = TRUE;
        }
        catch (EntityStorageException $e) {
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
      elseif (!empty($data->info['action'])) {
        try {
          $action_config = $data->info['action_config'] ?? [];
          /* @var $action ActionInterface */
          $action = $this->actionManager->createInstance($data->info['action'], $action_config);
          // Not all actions use context.
          $context = NULL;
          $context_keystore_id = NULL;
          if ($action && \method_exists($action, 'setContext')) {
            $context_keystore_id = 'ami_action_' . $data->info['set_id'] . '_' . $data->info['time_submitted'];
            // We set current_batch 0 so we can increment upfront?
            $context = [
              'sandbox' => [
                'processed' => 0,
                'total' => 0,
                'page' => 0,
                'current_batch' => 1,
              ],
              'results' => [
                'operations' => [],
              ],
            ];

            // Simulate a Batch Context on a Queue.
            $context = $this->privateStoreFactory->get('ami_action_context')
              ->get($context_keystore_id) ?? $context;
            $batch_size = count($data->info['uuids'] ?? []) ?? ($data->info['batch_size'] ?? 10);
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
          if (count($existing)) {
            $results = $action->executeMultiple($existing);
            // Increment the current batch
            // Even if nothing was processed or we did not have permissions we will increment the processed
            // by the original size. If not the Batch will basically never be assumed as DONE
            if ($context && $context_keystore_id) {
              $context['sandbox']['current_batch'] = ($context['sandbox']['current_batch'] ?? 1) + 1;
              $context['sandbox']['processed'] = $context['sandbox']['processed'] + $batch_size;
              // Not sure i should fill the Key Store with the results data. Might end being huge?
              // Better to output to the logs?
              //$context['results'] = array_merge($results, $results);
              // Write context back. Wonder if i should add a small pause here? Like a second?
              // Maybe in the future. People run huge batches and DB read committed might be an issue?
              $this->privateStoreFactory->get('ami_action_context')
                ->set($context_keystore_id, $context);
            }
            foreach ($results as $result) {
              $this->loggerFactory->get('ami_file')->info($result);
            }
          }
          else {
            // Nothing could be processed.
          }
        }
        catch (\Exception $e) {
          $message = $this->t('Sorry, the action @action configured to run on Set @setid does not exist. Skipping', [
            '@setid' => $data->info['set_id'],
            '@action' => $data->info['action'],
          ]);
          return FALSE;
        }
      }

    }
    else {
      $message = $this->t('Sorry none of the following UUIDs @uuids from Set @setid are present in your system. Skipping @action.', [
        '@uuids' => implode(",", $data->info['uuids'] ?? []),
        '@setid' => $data->info['set_id'],
        '@action' => $data->info['action'],
      ]);
      $this->loggerFactory->get('ami_file')->error($message, [
        'setid' => $data->info['set_id'] ?? NULL,
        'time_submitted' => $data->info['time_submitted'] ?? '',
      ]);
    }
    return $success;
  }

}

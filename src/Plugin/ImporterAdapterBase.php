<?php

namespace Drupal\ami\Plugin;

use Drupal\Component\Plugin\Exception\PluginException;
use Drupal\Component\Plugin\PluginBase;
use Drupal\Core\DependencyInjection\DependencySerializationTrait;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Messenger\MessengerTrait;
use Drupal\Core\Plugin\ContainerFactoryPluginInterface;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\ami\Entity\ImporterAdapterInterface;
use Drupal\ami\Plugin\ImporterAdapterInterface as ImporterPluginAdapterInterface ;
use GuzzleHttp\ClientInterface;
use Drupal\Core\Form\FormBuilderInterface;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\Core\Queue\QueueFactory;
use Drupal\Core\Queue\QueueWorkerManager;

/**
 * You can use this constant to set how many queued items
 * you want to be processed in one batch operation
 */
define("IMPORT_XML_BATCH_SIZE", 1);

/**
 * Base class for ImporterAdapter plugins.
 */
abstract class ImporterAdapterBase extends PluginBase implements ImporterPluginAdapterInterface, ContainerFactoryPluginInterface {

  use StringTranslationTrait;
  use DependencySerializationTrait;
  /**
   * @var \Drupal\Core\Entity\EntityTypeManager
   */
  protected $entityTypeManager;

  /**
   * @var \GuzzleHttp\Client
   */
  protected $httpClient;

  /**
   * @var \Drupal\Core\Form\FormBuilderInterface
   */
  protected $formBuilder;

  /**
   * @var \Drupal\Core\Queue\QueueFactory
   */
  protected $queue_factory;

  /**
   * @var \Drupal\Core\Queue\QueueWorkerManager
   */
  protected $queue_manager;

  /**
   * {@inheritdoc}
   */
  public function __construct(array $configuration, $plugin_id, $plugin_definition, EntityTypeManagerInterface $entityTypeManager, ClientInterface $httpClient, FormBuilderInterface $formBuilder,  QueueFactory $queue_factory, QueueWorkerManager $queue_manager) {
    parent::__construct($configuration, $plugin_id, $plugin_definition);
    $this->entityTypeManager = $entityTypeManager;
    $this->httpClient = $httpClient;
    $this->formBuilder = $formBuilder;
    $this->queue_factory = $queue_factory;
    $this->queue_manager = $queue_manager;
    if (!isset($configuration['config'])) {
      throw new PluginException('Missing AMI ImporterAdapter configuration.');
    }

    if (!$configuration['config'] instanceof ImporterAdapterInterface) {
      throw new PluginException('Wrong AMI ImporterAdapter configuration.');
    }
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
      $container->get('form_builder'),
      $container->get('queue'),
      $container->get('plugin.manager.queue_worker')
    );
  }

  /**
   * {@inheritdoc}
   */
  public function getConfig() {
    return $this->configuration['config'];
  }

  public function enqueue(array $jsondata = []) {
    /** @var \Drupal\ami\Entity\ImporterAdapterInterface $importer_config */
    $importer_config = $this->configuration['config'];
    $default_bundles = $importer_config->getTargetEntityTypes();
    $default_bundle = reset($default_bundles);
    // If we have no default bundle setup do not process anything
    if (!$default_bundle) { return; };
    $queue = $this->queue_factory->get('ami_ingest_ado');
    foreach ($jsondata as $itemdata) {
      $count++;
      // Create new queue item
      $item = new \stdClass();
      $item->jsonmetadata = $itemdata['sbf'];
      $item->uuid = $itemdata['uuid'];
      $item->label = $itemdata['label'];
      $item->type = isset($itemdata['bundle']) ? $itemdata['bundle'] : $default_bundle;
      $item->op = isset($itemdata['op']) ? $itemdata['op'] : 'create';
      $queue->createItem($item);
    }
    return $count;
  }
  /**
   * Common batch processing callback for all operations.
   */
  public static function batchProcess(&$context) {

    // We can't use here the Dependency Injection because this is static.
    $queue_factory = \Drupal::service('queue');
    $queue_manager = \Drupal::service('plugin.manager.queue_worker');

    // Get the queue implementation for import_content_from_xml queue
    $queue = $queue_factory->get('ami_ingest_ado');
    // Get the queue worker
    $queue_worker = $queue_manager->createInstance('ami_ingest_ado');

    // Get the number of items
    $number_of_queue = ($queue->numberOfItems() < IMPORT_XML_BATCH_SIZE) ? $queue->numberOfItems() : IMPORT_XML_BATCH_SIZE;

    // Repeat $number_of_queue times
    for ($i = 0; $i < $number_of_queue; $i++) {
      // Get a queued item
      if ($item = $queue->claimItem()) {
        try {
          // Process it
          $queue_worker->processItem($item->data);
          // If everything was correct, delete the processed item from the queue
          $queue->deleteItem($item);
        }
        catch (SuspendQueueException $e) {
          // If there was an Exception trown because of an error
          // Releases the item that the worker could not process.
          // Another worker can come and process it
          $queue->releaseItem($item);
          break;
        }
      }
    }
  }

  /**
   * Batch finished callback.
   */
  public static function batchFinished($success, $results, $operations) {
    if ($success) {
      \Drupal::messenger()->t("The ADOs where successfully processed by AMI.");
    }
    else {
      $error_operation = reset($operations);
      \Drupal::messenger()->t('An error occurred while processing @operation with arguments : @args', array('@operation' => $error_operation[0], '@args' => print_r($error_operation[0], TRUE)));
    }
  }

}

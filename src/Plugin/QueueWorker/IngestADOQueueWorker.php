<?php

namespace Drupal\ami\Plugin\QueueWorker;

use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Logger\LoggerChannelFactoryInterface;
use Drupal\Core\Plugin\ContainerFactoryPluginInterface;
use Drupal\Core\Queue\QueueWorkerBase;
use Drupal\Component\Serialization\Json;
use Drupal\strawberryfield\StrawberryfieldUtilityService;
use Symfony\Component\DependencyInjection\ContainerInterface;

/**
 * Process the JSON payload provided by the webhook.
 *
 * @QueueWorker(
 *   id = "ami_ingest_ado",
 *   title = @Translation("AMI Digital Object Ingester Queue Worker"),
 *   cron = {"time" = 5}
 * )
 */
class IngestADOQueueWorker extends QueueWorkerBase implements ContainerFactoryPluginInterface {

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
   * Constructor.
   *
   * @param array $configuration
   * @param string $plugin_id
   * @param mixed $plugin_definition
   * @param \Drupal\Core\Entity\EntityTypeManager $entity_field_manager
   */
  public function __construct(
    array $configuration,
    $plugin_id,
    $plugin_definition,
    EntityTypeManagerInterface $entity_type_manager,
    LoggerChannelFactoryInterface $logger_factory,
    StrawberryfieldUtilityService $strawberryfield_utility_service
  ) {
    parent::__construct($configuration, $plugin_id, $plugin_definition);
    $this->entityTypeManager = $entity_type_manager;
    $this->loggerFactory = $logger_factory;
    $this->strawberryfieldUtility = $strawberryfield_utility_service;
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
      $container->get('strawberryfield.utility')
    );
  }

  /**
   * {@inheritdoc}
   */
  public function processItem($data) {
    // Decode the JSON that was captured.
    $this->persistEntity($data);
  }

  /**
   * Saves a NODE entity from the remote data.
   *
   * @param \stdClass $data
   */
  private function persistEntity($data) {
    //@TODO persist needs to update too.
    $existing = $this->entityTypeManager->getStorage('node')->loadByProperties(
      ['uuid' => $data->uuid]
    );
    //@TODO make field_descriptive_metadata passed from the Configuration
    //@TODO status, and UID should also get settings
    //@TODO persist can be update, so that changes things.
    // We may want to test if there is a JSON:DIFF before saving. That way
    // WE save space and do not add extra DB inserts/updates to things that
    // Are not changing
    // Also Log. We need to Log a lot here, like crazy.
    if (!$existing) {
      $nodeValues = [
        'uuid' => $data->uuid,
        'type' => $data->bundle,
        'status' => 1,
        'title' => $data->label,
        'field_descriptive_metadata' => $data->jsonmetadata,
      ];

      /** @var \Drupal\Core\Entity\ContentEntityBase $node */
      $node = $this->entityTypeManager->getStorage('node')->create($nodeValues);
      $node->save();
    }
  }
}

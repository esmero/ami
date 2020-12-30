<?php

namespace Drupal\ami\Plugin\QueueWorker;

use Drupal\ami\AmiUtilityService;
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
 *   title = @Translation("AMI Digital Object Ingester Queue Worker")
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
   * @var \Drupal\ami\AmiUtilityService
   */
  protected $AmiUtilityService;


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
    StrawberryfieldUtilityService $strawberryfield_utility_service,
    AmiUtilityService $ami_utility
  ) {
    parent::__construct($configuration, $plugin_id, $plugin_definition);
    $this->entityTypeManager = $entity_type_manager;
    $this->loggerFactory = $logger_factory;
    $this->strawberryfieldUtility = $strawberryfield_utility_service;
    $this->AmiUtilityService = $ami_utility;
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
      $container->get('ami.utility')
    );
  }

  /**
   * {@inheritdoc}
   */
  public function processItem($data) {

    // Before we do any processing. Check if Parent(s) exists?
    // If not, re-enqueue: we try twice only. Should we try more?
    $parent_nodes = [];
    if (isset($data->info['row']['parent']) && is_array($data->info['row']['parent'])) {
      $parents = $data->info['row']['parent'];
      $parents = array_filter($parents);
      error_log('We got parents, checking if they exist');
      error_log(var_export($parents,true));
      foreach($parents as $parent_property => $parent_uuid) {
        error_log(var_export($parent_uuid,true));
        $parent_uuids = (array) $parent_uuid;
        $existing = $this->entityTypeManager->getStorage('node')->loadByProperties(['uuid' => $parent_uuids]);
        if (count($existing) != count($parent_uuids)) {
          error_log('Sorry, we can not process this one yet, there are missing parents');
          // Pushing to the end of the queue.
          $data->info['attempt']++;
          if ($data->info['attempt'] < 3) {
            error_log('Re-enqueueing');
            \Drupal::queue('ami_ingest_ado')
              ->createItem($data);
            return;
          } else {
            error_log('We tried twice. Will not re-enqueue');
            return;
            // We could enqueue in a "failed" queue?
          }
        } else {
          // Get the IDs!
          foreach($existing as $node) {
            $parent_nodes[$parent_property][] = (int) $node->id();
          }
        }
      }
    }

    $processed_metadata = $this->AmiUtilityService->processMetadataDisplay($data);
    if (!$processed_metadata) {
      error_log('Sorry, we could convert your row into metadata');
      return;
    }
    $cleanvalues = [];
    // Now process Files and Nodes
    $ado_columns = array_values(get_object_vars($data->adomapping->parents));
    if ($data->mapping->globalmapping == "custom") {
      $file_columns = array_values(get_object_vars($data->mapping->custommapping_settings->{$data->info['row']['type']}->files));
    }
    else
    {
      $file_columns = array_values(get_object_vars($data->mapping->globalmapping_settings->files));
    }

    $entity_mapping_structure['entity:file'] = $file_columns;
    $entity_mapping_structure['entity:node'] =  $ado_columns;
    $processed_metadata = json_decode($processed_metadata, true);
    $cleanvalues["ap:entitymapping"] = $entity_mapping_structure;
    $processed_metadata  = $processed_metadata + $cleanvalues;

    // Assign parents as NODE Ids.

    foreach ($parent_nodes as $parent_property => $node_ids) {
      $processed_metadata[$parent_property] = $node_ids;
    }
    error_log(var_export($processed_metadata,true));

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
        foreach($filenames as $filename) {
          $file = $this->AmiUtilityService->file_get($filename);
          if ($file) {
            $processed_metadata[$file_column][] = (int) $file->id();
          }
        }
      }
    }

    // Decode the JSON that was captured.
    $this->persistEntity($data, $processed_metadata);
  }

  /**
   * Saves an ADO (NODE Entity).
   *
   * @param \stdClass $data
   */
  private function persistEntity(\stdClass $data, array $processed_metadata) {

    $op = $data->pluginconfig->op;
    //@TODO persist needs to update too.
    $existing = $this->entityTypeManager->getStorage('node')->loadByProperties(
      ['uuid' => $data->info['row']['uuid']]
    );

    if ($existing && $op == 'create') {
      error_log('Sorry. You said create but there is already an Node with that UUID');
      //@TODO log correctly
      return;
    }


    if ($data->mapping->globalmapping == "custom") {
      $property_path = $data->mapping->custommapping_settings->{$data->info['row']['type']}->bundle;
    }
    else {
      $property_path = $data->mapping->globalmapping_settings->bundle;
    }
    $label_column = $data->adomapping->base->label;
    $label = $processed_metadata[$label_column];
    $property_path_split = explode(':', $property_path);
    if (!$property_path_split || count($property_path_split) != 2 ) {
      error_log('Sorry. Bundle/Field names are wrong');
      //@TODO log correctly
      return;
    }
    $bundle = $property_path_split[0];
    $field_name = $property_path_split[1];
    // JSON_ENCODE AGAIN
    $jsonstring = json_encode($processed_metadata, JSON_PRETTY_PRINT, 50);

    if ($jsonstring) {
      $nodeValues = [
        'uuid' =>  $data->info['row']['uuid'],
        'type' => $bundle,
        'status' => 1,
        'title' => $label,
        'uid' =>  $data->info['uid'],
        $field_name => $jsonstring
      ];

      /** @var \Drupal\Core\Entity\ContentEntityBase $node */
      try {
        $node = $this->entityTypeManager->getStorage('node')
          ->create($nodeValues);
        $node->save();
        error_log('Well well well! Ingested!');
      }
      catch (\Exception $exception) {
        error_log('Ups. Something went wrong during NODE creation/Save');
        return;
      }
    }
    else {
      error_log('Sorry last step json encoding failed while ingesting.');
      //@TODO log correctly
      return;
    }
  }
}

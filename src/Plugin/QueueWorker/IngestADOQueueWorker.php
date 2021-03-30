<?php

namespace Drupal\ami\Plugin\QueueWorker;

use Drupal\ami\AmiUtilityService;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Logger\LoggerChannelFactoryInterface;
use Drupal\Core\Messenger\MessengerInterface;
use Drupal\Core\Plugin\ContainerFactoryPluginInterface;
use Drupal\Core\Queue\QueueWorkerBase;
use Drupal\Component\Serialization\Json;
use Drupal\Core\StringTranslation\StringTranslationTrait;
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
    AmiUtilityService $ami_utility,
    MessengerInterface $messenger
  ) {
    parent::__construct($configuration, $plugin_id, $plugin_definition);
    $this->entityTypeManager = $entity_type_manager;
    $this->loggerFactory = $logger_factory;
    $this->strawberryfieldUtility = $strawberryfield_utility_service;
    $this->AmiUtilityService = $ami_utility;
    $this->messenger = $messenger;
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
      $container->get('messenger')
    );
  }

  /**
   * {@inheritdoc}
   */
  public function processItem($data) {
  /* Data info has this structire
    $data->info = [
      'row' => The actual data
      'set_id' => The Set id
      'uid' => The User ID that processed the Set
      'set_url' => A direct URL to the set.
      'attempt' => The number of attempts to process. We always start with a 1
    ];
  */

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
            error_log('Re-enqueueing');
            \Drupal::queue('ami_ingest_ado')
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

    $processed_metadata = $this->AmiUtilityService->processMetadataDisplay($data);
    if (!$processed_metadata) {
      $this->messenger->addWarning($this->t('Sorry, we can not cast ADO with @uuid into proper Metadata. Check the Metadata Display Template used and/or your data ROW in your CSV for set @setid.',[
        '@uuid' => $data->info['row']['uuid'],
        '@setid' => $data->info['set_id']
      ]));
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
          $file = $this->AmiUtilityService->file_get($filename, $data->info['zip_file']);
          if ($file) {
            $processed_metadata[$file_column][] = (int) $file->id();
          }
          else {
            $this->messenger->addWarning($this->t('Sorry, for ADO with @uuid, File @filename at column @filecolumn was not found. Skipping. Please check your CSV for set @setid.',[
              '@uuid' => $data->info['row']['uuid'],
              '@setid' => $data->info['set_id'],
              '@filename' => $filename,
              '@filecolumn' => $file_column,
            ]));
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

    //OP can be one of
    /*
    'create' => 'Create New ADOs',
    'update' => 'Update existing ADOs',
    'patch' => 'Patch existing ADOs',
    'delete' => 'Delete existing ADOs',
    */
    $op = $data->pluginconfig->op;
    $ophuman = [
      'create' => 'created',
      'update' => 'updated',
      'patched' => 'patched',
      'delete' => 'deleted',
    ];

    //@TODO persist needs to update too.
    $existing = $this->entityTypeManager->getStorage('node')->loadByProperties(
      ['uuid' => $data->info['row']['uuid']]
    );

    if ($existing && $op == 'create') {
      $this->messenger->addError($this->t('Sorry, you requested an ADO with @uuid to be created via Set @setid. But there is already one in your repo with that UUID. Skipping',[
        '@uuid' => $data->info['row']['uuid'],
        '@setid' => $data->info['set_id']
      ]));
      return;
    }


    if ($data->mapping->globalmapping == "custom") {
      $property_path = $data->mapping->custommapping_settings->{$data->info['row']['type']}->bundle;
    }
    else {
      $property_path = $data->mapping->globalmapping_settings->bundle;
    }
    $label_column = $data->adomapping->base->label;
    //@TODO check if the column is there!
    $label = $processed_metadata[$label_column];
    $property_path_split = explode(':', $property_path);
    if (!$property_path_split || count($property_path_split) != 2 ) {
      $this->messenger->addError($this->t('Sorry, your Bundle/Fields set for the requested an ADO with @uuid on Set @setid are wrong. You may have made a larger change in your repo and deleted a Content Type. Aborting.',[
        '@uuid' => $data->info['row']['uuid'],
        '@setid' => $data->info['set_id']
      ]));
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
        $this->messenger->addStatus($this->t('ADO %title with UUID:@uuid on Set @setid was @ophuman!',[
          '@uuid' => $data->info['row']['uuid'],
          '@setid' => $data->info['set_id'],
          '%title' => $label,
          '@ophuman' => $ophuman[$op]
        ]));
      }
      catch (\Exception $exception) {
        $this->messenger->addError($this->t('Sorry we did all right but failed persisting the ADO with @uuid on Set @setid are wrong. Please check your Drupal Logs and notify your admin.',[
          '@uuid' => $data->info['row']['uuid'],
          '@setid' => $data->info['set_id']
        ]));
        return;
      }
    }
    else {
      $this->messenger->addError($this->t('Sorry we did all right but JSON resulting at the end is flawed and we could not persist the ADO with @uuid on Set @setid are wrong. This is quite strange. Please check your Drupal Logs and notify your admin.',[
        '@uuid' => $data->info['row']['uuid'],
        '@setid' => $data->info['set_id']
      ]));
      return;
    }
  }
}

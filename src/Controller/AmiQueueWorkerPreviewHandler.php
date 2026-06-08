<?php

namespace Drupal\ami\Controller;

use Drupal\Component\Render\FormattableMarkup;
use Drupal\Core\Ajax\AjaxResponse;
use Drupal\Core\Controller\ControllerBase;
use Drupal\Core\Entity\ContentEntityInterface;
use Drupal\Core\Entity\EntityInterface;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Link;
use Drupal\Core\Render\RendererInterface;
use Drupal\file\Entity\File;
use Drupal\ami\AmiLoDService;
use Drupal\ami\AmiUtilityService;
use Drupal\ami\Entity\amiSetEntity;
use Drupal\Core\Datetime\DrupalDateTime;
use Drupal\file\FileInterface;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Logger\LoggerChannelFactoryInterface;
use Drupal\Core\Messenger\MessengerInterface;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\strawberryfield\Event\StrawberryfieldFileEvent;
use Drupal\strawberryfield\StrawberryfieldEventType;
use Drupal\strawberryfield\StrawberryfieldFilePersisterService;
use Drupal\strawberryfield\StrawberryfieldUtilityService;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Swaggest\JsonDiff\JsonDiff;
use Jfcherng\Diff\DiffHelper;
use Jfcherng\Diff\Factory\RendererFactory;
use Jfcherng\Diff\Options\DifferOptions;
use Jfcherng\Diff\Options\RendererOptions;
use Drupal\strawberryfield\Tools\StrawberryfieldJsonHelper;
use Drupal\Core\TempStore\PrivateTempStoreFactory;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;
use Drupal\views_bulk_operations\Service\ViewsBulkOperationsActionManager;
use Drupal\views_bulk_operations\Service\ViewsBulkOperationsActionProcessorInterface;
use Drupal\Core\Session\AccountSwitcherInterface;
use Drupal\Core\DependencyInjection\DependencySerializationTrait;
use Drupal\Core\Ajax\MessageCommand;
use Drupal\Core\Ajax\OpenModalDialogCommand;
use Drupal\Core\Render\RenderContext;

/**
 * Simulates AMI Queue Worker Ingest/Update/Sync without persisting an ADO.
 *
 */
class AmiQueueWorkerPreviewHandler extends ControllerBase {

  use StringTranslationTrait;
  use DependencySerializationTrait;
  /**
   * Drupal\Core\Entity\EntityTypeManager definition.
   *
   * @var \Drupal\Core\Entity\EntityTypeManager
   */
  protected $entityTypeManager;

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
   * The Drupal Renderer.
   *
   * @var \Drupal\Core\Render\RendererInterface
   */
  protected $renderer;

  /**
   * Helper property to keep the patched JSON around.
   */
   protected ?string $json_patch = NULL;

  /**
   * Constructor.
   *
   * @param EntityTypeManagerInterface $entity_type_manager
   * @param StrawberryfieldUtilityService $strawberryfield_utility_service
   * @param AmiUtilityService $ami_utility
   * @param AmiLoDService $ami_lod
   * @param MessengerInterface $messenger
   * @param StrawberryfieldFilePersisterService $strawberry_filepersister
   * @param PrivateTempStoreFactory $temp_store_factory
   * @param EventDispatcherInterface $event_dispatcher
   * @param AccountSwitcherInterface $accountSwitcher
   * @param \Drupal\Core\Render\RendererInterface $renderer
   */
  public function __construct(
    EntityTypeManagerInterface $entity_type_manager,
    StrawberryfieldUtilityService $strawberryfield_utility_service,
    AmiUtilityService $ami_utility,
    AmiLoDService $ami_lod,
    MessengerInterface $messenger,
    StrawberryfieldFilePersisterService $strawberry_filepersister,
    PrivateTempStoreFactory $temp_store_factory,
    EventDispatcherInterface $event_dispatcher,
    AccountSwitcherInterface $accountSwitcher,
    RendererInterface $renderer,
  ) {
    $this->entityTypeManager = $entity_type_manager;
    $this->strawberryfieldUtility = $strawberryfield_utility_service;
    $this->AmiUtilityService = $ami_utility;
    $this->messenger = $messenger;
    $this->AmiLoDService = $ami_lod;
    $this->strawberryfilepersister = $strawberry_filepersister;
    $this->store = $temp_store_factory->get('ami_queue_worker_file');
    $this->statusStore = $temp_store_factory->get('ami_queue_status');
    $this->contextStore = $temp_store_factory->get('ami_action_context');
    $this->eventDispatcher = $event_dispatcher;
    $this->accountSwitcher = $accountSwitcher;
    $this->renderer = $renderer;
  }


  /**
   * {@inheritdoc}
   */
  public static function create(ContainerInterface $container) {
    return new static(
      $container->get('entity_type.manager'),
      $container->get('strawberryfield.utility'),
      $container->get('ami.utility'),
      $container->get('ami.lod'),
      $container->get('messenger'),
      $container->get('strawberryfield.file_persister'),
      $container->get('tempstore.private'),
      $container->get('event_dispatcher'),
      $container->get('account_switcher'),
      $container->get('renderer'),
    );
  }

  /**
   * {@inheritdoc}
   */
  public function simulateItemAjax(array $form, FormStateInterface $form_state) {
    $response = new AjaxResponse();
    $parsed_json = [];

    /** @var \Drupal\format_strawberryfield\MetadataDisplayInterface $entity */
    $ami_set_entity = $form_state->getFormObject()->getEntity();
    // We have two choices here.
    // A, Keep the code as similar as \Drupal\ami\Plugin\QueueWorker\IngestADOQueueWorker
    // which implies generating a $data structure as it was meant for the CSV expander
    // then let the Same Code Generate a $data structure for the Ingest ADO queue woker
    // or B. skip steps/go for the final $data structure. Which requires on any future updates on processing code
    // to also mimic it here.
    // It is a bummer i can not just re-use logic, but this is a form driven process v/s a queue one and
    // there is no persistence so i need the ADO state in a single method as return.

    $manyfiles = $this->config('strawberryfield.filepersister_service_settings')->get('manyfiles') ?? 0;
    $statuses = $form_state->getValue('status', []);
    $status_keep = $form_state->getValue('status_keep', FALSE) ? TRUE : FALSE;
    $row_id = $form_state->getValue('ado_amiset_preview_row', 2);
    if ($row_id < 2) { $row_id = 2; }
    $render_diff = (bool) $form_state->getValue('ado_amiset_preview_diff_rendered', FALSE);
    $render_json_diff = (bool) $form_state->getValue('ado_amiset_preview_diff_json', FALSE);
    $content_selector = 'ami-preview-container';
    $message_selector = 'ami-preview-messages-container';
    $dialog_content = [
      '#type' => 'container',
      '#attributes' => ['id' => $message_selector], // Wrapper used for message placement
      'body' => [
        '#markup' => '<p></p>',
      ],
    ];
    // 3. Open the modal dialog overlay
    $dialog_title = $this->t('AMI Preview');
    $ops_skip_onmissing_file = (bool) $form_state->getValue('skip_onmissing_file', TRUE);
    $ops_forcemanaged_destination_file = (bool) $form_state->getValue('take_control_file', TRUE);

    // Normalize Un Moderadet Statuses if 0 and 1
    foreach ($statuses as &$value) {
      if ($value == "1") {
        $value = 1;
      }
      elseif ($value == "0") {
        $value = 0;
      }
    }

    $csv_file_reference = $ami_set_entity->get('source_data')->getValue();
    $file = NULL;
    if (isset($csv_file_reference[0]['target_id'])) {
      /** @var \Drupal\file\Entity\File $file */
      $file = $this->entityTypeManager->getStorage('file')->load(
        $csv_file_reference[0]['target_id']
      );
    }

    // Fetch Zip file if any
    $zip_file = NULL;
    $zip_file_reference = $ami_set_entity->get('zip_file')->getValue();
    if (isset($zip_file_reference[0]['target_id'])) {
      /** @var \Drupal\file\Entity\File $zip_file */
      $zip_file = $this->entityTypeManager->getStorage('file')->load(
        $zip_file_reference[0]['target_id']
      );
    }
    $data = new \stdClass();
    foreach ($ami_set_entity->get('set') as $item) {
      /** @var \Drupal\strawberryfield\Plugin\Field\FieldType\StrawberryFieldItem $item */
      $data = $item->provideDecoded(FALSE);
    }
    if ($file && $data !== new \stdClass()) {
      $SetURL = $ami_set_entity->toUrl('canonical', ['absolute' => TRUE])
        ->toString();

      $run_timestamp = \Drupal::time()->getCurrentTime();
      $op_secondary = NULL;
      // Only applies to Update/Patch operations but for contract reasons
      // we generate all $data->info the same.
      $ops_safefiles = TRUE;

      if (isset($data->pluginconfig->op) && $data->pluginconfig->op != 'create') {
        $op_secondary = $form_state->getValue(['ops_secondary','ops_secondary_update'], 'update');
        $ops_safefiles = $form_state->getValue(['ops_secondary','ops_safefiles'], TRUE);
      }
      $data_csv = clone $data;
      // Testing the CSV processor
      $data_csv->info = [
        'zip_file' => $zip_file,
        'csv_file' => $file,
        'set_id' => $ami_set_entity->id(),
        'uid' => $this->currentUser()->id(),
        'status' => $statuses,
        'status_keep' => $status_keep ? TRUE: FALSE,
        'op_secondary' => $op_secondary,
        'ops_safefiles' => $ops_safefiles ? TRUE : FALSE,
        'log_jsonpatch' => FALSE,
        'set_url' => $SetURL,
        'attempt' => 1,
        'queue_name' => NULL,
        'force_file_queue' => (bool)$form_state->getValue('force_file_queue', FALSE),
        'force_file_process' => (bool)$form_state->getValue('force_file_process', FALSE),
        'manyfiles' => $manyfiles,
        'ops_skip_onmissing_file' => $ops_skip_onmissing_file,
        'ops_forcemanaged_destination_file' => $ops_forcemanaged_destination_file,
        'time_submitted' => $run_timestamp
      ];

      try {
        $data_ado = clone $data_csv;
        $data_ado->info = NULL;
        $added = [];
        $csv_file = $data_csv->info['csv_file'] ?? NULL;
        if ($csv_file instanceof FileInterface) {
          $invalid = [];
          $uuids_sync_action = [];
          if ($data_ado->pluginconfig->op !== 'action') {
            $info = $this->AmiUtilityService->preprocessAmiSetSingleRow($csv_file, $data_ado, $invalid, FALSE, $row_id);
            if (!count($info)) {
              //@TODO tell the user which CSV failed please?
              $message = $this->t('So sorry. CSV @csv for @setid produced no ADOs. Please correct your source CSV data', [
                '@setid' => $data_ado->info['set_id'] ?? 'Undefined AMI set',
                '@csv' => $csv_file->getFilename() ?? 'Unknown CSV File',
              ]);
              $this->messenger->addError($message);
            }
            else {
              foreach ($info as $item) {
                // We set current User here since we want to be sure the final owner of
                // the object is this and not the user that runs the queue
                $data_ado->info = [
                  'zip_file' => $data_csv->info['zip_file'] ?? NULL,
                  'row' => $item,
                  'set_id' => $data_csv->info['set_id'],
                  'uid' => $data_csv->info['uid'],
                  'status_keep' => $data_csv->info['status_keep'] ?? FALSE,
                  'status' => $data_csv->info['status'],
                  'op_secondary' => $data_csv->info['op_secondary'] ?? NULL,
                  'ops_safefiles' => $data_csv->info['ops_safefiles'] ? TRUE : FALSE,
                  'log_jsonpatch' => FALSE,
                  'set_url' => $data_csv->info['set_url'],
                  'attempt' => 1,
                  'queue_name' => $data_csv->info['queue_name'],
                  'force_file_queue' => $data_csv->info['force_file_queue'],
                  'force_file_process' => $data_csv->info['force_file_process'],
                  'manyfiles' => $data_csv->info['manyfiles'],
                  'ops_skip_onmissing_file' => $data_csv->info['ops_skip_onmissing_file'],
                  'ops_forcemanaged_destination_file' => $data_csv->info['ops_forcemanaged_destination_file'],
                  'time_submitted' => $data_csv->info['time_submitted'],
                ];
                // Overrides in case we are in a sync operation.
                $skip = FALSE;
                if ($data_ado->pluginconfig->op == 'sync') {
                  // Important we will move the data driven (ami_sync_op) info the que info
                  // structure secondary.
                  // Fixed key:
                  $sync_op = $item['data']['ami_sync_op'] ?? 'create';
                  if ($sync_op === 'create') {
                    $data_ado->info['op_secondary'] = 'create';
                  }
                  elseif ($sync_op === 'update') {
                    $data_ado->info['op_secondary'] = 'update';
                  }
                  elseif ($sync_op === 'delete') {
                    // This needs to go a different queue.
                    $skip = TRUE;
                    // We only need the UUIDs to delete.
                    // Will we allow a Sync operation to delete a TOP and automatically delete all the children?
                    // If so we need to pass the CSV data also to this array.
                    // $uuids_sync_action should be formed the way $this->AmiUtilityService->getProcessedAmiSetNodeUUids($csv_file, $data, NULL); would.
                    // For now safer to not. We are deleting direct references of deletion of a ROW.
                    $uuids_sync_action[$item['uuid'] ?? ''] = [];
                  }
                  else {
                    $skip = TRUE;
                    // Flag as false?
                  }
                  // the actual behavior will be determined by a column named "ami_sync_op"
                }
                if ($data_ado->pluginconfig->op !== 'action' && !$skip) {
                  $ado_entity = $this->simulateQueueItem($data_ado, $parsed_json);
                  $build = [];

                  if ($ado_entity) {
                    $added[] = $ado_entity->uuid();
                    $render_array = $this->processAdoDiff($ado_entity, $render_diff);
                    $dialog_content['body'] = [
                      '#type' => 'container',
                      '#attributes' => ['id' => $content_selector],
                      'preview_wrapper' => $render_array
                    ];
                    if (!empty($parsed_json)) {
                      $dialog_content['body']['json'] = [
                        '#type' => 'fieldset',
                        '#collapsible' => TRUE,
                        '#collapsed' => TRUE,
                        '#title' => $this->t('Raw JSON produced by row @row', ['@row' => $row_id]),
                        '#attributes' => ['id' => $content_selector . '-json'],
                        'json_raw' => [
                          '#type' => 'markup',
                          '#prefix' => '<pre>',
                          '#suffix' => '</pre>',
                          '#markup' => json_encode($parsed_json, JSON_PRETTY_PRINT) ?? '{}'
                        ]
                      ];
                    }
                    if ($render_json_diff) {
                      if ($patched_json = $this->getJsonPatch()) {
                        $dialog_content['body']['json_diff'] = [
                          '#type' => 'fieldset',
                          '#collapsible' => TRUE,
                          '#collapsed' => TRUE,
                          '#title' => $this->t('JSON Diff as JSON Patch'),
                          '#attributes' => ['id' => $content_selector . '-json-diff'],
                          'json_raw' => [
                            '#type' => 'markup',
                            '#prefix' => '<pre>',
                            '#suffix' => '</pre>',
                            '#markup' => $patched_json
                          ]
                        ];
                      }
                    }
                  }

                  // 3. Open the modal dialog overlay
                  $dialog_title = $this->t('AMI ADO Preview for "@label" with UUID @uuid', [
                    '@label' => $ado_entity->label(),
                    '@uuid' => $ado_entity->uuid(),
                  ]);
                }
              }
            }
          }

          if (!in_array($data_ado->pluginconfig->op ?? 'missing op', [
            'action',
            'sync',
            'create',
            'update',
            'patch'
          ])) {
            $message = $this->t('Set @setid has a non valid Operation @op. We can not expand the CSV', [
              '@setid' => $data_ado->info['set_id'] ?? 'Undefined AMI set',
              '@op' => $data_ado->pluginconfig->op ?? 'Undefined Operation',
            ]);
            $this->messenger->addError($message);
          }
          if (count($invalid)) {
            $invalid_message = $this->formatPlural(count($invalid),
              'Source data Row @row had an issue, common cause is an invalid parent.',
              '@count rows, @row, had issues, common causes are invalid parents and/or non existing referenced rows.',
              [
                '@row' => implode(', ', array_keys($invalid)),
              ]
            );
            $this->messenger->addError($invalid_message);
          }
          if (!count($added)) {
            $message = $this->t('CSV @csv for Set @setid generated no ADOs. Check your CSV for missing UUIDs and other required elements', [
              '@setid' => $data_ado->info['set_id'] ?? 'Undefined AMI set ID',
              '@csv' => $csv_file->getFilename() ?? 'Unknown CSV File',
            ]);
            $this->messenger->addError($message);
          }
        }
        else {
          $message = $this->t('The referenced CSV @filename from Set @setid, enqueued to be expanded, could not be found. Skipping',
            [
              '@setid' => $data_ado->info['set_id'] ?? 'Undefined AMI set ID',
              '@filename' => $csv_file->getFilename() ?? 'Unknown CSV File',
            ]);
          $this->messenger->addError($message);
        }
      }
      catch (\Throwable $exception) {
        // Here $data_ado won't be available
        $message = $this->t('Sorry, something failed badly while we attempted to Expand an AMI set CSV into multiple ADO Ingest and Action Queue Items on Set @setid with error @error. This is quite strange. Please check your Drupal Logs and notify your admin.', [
          '@setid' => $data_csv->info['set_id'] ?? 'Undefined AMI set ID',
          '@error' => $exception->getMessage(),
        ]);
        $this->messenger->addError($message);
      }
    }
    $options =  [
      'height' => '85%',
      'width' => '85%',
    ];
    $response->addCommand(new OpenModalDialogCommand($dialog_title, $dialog_content, $options));
    $response->addAttachments([
      'library' => [
        'core/drupal.dialog.ajax',
        'ami/ami_preview_helper'
      ],
    ]);
    $messages = $this->messenger->deleteAll();
    foreach ($messages as $type => $type_messages) {
      foreach ($type_messages as $message) {
        $response->addCommand(new MessageCommand($message, '#'.$message_selector, ['type' => $type], FALSE));
      }
    }
    return $response;
  }


  private function simulateQueueItem($data, &$parsed_json) {
    try {
      $file_csv_columns = [];
      if ($data->mapping->globalmapping == "custom") {
        $csv_file_object = $data->mapping->custommapping_settings->{$data->info['row']['type']}->files_csv ?? NULL;
      }
      else {
        $csv_file_object = $data->mapping->globalmapping_settings->files_csv ?? NULL;
      }

      if ($csv_file_object && is_object($csv_file_object)) {
        $file_csv_columns = array_values(get_object_vars($csv_file_object));
      }

      $should_process_simulated = $this->canProcess($data);
      // On the actual queue worker we would bail out ::canProcess returning false. But here we want to accumulate the messages
      // and give the user feedback while stile trying to at least render a representation even if
      // actual processing would fail when done for real.
      $parent_nodes = [];
      if (isset($data->info['row']['parent']) && is_array($data->info['row']['parent'])) {
        $parents = $data->info['row']['parent'];
        $parents = array_filter($parents);
        foreach ($parents as $parent_property => $parent_uuid) {
          $parent_uuids = (array) $parent_uuid;
          // We should validate each member to be an UUID here (again). Just in case?
          $existing = $this->entityTypeManager->getStorage('node')
            ->loadByProperties(['uuid' => $parent_uuids]);

          // For preview. The parent could be literally another row to be ingested by the same AMI
          // set. So instead of bailing out in case the parent has not been ingested
          // we should just report as "make sure the parent ADO is ingested too"

          if (count($existing) != count($parent_uuids)) {
            $message = $this->t('Sorry, we can not process ADO with @uuid from Set @setid yet, there are missing parents with UUID(s) @parent_uuids.', [
              '@uuid' => $data->info['row']['uuid'] ?? 'Undefined UUID',
              '@setid' => $data->info['set_id'] ?? 'Undefined AMI set ID',
              '@parent_uuids' => implode(',', $parent_uuids)
            ]);
            $this->messenger->addError($message);
            $parent_nodes[$parent_property] = [];
          }
          foreach ($existing as $node) {
            $parent_nodes[$parent_property][] = (int) $node->id();
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
            '@uuid' => $data->info['row']['uuid'] ?? 'Undefined UUID',
            '@setid' => $data->info['set_id']
          ]);
          $this->messenger->addError($message);
          // Here we actuall return FALSE BC there is no possible rendering situation.
          return FALSE;
        }
      }
      if ($method == "direct") {
        if (isset($data->info['row']['data']) && !is_array($data->info['row']['data'])) {
          $message = $this->t('Sorry, we can not cast ADO with @uuid directly into proper Metadata. Check your data ROW in your CSV for set @setid for invalid data.', [
            '@uuid' => $data->info['row']['uuid'] ?? "MISSING UUID",
            '@setid' => $data->info['set_id']
          ]);
          $this->messenger->addError($message);
          return FALSE;
        }
        elseif (!isset($data->info['row']['data'])) {
          $message = $this->t('Sorry, we can not cast an ADO directly into proper Metadata. Check your data ROW in your CSV for set @setid for invalid data.',
            [
              '@setid' => $data->info['set_id'] ?? 'Undefined AMI set ID',
            ]);
          $this->messenger->addError($message);
          return FALSE;
        }

        $processed_metadata = $this->AmiUtilityService->expandJson($data->info['row']['data']);
        $processed_metadata = !empty($processed_metadata) ? json_encode($processed_metadata) : NULL;
        $json_error = json_last_error();
        if ($json_error !== JSON_ERROR_NONE || !$processed_metadata) {
          $message = $this->t('Sorry, we can not cast ADO with @uuid directly into proper Metadata. Check your data ROW in your CSV for set @setid for invalid JSON data.', [
            '@uuid' => $data->info['row']['uuid'] ?? 'Undefined UUID',
            '@setid' => $data->info['set_id']
          ]);
          $this->messenger->addError($message);
          return FALSE;
        }
      }

      // If at this stage $processed_metadata is empty or Null there was a wrong
      // Manual added wrong mapping or any other User input induced error
      // We do not process further
      // Maybe someone wants to ingest FILES only without any Metadata?
      // Not a good use case so let's stop that nonsense here.

      if (empty($processed_metadata)) {
        $message = $this->t('Sorry, ADO with @uuid is empty or has wrong data/metadata. Check your data ROW in your CSV for set @setid or your Set Configuration for manually entered JSON that may break your setup.', [
          '@uuid' => $data->info['row']['uuid'] ?? 'Undefined UUID',
          '@setid' => $data->info['set_id'] ?? 'Undefined AMI set ID',
        ]);
        $this->messenger->addError($message);
        return FALSE;
      }

      $cleanvalues = [];
      // Now process Files and Nodes
      $ado_object = $data->adomapping->parents ?? NULL;

      if (($data->mapping->globalmapping ?? FALSE) == "custom") {
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
        if (isset($data->info['row']['data'][$file_column]) && is_string($data->info['row']['data'][$file_column]) && strlen(trim($data->info['row']['data'][$file_column])) >= 5) {
          $filenames = trim($data->info['row']['data'][$file_column]);
          $filenames = array_map(function($value) {
            $value = $value ?? '';
            $value = is_string($value) ? $value : '';
            return trim($value);
          }, explode(';', $filenames));

          // Someone can just pass a ";" and we end with an empty which means a while folder, remove the thing!
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
            $number_of_files = count($filenames ?? []) + $processed_files ?? 0;
            $message = $this->t('Associated Files for ADO with @uuid would be automatically processed as individual Queue items bc of their count: @count', [
              '@uuid' => $data->info['row']['uuid'] ?? 'Undefined UUID',
              '@setid' => $data->info['set_id'] ?? 'Undefined AMI set ID',
              '@count' => $number_of_files
            ]);
            $this->messenger->addStatus($message);
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
                'ops_forcemanaged_destination_file' => $data->info['ops_forcemanaged_destination_file'] ?? TRUE,
                'time_submitted' => $data->info['time_submitted'],
              ];

              // Do the file processing synchronously bc we are simulating
              $file_exists = $this->checkFileAvailability($data_file);
              if (!$file_exists) {
                $allfiles = FALSE;
                $message = $this->t('Sorry, for ADO with UUID:@uuid, File @filename at column @filecolumn would not be processed. We would Skip. Please check your CSV for set @setid.',
                  [
                    '@uuid' => $data->info['row']['uuid'] ?? 'Undefined UUID',
                    '@setid' => $data->info['set_id'] ?? 'Undefined AMI set ID',
                    '@filename' => $filename ?? 'Undefined Filename',
                    '@filecolumn' => $file_column ?? 'Undefined File CSV Column',
                  ]);
                $this->messenger->addError($message);
              }
            }
          }
        }
      }


      if (!empty($data->info['ops_skip_onmissing_file'] && $data->info['ops_skip_onmissing_file'] == TRUE && !$allfiles)) {
        $message = $this->t('We would skip ADO with UUID:@uuid because one or more files could not be processed and <em>Skip ADO processing on missing File</em> is enabled',
          [
            '@uuid' => $data->info['row']['uuid'] ?? 'Undefined UUID',
            '@setid' => $data->info['set_id'] ?? 'Undefined AMI set ID',
          ]);
        $this->messenger->addError($message);
      }
      // Only persist if we passed this.
      // True if all ok, to the best of our knowledge of course
      $ado = $this->processEntity($data, $processed_metadata);
      $parsed_json = $processed_metadata;
      return $ado;

      // We only process a CSV column IF and only if the row that contains it generated an ADO
      // or the ADO was already there.
      if (($ado || !$should_process) && !empty($file_csv_columns)) {
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
            foreach ($filenames as $filename) {
              $data_csv->info['csv_filename'] = $filename;
              // $csv_file = $this->processCSvFile($data_csv);
              //if ($csv_file) {
              //$data_csv->info['csv_file'] = $csv_file;
              // Push to the CSV  queue
              // }
            }
          }
        }
      }
    }
    catch (\Throwable $exception) {
      // Catch for not handled File processing errors.
      $message = $this->t('Error Processing ADOs with UUID @uuid for set @setid with unexpected error @error. This queue item will be skipped and will not be re-queued.',
        [
          '@setid' => $data->info['set_id'] ?? 'Undefined AMI set ID',
          '@error' => $exception->getMessage(),
          '@uuid' => $data->info['uuids'] ?? 'Unknown UUID',
        ]);
      $this->messenger->addError($message);
      return FALSE;
    }
  }

  public function getJsonPatch(): ?string {
    return $this->json_patch;
  }

  public function setJsonPatch(?string $json_patch): void {
    $this->json_patch = $json_patch;
  }

  /**
   * Processes an ADO (NODE Entity) without saving
   *
   * @param \stdClass $data
   * @param array $processed_metadata
   *
   * @return \Drupal\Core\Entity\ContentEntityInterface|\Drupal\Core\Entity\EntityInterface|\Drupal\Core\Entity\EntityPublishedInterface|false
   *   Returns ADO or FALSE if it could not be processed.
   */
  private function processEntity(\stdClass $data, array $processed_metadata) {
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

    try {
      // @TODO: $property_path, if null should return with failure. Also on the Queue worker
      if ($data->mapping->globalmapping == "custom") {
        $property_path = $data->mapping->custommapping_settings->{$data->info['row']['type']}->bundle ?? '';
      }
      else {
        $property_path = $data->mapping->globalmapping_settings->bundle ?? '';
      }

      $label_column = $data->adomapping->base->label ?? 'label';
      // Always (because of processed metadata via template) try to fetch again the mapped version
      $label = $processed_metadata[$label_column] ?? ($processed_metadata['label'] ?? NULL);
      // SOME PEOPLE USING LABEL AS AN ARRAY? GOSH.
      if (is_array($label)) {
        $label = reset($label);
        $label = is_string($label) ? $label : NULL;
      }
      elseif (is_object($label)) {
        // No guessing. People get your data right. We NULL-i-fy
        $label = NULL;
      }

      $property_path_split = explode(':', $property_path ?? '');

      if (!$property_path_split || count($property_path_split) < 2) {
        $message = $this->t('Sorry, your Bundle/Fields to Type mapping for the requested an ADO with @uuid on Set @setid are wrong. You might have an unmapped type, or might have made a larger change in your repo and deleted a Content Type. Aborting.', [
          '@uuid' => $data->info['row']['uuid'] ?? 'Undefined UUID',
          '@setid' => $data->info['set_id']
        ]);
        $this->messenger->addError($message);
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
      $sort_files = isset($processed_metadata['ap:tasks']) && isset($processed_metadata['ap:tasks']['ap:sortfiles']) ? $processed_metadata['ap:tasks']['ap:sortfiles'] : 'index';
      // We can't blindly override ap:tasks if we are dealing with an update operation. So make an exception here for only create
      // And deal with the same for update but later
      if ($op === 'create') {
        if (isset($processed_metadata['ap:tasks']) && is_array($processed_metadata['ap:tasks'])) {
          $processed_metadata['ap:tasks']['ap:sortfiles'] = $sort_files;
        }
        else {
          $processed_metadata['ap:tasks'] = [];
          $processed_metadata['ap:tasks']['ap:sortfiles'] = $sort_files;
        }
      }

      // JSON_ENCODE AGAIN!
      $jsonstring = json_encode($processed_metadata, JSON_PRETTY_PRINT, 50);

      if ($jsonstring) {
        // Correct to send Label as NULL here if the user messed up..
        $nodeValues = [
          'uuid' => $data->info['row']['uuid'],
          'type' => $bundle,
          'title' => $label,
          'uid' => $data->info['uid'],
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
            $existing = $this->entityTypeManager->getStorage('node')
              ->loadByProperties(
                ['uuid' => $data->info['row']['uuid']]
              );
            // Ups ... should never happen but what if just after checking race/condition it is gone?
            $existing_object = reset($existing);

            $vid = $this->entityTypeManager
              ->getStorage('node')
              ->getLatestRevisionId($existing_object->id());

            $node = $vid ? $this->entityTypeManager->getStorage('node')
              ->loadRevision($vid) : $existing_object;

            /** @var \Drupal\Core\Field\FieldItemInterface $field */
            $field = $node->get($field_name);
            // Ignore status for updates if status_keep == TRUE.
            if ($status && is_string($status) && $status_keep == FALSE) {
              $node->set('moderation_state', $status);
              $nodeValues['moderation_state'] = $status;
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
                      // Means new processing is adding these, and they are already in the mapping. Is safe Files enabled?
                      if ($data->info['ops_safefiles']) {
                        // We take the old file ids and merge the new ones. Nothing gets lost ever.
                        // If there were no new ones, or new ones were URLs and failed processing
                        // The $processed_metadata[$filekey] might be a string or empty!
                        $processed_metadata[$filekey] = array_merge((array) $original_value[$filekey] ?? [], is_array($processed_metadata[$filekey]) ? $processed_metadata[$filekey] : []);
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
                  foreach (StrawberryfieldJsonHelper::AS_FILE_TYPE as $as_file_type) {
                    if (isset($original_value[$as_file_type])) {
                      $processed_metadata[$as_file_type] = $original_value[$as_file_type];
                    }
                  }
                  // Now deal with Update modes.
                  if ($op_secondary == 'replace') {
                    $processed_metadata_keys = array_keys($processed_metadata);
                    // We want to avoid replacing File keys.if
                    if (isset($data->info['ops_safefiles']) && $data->info['ops_safefiles']) {
                      // This will exclude all keys that contained files initially.
                      // Making touching/deleting files basically impossible.
                      $processed_metadata_keys = array_diff($processed_metadata_keys, $original_file_mappings);
                    }

                    foreach ($processed_metadata_keys as $processed_metadata_key) {
                      $original_value[$processed_metadata_key] = $processed_metadata[$processed_metadata_key];
                    }
                    $processed_metadata = $original_value;
                  }


                  if ($op_secondary == 'append') {
                    $processed_metadata_keys = array_keys($processed_metadata);
                    // We will exclude a bunch of keys from appending since
                    // we already did a smart-ish processing moving them to
                    // processed_metadata
                    /* @TODO remove this once PHP 7 is deprecated
                     * // Symfony\Polyfill\Php80 alread implements somthing similar.
                     */
                    if (!function_exists('str_starts_with')) {
                      function str_starts_with($haystack, $needle) {
                        return (string) $needle !== '' && strncmp($haystack, $needle, strlen($needle)) === 0;
                      }
                    }
                    // New in 0.9.0. Also protect "label" and "type" those are always strings.
                    // We should never add to them.
                    $contract_keys = array_filter($processed_metadata_keys, function($value) {
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
                          $original_value[$processed_metadata_key] = [
                            ...$old,
                            ...$new
                          ];
                          $original_value[$processed_metadata_key] = _update_append_array_unique_multidimensional($original_value[$processed_metadata_key]);
                        }
                        else {
                          if (StrawberryfieldJsonHelper::arrayIsMultiSimple($old) && StrawberryfieldJsonHelper::arrayIsMultiSimple($new)) {
                            $original_value[$processed_metadata_key] = $old + $new;
                          }
                          else {
                            if (StrawberryfieldJsonHelper::arrayIsMultiSimple($old) && !StrawberryfieldJsonHelper::arrayIsMultiSimple($new)) {
                              $original_value[$processed_metadata_key] = [
                                ...[$old],
                                ...$new
                              ];
                              $original_value[$processed_metadata_key] = _update_append_array_unique_multidimensional($original_value[$processed_metadata_key]);
                            }
                            else {
                              if (!StrawberryfieldJsonHelper::arrayIsMultiSimple($old) && StrawberryfieldJsonHelper::arrayIsMultiSimple($new)) {
                                $original_value[$processed_metadata_key] = [
                                  ...$old,
                                  ...[$new]
                                ];
                                $original_value[$processed_metadata_key] = _update_append_array_unique_multidimensional($original_value[$processed_metadata_key]);
                              }
                            }
                          }
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
                  }
                  else {
                    // Only set if after all the options it was removed at all.
                    $processed_metadata['ap:tasks'] = [];
                    $processed_metadata['ap:tasks']['ap:sortfiles'] = $sort_files;
                  }

                  $itemfield->setMainValueFromArray($processed_metadata);

                  $this->setJsonPatch($this->patchJson($original_value ?? [], $processed_metadata ?? [], TRUE));
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
            }
          }
          elseif (!isset($nodeValues['moderation_state'])) {
            // Only unpublish if not moderated and status keep is FALSE.
            if ($op == 'update' && $status_keep == FALSE) {
              $node->setUnpublished();
            }
          }

          $message = $this->t('ADO %title  with UUID:@uuid on Set @setid would be @ophuman if no other warning/error exists.', [
            '@uuid' => $data->info['row']['uuid'] ?? 'Undefined UUID',
            '@setid' => $data->info['set_id'] ?? 'Undefined AMI set ID',
            '%title' => $label ?? 'Unnamed ADO',
            '@ophuman' => $op == $op_original ? static::OP_HUMAN[$op] : static::OP_HUMAN[$op] . ' and ' . static::OP_HUMAN[$op_original]
          ]);
          $this->messenger->addStatus($message);
          return $node ?? FALSE;
        }
        catch (\Throwable $exception) {
          $message = $this->t('Sorry we did all right but we would fail to have the ADO with UUID @uuid on Set @setid to be @ophuman. Something went wrong with error @e. Please check your Drupal Logs and notify your admin.', [
            '@uuid' => $data->info['row']['uuid'] ?? 'Undefined UUID',
            '@setid' => $data->info['set_id'] ?? 'Undefined AMI set ID',
            '@ophuman' => $op == $op_original ? static::OP_HUMAN[$op] : static::OP_HUMAN[$op] . ' and ' . static::OP_HUMAN[$op_original],
            '@e' => $exception->getMessage()
          ]);
          $this->messenger->addError($message);
          return FALSE;
        }
      }
      else {
        $message = $this->t('Sorry we did all right but JSON resulting at the end is flawed and we would fail to have the ADO with UUID @uuid on Set @setid to be @ophuman. This is quite strange. Please check your Drupal Logs and notify your admin.', [
          '@uuid' => $data->info['row']['uuid'] ?? 'Undefined UUID',
          '@setid' => $data->info['set_id'] ?? 'Undefined AMI set ID',
          '@ophuman' => $op == $op_original ? static::OP_HUMAN[$op] : static::OP_HUMAN[$op] . ' and ' . static::OP_HUMAN[$op_original]
        ]);
        $this->messenger->addError($message);
        return FALSE;
      }
    }
    catch (\Throwable $exception) {
      $message = $this->t('Sorry, something failed badly at a last stage and we would fail to have the ADO with UUID @uuid on Set @setid to be @ophuman with error @error. This is quite strange. Please check your Drupal Logs and notify your admin.', [
        '@uuid' => $data->info['row']['uuid'] ?? 'Undefined UUID',
        '@setid' => $data->info['set_id'] ?? 'Undefined AMI set ID',
        '@ophuman' => $op == $op_original ? (static::OP_HUMAN[$op] ?? "Undefined operation") : (static::OP_HUMAN[$op] ?? "Undefined operation"). ' and ' . (static::OP_HUMAN[$op_original] ?? "Undefined operation"),
        '@error' => $exception->getMessage(),
      ]);
      $this->messenger->addError($message);
      return FALSE;
    }
  }

  /**
   * Will return a Patched array as string using on original/new arrays.
   *
   * @param array $original
   * @param array $new
   *
   * @param bool $reverse
   *     If true we will generate an UNDO patch
   *
   * @return string
   *    On failure, it will return an empty JSON encoded object.
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
      // We do not want to make processing slower. Just return '{}';
      return '{}';
    }
    return json_encode($patch ?? []) ?? '{}';
  }

  /**
   * Processes a File and technical metadata to avoid congestion.
   *
   * @param mixed $data
   *
   */
  protected function checkFileAvailability($data) {
    try {
      $zip_file_id = is_object($data->info['zip_file']) && $data->info['zip_file'] instanceof FileInterface ? (string) $data->info['zip_file']->id() : '0';
      $file_exists = $this->AmiUtilityService->file_check_availability(trim($data->info['filename'] ?? ''),
        $data->info['zip_file'], $data->info['force_file_process']);
      if ($file_exists) {
        $message = $this->t('The referenced file @filename from Set @setid was found!',
          [
            '@setid' => $data->info['set_id'] ?? 'Unknown Set',
            '@filename' => $data->info['filename']
          ]);
        $this->messenger->addStatus($message);
        return TRUE;
      }
      else {
        $message = $this->t('Ups. The referenced file @filename from Set @setid could not be found.',
          [
            '@setid' => $data->info['set_id'] ?? 'Unknown Set',
            '@filename' => $data->info['filename']
          ]);
        $this->messenger->addError($message);
        return FALSE;
      }
    }
    catch (\Throwable $e) {
      $message = $this->t('Processing File @filename from Set @setid failed with unexpected error @error. Giving up.',
        [
          '@setid' => $data->info['set_id'] ?? 'Unknown Set',
          '@filename' => $data->info['filename'],
          '@error' => $e->getMessage(),
        ]);
      $this->messenger->addError($message);
      return FALSE;
    }
  }

  /**
   * Processes a CSV File without technical metadata. This is just for the purpose of input for the CSV queue worker
   *
   * @param mixed $data
   */
  protected function processCSvFile($data): EntityInterface|File|null {
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
          '@setid' => $data->info['set_id'] ?? 'Undefined AMI set ID',
          '@filename' => $data->info['csv_filename'],
          '@rowid' => $data->info['row']['row_id'] ?? '0',
        ]);
      $this->loggerFactory->get('ami_file')->warning($message ,[
        'setid' => $data->info['set_id'] ?? 'Undefined AMI set ID',
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
   */
  private function canProcess($data): bool|null {

    //OP can be one of
    /*
    'create' => 'Create New ADOs',
    'update' => 'Update existing ADOs',
    'patch' => 'Patch existing ADOs',
    'delete' => 'Delete existing ADOs',
    'sync' => 'Sync' with either create/update/delete as sub operation.
    */
    try {
      $op = $data->pluginconfig->op ?? NULL;
      if ($data->pluginconfig->op == 'sync') {
        // We relay on the CSV expander to set this correctly
        // We always default to create. Worst case scenario it will
        // fail bc we can't if already there
        $op = $data->info['op_secondary'] ?? 'create';
        $op = in_array($op, ['create', 'update', 'delete']) ? $op : NULL;
      }

      /** @var \Drupal\Core\Entity\ContentEntityInterface[] $existing */
      $existing = $this->entityTypeManager->getStorage('node')
        ->loadByProperties(
          ['uuid' => $data->info['row']['uuid']]
        );

      if (count($existing) && $op === 'create') {
        $message = $this->t('Sorry, you requested an ADO with UUID @uuid to be created via Set @setid. But there is already one in your repo with that UUID. So we would skip this ADO.', [
          '@uuid' => $data->info['row']['uuid'] ?? 'Undefined UUID',
          '@setid' => $data->info['set_id']
        ]);
        $this->messenger->addWarning($message);

        return NULL;
      }
      elseif (!count($existing) && $op !== 'create') {
        $message = $this->t('Sorry, the ADO with UUID @uuid you requested to be @ophuman via Set @setid does not exist. So we would skip this ADO.', [
          '@uuid' => $data->info['row']['uuid'] ?? 'Undefined UUID',
          '@setid' => $data->info['set_id'] ?? 'Undefined AMI set ID',
          '@ophuman' => static::OP_HUMAN[$op],
        ]);

        $this->messenger->addWarning($message);
        return FALSE;
      }
      $account = $data->info['uid'] == \Drupal::currentUser()
        ->id() ? \Drupal::currentUser() : $this->entityTypeManager->getStorage('user')
        ->load($data->info['uid']);

      if ($op !== 'create' && $account && $existing && count($existing) == 1) {
        $existing_object = reset($existing);
        if (!$existing_object->access('update', $account)) {
          $message = $this->t('Sorry you have no system permission to @ophuman ADO with UUID @uuid via Set @setid. So we would skip this ADO.', [
            '@uuid' => $data->info['row']['uuid'] ?? 'Undefined UUID',
            '@setid' => $data->info['set_id'] ?? 'Undefined AMI set ID',
            '@ophuman' => static::OP_HUMAN[$op],
          ]);
          $this->messenger->addWarning($message);
          return FALSE;
        }
      }
      return TRUE;
    }
    catch (\Throwable $e) {
      $message = $this->t('Sorry. @ophuman ADO with UUID @uuid via Set @setid could not be validated because of an unexpected error @error. So we would skip this ADO.', [
        '@uuid' => $data->info['row']['uuid'] ?? 'Undefined UUID',
        '@setid' => $data->info['set_id'] ?? 'Undefined AMI set ID',
        '@ophuman' => static::OP_HUMAN[$op],
        '@error' => $e->getMessage()
      ]);
      $this->messenger->addError($message);

      return FALSE;
    }
  }

  /**
   * Sets AMI set processing status
   *
   * @param string    $status
   * @param \stdClass $data
   */
  protected function setStatus(string $status, \stdClass $data): void {
    try {
      $set_id = $data->info['set_id'] ?? NULL;
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
            '@setid' => $data->info['set_id'] ?? 'Undefined AMI set ID',
            '@status' => $status ?? 'Unknown',
          ]);
          $this->loggerFactory->get('ami')->warning($message ,[
            'setid' => $data->info['set_id'] ?? 'Undefined AMI set ID',
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
      else {
        throw new \Exception("Can't set status for AMI set if no Set ID is present in the Queue item's data. Did you generate this item manually?");
      }
    }
    catch (\Throwable $exception)  {
      $message = $this->t('The original AMI Set ID @setid does not longer exist or some other error happened while setting the status: @e.',[
        '@setid' => $data->info['set_id'] ?? 'Undefined AMI set ID',
        '@e' => $exception->getMessage(),
      ]);
      // Makes no sense to log to the AMI file logger anymore?
      // Send to the global ami logger (DB)
      $this->loggerFactory->get('ami')->warning($message ,[
        'setid' => $data->info['set_id'] ?? 'Undefined AMI set ID',
        'time_submitted' => $data->info['time_submitted'] ?? '',
      ]);
    }
  }


  private function processAdoDiff(ContentEntityInterface $ado, bool $compare = FALSE):array {
    // Because we are under AJAX and deprecations (Drupal 11 +) could
    // trigger a throwable, we need to ignore those
    set_error_handler(function ($errno, $errstr) {
      // Ignore deprecations
      if ($errno === E_DEPRECATED || $errno === E_USER_DEPRECATED) {
        return true; // Return true tells PHP the error was handled internally
      }
      return false; // Pass other errors to standard PHP error handlers
    });
    $render_array = [];


    // We could instead of loading the ADO from storage, check if during this process
    // we generated a new revision on $ado. BUT. We can't assume EVERY strawberryfield
    // will have revisions. The user could create a new Field/New Bundle and set no revisions
    // SO re-loading is safer.
    $uuid = $ado->uuid();
    $viewBuilder = \Drupal::entityTypeManager()->getViewBuilder('node');
    $view_mode = \Drupal::service('format_strawberryfield.view_mode_resolver')->get($ado);
    $view_mode_link = Link::createFromRoute($this->t('@entity_view_display_label',['@entity_view_display_label' => $view_mode]),
      "entity.entity_view_display.node.view_mode",
      [
        'entity_type_id' => 'node',
        'node_type' => $ado->bundle(),
        'view_mode_name' => $view_mode
      ],
      [
        'attributes' => [
          'target' => '_blank',
          'rel' => 'noopener noreferrer',
        ]
      ]);

    $ado->preview_view_mode = $view_mode;
    $ado->in_preview = TRUE;
    $build = NULL;
    try {
      $build_array = $viewBuilder->view($ado, $view_mode);
      unset($build_array['#cache']);
      $errored = FALSE;
      $context = new RenderContext();
      $build = $this->renderer->executeInRenderContext($context, function() use ($build_array) {
        return $this->renderer->render($build_array);
      });

      if (!$context->isEmpty()) {
        // Bubbled metadata is captured inside $context instead of crashing the site
        $bubbleable_metadata = $context->pop();
      }
      $build =  (string) $build;
      $render_array['preview']['#markup'] = $build;
      $render_array['preview']['#type']= 'container';
      $render_array['preview']['#attributes']['id'] = 'ami-preview-container-ado';
    }
    catch (\Throwable $e) {
      $errored = TRUE;
      $messages = '';
      while ($e !== null) {
        $messages = $messages . '<p><em>'.$e->getMessage().'</em></p>';
        $e = $e->getPrevious();
      }
      $messages = $messages."<p>Most likely a twig template used in your View Mode is calling a native/drupal extension on invalid data provided by the previewed CSV Row/Future ADO. This does not mean the ADO won't be processed but will for sure FAIL rendering to end users. Please review your Drupal logs and any display time Metadata Display Entities (Twig Templates) used to render ADOs of this type.</p>";
      $view_mode_link_renderable = $view_mode_link->toRenderable();
      $view_mode_link_rendered = $this->renderer->renderInIsolation($view_mode_link_renderable);
      $messages = new FormattableMarkup($messages,[]);
      $render_array['error']['#markup'] = $this->t("<h3>Rendering Error on view mode <em>@link</em> with message:</h3><div>@messages</div>",
        [
          '@link' =>  new FormattableMarkup($view_mode_link_rendered, []),
          '@messages' => $messages
        ],
      );
    }


    if ($compare && $build && !$errored) {
      $errored_existing = FALSE;
      $build_existing = NULL;
      $exists_in_storage = $this->entityTypeManager->getStorage('node')->loadByProperties(['uuid'=> $uuid]);
      if (!empty($exists_in_storage)) {
        $existing_ado = reset($exists_in_storage);
        $view_mode = \Drupal::service('format_strawberryfield.view_mode_resolver')->get($existing_ado);
        $build_existing = $viewBuilder->view($existing_ado, $view_mode);
        unset($build_existing['#cache']);
        try {
          $context = new RenderContext();
          $build_existing = $this->renderer->executeInRenderContext($context, function() use ($build_existing) {
            return $this->renderer->render($build_existing);
          });

          if (!$context->isEmpty()) {
            // Bubbled metadata is captured inside $context instead of crashing the site
            $bubbleable_metadata = $context->pop();
          }
          $build_existing = (string) $build_existing;
        }
        catch (\Throwable $e) {
          $view_mode_existing = \Drupal::service('format_strawberryfield.view_mode_resolver')->get($existing_ado);
          $view_mode_link_existing = Link::createFromRoute($this->t('@entity_view_display_label',['@entity_view_display_label' => $view_mode_existing]),
            "entity.entity_view_display.node.view_mode",
            [
              'entity_type_id' => 'node',
              'node_type' => $existing_ado->bundle(),
              'view_mode_name' => $view_mode_existing
            ],
            [
              'attributes' => [
                'target' => '_blank',
                'rel' => 'noopener noreferrer',
              ]
            ]);
          $errored_existing = TRUE;
          $messages = '';
          while ($e !== null) {
            $messages = $messages . '<p><em>'.$e->getMessage().'</em></p>';
            $e = $e->getPrevious();
          }
          $messages = $messages."<p>Most likely a twig template used in your View Mode is calling a native/drupal extension on invalid data provided by the existing ADO matching the UUID provided in your preview ROW</p>.<p> Please review this AMI Set Settings, Drupal logs and any display time Metadata Display Entities (Twig Templates) used to render ADOs of this type.</p>";
          $view_mode_link_renderable = $view_mode_link_existing->toRenderable();
          $view_mode_link_rendered = $this->renderer->renderInIsolation($view_mode_link_renderable);
          $messages = new FormattableMarkup($messages,[]);
          $render_array['error_existing']['#markup'] = $this->t("<h3>Rendering Error on Existing ADO on view mode <em>@link</em> with message:</h3><div>@messages</div>",
            [
              '@link' => new FormattableMarkup($view_mode_link_rendered,[]),
              '@messages' => $messages
            ],
          );
        }
        if ($build_existing) {
          // renderer class name:
          //     Text renderers: Context, JsonText, Unified
          //     HTML renderers: Combined, Inline, JsonHtml, SideBySide
          $rendererName = 'SideBySide';

          // the Differ options
          $differOptions = new DifferOptions(
          // show how many neighbor lines; Differ::CONTEXT_ALL shows the whole file
            context: 3,
            // ignore case difference
            ignoreCase: false,
            // ignore line ending difference
            ignoreLineEnding: true,
            // ignore whitespace difference
            ignoreWhitespace: false,
            // if the input sequence is too long, give up (especially for char-level diff)
            lengthLimit: 2000,
            // when inputs are identical, render the whole content rather than an empty result
            fullContextIfIdentical: true,
          );

          // the renderer options
          $rendererOptions = new RendererOptions(
          // how detailed the rendered HTML in-line diff is? (none, line, word, char)
            detailLevel: 'word',
            // renderer language: eng, cht, chs, jpn, ...
            // or an array which has the same keys with a language file
            // check the "Custom Language" section in the readme for more advanced usage
            language: 'eng',
            // show line numbers in HTML renderers
            lineNumbers: true,
            // show a separator between different diff hunks in HTML renderers
            separateBlock: true,
            // show the (table) header
            showHeader: true,
            // render spaces/tabs as <span class="ch sp"> </span> tags (visualised via CSS)
            spaceToHtmlTag: false,
            // convert consecutive spaces to &nbsp; in HTML output
            spacesToNbsp: false,
            // HTML renderer tab width (negative = do not convert into spaces)
            tabSize: 4,
            // Combined renderer: merge replace-blocks whose changed ratio is at or below this threshold (0–1)
            mergeThreshold: 0.8,
            // Unified/Context renderers CLI colorization:
            // RendererConstant::CLI_COLOR_AUTO   = colorize if possible (default)
            // RendererConstant::CLI_COLOR_ENABLE = force colorize
            // RendererConstant::CLI_COLOR_DISABLE = force no color
            cliColorization: -1,
            // JSON renderer: emit op tags as human-readable strings instead of ints
            outputTagAsString: false,
            // JSON renderer: flags passed to json_encode()
            // see https://www.php.net/manual/en/function.json-encode.php
            jsonEncodeFlags: \JSON_UNESCAPED_SLASHES | \JSON_UNESCAPED_UNICODE,
            // word-level diff: adjacent segments joined by these characters are merged into one
            // e.g. "<del>good</del>-<del>looking</del>" → "<del>good-looking</del>"
            wordGlues: ['-', ' '],
            // return this string verbatim when the two inputs are identical; null = renderer default
            resultForIdenticals: null,
            // extra CSS classes added to the diff container <div> in HTML renderers
            wrapperClasses: ['diff-wrapper'],
          );
          $jsonResult = DiffHelper::calculate($build_existing, $build, 'Json', $differOptions); // may store the JSON result in your database
          $htmlRenderer = RendererFactory::make($rendererName, $rendererOptions);
          $result = $htmlRenderer->renderArray(json_decode($jsonResult, true));
          $render_array['preview']['#attributes']['id'] = 'ami-preview-container-ado';
          $render_array['preview']['#markup'] = $result;
        }
      }
    }
    return $render_array;
  }
}



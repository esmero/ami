<?php

namespace Drupal\ami\Form;

use Drupal\ami\AmiUtilityService;
use Drupal\Component\Datetime\TimeInterface;
use Drupal\Core\Entity\ContentEntityConfirmFormBase;
use Drupal\Core\Entity\EntityRepositoryInterface;
use Drupal\Core\Entity\EntityTypeBundleInfoInterface;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\TempStore\PrivateTempStoreFactory;
use Drupal\Core\Url;
use Drupal\ami\Entity\amiSetEntity;
use Symfony\Component\DependencyInjection\ContainerInterface;

/**
 * Form controller for the MetadataDisplayEntity entity delete form.
 *
 * @ingroup ami
 */
class amiSetEntityProcessForm extends ContentEntityConfirmFormBase {

  /**
   * @var \Drupal\ami\AmiUtilityService
   */
  protected $AmiUtilityService;

  /**
   * Private Store used to keep the Set Processing Status/count
   *
   * @var \Drupal\Core\TempStore\PrivateTempStore
   */
  protected $statusStore;

  /**
   * Constructs a ContentEntityForm object.
   *
   * @param \Drupal\Core\Entity\EntityRepositoryInterface $entity_repository
   *   The entity repository service.
   * @param \Drupal\Core\Entity\EntityTypeBundleInfoInterface|null $entity_type_bundle_info
   *   The entity type bundle service.
   * @param \Drupal\Component\Datetime\TimeInterface|null $time
   *   The time service.
   * @param \Drupal\ami\AmiUtilityService $ami_utility
   * @param \Drupal\Core\TempStore\PrivateTempStoreFactory $temp_store_factory
   */
  public function __construct(
    EntityRepositoryInterface $entity_repository,
    EntityTypeBundleInfoInterface $entity_type_bundle_info = NULL,
    TimeInterface $time = NULL, AmiUtilityService $ami_utility, PrivateTempStoreFactory $temp_store_factory) {
    parent::__construct($entity_repository, $entity_type_bundle_info, $time);
    $this->AmiUtilityService = $ami_utility;
    $this->statusStore = $temp_store_factory->get('ami_queue_status');
  }

  /**
   * {@inheritdoc}
   */
  public static function create(ContainerInterface $container) {
    return new static(
      $container->get('entity.repository'),
      $container->get('entity_type.bundle.info'),
      $container->get('datetime.time'),
      $container->get('ami.utility'),
      $container->get('tempstore.private')
    );
  }


  public function getQuestion() {
    return $this->t(
      'Are you sure you want to process %name?',
      ['%name' => $this->entity->label()]
    );
  }

  /**
   * {@inheritdoc}
   */
  public function getCancelUrl() {
    return new Url('entity.ami_set_entity.collection');
  }


  /**
   * {@inheritdoc}
   */
  public function submitForm(array &$form, FormStateInterface $form_state) {
    $manyfiles = $this->configFactory()->get('strawberryfield.filepersister_service_settings')->get('manyfiles') ?? 0;
    $statuses = $form_state->getValue('status', []);
    $ops_skip_onmissing_file = (bool) $form_state->getValue('skip_onmissing_file', TRUE);
    $ops_forcemanaged_destination_file = (bool) $form_state->getValue('take_control_file', TRUE);

    $csv_file_reference = $this->entity->get('source_data')->getValue();
    if (isset($csv_file_reference[0]['target_id'])) {
      /** @var \Drupal\file\Entity\File $file */
      $file = $this->entityTypeManager->getStorage('file')->load(
        $csv_file_reference[0]['target_id']
      );
    }

    // Fetch Zip file if any
    $zip_file = NULL;
    $zip_file_reference = $this->entity->get('zip_file')->getValue();
    if (isset($zip_file_reference[0]['target_id'])) {
      /** @var \Drupal\file\Entity\File $zip_file */
      $zip_file = $this->entityTypeManager->getStorage('file')->load(
        $zip_file_reference[0]['target_id']
      );
    }
    $data = new \stdClass();
    foreach ($this->entity->get('set') as $item) {
      /** @var \Drupal\strawberryfield\Plugin\Field\FieldType\StrawberryFieldItem $item */
      $data = $item->provideDecoded(FALSE);
    }
    if ($file && $data !== new \stdClass()) {
      $invalid = [];
      $SetURL = $this->entity->toUrl('canonical', ['absolute' => TRUE])
        ->toString();

      $run_timestamp = $this->time->getCurrentTime();


      $notprocessnow = $form_state->getValue('not_process_now', NULL);
      $queue_name = 'ami_ingest_ado';
      if (!$notprocessnow) {
        // These queues have no queue workers. That is intended since they
        // are always processed by the ami_ingest_ado one manually.
        $queue_name = 'ami_ingest_ado_set_' . $this->entity->id();
        // Clear the queue in case there is already data there from a failed one.
        \Drupal::queue($queue_name)->deleteQueue();
        \Drupal::queue($queue_name, TRUE)->createQueue();
        // @TODO acquire a Lock that is renewed for each queue item processing
        // To avoid same batch to be send to processing by different users at
        // the same time.
      }
      $added = [];
      $op_secondary = NULL;
      // Only applies to Update/Patch operations but for contract reasons
      // we generate all $data->info the same.
      $ops_safefiles = TRUE;

      if (isset($data->pluginconfig->op) && $data->pluginconfig->op != 'create') {
        $op_secondary = $form_state->getValue(['ops_secondary','ops_secondary_update'], 'update');
        $ops_safefiles = $form_state->getValue(['ops_secondary','ops_safefiles'], TRUE);
      }
      if ($notprocessnow) {
        $data_csv = clone $data;
        // Testing the CSV processor
        $data_csv->info = [
          'zip_file' => $zip_file,
          'csv_file' => $file,
          'set_id' => $this->entity->id(),
          'uid' => $this->currentUser()->id(),
          'status' => $statuses,
          'op_secondary' => $op_secondary,
          'ops_safefiles' => $ops_safefiles ? TRUE : FALSE,
          'log_jsonpatch' => FALSE,
          'set_url' => $SetURL,
          'attempt' => 1,
          'queue_name' => $queue_name,
          'force_file_queue' => (bool)$form_state->getValue('force_file_queue', FALSE),
          'force_file_process' => (bool)$form_state->getValue('force_file_process', FALSE),
          'manyfiles' => $manyfiles,
          'ops_skip_onmissing_file' => $ops_skip_onmissing_file,
          'ops_forcemanaged_destination_file' => $ops_forcemanaged_destination_file,
          'time_submitted' => $run_timestamp
        ];
        \Drupal::queue('ami_csv_ado')
          ->createItem($data_csv);
      }
      else {
        // Add 'uid' to $data->info to unify with new account loader at \Drupal\ami\AmiUtilityService::preprocessAmiSet
        $data->info['uid'] = $this->currentUser()->id();
        $info = $this->AmiUtilityService->preprocessAmiSet($file, $data, $invalid, FALSE);
        // Means preprocess set
        if (count($invalid)) {
          $invalid_message = $this->formatPlural(count($invalid),
            'Source data Row @row had an issue, common cause is an invalid parent.',
            '@count rows, @row, had issues, common causes are invalid parents and/or non existing referenced rows.',
            [
              '@row' => implode(', ', array_keys($invalid)),
            ]
          );
          $this->messenger()->addWarning($invalid_message);
        }
        if (!count($info)) {
          $this->messenger()->addError(
            $this->t(
              'So Sorry. Ami Set @label produced no ADOs. Please correct your source CSV data.',
              [
                '@label' => $this->entity->label(),
              ]
            )
          );
          $form_state->setRebuild();
          return;
        }

        foreach ($info as $item) {
          // We set current User here since we want to be sure the final owner of
          // the object is this and not the user that runs the queue
          $data->info = [
            'zip_file' => $zip_file,
            'row' => $item,
            'set_id' => $this->entity->id(),
            'uid' => $this->currentUser()->id(),
            'status' => $statuses,
            'op_secondary' => $op_secondary,
            'ops_safefiles' => $ops_safefiles ? TRUE : FALSE,
            'log_jsonpatch' => FALSE,
            'set_url' => $SetURL,
            'attempt' => 1,
            'queue_name' => $queue_name,
            'force_file_queue' => (bool)$form_state->getValue('force_file_queue', FALSE),
            'force_file_process' => (bool)$form_state->getValue('force_file_process', FALSE),
            'manyfiles' => $manyfiles,
            'ops_skip_onmissing_file' => $ops_skip_onmissing_file,
            'ops_forcemanaged_destination_file' => $ops_forcemanaged_destination_file,
            'time_submitted' => $run_timestamp
          ];
          $added[] = \Drupal::queue($queue_name)
            ->createItem($data);
        }
        $count = count(array_filter($added));
      }

      if ($notprocessnow) {
        $this->messenger()->addMessage(
          $this->t(
            'Set @label enqueued and processed .',
            [
              '@label' => $this->entity->label(),
            ]
          )
        );

        $processed_set_status['processed'] =  0;
        $processed_set_status['errored'] =  0;
        $processed_set_status['total'] = 0;
        // So far here, with the new CSV enqueue plugin we have no idea how many. But the CSV queue entry will fill up the gap
        $this->statusStore->set('set_' . $this->entity->id(), $processed_set_status);
        $this->entity->setStatus(amiSetEntity::STATUS_ENQUEUED);
        $this->entity->save();
        $form_state->setRedirectUrl($this->getCancelUrl());
      }
      elseif ($count) {
          $processed_set_status['processed'] =  0;
          $processed_set_status['errored'] =  0;
          $processed_set_status['total'] = $count;
          $this->statusStore->set('set_' . $this->entity->id(), $processed_set_status);
          $this->submitBatch($form_state, $queue_name, $count);
      }
      else {
        $this->messenger()->addError(
          $this->t(
            'So Sorry. Ami Set @label has issues, is either empty or could not be sent to processing. Please check your CSV, correct or delete and generate a new AMI set.',
            [
              '@label' => $this->entity->label(),
            ]
          )
        );
        $form_state->setRebuild();
      }
    }
    else {
      $this->messenger()->addError(
        $this->t(
          'So Sorry. Ami Set @label has incorrect Metadata and/or has its CSV file missing. Please correct or delete and generate a new AMI set.',
          [
            '@label' => $this->entity->label(),
          ]
        )
      );
      $form_state->setRebuild();
    }
  }


  /**
   * {@inheritdoc}
   */
  public function buildForm(array $form, FormStateInterface $form_state) {
    // Read Config first to get the Selected Bundles based on the Config
    // type selected. Based on that we can set Moderation Options here

    $data = new \stdClass();
    foreach ($this->entity->get('set') as $item) {
      /** @var \Drupal\strawberryfield\Plugin\Field\FieldType\StrawberryFieldItem $item */
      $data = $item->provideDecoded(FALSE);
    }
    if ($data !== new \stdClass()) {
      // Only Show this form if we got data from the SBF field.
      if ($data->mapping->globalmapping == "custom") {
        foreach($data->mapping->custommapping_settings as $key => $mappings) {
          $bundles[] = $mappings->bundle ?? NULL;
        }
      }
      else {
        $bundles[] = $data->mapping->globalmapping_settings->bundle ?? NULL;
      }
      $bundles = array_values(array_unique($bundles));
      // we can't assume the user did not mess with the AMI set data?
      $op = $data->pluginconfig->op ?? NULL;
      $ops = [
        'create',
        'update',
        'patch',
        'sync',
      ];
      $ops_update = [
        'replace' =>  $this->t("Replace Update. Will replace JSON keys found in an ADO's configured target field(s) with new JSON values. Not provided JSON keys will be kept."),
        'update' =>  $this->t("Complete (All JSON keys) Update. Will update a complete existing ADO's JSON data with all new JSON data."),
        'append' =>  $this->t("Append Update. Will append values to existing JSON key(s) in an ADO's configured target field(s). New JSON keys will be added too."),
      ];
      $ops_update_sync = [
        'update' =>  $this->t("Complete Update. Sync operations perform a complete (All JSON keys) Update. A complete existing ADO's JSON data with be replaced with new JSON data."),
      ];

      if (!in_array($op, $ops)) {
        $form['status'] = [
          '#tree' => TRUE,
          '#type' => 'fieldset',
          '#title' =>  $this->t(
            'Error'
          ),
          '#markup' => $this->t(
            'Sorry. This AMI set has no right Operation (Create, Update, Patch, Sync) set. Please fix this or contact your System Admin to fix it.'
          ),
        ];
        return $form;
      }
      // Updates can be normal update, replace and append
      if ($op == 'update') {
        $form['ops_secondary'] = [
          '#tree' => TRUE,
          '#type' => 'fieldset',
          '#title' => $this->t('Desired type of <em><b>@op</b></em> operation.',
            ['@op' => $op]),
          'ops_safefiles' => [
             '#type' => 'checkbox',
             '#title' => $this->t("Do not touch existing files"),
             '#description' => $this->t("If enabled, update operations will not be able to remove/change/destroy any files already present in an ADO. Enabled by default for your own safety."),
             '#default_value' => TRUE,
          ],
          'ops_secondary_update' => [
            '#type' => 'select',
            '#title' => $this->t('Update Operation'),
            '#description' => $this->t(
            'Please review the <a href="https://docs.archipelago.nyc/1.4.0/ami_update/">AMI Update Sets Documentation</a> before proceeding, and consider first testing your planned updates against a single row/object CSV before executing updates across a larger batch of objects. There is no "undo" operation for AMI Update Sets.'),
            '#options' => $ops_update,
            '#default_value' => 'replace',
            '#wrapper_attributes' => [
              'class' => ['container-inline'],
            ],
          ],
        ];
      }
      if ($op == 'sync') {
        $form['ops_secondary'] = [
          '#tree' => TRUE,
          '#type' => 'fieldset',
          '#title' => $this->t('Desired type of <em><b>@op</b></em> operation.',
            ['@op' => $op]),
          'ops_safefiles' => [
            '#type' => 'checkbox',
            '#title' => $this->t("Do not touch existing files"),
            '#description' => $this->t("Sync Operations are not file save, and any existing ADO to be updated will get their files replaced."),
            '#default_value' => FALSE,
            '#disabled' => TRUE,
          ],
          'ops_secondary_update' => [
            '#type' => 'select',
            '#disabled' => TRUE,
            '#title' => $this->t('Update Operation'),
            '#description' => $this->t(
              'Please review the <a href="https://docs.archipelago.nyc/1.4.0/ami_update/">AMI Update Sets Documentation</a> before proceeding, and consider first testing your planned updates against a single row/object CSV before executing updates across a larger batch of objects. There is no "undo" operation for AMI Update Sets.'),
            '#options' => $ops_update_sync,
            '#default_value' => 'update',
            '#wrapper_attributes' => [
              'class' => ['container-inline'],
            ],
            ]
          ];
      }

      /* Give users a view of Free space in temporary */
      $bytes = disk_free_space(\Drupal::service('file_system')->getTempDirectory());
      $si_prefix = array( 'B', 'KB', 'MB', 'GB', 'TB', 'EB', 'ZB', 'YB' );
      $base = 1024;
      $class = min((int)log($bytes , $base) , count($si_prefix) - 1);

      $form['skip_onmissing_file'] = [
        '#type' => 'checkbox',
        '#title' => $this->t("Skip ADO processing on missing File"),
        '#description' => $this->t("If enabled a referenced missed file or one that can not be processed from the source, remote or local will make AMI skip the affected ROW. Enabled by default for better QA during processing."),
        '#default_value' => TRUE,
      ];
      $config = \Drupal::config('strawberryfield.general');
      if ($config->get('override_persistent_storage') === TRUE) {
        $form['take_control_file'] = [
          '#type'          => 'checkbox',
          '#title'         => $this->t("Let Archipelago organize my files"),
          '#description'   => $this->t(
            "Enabled by default. All files referenced in this AMI set will be copied into an Archipelago managed location and sanitized.<br> Danger: If disabled files that share source location with configured <em>Storage Scheme for Persisting Files</em> for this repository will maintain its original location, and it will be up to the manager to ensure they are not removed from there."
          ),
          '#default_value' => TRUE,
          '#access'        => $this->currentUser()->hasPermission(
              'override file destination ami entity'
            )
            || $this->currentUser()->hasRole('administrator'),
        ];
      }

      $form['status'] = [
        '#tree' => TRUE,
        '#type' => 'fieldset',
        '#title' => $this->t('Desired ADOs statuses after this <em><b>@op</b></em> operation process.',
          ['@op' => $op]
        ),
        '#description' => $this->t('You have @free remaining free space on your Drupal temporary filesystem. Please be aware of that before running a batch with large files', [
          '@free' => sprintf('%1.2f' , $bytes / pow($base,$class)) . ' ' . $si_prefix[$class],
          ]
        ),
      ];
      if ($op == 'sync') {
        $form['status']['#title'] = $this->t('Desired statuses for newly created ADOs, after this <em><b>@op</b></em> operation process. Existing ones that are to be updated will keep their status.',
          ['@op' => $op]);
      }
      $access = TRUE;
      foreach($bundles as $propertypath) {
        // First Check which SBF bearing bundles the user has access to.
        $allowed_bundles = $this->AmiUtilityService->getBundlesAndFields();
        if (isset($allowed_bundles[$propertypath])) {
          $split = explode(':', $propertypath);
          $form['status'] += $this->getModerationElementForBundle($split[0]);
        }
        else {
          $access = $access && FALSE;
        }
      }
      if (!$access) {
        $form['status'] = [
          '#tree' => TRUE,
          '#type' => 'fieldset',
          '#title' =>  $this->t(
            'Error'
          ),
          '#markup' => $this->t(
            'Sorry. You have either no permissions to create ADOs of some configured <em>bundles</em> (Content Types) or the <em>bundles</em> are non existent in this system. Correct your CSV data or ask for access. You can also ask an administrator to process the set for you.'
          ),
        ];
        return $form;
      }
      $notprocessnow = $form_state->getValue('not_process_now', NULL);

      $form['not_process_now'] = [
        '#type' => 'checkbox',
        '#title' => $this->t(
          'Enqueue but do not process Batch in realtime.'
        ),
        '#description' => $this->t(
          'Check this to enqueue but not trigger the interactive Batch processing. Cron or any other mechanism you have enabled will do the actual operation. This queue is shared by all AMI Sets in this repository and will be processed on a First-In First-Out basis.'
        ),
        '#required' => FALSE,
        '#default_value' => !empty($notprocessnow) ? $notprocessnow : FALSE,
      ];
      $form['force_file_queue'] = [
        '#type' => 'checkbox',
        '#title' => $this->t(
          'Force every File attached to an ADO to be processed in its own Queue item.'
        ),
        '#description' => $this->t(
          'Warning: This may make your ingest slower. Check this to force every file attached to an ADO to be downloaded and characterized as an independent process. This bypasses the <a href="@url">Number of files Global setting</a> that would otherwise trigger this behavior.',
          ['@url' => Url::fromRoute('strawberryfield.file_persister_settings_form')->toString()]
        ),
        '#required' => FALSE,
        '#default_value' => FALSE,
      ];
      $form['force_file_process'] = [
        '#type' => 'checkbox',
        '#title' => $this->t(
          'Re download and reprocess every file'
        ),
        '#description' => $this->t(
          'Check this to force every file attached to an ADO to be downloaded and characterized again, even if on a previous Batch run that data was already generated for reuse. IMPORTANT: Needed if e.g the URL of a file is the same but the remote source changed, if you have custom code that modifies the backend naming strategy of files.'
        ),
        '#required' => FALSE,
        '#default_value' => FALSE,
      ];
    }
    return $form + parent::buildForm($form, $form_state);
  }

  /*
  * Process queue(s) with batch.
  *
  * @param \Drupal\Core\Form\FormStateInterface $form_state
  * @param $queue
  */
  public function submitBatch(FormStateInterface $form_state, $queue_name) {
    $batch = [
      'title' => $this->t('Batch processing your Set'),
      'operations' => [],
      'finished' => ['\Drupal\ami\AmiBatchQueue', 'finish'],
      'progress_message' => t('Processing Set @current of @total. Estimated time left: @estimate, elapsed: @elapsed.'),
    ];
    $batch['operations'][] = [
      '\Drupal\ami\AmiBatchQueue::takeOne',
      [$queue_name, $this->entity->id()],
    ];
    $this->entity->setStatus(\Drupal\ami\Entity\amiSetEntity::STATUS_PROCESSING);
    $this->entity->save();
    batch_set($batch);
  }

  /**
   * Provides a Moderation Selection for a given Bundle.
   *
   * @param string $bundle
   *
   * @return array
   */
  protected function getModerationElementForBundle(string $bundle) {
    $element = [];
    // Simplest way for this is to create a fake Node of type bundle
    $entity = $this->entityTypeManager->getStorage('node')->create(array(
      'type'  => $bundle,
    ));
    // previously we were accessing a protected method ->type and then ->label(), that would explode in PHP 8.4. So we do this now.
    $bundle_label = $this->entityTypeManager->getStorage('node_type')->load($bundle)->label() ?? $bundle;

    if (\Drupal::moduleHandler()->moduleExists('content_moderation')) {
      /** @var \Drupal\content_moderation\ModerationInformation $moderationInformation */
      $moderationInformation = \Drupal::service('content_moderation.moderation_information');
      if ($moderationInformation->canModerateEntitiesOfEntityType($entity->getEntityType()) && $moderationInformation->getWorkflowForEntity($entity)) {
        $default = $moderationInformation->getOriginalState($entity);
        /** @var \Drupal\workflows\Transition[] $transitions */
        $transitions = \Drupal::service('content_moderation.state_transition_validation')
          ->getValidTransitions($entity, $this->currentUser());
        foreach ($transitions as $transition) {
          $transition_to_state = $transition->to();
          $transition_labels[$transition_to_state->id()] = $transition_to_state->label();
        }
        $element += [
          '#type' => 'container',
          $bundle => [
            '#type' => 'select',
            '#title' => $this->t('Statuses for @bundle', ['@bundle' => $bundle_label]),
            '#options' => $transition_labels,
            '#default_value' => $default->id(),
            '#access' => !empty($transition_labels),
            '#wrapper_attributes' => [
              'class' => ['container-inline'],
            ],
          ],
        ];
      }
    }
    // All Nodes can be published/unpublished and we are focusing on Nodes.
    if (empty($element)) {
      $element =  [
        '#type' => 'container',
        'state' => [
          '#type' => 'select',
          '#title' => $this->t('Status for @bundle', ['@bundle' => $bundle_label]),
          '#options' => [
            0 => t('Unpublished'),
            1 => t('Published'),
          ],
          '#default_value' => 0,
          '#wrapper_attributes' => [
            'class' => ['container-inline'],
          ],
        ],
      ];
    }

    return $element;
  }

}


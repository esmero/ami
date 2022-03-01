<?php

namespace Drupal\ami\Form;

use Drupal\ami\AmiLoDService;
use Drupal\ami\AmiUtilityService;
use Drupal\Component\Datetime\TimeInterface;
use Drupal\Core\Ajax\AjaxResponse;
use Drupal\Core\Ajax\OpenOffCanvasDialogCommand;
use Drupal\Core\Entity\ContentEntityConfirmFormBase;
use Drupal\Core\Entity\EntityRepositoryInterface;
use Drupal\Core\Entity\EntityTypeBundleInfoInterface;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Messenger\MessengerInterface;
use Drupal\Core\Url;
use Symfony\Component\DependencyInjection\ContainerInterface;

/**
 * Form controller for the MetadataDisplayEntity entity delete form.
 *
 * @ingroup ami
 */
class amiSetEntityReconcileForm extends ContentEntityConfirmFormBase {

  /**
   * @var \Drupal\ami\AmiUtilityService
   */
  protected $AmiUtilityService;

  /**
   * @var \Drupal\ami\AmiLoDService
   */
  protected $AmiLoDService;

  /**
   * Constructs a ContentEntityForm object.
   *
   * @param \Drupal\Core\Entity\EntityRepositoryInterface $entity_repository
   *   The entity repository service.
   * @param \Drupal\Core\Entity\EntityTypeBundleInfoInterface|null $entity_type_bundle_info
   *   The entity type bundle service.
   * @param \Drupal\Component\Datetime\TimeInterface|null $time
   *   The AMI Utility service.
   * @param \Drupal\ami\AmiUtilityService $ami_utility
   *  The AMI LoD service.
   * @param \Drupal\ami\AmiLoDService $ami_lod
   */
  public function __construct(
    EntityRepositoryInterface $entity_repository,
    EntityTypeBundleInfoInterface $entity_type_bundle_info = NULL,
    TimeInterface $time = NULL, AmiUtilityService $ami_utility, AmiLoDService $ami_lod) {
    parent::__construct($entity_repository, $entity_type_bundle_info, $time);
    $this->AmiUtilityService = $ami_utility;
    $this->AmiLoDService = $ami_lod;
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
      $container->get('ami.lod'),
      $container->get('strawberryfield.utility')
    );
  }

  public function getConfirmText() {
    return $this->t('Process LoD from Source');
  }

  public function getQuestion() {
    return $this->t(
      'Are you sure you want to Reconcile Lod for %name?',
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
      // we can't assume the user did not mess with the AMI set data?
      $op = $data->pluginconfig->op ?? NULL;
      $ops = [
        'create',
        'update',
        'patch',
      ];
      if (!in_array($op, $ops)) {
        $form['status'] = [
          '#tree' => TRUE,
          '#type' => 'fieldset',
          '#title' =>  $this->t(
            'Error'
          ),
          '#markup' => $this->t(
            'Sorry. This AMI set has no right Operation (Create, Update, Patch) set. Please fix this or contact your System Admin to fix it.'
          ),
        ];
        return $form;
      }
      $csv_file_processed = $this->entity->get('processed_data')->getValue();
      if (isset($csv_file_processed[0]['target_id'])) {
        /** @var \Drupal\file\Entity\File $file */
        $lod_file = $this->entityTypeManager->getStorage('file')->load(
          $csv_file_processed[0]['target_id']
        );
        if ($lod_file) {
          $form['status'] = [
            '#tree' => TRUE,
            '#type' => 'fieldset',
            '#title' =>  $this->t(
              'You have LoD reconciled data!'
            ),
            '#markup' => $this->t(
              'Please use the Edit Reconciled LoD tab to Fix/Correct/Enhance or '
            ),
          ];
          $form['status']['download'] = Url::fromUri(file_create_url($lod_file->getFileUri()))->toRenderArray();
          $form['status']['download']['#type'] = 'link';
          $form['status']['download']['#title'] = $this->t('Download LoD CSV');

        }
      }
      $form['mapping'] = [
        '#tree' => TRUE,
        '#type' => 'fieldset',
        '#title' => $this->t('LoD reconciling'),
      ];
      $access = TRUE;
      $csv_file_reference = $this->entity->get('source_data')->getValue();
      if (isset($csv_file_reference[0]['target_id'])) {
        /** @var \Drupal\file\Entity\File $file */
        $file = $this->entityTypeManager->getStorage('file')->load(
          $csv_file_reference[0]['target_id']
        );
        if ($file) {

          $reconcile_column_settings = $form_state->getValue(['mapping', 'lod_columns'], NULL) ?? ($data->reconcileconfig->columns ?? []);
          $reconcile_column_settings = (array) $reconcile_column_settings;
          $file_data_all = $this->AmiUtilityService->csv_read($file);
          $column_keys = $file_data_all['headers'] ?? [];
          $form['mapping']['lod_columns'] = [
            '#type' => 'select',
            '#title' => $this->t('Select which columns you want to reconcile against LoD providers'),
            '#default_value' => $reconcile_column_settings,
            '#options' => array_combine($column_keys, $column_keys),
            '#size' => count($column_keys),
            '#multiple' => TRUE,
            '#description' => $this->t('Columns that contain data you want to reconcile against LoD providers'),
            '#empty_option' => $this->t('- Please select columns -'),
            '#ajax' => [
              'callback' => [$this, 'lodOptionsAjaxCallback'],
              'wrapper' => 'lod-options-wrapper',
              'event' => 'change',
            ],
          ];
          $form['lod_options'] = [
            '#type' => 'hidden',
            '#prefix' => '<div id="lod-options-wrapper">',
            '#suffix' => '</div>',
          ];
          $reconcile_mapping_settings = $form_state->getValue(['mapping', 'lod_columns'], NULL) ?? ($data->reconcileconfig->mappings ?? NULL);
          $reconcile_mapping_settings = (array) $reconcile_mapping_settings;
          if ($reconcile_column_settings) {

            $source_options = $reconcile_column_settings;
            $column_options = [
              'loc;subjects;thing' => 'LoC subjects(LCSH)',
              'loc;names;thing' => 'LoC Name Authority File (LCNAF)',
              'loc;genreForms;thing' => 'LoC Genre/Form Terms (LCGFT)',
              'loc;graphicMaterials;thing' => 'LoC Thesaurus of Graphic Materials (TGN)',
              'loc;geographicAreas;thing' => 'LoC MARC List for Geographic Areas',
              'loc;relators;thing' => 'LoC Relators Vocabulary (Roles)',
              'loc;rdftype;CorporateName' => 'LoC MADS RDF by type: Corporate Name',
              'loc;rdftype;PersonalName' => 'LoC MADS RDF by type: Personal Name',
              'loc;rdftype;FamilyName' => 'LoC MADS RDF by type: Family Name',
              'loc;rdftype;Topic' => 'LoC MADS RDF by type: Topic',
              'loc;rdftype;GenreForm' =>  'LoC MADS RDF by type: Genre Form',
              'loc;rdftype;Geographic' => 'LoC MADS RDF by type: Geographic',
              'loc;rdftype;Temporal' =>  'LoC MADS RDF by type: Temporal',
              'loc;rdftype;ExtraterrestrialArea' => 'LoC MADS RDF by type: Extraterrestrial Area',
              'viaf;subjects;thing' => 'Viaf',
              'getty;aat;fuzzy' => 'Getty aat Fuzzy',
              'getty;aat;terms' => 'Getty aat Terms',
              'getty;aat;exact' => 'Getty aat Exact Label Match',
              'wikidata;subjects;thing' => 'Wikidata Q Items'
            ];
            $form['lod_options']['#type'] = 'fieldset';
            $form['lod_options']['#tree'] = TRUE;

            $form['lod_options']['mappings'] = [
              '#type' => 'webform_mapping',
              '#title' => $this->t('LoD Sources'),
              '#description' => $this->t(
                'Please select how your chosen Columns will be LoD reconciled'
              ),
              '#description_display' => 'before',
              '#empty_option' => $this->t('- Let AMI decide -'),
              '#empty_value' => NULL,
              '#default_value' => $reconcile_mapping_settings,
              '#required' => TRUE,
              '#destination__multiple' => TRUE,
              '#source' => $source_options,
              '#source__title' => $this->t('LoD reconcile options'),
              '#destination__title' => $this->t('LoD Authority Sources'),
              '#destination' => $column_options,
              '#destination__size' => count($column_options),
            ];
            $form['lod_options']['select_preview'] = [
              '#type' => 'select',
              '#title' => $this->t('Choose a Column to Preview'),
              '#options' => array_combine($source_options, $source_options),
              '#default_value' => $form_state->getValue(['lod_options','select_preview'])
            ];
            $form['lod_options']['preview'] = [
              '#type' => 'button',
              '#op' => 'preview',
              '#value' => $this->t('Inspect cleaned/split up column values'),
              '#ajax' => [
                'callback' => [$this, 'ajaxColumPreview'],
              ],
              /* '#states' => [
                'visible' => ['input[name="ado_context_preview"' => ['filled' => true]],
              ],*/
            ];
          }
        }
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
    }
    $form = $form + parent::buildForm($form, $form_state);
    return $form;
  }

  /**
   * {@inheritdoc}
   */
  public function submitForm(array &$form, FormStateInterface $form_state) {
    $csv_file_reference = $this->entity->get('source_data')->getValue();
    if (isset($csv_file_reference[0]['target_id'])) {
      /** @var \Drupal\file\Entity\File $file */
      $file = $this->entityTypeManager->getStorage('file')->load(
        $csv_file_reference[0]['target_id']
      );
    }

    $csv_file_processed = $this->entity->get('processed_data')->getValue();
    if (isset($csv_file_processed[0]['target_id'])) {
      /** @var \Drupal\file\Entity\File $file_lod */
      $file_lod = $this->entityTypeManager->getStorage('file')->load(
        $csv_file_processed[0]['target_id']);
      // Reset all values
      if ($file_lod) {
        $this->AmiUtilityService->csv_touch($file_lod->getFilename());
      }
    }
    else {
      $file_lod_id = $this->AmiUtilityService->csv_touch();
      $file_lod = $file_lod_id ? $this->entityTypeManager->getStorage('file')->load(
        $file_lod_id) : NULL;
      if ($file_lod) {
        $this->entity->set('processed_data', $file_lod_id);
        $this->entity->save();
      }
      else {
        $this->messenger()->addError(
          $this->t(
            'So Sorry. We could not create a new CSV to store your LoD Reconciled data for @label. Please check your filesystem permissions or contact your System Admin',
            [
              '@label' => $this->entity->label(),
            ]
          )
        );
        $form_state->setRebuild();
        return;
      }
    }

    $data = new \stdClass();
    foreach ($this->entity->get('set') as $item) {
      /** @var \Drupal\strawberryfield\Plugin\Field\FieldType\StrawberryFieldItem $item */
      $data = $item->provideDecoded(FALSE);
      // Set also the new config back
      $data->reconcileconfig = new \stdClass();
      $data->reconcileconfig->columns = $form_state->getValue(['mapping', 'lod_columns'], NULL);
      $data->reconcileconfig->mappings = $form_state->getValue(['lod_options','mappings'], NULL);
      $jsonvalue = json_encode($data, JSON_PRETTY_PRINT);
      $this->entity->set('set', $jsonvalue);
      try {
        $this->entity->save();
      }
      catch (\Exception $exception) {
        $this->messenger()->addError(
          t(
            'Ami Set LoD Settings Failed to be persisted because of @message',
            ['@message' => $exception->getMessage()]
          )
        );
        $form_state->setRebuild(TRUE);
        return;
      }
    }
    if ($file && $file_lod && $data !== new \stdClass()) {
      $domain = $this->getRequest()->getSchemeAndHttpHost();
      $mappings = $form_state->getValue(['lod_options','mappings']);
      $form_state->setRebuild(TRUE);
      $output = [];
      $output['table'] = [
        '#type' => 'table',
        '#caption' => t('Unique processed values for this column'),
      ];
      $columns = array_keys($mappings) ?? [];
      $values_per_column = $this->AmiUtilityService->provideDifferentColumnValuesFromCSV($file,
        $columns);
      $inverted = [];
      $column_map_inverted = [];
      $headers = ['original','csv_columns', 'checked'];
      foreach($values_per_column as $column => $labels) {
        foreach($labels as $label) {
          $inverted[$label] = $inverted[$label] ?? [];
          $headers = array_unique(array_merge($headers,$mappings[$column]));
          $inverted[$label] = array_unique(array_merge($inverted[$label], $mappings[$column]));
          $column_map_inverted[$label][] = $column;
          $column_map_inverted[$label] = array_unique($column_map_inverted[$label]);
        }
      }
      $normalized_mapping = [];
      foreach($mappings as $source_column => $approaches) {
        foreach($approaches as $approach) {
          $exploded =  explode(';', $approach);
          $normalized_mapping[$source_column][] = strtolower(implode('_', $exploded));
        }
      }

      // This will be used to fetch the right values when passing to the twig template
      // Could be read from the config but this is faster during process.

      // Clears old values before processing new ones.
      $this->AmiLoDService->cleanKeyValuesPerAmiSet($this->entity->id());
      $this->AmiLoDService->setKeyValueMappingsPerAmiSet($normalized_mapping, $this->entity->id());


      ksort($inverted,SORT_NATURAL);
      foreach($headers as &$header) {
        // same is done in \Drupal\ami\Plugin\QueueWorker\LoDQueueWorker::processItem
        $exploded =  explode(';', $header);
        $header = strtolower(implode('_', $exploded));
      }

      if (!count($inverted)) {
        $this->messenger()->addError(
          $this->t(
            'So Sorry. Your Ami Set @label selected column(s) has(have) not values. Please select of Columns and inspect them before submitting',
            [
              '@label' => $this->entity->label(),
            ]
          )
        );
        $form_state->setRebuild();
        return;
      }
      // Append the header to the CSV
      $file_lod_id = $this->AmiUtilityService->csv_append(['headers' => $headers, 'data' => []], $file_lod, NULL, TRUE );
      $SetURL = $this->entity->toUrl('canonical', ['absolute' => TRUE])
        ->toString();

      $notprocessnow = $form_state->getValue('not_process_now', NULL);
      $queue_name = 'ami_lod_ado';
      if (!$notprocessnow) {
        // This queues have no queue workers. That is intended since they
        // are always processed by the ami_ingest_ado one manually.
        $queue_name = 'ami_ingest_lod_set_' . $this->entity->id();
        // Destroy first here.
        \Drupal::queue($queue_name)->deleteQueue();
        \Drupal::queue($queue_name, TRUE)->createQueue();
        // @TODO acquire a Lock that is renewed for each queue item processing
        // To avoid same batch to be send to processing by different users at
        // the same time.
      }
      $added = [];
      foreach ($inverted as $label => $lodconfig) {
        // We pass all reconciliation endpoints to a single queue item per label
        // because we need to create a CSV row per label
        // If we split it would get super messy?
        // Or we could use key storage instead too (i guess)
        // @TODO explore single LoD endpoint per queue item
        $data->info = [
          'label' => $label,
          'domain' => $domain,
          'headers' => $headers,
          'csv_columns' => $column_map_inverted[$label],
          'normalized_mappings' => $normalized_mapping,
          'lodconfig' => $lodconfig,
          'set_id' => $this->entity->id(),
          'csv' => $file_lod_id,
          'uid' => $this->currentUser()->id(),
          'set_url' => $SetURL,
          'attempt' => 1,
        ];
        $added[] = \Drupal::queue($queue_name)
          ->createItem($data);
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
        $form_state->setRedirectUrl($this->getCancelUrl());
      }
      else {
        $count = count(array_filter($added));
        if ($count) {
          $form_state->setRebuild();
          $this->submitBatch($form_state, $queue_name);
        }
      }
    }
    else {
      $this->messenger()->addError(
        $this->t(
          'So Sorry. Ami Set @label has incorrect Metadata and/or has its Source CSV file missing or its LoD Reconciled CSV file missing. Please correct or delete and generate a new AMI set.',
          [
            '@label' => $this->entity->label(),
          ]
        )
      );
      $form_state->setRebuild();
    }
  }

  /*
  * Process queue(s) with batch.
  *
  * @param \Drupal\Core\Form\FormStateInterface $form_state
  * @param $queue
  */
  public function submitBatch(FormStateInterface $form_state, $queue_name) {
    $batch = [
      'title' => $this->t('Batch processing LoD Reconciling'),
      'operations' => [],
      'finished' => ['\Drupal\ami\AmiLoDBatchQueue', 'finish'],
      'progress_message' => t('Processing Set @current of @total.'),
    ];
    $batch['operations'][] = [
      '\Drupal\ami\AmiLoDBatchQueue::takeOne',
      [$queue_name, $this->entity->id()],
    ];
    /* Because batch set will run on Ajax
    and we want that afterwards the form is fresh
    we remove $userInput to force a rebuild */
    $userInput = $form_state->getUserInput();
    $keys = $form_state->getCleanValueKeys();

    $newInputArray = [];
    foreach ($keys as $key) {
      if ($key == "op")  continue;
      $newInputArray[$key] = $userInput[$key];
    }
    $form_state->setUserInput($newInputArray);

    batch_set($batch);
  }


  /**
   * Ajax callback for the plugin configuration form elements.
   *
   * @param $form
   * @param \Drupal\Core\Form\FormStateInterface $form_state
   *
   * @return array
   */
  public function lodOptionsAjaxCallback($form, FormStateInterface $form_state) {
    return $form['lod_options'] ?? [];
  }

  /**
   * AJAX callback.
   */
  public function ajaxColumPreview($form, FormStateInterface $form_state) {
    $response = new AjaxResponse();
    $form['#attached']['library'][] = 'core/drupal.dialog.off_canvas';
    $response->setAttachments($form['#attached']);

    if (!empty($form_state->getValue(['lod_options','select_preview']))) {
      $entity = $form_state->getFormObject()->getEntity();
      $csv_file_reference = $entity->get('source_data')->getValue();
      if (isset($csv_file_reference[0]['target_id'])) {
        /** @var \Drupal\file\Entity\File $file */
        $file = $this->entityTypeManager->getStorage('file')->load(
          $csv_file_reference[0]['target_id']
        );
        if ($file) {
          $file_data_all = $this->AmiUtilityService->csv_read($file);
          $output = [];
          $output['table'] = [
            '#type' => 'table',
            '#caption' => t('Unique processed values for this column'),
          ];
          $column_preview = (array) $form_state->getValue(['lod_options','select_preview']) ?? [];
          $values_per_column = $this->AmiUtilityService->provideDifferentColumnValuesFromCSV($file,
            $column_preview);
          $rows = $values_per_column[$form_state->getValue(['lod_options','select_preview'])] ?? ['Emtpy Column'];
          sort($rows, SORT_STRING);

          foreach ($rows as &$row) {
            $row = [$row];
          }
          $output['table']['#rows'] = $rows;
        }
        $response->addCommand(new OpenOffCanvasDialogCommand(t('Values for @column', [
          '@column' => reset($column_preview),
        ]),
          $output, ['width' => '30%']));
        if ($form_state->getErrors()) {
          // Clear errors so the user does not get confused when reloading.
          \Drupal::messenger()->deleteByType(MessengerInterface::TYPE_ERROR);
          $form_state->clearErrors();
        }
      }
    }
    return $response;
  }
}


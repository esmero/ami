<?php

namespace Drupal\ami\Form;

use Drupal\ami\AmiLoDService;
use Drupal\ami\AmiUtilityService;
use Drupal\Component\Datetime\TimeInterface;
use Drupal\Core\Entity\ContentEntityConfirmFormBase;
use Drupal\Core\Entity\EntityRepositoryInterface;
use Drupal\Core\Entity\EntityTypeBundleInfoInterface;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Url;
use Symfony\Component\DependencyInjection\ContainerInterface;

/**
 * Form controller for the MetadataDisplayEntity entity delete form.
 *
 * @ingroup ami
 */
class amiSetEntityReconcileCleanUpForm extends ContentEntityConfirmFormBase {


  CONST LOD_COLUMN_TO_ARGUMENTS = [
    'loc_subjects_thing' => 'loc;subjects;thing',
    'loc_names_thing' => 'loc;names;thing',
    'loc_genreforms_thing' => 'loc;genreForms;thing',
    'loc_graphicmaterials_thing' => 'loc;graphicMaterials;thing',
    'loc_geographicareas_thing' => 'loc;geographicAreas;thing',
    'loc_relators_thing' => 'loc;relators;thing',
    'loc_rdftype_corporatename' => 'loc;rdftype;CorporateName',
    'loc_rdftype_personalname' =>  'loc;rdftype;PersonalName',
    'loc_rdftype_familyname' => 'loc;rdftype;FamilyName',
    'loc_rdftype_topic' => 'loc;rdftype;Topic',
    'loc_rdftype_genreform' =>  'loc;rdftype;GenreForm',
    'loc_rdftype_geographic' => 'loc;rdftype;Geographic',
    'loc_rdftype_temporal' =>  'loc;rdftype;Temporal',
    'loc_rdftype_extraterrestrialarea' => 'loc;rdftype;ExtraterrestrialArea',
    'viaf_subjects_thing' => 'viaf;subjects;thing',
    'getty_aat_fuzzy' => 'getty;aat;fuzzy',
    'getty_aat_terms' => 'getty;aat;terms',
    'getty_aat_exact' => 'getty;aat;exact',
    'wikidata_subjects_thing' => 'wikidata;subjects;thing'
  ];
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
      $container->get('ami.lod')
    );
  }

  public function getConfirmText() {
    return $this->t('Save Current LoD Page');
  }


  public function getQuestion() {
    return $this->t(
      'Are you sure you want to Save Modified Reconcile Lod for %name?',
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
  protected function actions(array $form, FormStateInterface $form_state) {
    $actions = parent::actions($form, $form_state);
    $actions['submit_csv'] = [
          '#type' => 'submit',
          '#value' => t('Save all LoD back to CSV File'),
          '#submit' => [
            [$this, 'submitFormPersistCSV'],
          ],
        ];
    return $actions;

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
    $csv_file_processed = $this->entity->get('processed_data')->getValue();
    if (isset($csv_file_processed[0]['target_id'])) {
      /** @var \Drupal\file\Entity\File $file_lod */
      $file_lod = $this->entityTypeManager->getStorage('file')->load(
        $csv_file_processed[0]['target_id']);

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
            '#title' => $this->t(
              'Error'
            ),
            '#markup' => $this->t(
              'Sorry. This AMI set has no right Operation (Create, Update, Patch) set. Please fix this or contact your System Admin to fix it.'
            ),
          ];
          return $form;
        }
        $form['lod_cleanup'] = [
          '#tree' => TRUE,
          '#type' => 'fieldset',
          '#title' => $this->t('LoD reconciled Clean Up'),
        ];

        if ($file_lod) {
          $num_per_page = 10;
          $total_rows =  $this->AmiUtilityService->csv_count($file_lod);
          // Remove the header in the calculations.
          $total_rows = $total_rows - 1;
          $pager = \Drupal::service('pager.manager')->createPager($total_rows, $num_per_page);
          $page = $pager->getCurrentPage();
          $offset = $num_per_page * $page;
          if (PHP_VERSION_ID > 80000) {
            // @TODO fgetcsv has a bug when called after a seek, offsets on 1 always.
            // We are trying to skip the header too (but get it)
            $offset  = $offset + 2;
            // @TODO CHECK IF THIS WILL WORK ON PHP 8.x when we get there.
          }
          $file_data_all = $this->AmiUtilityService->csv_read($file_lod, $offset, $num_per_page);

          $column_keys = $file_data_all['headers'] ?? [];
          $form['lod_cleanup']['pager_top'] = ['#type' => 'pager'];
          $form['lod_cleanup']['table-row'] = [
            '#type' => 'table',
            '#tree' => TRUE,
            '#prefix' => '<div id="table-fieldset-wrapper">',
            '#suffix' => '</div>',
            '#header' => $column_keys,
            '#empty' => $this->t('Sorry, There are LoD no items or you have not a header column. Check your CSV for errors.'),
          ];
          $elements = [];
          $form['lod_cleanup']['offset'] = [
            '#type' => 'value',
            '#value' => $offset,
          ];
          $form['lod_cleanup']['num_per_page'] = [
            '#type' => 'value',
            '#value' => $num_per_page,
          ];
          $form['lod_cleanup']['column_keys'] = [
            '#type' => 'value',
            '#value' => $column_keys,
          ];
          $form['lod_cleanup']['total_rows'] = [
            '#type' => 'value',
            '#value' => $total_rows,
          ];


          foreach ($column_keys as $column) {
            if ($column !== 'original' && $column != 'csv_columns' && $column !='checked') {
              $argument_string = static::LOD_COLUMN_TO_ARGUMENTS[$column] ?? NULL;
              if ($argument_string) {
                $arguments = explode(';', $argument_string);
                $elements[$column] = [
                  '#type' => 'webform_metadata_' . $arguments[0],
                  '#title' => implode(' ', $arguments),
                ];

                if ($arguments[1] == 'rdftype') {
                  $elements[$column]['#rdftype'] = $arguments[2] ?? '';
                  $elements[$column]['#vocab'] = 'rdftype';
                }
                else {
                  $elements[$column]['#vocab'] = $arguments[1] ?? '';
                }

              }
              else {
                // Fallback to WIKIDATA
                $elements[$column] = ['#type' => 'webform_metadata_wikidata'];
              }
            }
          }
          $original_index = array_search('original', $column_keys);
          foreach ($file_data_all['data'] as $index => $row) {
            // Find the label first
            $label = $row[$original_index];
            $persisted_lod_reconciliation = $this->AmiLoDService->getKeyValuePerAmiSet($label, $this->entity->id());
            foreach($file_data_all['headers'] as $key => $header) {
              if ($header == 'original' || $header == 'csv_columns') {
                $form['lod_cleanup']['table-row'][($index - 1)][$header.'-'.($index-1)] = [
                  '#type' => 'markup',
                  '#markup' => $row[$key],
                  $header.'-'.($index-1) => [
                    '#tree' => true,
                  '#type' => 'hidden',
                  '#value' => $row[$key],
                  ]
                ];
              }
              elseif ($header == 'checked') {
                $checked = $persisted_lod_reconciliation[$header] ?? $row[$key];
                $checked = (bool) $checked;
                $form['lod_cleanup']['table-row'][($index - 1)][$header.'-'.($index-1)] = [
                  '#type' => 'checkbox',
                  '#default_value' => $checked
                  ];
              }
              else {
                // Given the incremental save option we have now
                // We need to load check first if there is
                // A Key Value equivalent of the row

                $form['lod_cleanup']['table-row'][($index - 1)][$header.'-'.($index-1)] = [
                    '#multiple' => 5,
                    '#multiple__header' => FALSE,
                    '#multiple__no_items_message' => '',
                    '#multiple__min_items' => 1,
                    '#multiple__empty_items' => 0,
                    '#multiple__sorting' => FALSE,
                    '#multiple__add_more' => FALSE,
                    '#multiple__add_more_input' => FALSE,
                    '#label__title' => 'Label',
                    '#default_value' => $persisted_lod_reconciliation[$header]['lod'] ?? json_decode($row[$key], TRUE),
                  ] +  $elements[$header];
              }
            }
          }
          \Drupal::service('plugin.manager.webform.element')->processElements($form);
          // Attach the webform library.
          $form['#attached']['library'][] = 'webform/webform.form';
          $form['lod_cleanup']['pager'] = ['#type' => 'pager'];
        }
      }
      $form = $form + parent::buildForm($form, $form_state);
      return $form;
    }
    else {
      $form['status'] = [
        '#tree' => TRUE,
        '#type' => 'fieldset',
        '#title' =>  $this->t(
          'No Reconciled LoD Found.'
        ),
        '#markup' => $this->t(
          'Start by visiting the <em>LoD Reconcile</em> tab and running a reconciliation. Once done you can come back here.'
        ),
      ];
      return $form;
    }
  }

  /**
   * {@inheritdoc}
   */
  public function submitForm(array &$form, FormStateInterface $form_state) {
    $csv_file_processed = $this->entity->get('processed_data')->getValue();
    if (isset($csv_file_processed[0]['target_id'])) /** @var \Drupal\file\Entity\File $file_lod */ {
      $file_lod = $this->entityTypeManager->getStorage('file')->load(
        $csv_file_processed[0]['target_id']);
    }
    if ($file_lod) {
      /* $lod_settings will contain this
         'lod_cleanup' => [
            'offset' => 0
            'num_per_page' => 10
            'column_keys' = > [
              "original",
              "csv_columns",
              "any_lod_vocabulary_selected",
           ...
      */
      $lod_settings = $form_state->getValue('lod_cleanup', []);
      $column_keys = $lod_settings['column_keys'] ?? [];
      $iterations = min($lod_settings['total_rows'] - $lod_settings['offset'],
        $lod_settings['num_per_page']);
      for ($id = 1; $id <= $iterations ?? 10; $id++) {
        $label = $form_state->getValue('original-' . $id, NULL);
        $csv_columns = $form_state->getValue('csv_columns-' . $id, NULL);
        $checked = $form_state->getValue('checked-' . $id, FALSE);
        $csv_columns = json_decode($csv_columns, TRUE);
        // If these do not exist, we can not process.
        if ($label && $csv_columns) {
          foreach ($column_keys as $index => $column) {
            if ($column !== 'original' && $column !== 'csv_columns' && $column !== 'checked') {
              $lod = $form_state->getValue($column . '-' . $id, NULL);
              $context_data[$column]['lod'] = $lod;
              $context_data[$column]['columns'] = $csv_columns;
              $context_data['checked'] = $checked;
              $this->AmiLoDService->setKeyValuePerAmiSet($label,
                $context_data, $this->entity->id());
            }
          }
        }
        else {
          $this->messenger()->addError(
            $this->t(
              'So Sorry. We can not process row @row. Check for missing "column_keys", wrong JSON for column_keys and/or "original" values.',
              [
                '@row' => $id,
              ]
            )
          );
        }
      }
      $this->messenger()->addMessage(
        $this->t(
          'LoD Reconciled data for @label was updated.',
          [
            '@label' => $this->entity->label(),
          ]
        )
      );
    }
    else {
      $this->messenger()->addError(
        $this->t(
          'LoD Reconciled source CSV for @label was not found. Please attach one or run Reconciliation again to generate on from your Source data',
          [
            '@label' => $this->entity->label(),
          ]
        )
      );
    }

    $form_state->setRebuild(TRUE);
  }

  /**
   * {@inheritdoc}
   */
  public function submitFormPersistCSV(array &$form, FormStateInterface $form_state) {
    // Call the parent one so we also persist the current page
    $this->submitForm($form, $form_state);
    $csv_file_processed = $this->entity->get('processed_data')->getValue();
    if (isset($csv_file_processed[0]['target_id'])) {
      /** @var \Drupal\file\Entity\File $file_lod */
      $file_lod = $this->entityTypeManager->getStorage('file')->load(
        $csv_file_processed[0]['target_id']);
      if ($file_lod) {
        $file_data_all = $this->AmiUtilityService->csv_read($file_lod);
        $column_keys = $file_data_all['headers'] ?? [];
        $original_index = array_search('original', $column_keys);
        foreach ($file_data_all['data'] as $id => &$row) {
          $label = $row[$original_index];
          $persisted_lod_reconciliation = $this->AmiLoDService->getKeyValuePerAmiSet($label, $this->entity->id());
          $checked = $persisted_lod_reconciliation['checked'] ?? NULL;
          foreach ($file_data_all['headers'] as $index => $column) {
            if ($column !== 'original' && $column !== 'csv_columns' && $column!== 'checked') {
              $lod = $persisted_lod_reconciliation[$column]['lod'] ?? NULL;
              if ($lod) {
                $row[$index] = json_encode($lod,
                    JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE) ?? '';
              }
            }
            elseif ($column === 'checked') {
              $row[$index] = $checked;
            }
          }
        }
        $this->AmiUtilityService->csv_touch($file_lod->getFilename());
        $success = $this->AmiUtilityService->csv_append($file_data_all, $file_lod, NULL, TRUE);
        if (!$success) {
          $this->messenger()->addError(
            $this->t(
              'So Sorry. We could not update the CSV to store your Fixed LoD Reconciled data for @label. Please check your filesystem permissions or contact your System Admin',
              [
                '@label' => $this->entity->label(),
              ]
            )
          );
        }
        else {
          $this->messenger()->addMessage(
            $this->t(
              'Success. your Fixed LoD Reconciled data for @label was updated and is ready to be used.',
              [
                '@label' => $this->entity->label(),
              ]
            )
          );
        }
      }
    }
    $form_state->setRebuild(TRUE);
  }
}


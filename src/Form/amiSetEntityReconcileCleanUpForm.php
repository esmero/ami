<?php

namespace Drupal\ami\Form;

use Drupal\ami\AmiLoDService;
use Drupal\ami\AmiUtilityService;
use Drupal\Component\Datetime\TimeInterface;
use Drupal\Core\Ajax\AjaxResponse;
use Drupal\Core\Ajax\OpenOffCanvasDialogCommand;
use Drupal\Core\Entity\ContentEntityConfirmFormBase;
use Drupal\Core\Entity\ContentEntityForm;
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
class amiSetEntityReconcileCleanUpForm extends ContentEntityConfirmFormBase {

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
    return $this->t('Save LoD');
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
        $access = TRUE;

        if ($file_lod) {
          $file_data_all = $this->AmiUtilityService->csv_read($file_lod);
          $column_keys = $file_data_all['headers'] ?? [];

          $form['lod_cleanup']['table-row'] = [
            '#type' => 'table',
            '#tree' => TRUE,
            '#prefix' => '<div id="table-fieldset-wrapper">',
            '#suffix' => '</div>',
            '#header' => $column_keys,
            '#empty' => $this->t('Sorry, There are LoD no items!'),
          ];
          foreach ($column_keys as $column) {
            $arguments =  explode('_', $column);
            if ($column !== 'original' && $column != 'csv_columns') {
              $elements[$column] = [
                '#type' => 'webform_metadata_' . $arguments[0],
                '#title' => implode(' ' , $arguments),
              ];
              if ($arguments[1] == 'rdftype') {
                $elements[$column]['#rdftype'] = $arguments[2] ?? '';
                $elements[$column]['#vocab'] = 'rdftype';
              }
            }
          }

          foreach ($file_data_all['data'] as $index => $row) {
            foreach($file_data_all['headers'] as $key => $header) {
              if ($header == 'original' || $header == 'csv_columns') {
                $form['lod_cleanup']['table-row'][$index - 1][$header.'-'.$index] = [
                  '#type' => 'markup',
                  '#markup' => $row[$key],
                ];
              }
              else {
                $form['lod_cleanup']['table-row'][$index - 1][$header.'-'.$index] = [
                    '#multiple' => 5,
                    '#multiple__header' => FALSE,
                    '#multiple__no_items_message' => '',
                    '#multiple__min_items' => 1,
                    '#multiple__empty_items' => 0,
                    '#multiple__sorting' => FALSE,
                    '#multiple__add_more' => FALSE,
                    '#multiple__add_more_input' => FALSE,
                    '#label__title' => 'Label',
                    '#default_value' => json_decode($row[$key], TRUE),
                  ] +  $elements[$header];

              }
            }
          }
          \Drupal::service('plugin.manager.webform.element')->processElements($form);
          // Attach the webform library.
          $form['#attached']['library'][] = 'webform/webform.form';
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
    if (isset($csv_file_processed[0]['target_id'])) {
      /** @var \Drupal\file\Entity\File $file_lod */
      $file_lod = $this->entityTypeManager->getStorage('file')->load(
        $csv_file_processed[0]['target_id']);
      if ($file_lod) {
        $file_data_all = $this->AmiUtilityService->csv_read($file_lod);
        $column_keys = $file_data_all['headers'] ?? [];
        foreach ($file_data_all['data'] as $id => &$row) {
          foreach ($file_data_all['headers'] as $index => $column) {
            if ($column !== 'original' && $column != 'csv_columns') {
              $lod = $form_state->getValue($column . '-' . ((int)$id), NULL);
              $row[$index] = json_encode($lod,
                  JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE) ?? '';
              $context_data[$column]['lod'] = $lod;
              $context_data[$column]['columns'] = json_decode($row[1], TRUE);
              $this->AmiUtilityService->setKeyValuePerAmiSet($row[0], $context_data, $this->entity->id());
            }
          }
        }
        $file_lod_id = $this->AmiUtilityService->csv_touch($file_lod->getFilename());
        $success = $this->AmiUtilityService->csv_append($file_data_all, $file_lod,NULL, TRUE);
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


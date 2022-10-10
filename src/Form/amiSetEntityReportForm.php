<?php

namespace Drupal\ami\Form;

use Drupal\ami\AmiLoDService;
use Drupal\ami\AmiUtilityService;
use Drupal\Component\Datetime\TimeInterface;
use Drupal\Core\Entity\ContentEntityConfirmFormBase;
use Drupal\Core\Entity\ContentEntityForm;
use Drupal\Core\Entity\EntityRepositoryInterface;
use Drupal\Core\Entity\EntityTypeBundleInfoInterface;
use Drupal\Core\File\FileSystemInterface;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Url;
use Symfony\Component\DependencyInjection\ContainerInterface;

/**
 * Form controller for the MetadataDisplayEntity entity delete form.
 *
 * @ingroup ami
 */
class amiSetEntityReportForm extends ContentEntityForm {

  /**
   * @var \Drupal\ami\AmiUtilityService
   */
  protected $AmiUtilityService;

  /**
   * File system service.
   *
   * @var \Drupal\Core\File\FileSystemInterface
   */
  protected $fileSystem;

  /**
   * Constructs a ContentEntityForm object.
   *
   * @param \Drupal\Core\Entity\EntityRepositoryInterface          $entity_repository
   *   The entity repository service.
   * @param \Drupal\Core\Entity\EntityTypeBundleInfoInterface|null $entity_type_bundle_info
   *   The entity type bundle service.
   * @param \Drupal\Component\Datetime\TimeInterface|null          $time
   *   The AMI Utility service.
   * @param \Drupal\ami\AmiUtilityService                          $ami_utility
   *   The AMI LoD service.
   * @param \Drupal\Core\File\FileSystemInterface                  $file_system
   */
  public function __construct(
    EntityRepositoryInterface $entity_repository,
    EntityTypeBundleInfoInterface $entity_type_bundle_info = NULL,
    TimeInterface $time = NULL, AmiUtilityService $ami_utility, FileSystemInterface $file_system) {
    parent::__construct($entity_repository, $entity_type_bundle_info, $time);
    $this->AmiUtilityService = $ami_utility;
    $this->fileSystem = $file_system;
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
      $container->get('file_system')
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
    $actions['submit_download'] = [
      '#type' => 'submit',
      '#value' => t('Download Logs'),
      '#submit' => [
        [$this, 'submitFormDownload'],
      ],
    ];
    return [];
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
    $logfilename = "private://ami/logs/set{$this->entity->id()}.log";
    $logfilename = $this->fileSystem->realpath($logfilename);
    $last_op = NULL;
    if ($logfilename !== FALSE && file_exists($logfilename)) {
      $fp = fopen($this->fileSystem->realpath($logfilename), 'r');
      $pos = -2; // Skip final new line character (Set to -1 if not present)
      $rows = [];
      $currentLine = '';
      $line_count = 0;
      while (-1 !== fseek($fp, $pos, SEEK_END)) {
        $char = fgetc($fp);
        if (PHP_EOL == $char) {
          $line_count++;
          $currentLineExpanded = json_decode($currentLine, TRUE);
          if (json_last_error() == JSON_ERROR_NONE) {
            $row = [];
            $row['datetime'] = $currentLineExpanded['datetime'];
            $row['level'] = $currentLineExpanded['level_name'];
            $row['message'] = $this->t($currentLineExpanded['message'], []);
            $row['details']  = json_encode($currentLineExpanded['context']);
            $rows[] = $row;
          }
          $currentLine = '';
        }
        else {
          $currentLine = $char . $currentLine;
        }
        $pos--;
        if ($line_count == 50) {
          break;
        }
      }

     $currentLineExpanded = json_decode($currentLine, TRUE);
      if (json_last_error() == JSON_ERROR_NONE) {
        $row = [];
        $row['datetime'] = $currentLineExpanded['datetime'];
        $row['level'] = $currentLineExpanded['level_name'];
        $row['message'] = $this->t($currentLineExpanded['message'], []);
        $row['details']  = json_encode($currentLineExpanded['context']);
        $rows[] = $row;
      }
      $form['status'] = [
        '#tree'   => TRUE,
        '#type'   => 'fieldset',
        '#title'  => $this->t(
          'Info'
        ),
        '#markup' => $this->t(
          'Your last Process logs'
        ),
        'logs' => [
          '#type' => 'table',
          '#header' => [
            $this->t('datetime'),
            $this->t('level'),
            $this->t('message'),
            $this->t('details'),
          ],
          '#rows' => $rows,
          '#sticky' => TRUE,
        ]
      ];
    }
    else {
      $form['status'] = [
        '#tree'   => TRUE,
        '#type'   => 'fieldset',
        '#title'  => $this->t(
          'Info'
        ),
        '#markup' => $this->t(
          'Sorry. No Logs have been generated yet.'
        ),
      ];
    }
    return $form;




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
          $total_rows =  $this->AmiUtilityService->csv_count($file_lod, FALSE);
          // Remove the header in the calculations.
          $total_rows = $total_rows - 1;
          $pager = \Drupal::service('pager.manager')->createPager($total_rows, $num_per_page);
          $page = $pager->getCurrentPage();
          $offset = $num_per_page * $page;
          if (PHP_VERSION_ID > 80000) {
            // @TODO fgetcsv has a bug when called after a seek, offsets on 1 always.
            // We are trying to skip the header too (but get it)
            //$offset  = $offset + 2;
            // @TODO CHECK IF THIS WILL WORK ON PHP 8.x when we get there.
          }
          $file_data_all = $this->AmiUtilityService->csv_read($file_lod, $offset, $num_per_page, TRUE, FALSE);

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
              $argument_string = $this->AmiLoDService::LOD_COLUMN_TO_ARGUMENTS[$column] ?? NULL;
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
                  '#default_value' => $checked,
                  '#title' => $this->t('revisioned'),
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




}


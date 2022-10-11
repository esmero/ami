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
use LimitIterator;

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
    $num_per_page = 50;
    $logfilename = "private://ami/logs/set{$this->entity->id()}.log";
    $logfilename = $this->fileSystem->realpath($logfilename);
    $last_op = NULL;
    if ($logfilename !== FALSE && file_exists($logfilename)) {
      // How many lines?
      $file = new \SplFileObject($logfilename, 'r');
      $file->seek(PHP_INT_MAX);
      $total_lines = $file->key(); // last line number
      $pager = \Drupal::service('pager.manager')->createPager($total_lines, $num_per_page);
      $page = $pager->getCurrentPage();
      $page = $page + 1;
      $offset = $total_lines - ($num_per_page * $page);
      $num_per_page = $offset < 0 ? $num_per_page + $offset : $num_per_page;

      $offset = $offset < 0 ? 0 : $offset;
      $reader = new LimitIterator($file, $offset, $num_per_page);
      $rows = [];
      foreach ($reader as $line) {
        $currentLineExpanded = json_decode($line, TRUE);
        if (json_last_error() == JSON_ERROR_NONE) {
          $row = [];
          $row['datetime'] = $currentLineExpanded['datetime'];
          $row['level'] = $currentLineExpanded['level_name'];
          $row['message'] = $this->t($currentLineExpanded['message'], []);
          $row['details']  = json_encode($currentLineExpanded['context']);
        }
        else {
          $row = ['Wrong Format for this entry','','', $line];
        }
        array_unshift($rows, $row);
      }
      $file = null;

      $form['logs'] = [
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
      $form['logs']['pager'] = ['#type' => 'pager'];
    }
    else {
      $form['status'] = [
        '#tree'   => TRUE,
        '#type'   => 'fieldset',
        '#title'  => $this->t(
          'Info'
        ),
        '#markup' => $this->t(
          'No Logs Found'
        ),
      ];
    }
    return $form;
  }
}


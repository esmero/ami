<?php

namespace Drupal\ami\Form;

use Drupal\ami\AmiLoDService;
use Drupal\ami\AmiUtilityService;
use Drupal\Component\Datetime\TimeInterface;
use Drupal\Core\Ajax\AjaxResponse;
use Drupal\Core\Ajax\InvokeCommand;
use Drupal\Core\Ajax\RemoveCommand;
use Drupal\Core\Ajax\ReplaceCommand;
use Drupal\Core\Entity\ContentEntityConfirmFormBase;
use Drupal\Core\Entity\ContentEntityForm;
use Drupal\Core\Entity\EntityRepositoryInterface;
use Drupal\Core\Entity\EntityTypeBundleInfoInterface;
use Drupal\Core\File\FileSystemInterface;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Url;
use Monolog\Logger;
use Symfony\Component\DependencyInjection\ContainerInterface;
use LimitIterator;
use Drupal\Core\Form\FormBuilderInterface;
use Drupal\Core\EventSubscriber\AjaxResponseSubscriber;
use Drupal\Core\EventSubscriber\MainContentViewSubscriber;

/**
 * Form controller for the MetadataDisplayEntity entity delete form.
 *
 * @ingroup ami
 */
class amiSetEntityReportForm extends ContentEntityConfirmFormBase {

  /**
   * @var
   */
  private CONST LOG_LEVELS = [
    'all'       => 'All Levels',
    'INFO'      => 'INFO',
    'WARNING'   => 'WARN',
    'ERROR'   => 'ERROR',
  ];


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
    return $this->t('Download');
  }


  public function getQuestion() {
    // Not really a question but a statement! :)
    return $this->t(
      'Processing logs for %name',
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
    // HACK!
    // Drupal is stupid. It adds arguments that aren't supposed to
    // Go into the pager, ending with broken URLs
    // This removes from the request those urls so we can
    // return the updated pager VIA AJAX
    // But still retain the added filter



    /*foreach ([
      //AjaxResponseSubscriber::AJAX_REQUEST_PARAMETER,
      FormBuilderInterface::AJAX_FORM_REQUEST,
      MainContentViewSubscriber::WRAPPER_FORMAT,
    ] as $key) {
      if (!$this->getRequest()) {
        $this->getRequest()->query->remove($key);
        $this->getRequest()->request->remove($key);
      }
    }*/

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
      $level = $this->getRequest()->query->get('level', 'all');
      $level = $form_state->getValue(['logs','level']) ?? $level;
      $level =  in_array($level,array_keys(static::LOG_LEVELS)) ? $level : 'all';
      $pager = \Drupal::service('pager.manager')->createPager(
        $total_lines, $num_per_page
      );
      /* @var $pager \Drupal\Core\Pager\Pager */
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
          $row['details'] = json_encode($currentLineExpanded['context']);
        }
        else {
          $row = ['Wrong Format for this entry', '', '', $line];
        }
        array_unshift($rows, $row);
      }
      $file = NULL;

      $form['logs'] = [
        '#tree'   => TRUE,
        '#type'   => 'fieldset',
        '#prefix' => '<div id="edit-log">',
        '#suffix' => '</div>',
        '#title'  => $this->t(
          'Info'
        ),
        '#markup' => $this->t(
          'Your last logs'
        ),
        'level'   => [
          '#type'          => 'select',
          '#options'       => static::LOG_LEVELS,
          '#default_value' => $level ,
          '#title' => $this->t('Filter by'),
          '#submit' => ['::submitForm'],
          '#ajax' => [
            'callback' => '::myAjaxCallback', // don't forget :: when calling a class method.
            //'callback' => [$this, 'myAjaxCallback'], //alternative notation
            'disable-refocus' => FALSE, // Or TRUE to prevent re-focusing on the triggering element.
            'event' => 'change',
            'wrapper' => 'edit-log', // This element is updated with this AJAX callback.
            'progress' => [
              'type' => 'throbber',
              'message' => $this->t('Filtering Logs...'),
            ],
          ],
          //'#limit_validation_errors' => [],
        ],
        'logs'    => [
          '#type'   => 'table',
          '#header' => [
            $this->t('datetime'),
            $this->t('level'),
            $this->t('message'),
            $this->t('details'),
          ],
          '#rows'   => $rows,
          '#sticky' => TRUE,
        ]
      ];


      // @NOTE no docs for the #parameter argument! Good i can read docs,
      // @see https://www.drupal.org/project/drupal/issues/2504709#comment-13795142
      $form['logs']['pager'] = [
        '#type' => 'pager',
        '#prefix' => '<div id="edit-log-pager">',
        '#suffix' => '</div>',
        '#parameters' => ['level' => $level]
      ];
    }
    else {
      $form['logs'] = [
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
    // Add a submit button that handles the submission of the form.
    $form['actions']['submit'] = array(
      '#type' => 'submit',
      '#value' => $this->t('Submit'),
    );
    return $form + parent::buildForm($form, $form_state);
  }

  public function myAjaxCallback(array &$form, FormStateInterface $form_state) {
    foreach ([
      AjaxResponseSubscriber::AJAX_REQUEST_PARAMETER,
      FormBuilderInterface::AJAX_FORM_REQUEST,
      MainContentViewSubscriber::WRAPPER_FORMAT,
    ] as $key) {
      if ($this->getRequest()) {
        $this->getRequest()->query->remove($key);
        $this->getRequest()->request->remove($key);
      }
    }
    return $form['logs'];
  }

  /**
   * {@inheritdoc}
   */
  public function submitForm(array &$form, FormStateInterface $form_state) {
    $form_state->setRebuild(TRUE);
  }

}


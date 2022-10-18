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
    'all'       => 'All Levels (all time)',
    'INFO'      => 'INFO (Last time processed)',
    'WARNING'   => 'WARNING (Last time processed)',
    'ERROR'   => 'ERRORS (Last time processed)',
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

  public function getDescription() {
    return $this->t('Download Log File');
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

    /* Fetch the status from the private store also: */
    $store = \Drupal::service('tempstore.private')->get('ami_queue_status');
    $last_status = $store->get('set_'.$this->entity->id());


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

      // Initialize Pager based on the total here
      // We might re-do it afterwards, but we need the current page
      $pager = \Drupal::service('pager.manager')->createPager(
        $total_lines, $num_per_page
      );
      /* @var $pager \Drupal\Core\Pager\Pager */
      $page = $pager->getCurrentPage();
      $page = $page + 1;

      // Won't override $total_lines because i still need this for the offset
      // Independently of the level selection.
      $total_lines_for_pager = $total_lines;
      $level = $this->getRequest()->query->get('level', 'all');
      $level = $form_state->getValue(['logs','level']) ?? $level;
      $level =  in_array($level,array_keys(static::LOG_LEVELS)) ? $level : 'all';
      $total_lines_current = 0;
      $prev_time_stamp = NULL;
      $our_time_stamp = NULL;
      $endcurrent = FALSE;
      $rows = [];
      // Find based on the level what we need.
        $current_offset = $total_lines - $num_per_page;
        // Means we will count only until current process records and filter
        while ($current_offset > 0 && !$endcurrent) {
          $reader = new LimitIterator($file, $current_offset, $num_per_page);
          foreach ($reader as $line) {
            $row = [];
            $currentLineExpanded = json_decode($line, TRUE);
            if (json_last_error() == JSON_ERROR_NONE) {
              $current_time_stamp = isset($currentLineExpanded['context']['time_submitted']) ? $currentLineExpanded['context']['time_submitted'] : $prev_time_stamp;
              // But we can not bail yet here! We are reading from older to newer so this could be a page (still) where the
              // First records are not of this set but later on they are! Damn Diego
              if ($prev_time_stamp == NULL) {

                //means our first iteration
                $our_time_stamp = $current_time_stamp;
                if ($level === 'all') {
                  $endcurrent = TRUE;
                  // In this case we just want the last timestamp, nothing else.
                  break 2;
                }
              }

              if ($prev_time_stamp != NULL && ($current_time_stamp != $prev_time_stamp)) {
                $endcurrent = TRUE; // Double safety? I'm already bailing ...
                //break 2;
              }
              if (isset($currentLineExpanded['level_name']) && $currentLineExpanded['level_name'] == $level && $our_time_stamp == $current_time_stamp) {
                $total_lines_current++;
                $row['datetime'] = $currentLineExpanded['datetime'];
                $row['level'] = $currentLineExpanded['level_name'];
                $row['message'] = $this->t($currentLineExpanded['message'], []);
                $row['details'] = json_encode($currentLineExpanded['context']);
                $rows[$reader->getPosition()] = $row;
              }
              $prev_time_stamp = $current_time_stamp;
            }
          }
          $current_offset = $current_offset - $num_per_page;
          $current_offset = $current_offset > 0 ? $current_offset: 0;
        }
      if ($level != 'all') {
        $total_lines_for_pager = $total_lines_current;
        // resorts the array based on the actual timestamp with microseconds
        ksort($rows, SORT_NUMERIC);
        $slice_offset = ($page * $num_per_page <= $total_lines_current)
          ? $total_lines_current - ($page * $num_per_page) : 0;
        $slice_amount = ($page * $num_per_page <= $total_lines_current)
          ? $num_per_page
          : $total_lines_current - (($page - 1) * $num_per_page);
        $rows = array_slice($rows, $slice_offset, $slice_amount);
        $rows = array_reverse($rows);
      }



      $timestamp = $our_time_stamp;
      $pager = \Drupal::service('pager.manager')->createPager(
        $total_lines_for_pager, $num_per_page
      );
      /* @var $pager \Drupal\Core\Pager\Pager */
      $page = $pager->getCurrentPage();

      $page = $page + 1;
      $offset = $total_lines - ($num_per_page * $page);
      $num_per_page = $offset < 0 ? $num_per_page + $offset : $num_per_page;
      $offset = $offset < 0 ? 0 : $offset;
      $fetch = TRUE;
      // This only RUNS if all is selected.
      // ROWS have been already fetched for the other levels before.
      while ($offset > 0 && count($rows) < $num_per_page && $level == 'all') {
        $reader = new LimitIterator($file, $offset, $num_per_page);
        foreach ($reader as $line) {
          $currentLineExpanded = json_decode($line, TRUE);
          $row = [];
          $fetch = TRUE;
          if (json_last_error() == JSON_ERROR_NONE) {
            $row['datetime'] = $currentLineExpanded['datetime'];
            $row['level'] = $currentLineExpanded['level_name'];
            $row['message'] = $this->t($currentLineExpanded['message'], []);
            $row['details'] = json_encode($currentLineExpanded['context']);
            $rows[] = $row;
          }
          else {
            // Only show wrongly formatter if no filter present.
            $row = ['Wrong Format for this entry', '', '', $line];
            $rows[] = $row;
          }
          if (count($rows) ==  $num_per_page) { break;}
        }
        $rows = array_reverse($rows);
      }
      $file = NULL;
      if (count($last_status)) {
        $message = $this->t(
          'You have @count entries for your current Filter, last time this set was sent to processing was: <em>@date</em>.<br>Successfully processed: <em>@processed</em>, Errors: <em>@errors</em> of a total of <em>@total</em>', [
          '@count' => $total_lines_for_pager,
          '@date' => !empty($timestamp) ? date('D, d M Y \a\t H:i:s', $timestamp) : " Unknown ",
          '@errors' => $last_status['errored'] ?? 0,
          '@processed' => $last_status['processed'] ?? 0,
          '@total' =>  $last_status['total'] ?? 0
        ]);
      }
      else {
        $message = $this->t(
          'You have @count entries for your current Filter, last time this set was sent to processing was: <em>@date</em>',
          [
            '@count' => $total_lines_for_pager,
            '@date'  => !empty($timestamp) ? date(
              'D, d M Y \a\t H:i:s', $timestamp
            ) : " Unknown ",
          ]
        );
      }
      $form['logs'] = [
        '#tree'   => TRUE,
        '#type'   => 'fieldset',
        '#prefix' => '<div id="edit-log">',
        '#suffix' => '</div>',
        '#title'  => $this->t(
          'Info'
        ),
        '#markup' => $message,
        'level'   => [
          '#type'          => 'select',
          '#options'       => static::LOG_LEVELS,
          '#default_value' => $level ,
          '#title' => $this->t('Filter by log level:'),
          '#submit' => ['::submitForm'],
          '#ajax' => [
            'callback' => '::myAjaxCallback',
            'disable-refocus' => FALSE,
            'event' => 'change',
            'wrapper' => 'edit-log',
            'progress' => [
              'type' => 'throbber',
              'message' => $this->t('Filtering Logs...'),
            ],
          ],
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


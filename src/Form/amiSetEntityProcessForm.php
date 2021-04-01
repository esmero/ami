<?php

namespace Drupal\ami\Form;

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
class amiSetEntityProcessForm extends ContentEntityConfirmFormBase {

  /**
   * @var \Drupal\ami\AmiUtilityService
   */
  protected $AmiUtilityService;

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
   */
  public function __construct(
    EntityRepositoryInterface $entity_repository,
    EntityTypeBundleInfoInterface $entity_type_bundle_info = NULL,
    TimeInterface $time = NULL, AmiUtilityService $ami_utility) {
    parent::__construct($entity_repository, $entity_type_bundle_info, $time);
    $this->AmiUtilityService = $ami_utility;
  }

  /**
   * {@inheritdoc}
   */
  public static function create(ContainerInterface $container) {
    return new static(
      $container->get('entity.repository'),
      $container->get('entity_type.bundle.info'),
      $container->get('datetime.time'),
      $container->get('ami.utility')
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
    // @TODO We should here make sure we get rid of any files
    // But if the queue has elements from this Set we should not be able to delete?
    // $this->entity->delete();
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

      $info = $this->AmiUtilityService->preprocessAmiSet($file, $data);
      $SetURL = $this->entity->toUrl('canonical', ['absolute' => TRUE])
        ->toString();
      $notprocessnow = $form_state->getValue('not_process_now', NULL);
      $queue_name = 'ami_ingest_ado';
      if (!$notprocessnow) {
        // This queues have no queue workers. That is intended since they
        // are always processed by the ami_ingest_ado one manually.
        $queue_name = 'ami_ingest_ado_set_' . $this->entity->id();
        \Drupal::queue($queue_name, TRUE)->createQueue();
        // @TODO acquire a Lock that is renewed for each queue item processing
        // To avoid same batch to be send to processing by different users at
        // the same time.
      }
      $added = [];
      foreach ($info as $item) {
        // We set current User here since we want to be sure the final owner of
        // the object is this and not the user that runs the queue
        $data->info = [
          'zip_file' => $zip_file,
          'row' => $item,
          'set_id' => $this->entity->id(),
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
        // TODO check if count($info) == $count
        if ($count) {
          $this->submitBatch($form_state, $queue_name, $count);
        }
      }
    }
    else {
      $this->messenger()->addError(
        $this->t(
          'So Sorry. This Ami Set has incorrect Metadata and/or has its CSV file missing. Please correct or delete and generate a new one.',
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
      'progress_message' => t('Processing Set @current of @total.'),
    ];
    $batch['operations'][] = [
      '\Drupal\ami\AmiBatchQueue::takeOne',
      [$queue_name, $this->entity->id()],
    ];
    batch_set($batch);
  }

}


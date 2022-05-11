<?php

namespace Drupal\ami\EventSubscriber;

use Drupal\ami\AmiLoDService;
use Drupal\Component\Utility\Unicode;
use Drupal\Core\Logger\LoggerChannelFactoryInterface;
use Drupal\Core\Messenger\MessengerInterface;
use Drupal\Core\Session\AccountInterface;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\Core\StringTranslation\TranslationInterface;
use Drupal\ami\AmiEventType;
use Drupal\ami\Event\AmiCrudEvent;
use Drupal\ami\AmiUtilityService;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Drupal\Core\Entity\EntityTypeManagerInterface;

/**
 * Event subscriber for AMI entity presave event, injects CSV LoD into keystore.
 */
class AmiEventPresaveSubscriberProcessedLoDUpdater extends AmiEventPresaveSubscriber {

  use StringTranslationTrait;

  /**
   * @var int
   */
  protected static $priority = -900;

  /**
   * The messenger.
   *
   * @var \Drupal\Core\Messenger\MessengerInterface
   */
  protected $messenger;

  /**
   * The Storage Destination Scheme.
   *
   * @var string;
   */
  protected $destinationScheme = NULL;

  /**
   * The logger factory.
   * @var \Drupal\Core\Logger\LoggerChannelFactoryInterface
   */
  protected $loggerFactory;

  /**
   * The current user.
   *
   * @var \Drupal\Core\Session\AccountInterface
   */
  protected $account;

  /**
   * @var \Drupal\ami\AmiUtilityService
   */
  protected $AmiUtilityService;

  /**
   * The entity manager service.
   *
   * @var \Drupal\Core\Entity\EntityTypeManagerInterface
   */
  protected $entityTypeManager;

  /**
   * @var \Drupal\ami\AmiLoDService
   */
  protected $AmiLoDService;

  /**
   * StrawberryfieldEventPresaveSubscriberSetTitlefromMetadata constructor.
   *
   * @param \Drupal\Core\StringTranslation\TranslationInterface $string_translation
   * @param \Drupal\Core\Messenger\MessengerInterface $messenger
   * @param \Drupal\Core\Logger\LoggerChannelFactoryInterface $logger_factory
   * @param \Drupal\ami\AmiUtilityService $ami_utility
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface $entity_type_manager
   * @param \Drupal\ami\AmiLoDService $ami_lod,
   */
  public function __construct(
    TranslationInterface $string_translation,
    MessengerInterface $messenger,
    LoggerChannelFactoryInterface $logger_factory,
    AccountInterface $account,
    AmiUtilityService $ami_utility,
    EntityTypeManagerInterface $entity_type_manager,
    AmiLoDService $ami_lod
  ) {
    $this->stringTranslation = $string_translation;
    $this->messenger = $messenger;
    $this->loggerFactory = $logger_factory;
    $this->account = $account;
    $this->AmiUtilityService = $ami_utility;
    $this->AmiLoDService = $ami_lod;
    $this->entityTypeManager = $entity_type_manager;
  }


  /**
   * @param \Drupal\ami\Event\AmiCrudEvent $event
   */
  public function onEntityPresave(AmiCrudEvent $event) {

    /* @var $entity \Drupal\ami\Entity\AmiSetEntity */
    $entity = $event->getEntity();
    $sbf_fields = $event->getFields();
    $originallabel = NULL;
    $forceupdate = TRUE;
    if (!$entity->isNew()) {
      $originalLodCSV = $entity->original->processed_data->getValue();
      $newLodCSV = $entity->processed_data->getValue();
      $originalLodCSV = $originalLodCSV[0]['target_id'] ?? null;
      $newLodCSV = $newLodCSV[0]['target_id'] ?? null;
      // Only act on changes on the processed CSV. We do not want to execute
      // this costly operation on every save.
      if (($originalLodCSV != $newLodCSV) && !empty($newLodCSV)) {
        $lod_options = array_keys(AmiLoDService::LOD_COLUMN_TO_ARGUMENTS);
        $enforced_headers = ['original','csv_columns', 'checked'];
        $csv_file_reference = $entity->get('source_data')->getValue();
        /** @var \Drupal\file\Entity\File $file */
        $file_lod = $this->entityTypeManager->getStorage('file')->load(
          $newLodCSV
        );
        // We avoid the escape characters here to allow JSON with double quotes
        // In its values to be read from the CSV.
        if ($file_lod) {
          $file_data_all = $this->AmiUtilityService->csv_read(
            $file_lod, 0, 0, TRUE, FALSE
          );
          $header = $file_data_all['headers'] ?? [];
          // Validation is not done here
          $required_headers = ['original','csv_columns', 'checked'];

          // Clears old values before processing new ones.
          $this->AmiLoDService->cleanKeyValuesPerAmiSet($entity->id());
          $update_config = FALSE;
          $normalized_mapping = [];
          if (count($file_data_all['data']) > 0) {
            foreach ($file_data_all['data'] as $rownumber => $row) {
              $keyed_row = array_combine($header, $row);
              $label = $keyed_row['original'] ?? NULL;
              $csv_columns = json_decode($keyed_row['csv_columns'], TRUE);
              // If these do not exist, we can not process.
              if (!$label || !$csv_columns) {
                $this->loggerFactory->get('ami')->warning(
                  "Uploaded LoD CSV for AMI Set @ami_set_label on row @row had missing required values for csv_columns and/or label. Skipping",
                  [
                    '@ami_set_label' => $entity->label(),
                    '@row'           => $rownumber,
                  ]
                );
                continue;
              }
              foreach ($keyed_row as $column => $value) {
                if (!in_array($column, $required_headers)
                  && in_array(
                    $column, $lod_options
                  )
                ) {
                  $lod = json_decode($value, TRUE);
                  if ($lod) {
                    foreach ($csv_columns as $source_column) {
                      $normalized_mapping[$source_column][] = $column;
                      $normalized_mapping[$source_column] = array_unique(
                        $normalized_mapping[$source_column]
                      );
                    }
                  }
                  $context_data[$column]['lod'] = $lod ?? NULL;
                  $context_data[$column]['columns'] = $csv_columns;
                  $context_data['checked'] = $keyed_row['checked'] ?? FALSE;
                }
              }
              $this->AmiLoDService->setKeyValuePerAmiSet(
                $label,
                $context_data, $entity->id()
              );
            }

            // This happen in reverse order as in \Drupal\ami\Form\amiSetEntityReconcileCleanUpForm::submitForm
            // given than mappings are deduced from the actual ROW data of the CSV
            $this->AmiLoDService->setKeyValueMappingsPerAmiSet(
              $normalized_mapping, $entity->id()
            );
            $update_config = TRUE;
          }
        }

      if ($update_config) {
        foreach ($sbf_fields as $field_name) {
          /* @var $field \Drupal\Core\Field\FieldItemInterface */
          $field = $entity->get($field_name);
          /* @var \Drupal\strawberryfield\Field\StrawberryFieldItemList $field */
          // This will try with any possible match.
          foreach ($field->getIterator() as $delta => $itemfield) {
            $fullvalues = $itemfield->provideDecoded(TRUE);
            $fullvalues["reconcileconfig"]["columns"] = [];
            $fullvalues["reconcileconfig"]["mappings"] = [];
            $fullvalues["reconcileconfig"]["columns"] = array_combine(array_keys($normalized_mapping), array_keys($normalized_mapping));
            foreach ($normalized_mapping as $source => $approaches) {
              foreach ($approaches as $approach) {
                $fullvalues["reconcileconfig"]["mappings"][$source][]
                  = $this->AmiLoDService::LOD_COLUMN_TO_ARGUMENTS[$approach];
                }
              $fullvalues["reconcileconfig"]["mappings"][$source] = array_unique($fullvalues["reconcileconfig"]["mappings"][$source]);
            }

            if (!$itemfield->setMainValueFromArray((array) $fullvalues)) {
              $this->messenger->addError($this->t('We could not persist LoD Configuration inferred from the CSV. Please contact the site admin.'));
            }
          }
        }
      }
    }
    $current_class = get_called_class();
    $event->setProcessedBy($current_class, TRUE);
  }
}
}

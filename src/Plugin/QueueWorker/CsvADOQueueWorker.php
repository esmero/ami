<?php

namespace Drupal\ami\Plugin\QueueWorker;

use Drupal\ami\AmiLoDService;
use Drupal\ami\AmiUtilityService;
use Drupal\ami\Entity\amiSetEntity;
use Drupal\file\FileInterface;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Logger\LoggerChannelFactoryInterface;
use Drupal\Core\Messenger\MessengerInterface;
use Drupal\Core\Plugin\ContainerFactoryPluginInterface;
use Drupal\Core\Queue\QueueWorkerBase;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\strawberryfield\StrawberryfieldFilePersisterService;
use Drupal\strawberryfield\StrawberryfieldUtilityService;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Monolog\Formatter\JsonFormatter;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Swaggest\JsonDiff\JsonDiff;
use Drupal\strawberryfield\Tools\StrawberryfieldJsonHelper;
use \Drupal\Core\TempStore\PrivateTempStoreFactory;

/**
 * Processes CSVs generating in turn Ingest ADO Queue worker entries.
 *
 * @QueueWorker(
 *   id = "ami_csv_ado",
 *   title = @Translation("AMI CSV expander and row Enqueuer Queue Worker")
 * )
 */
class CsvADOQueueWorker extends IngestADOQueueWorker {

  /**
   * {@inheritdoc}
   */
  public function processItem($data) {
    $log = new Logger('ami_file');
    $private_path = \Drupal::service('stream_wrapper_manager')->getViaUri('private://')->getDirectoryPath();
    $handler = new StreamHandler($private_path.'/ami/logs/set'.$data->info['set_id'].'.log', Logger::DEBUG);
    $handler->setFormatter(new JsonFormatter());
    $log->pushHandler($handler);
    // This will add the File logger not replace the DB
    // We can not use addLogger because in a single PHP process multiple Queue items might be invoked
    // And loggers are additive. Means i can end with a few duplicated entries!
    // @TODO: i should inject this into the Containers but i wanted to keep
    // it simple for now.
    $this->loggerFactory->get('ami_file')->setLoggers([[$log]]);

    /* Data info for an ADO has this structure
      $data->info = [
        'set_id' => The Set id
        'uid' => The User ID that processed the Set
        'set_url' => A direct URL to the set.
        'op_secondary' => applies only to Update/Patch operations. Can be one of 'update','replace','append'
        'ops_safefiles' => Boolean, True if we will not allow files/mappings to be removed/we will keep them warm and safe
        'log_jsonpatch' => If for Update operations we will generate a single PER ADO Log with a full JSON Patch,
        'attempt' => The number of attempts to process. We always start with a 1
        'zip_file' => Zip File/File Entity
        'csv_file' => The CSV that will generate the ADO queue items.
        'queue_name' => because well ... we use Hydroponics too
        'time_submitted' => Timestamp on when the queue was sent. All Entries will share the same
      ];
    // This will simply go to an alternate processing on this same Queue Worker
    // Just for files.
    */
    $adodata = clone $data;
    $adodata->info = NULL;
    $added = [];
    if (!empty($data->info['csv_file'])) {
      $invalid = [];
      // Note. We won't process the nested CSV here. This queue worker only takes a CSV and splits into smaller
      // chunks. Basically what the \Drupal\ami\Form\amiSetEntityProcessForm::submitForm already does.
      // But the ADO worker itself will (new code) extract a CSV and then again, enqueue back to this so this one can yet again
      // split into smaller chuncks and so on.
      $info = $this->AmiUtilityService->preprocessAmiSet($data->info['csv_file'], $data, $invalid, FALSE);
      foreach ($info as $item) {
        // We set current User here since we want to be sure the final owner of
        // the object is this and not the user that runs the queue
        $adodata->info = [
          'zip_file' => $data->info['zip_file'] ?? NULL,
          'row' => $item,
          'set_id' =>  $data->info['set_id'],
          'uid' => $data->info['uid'],
          'status' => $data->info['status'],
          'op_secondary' => $data->info['op_secondary'] ?? NULL,
          'ops_safefiles' => $data->info['ops_safefiles'] ? TRUE: FALSE,
          'log_jsonpatch' => FALSE,
          'set_url' => $data->info['set_url'],
          'attempt' => 1,
          'queue_name' =>  $data->info['queue_name'],
          'force_file_queue' =>   $data->info['force_file_queue'],
          'force_file_process' => $data->info['force_file_process'],
          'manyfiles' =>  $data->info['manyfiles'],
          'ops_skip_onmissing_file' => $data->info['ops_skip_onmissing_file'],
          'ops_forcemanaged_destination_file' => $data->info['ops_forcemanaged_destination_file'],
          'time_submitted' => $data->info['time_submitted'],
        ];
        $added[] = \Drupal::queue($data->info['queue_name'])
          ->createItem($adodata);
      }
      if (count($added)) {
        $message = $this->t('CSV for Set @setid was expanded to ADOs',[
          '@setid' => $data->info['set_id']
        ]);
        $this->loggerFactory->get('ami_file')->info($message ,[
          'setid' => $data->info['set_id'] ?? NULL,
          'time_submitted' => $data->info['time_submitted'] ?? '',
        ]);
      }
      $processed_set_status = $this->statusStore->get('set_' . $this->entity->id());
      $processed_set_status['processed'] =  $processed_set_status['processed'] ?? 0;
      $processed_set_status['errored'] =  $processed_set_status['errored'] ?? 0;
      $processed_set_status['total'] = $processed_set_status['total'] ?? 0 + count($added);
      $this->statusStore->set('set_' . $this->entity->id(), $processed_set_status);
      return;
    }
    return;
    // Before we do any processing. Check if Parent(s) exists?
    // If not, re-enqueue: we try twice only. Should we try more?
    $parent_nodes = [];
    if (isset($data->info['row']['parent']) && is_array($data->info['row']['parent'])) {
      $parents = $data->info['row']['parent'];
      $parents = array_filter($parents);
      foreach($parents as $parent_property => $parent_uuid) {
        $parent_uuids = (array) $parent_uuid;
        // We should validate each member to be an UUID here (again). Just in case.
        $existing = $this->entityTypeManager->getStorage('node')->loadByProperties(['uuid' => $parent_uuids]);
        if (count($existing) != count($parent_uuids)) {
          $message = $this->t('Sorry, we can not process ADO with @uuid from Set @setid yet, there are missing parents with UUID(s) @parent_uuids. We will retry.',[
            '@uuid' => $data->info['row']['uuid'],
            '@setid' => $data->info['set_id'],
            '@parent_uuids' => implode(',', $parent_uuids)
          ]);
          $this->loggerFactory->get('ami_file')->warning($message ,[
            'setid' => $data->info['set_id'] ?? NULL,
            'time_submitted' => $data->info['time_submitted'] ?? '',
          ]);

          // Pushing to the end of the queue.
          $data->info['attempt']++;
          if ($data->info['attempt'] < 3) {
            \Drupal::queue($data->info['queue_name'])
              ->createItem($data);
            return;
          }
          else {
            $message = $this->t('Sorry, We tried twice to process ADO with @uuid from Set @setid yet, but you have missing parents. Please check your CSV file and make sure parents with an UUID are in your REPO first and that no other parent generated by the set itself is failing',[
              '@uuid' => $data->info['row']['uuid'],
              '@setid' => $data->info['set_id']
            ]);
            $this->loggerFactory->get('ami_file')->error($message ,[
              'setid' => $data->info['set_id'] ?? NULL,
              'time_submitted' => $data->info['time_submitted'] ?? '',
            ]);
            $this->setStatus(amiSetEntity::STATUS_PROCESSING_WITH_ERRORS, $data);
            return;
            // We could enqueue in a "failed" queue?
            // @TODO for 0.6.0: Or better. We could keep track of the dependency
            // and then afterwards update one and then the other
            // Why? Because if we have one object pointing to X
            // and the other pointing back the graph is not acyclic
            // but we could still via an update operation
            // Ingest without the relations both. Then update both once the
            // Ingest is ready IF both have IDs.
          }
        }
        else {
          // Get the IDs!
          foreach($existing as $node) {
            $parent_nodes[$parent_property][] = (int) $node->id();
          }
        }
      }
    }

    $processed_metadata = NULL;

    $method = $data->mapping->globalmapping ?? "direct";
    if ($method == 'custom') {
      $method = $data->mapping->custommapping_settings->{$data->info['row']['type']}->metadata ?? "direct";
    }
    if ($method == 'template') {
      $processed_metadata = $this->AmiUtilityService->processMetadataDisplay($data);
      if (!$processed_metadata) {
        $message = $this->t('Sorry, we can not cast ADO with @uuid into proper Metadata. Check the Metadata Display Template used, your permissions and/or your data ROW in your CSV for set @setid.',[
          '@uuid' => $data->info['row']['uuid'],
          '@setid' => $data->info['set_id']
        ]);
        $this->loggerFactory->get('ami_file')->error($message ,[
          'setid' => $data->info['set_id'] ?? NULL,
          'time_submitted' => $data->info['time_submitted'] ?? '',
        ]);
        $this->setStatus(amiSetEntity::STATUS_PROCESSING_WITH_ERRORS, $data);
        return;
      }
    }
    if ($method == "direct") {
      if (isset($data->info['row']['data']) && !is_array($data->info['row']['data'])) {
        $message = $this->t('Sorry, we can not cast ADO with @uuid directly into proper Metadata. Check your data ROW in your CSV for set @setid for invalid data.',[
          '@uuid' => $data->info['row']['uuid'] ?? "MISSING UUID",
          '@setid' => $data->info['set_id']
        ]);

        $this->loggerFactory->get('ami_file')->error($message ,[
          'setid' => $data->info['set_id'] ?? NULL,
          'time_submitted' => $data->info['time_submitted'] ?? '',
        ]);
        $this->setStatus(amiSetEntity::STATUS_PROCESSING_WITH_ERRORS, $data);
        return;
      }
      elseif (!isset($data->info['row']['data'])) {
        $message = $this->t('Sorry, we can not cast an ADO directly into proper Metadata. Check your data ROW in your CSV for set @setid for invalid data.',
          [
            '@setid' => $data->info['set_id'],
          ]);
        $this->loggerFactory->get('ami_file')->error($message ,[
          'setid' => $data->info['set_id'] ?? NULL,
          'time_submitted' => $data->info['time_submitted'] ?? '',
        ]);
        $this->setStatus(amiSetEntity::STATUS_PROCESSING_WITH_ERRORS, $data);
        return;
      }

      $processed_metadata = $this->AmiUtilityService->expandJson($data->info['row']['data']);
      $processed_metadata = !empty($processed_metadata) ? json_encode($processed_metadata) : NULL;
      $json_error = json_last_error();
      if ($json_error !== JSON_ERROR_NONE || !$processed_metadata) {
        $message = $this->t('Sorry, we can not cast ADO with @uuid directly into proper Metadata. Check your data ROW in your CSV for set @setid for invalid JSON data.',[
          '@uuid' => $data->info['row']['uuid'],
          '@setid' => $data->info['set_id']
        ]);
        $this->loggerFactory->get('ami_file')->error($message ,[
          'setid' => $data->info['set_id'] ?? NULL,
          'time_submitted' => $data->info['time_submitted'] ?? '',
        ]);
        $this->setStatus(amiSetEntity::STATUS_PROCESSING_WITH_ERRORS, $data);
        return;
      }
    }
  }
}

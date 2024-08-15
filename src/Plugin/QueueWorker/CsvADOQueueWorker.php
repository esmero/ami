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
 * Processes CSVs generating in turn Other ADO Queue worker entries.
 *
 * @QueueWorker(
 *   id = "ami_csv_ado",
 *   title = @Translation("AMI CSV Expander and ADO Enqueuer Queue Worker")
 * )
 */
class CsvADOQueueWorker extends IngestADOQueueWorker
{
  /**
   * {@inheritdoc}
   */
  public function processItem($data) {
    $log = new Logger('ami_file');
    $private_path = \Drupal::service('stream_wrapper_manager')->getViaUri('private://')->getDirectoryPath();
    $handler = new StreamHandler($private_path . '/ami/logs/set' . $data->info['set_id'] . '.log', Logger::DEBUG);
    $handler->setFormatter(new JsonFormatter());
    $log->pushHandler($handler);
    // This will add the File logger not replace the DB
    // We can not use addLogger because in a single PHP process multiple Queue items might be invoked
    // And loggers are additive. Means i can end with a few duplicated entries!
    // @TODO: i should inject this into the Containers but i wanted to keep
    // it simple for now.
    $this->loggerFactory->get('ami_file')->setLoggers([[$log]]);

    /* Data info for an AMI Process CSV has this structure
      $data->info = [
        'csv_file' => The CSV File that will (or we hope so if well formed) generate multiple ADO Queue items
        'csv_file_name' => Only present if this is called not from the root
        'set_id' => The Set id
        'uid' => The User ID that processed the Set
        'set_url' => A direct URL to the set.
        'status' => Either a string (moderation state) or a 1/0 for published/unpublished if not moderated
        'op_secondary' => applies only to Update/Patch operations. Can be one of 'update','replace','append'
        'ops_safefiles' => Boolean, True if we will not allow files/mappings to be removed/we will keep them warm and safe
        'log_jsonpatch' => If for Update operations we will generate a single PER ADO Log with a full JSON Patch,
        'attempt' => The number of attempts to process. We always start with a 1
        'zip_file' => Zip File/File Entity
        'queue_name' => because well ... we use Hydroponics too
        'force_file_queue' => defaults to false, will always treat files as separate queue items.
        'force_file_process' => defaults to false, will force all techmd and file fetching to happen from scratch instead of using cached versions.
        'manyfiles' => Number of files (passed by \Drupal\ami\Form\amiSetEntityProcessForm::submitForm) that will trigger queue processing for files,
        'ops_skip_onmissing_file' => Skips ADO operations if a passed/mapped file is not present,
        'ops_forcemanaged_destination_file' => Forces Archipelago to manage a files destination when the source matches the destination Schema (e.g S3),
        'time_submitted' => Timestamp on when the queue was send. All Entries will share the same
      ];

    Most of this data will simply be relayed to another queue item.
    // This will simply go to an alternate processing on this same Queue Worker
    // Just for files.
    */

    /* Data info for an AMI Action CSV has this structure
     $data->info = [
       'csv_file' => The CSV File that will (or we hope so if well formed) generate multiple ADO Queue items
       'csv_file_name' => Only present if this is called not from the root
       'set_id' => The Set id
       'uid' => The User ID that processed the Set
       'set_url' => A direct URL to the set.
       'action' => The action to run
       'action_config' => An array of additional configs/settings the particular action takes.
       'attempt' => The number of attempts to process. We always start with a 1
       'zip_file' => Zip File/File Entity
       'queue_name' => because well ... we use Hydroponics too
       'time_submitted' => Timestamp on when the queue was send. All Entries will share the same
       'batch_size' =>  the number of ADOs to process via a batch action. Some actions like detele can/should handle multiple UUIDs at the same time in a single Queue item
     ];
   */
    /*
    $data->pluginconfig->op will be 'action' for actions.
    */


    $adodata = clone $data;
    $adodata->info = NULL;
    $added = [];
    // @TODO discuss with Allison the idea that one could ingest with "AMI set" data but without an actual AMI set?
    // That would require, e.g generating a fake $data->info['set_id']
    $csv_file = $data->info['csv_file'] ?? NULL;
    if ($csv_file instanceof FileInterface) {
      $invalid = [];

      // we will handle AMI processing v/s actions differently

      // Note. We won't process the nested CSV here. This queue worker only takes a CSV and splits into smaller
      // chunks. Basically what the \Drupal\ami\Form\amiSetEntityProcessForm::submitForm already does.
      // But the ADO worker itself will (new code) extract a CSV and then again, enqueue back to this so this one can yet again
      // split into smaller chuncks and so on.
      if ($data->pluginconfig->op !== 'action') {
        $info = $this->AmiUtilityService->preprocessAmiSet($data->info['csv_file'], $data, $invalid, FALSE);
        if (!count($info)) {
          //@TODO tell the user which CSV failed please?
          $message = $this->t('So sorry. CSV @csv for @setid produced no ADOs. Please correct your source CSV data', [
            '@setid' => $data->info['set_id'],
            '@csv' => $csv_file->getFilename(),
          ]);
          $this->loggerFactory->get('ami_file')->warning($message, [
            'setid' => $data->info['set_id'] ?? NULL,
            'time_submitted' => $data->info['time_submitted'] ?? '',
          ]);
          return;
        }

        foreach ($info as $item) {
          // We set current User here since we want to be sure the final owner of
          // the object is this and not the user that runs the queue
          $adodata->info = [
            'zip_file' => $data->info['zip_file'] ?? NULL,
            'row' => $item,
            'set_id' => $data->info['set_id'],
            'uid' => $data->info['uid'],
            'status' => $data->info['status'],
            'op_secondary' => $data->info['op_secondary'] ?? NULL,
            'ops_safefiles' => $data->info['ops_safefiles'] ? TRUE : FALSE,
            'log_jsonpatch' => FALSE,
            'set_url' => $data->info['set_url'],
            'attempt' => 1,
            'queue_name' => $data->info['queue_name'],
            'force_file_queue' => $data->info['force_file_queue'],
            'force_file_process' => $data->info['force_file_process'],
            'manyfiles' => $data->info['manyfiles'],
            'ops_skip_onmissing_file' => $data->info['ops_skip_onmissing_file'],
            'ops_forcemanaged_destination_file' => $data->info['ops_forcemanaged_destination_file'],
            'time_submitted' => $data->info['time_submitted'],
          ];
          $added[] = \Drupal::queue($data->info['queue_name'])
            ->createItem($adodata);
        }
      }
      elseif ( $data->pluginconfig->op === 'action') {
        // We pass NULL as op here since access control will be done at the queue action worker level
        // based on what the actual action does. E.g if exporting to another format, there is no need to check
        // for delete/update/etc.
        // Top level UUIDs.
        $uuids_and_csvs = $this->AmiUtilityService->getProcessedAmiSetNodeUUids($csv_file, $data, NULL);
        $uuids = array_unique(array_keys($uuids_and_csvs));
        if (empty($uuids)) {
          $message = $this->t('There are no ADO UUIDs in @csv for Set @setid that can be processed via an action.', [
            '@setid' => $data->info['set_id'],
            '@csv' => $csv_file->getFilename(),
          ]);
          $this->loggerFactory->get('ami_file')->error($message, [
            'setid' => $data->info['set_id'] ?? NULL,
            'time_submitted' => $data->info['time_submitted'] ?? '',
          ]);
          return;
        }
        else {
          foreach (array_chunk($uuids, $data->info['batch_size']?? 10) as $batch_data_uuid) {
            $adodata->info = [
              'uuids' => $batch_data_uuid,
              'set_id' => $data->info['set_id'],
              'uid' => $data->info['uid'],
              'action' => $data->info['action'] ?? NULL,
              'action_config' => $data->info['action_config'] ?? [],
              'set_url' => $data->info['set_url'],
              'attempt' => 1,
              'queue_name' => "ami_action_ado",
              'time_submitted' => $data->info['time_submitted'],
              'batch_size' => $data->info['batch_size'] ?? 10,
            ];
            $added[] = \Drupal::queue("ami_action_ado")
              ->createItem($adodata);
          }
          foreach ($uuids_and_csvs as $uuid => $children_csvs) {
            if (count($children_csvs)) {
              $current_uuid = $uuid;
              $data_csv = clone $data;
              if (!is_array($children_csvs)) { continue;}
              foreach ($children_csvs as $child_csv) {
                if (strlen(trim($child_csv ?? '')) >= 5) {
                  $filenames = array_map(function ($value) {
                    $value = $value ?? '';
                    return trim($value);
                  }, explode(';', $child_csv));
                  $filenames = array_filter($filenames);
                  // We will keep the original row ID, so we can log it.
                  foreach ($filenames as $filename) {
                    $data_csv->info['csv_filename'] = $filename;
                    $csv_file = $this->processCSvFile($data_csv);
                    if ($csv_file) {
                      // This will enqueue another CSV to expand.
                      $data_csv->info['csv_file'] = $csv_file;
                      // Push to the CSV  queue
                      \Drupal::queue('ami_csv_ado')
                        ->createItem($data_csv);
                    }
                  }
                }
              }
            }
          }
        }
      }
      else {
        error_log('wrong op');
      }
      if (count($added)) {
        $message = $this->t('CSV @csv for Set @setid was expanded to @count ADOs', [
          '@setid' => $data->info['set_id'],
          '@csv' => $csv_file->getFilename(),
          '@count' => count($added),
        ]);
        $this->loggerFactory->get('ami_file')->info($message, [
          'setid' => $data->info['set_id'] ?? NULL,
          'time_submitted' => $data->info['time_submitted'] ?? '',
        ]);
      }
      if (count($invalid)) {
        $invalid_message = $this->formatPlural(count($invalid),
          'Source data Row @row had an issue, common cause is an invalid parent.',
          '@count rows, @row, had issues, common causes are invalid parents and/or non existing referenced rows.',
          [
            '@row' => implode(', ', array_keys($invalid)),
          ]
        );
        $this->loggerFactory->get('ami_file')->warning($invalid_message, [
          'setid' => $data->info['set_id'] ?? NULL,
          'time_submitted' => $data->info['time_submitted'] ?? '',
        ]);
      }
      if (!count($added)) {
        $message = $this->t('CSV @csv for Set @setid generated no ADOs. Check your CSV for missing UUIDs and other required elements', [
          '@setid' => $data->info['set_id'],
          '@csv' => $csv_file->getFilename(),
        ]);
        $this->loggerFactory->get('ami_file')->warning($message, [
          'setid' => $data->info['set_id'] ?? NULL,
          'time_submitted' => $data->info['time_submitted'] ?? '',
        ]);
      }
      $processed_set_status = $this->statusStore->get('set_' . $data->info['set_id']);
      $processed_set_status['processed'] = $processed_set_status['processed'] ?? 0;
      $processed_set_status['errored'] = $processed_set_status['errored'] ?? 0;
      $processed_set_status['total'] = $processed_set_status['total'] ?? 0 + count($added);
      $this->statusStore->set('set_' . $data->info['set_id'], $processed_set_status);
      return;
    }
    else {
      $message = $this->t('The referenced CSV @filename from Set @setid, enqueued to be expanded, could not be found. Skipping',
        [
          '@setid' => $data->info['set_id'],
          '@filename' => $data->info['csv_filename'],
        ]);
      $this->loggerFactory->get('ami_file')->error($message ,[
        'setid' => $data->info['set_id'] ?? NULL,
        'time_submitted' => $data->info['time_submitted'] ?? '',
      ]);
    }
    return;
  }
}

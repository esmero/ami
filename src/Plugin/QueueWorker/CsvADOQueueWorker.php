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

    /* Data info for an CSV has this structure
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
    $adodata = clone $data;
    $adodata->info = NULL;
    $added = [];
    // @TODO discuss with Allison the idea that one could ingest with "AMI set" data but without an actual AMI set?
    // That would require, e.g generating a fake $data->info['set_id']
    if (!empty($data->info['csv_file'])) {
      $invalid = [];
      // Note. We won't process the nested CSV here. This queue worker only takes a CSV and splits into smaller
      // chunks. Basically what the \Drupal\ami\Form\amiSetEntityProcessForm::submitForm already does.
      // But the ADO worker itself will (new code) extract a CSV and then again, enqueue back to this so this one can yet again
      // split into smaller chuncks and so on.
      $info = $this->AmiUtilityService->preprocessAmiSet($data->info['csv_file'], $data, $invalid, FALSE);

      if (!count($info)) {
        //@TODO tell the user which CSV failed please?
        $message = $this->t('So sorry. CSV for @setid produced no ADOs. Please correct your source CSV data', [
          '@setid' => $data->info['set_id']
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
      if (count($added)) {
        $message = $this->t('CSV for Set @setid was expanded to ADOs', [
          '@setid' => $data->info['set_id']
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
      $processed_set_status = $this->statusStore->get('set_' . $data->info['set_id']);
      $processed_set_status['processed'] = $processed_set_status['processed'] ?? 0;
      $processed_set_status['errored'] = $processed_set_status['errored'] ?? 0;
      $processed_set_status['total'] = $processed_set_status['total'] ?? 0 + count($added);
      $this->statusStore->set('set_' . $data->info['set_id'], $processed_set_status);
      return;
    }
    // @TODO add a logger error saying it was enqueued as CSV but there was no CSV file to be found
    return;
  }
}

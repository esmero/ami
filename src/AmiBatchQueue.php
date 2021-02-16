<?php


namespace Drupal\ami;

use Drupal\Core\Queue\RequeueException;
use Drupal\Core\Queue\SuspendQueueException;
use Drupal\Core\Render\Markup;

/**
 * Batch Class to process a AMI Sets
 *
 * Class AmiBatchQueue
 *
 * @package Drupal\ami
 */
class AmiBatchQueue {

  /**
   *  Batch processes on Queue item at the time for an AMI Set.
   * @param string $queue_name
   * @param string $set_id
   * @param array $context
   *
   * @throws \Drupal\Component\Plugin\Exception\PluginException
   */
  public static function takeOne(string $queue_name, string $set_id, array &$context) {
    /** @var $queue_manager \Drupal\Core\Queue\QueueWorkerManagerInterface */
    $queue_manager = \Drupal::service('plugin.manager.queue_worker');
    /** @var \Drupal\Core\Queue\QueueFactory $queue_factory */
    $queue_factory = \Drupal::service('queue');

    $queue_factory->get($queue_name)->createQueue();
    // The actual queue worker is the one from the general AMI ingest Queue
    // That way this "per set" queue does not appear in any queue_ui listings
    // not can be processed out of the context of a UI facing batch.
    $queue_worker = $queue_manager->createInstance('ami_ingest_ado');
    $queue = $queue_factory->get($queue_name);

    $num_of_items = $queue->numberOfItems();
    if (!array_key_exists('max', $context['sandbox'])
      || $context['sandbox']['max'] < $num_of_items
    ) {
      $context['sandbox']['max'] = $num_of_items;
    }

    $context['finished'] = 0;
    $context['results']['queue_name']  = $queue_name;
    $context['results']['queue_label'] = 'AMI Set '. ($set_id ?? '');



    try {
      // Only process Items of this Set if $context['set_id'] is set.
      if ($item = $queue->claimItem()) {
        $ado_title = isset($item->data->info['row']['uuid']) ? 'ADO with UUID '.$item->data->info['row']['uuid'] : 'Unidentifed ADO without UUID';
        $title = t('For %name processing %adotitle, <b>%count</b> items remaining', [
          '%name' => $context['results']['queue_label'],
          '%adotitle' => $ado_title,
          '%count' => $num_of_items,
        ]);
        $context['message'] = $title;

        // Process and delete item
        $queue_worker->processItem($item->data);
        $queue->deleteItem($item);

        $num_of_items = $queue->numberOfItems();

        // Update context
        $context['results']['processed'][] = $item->item_id;
        $context['finished'] = ($context['sandbox']['max'] - $num_of_items) / $context['sandbox']['max'];
      }
      else {
        // Done processing if can not claim.
        $context['finished'] = 1;
      }
    } catch (RequeueException $e) {
      if (isset($item)) {
        $queue->releaseItem($item);
      }
    } catch (SuspendQueueException $e) {
      if (isset($item)) {
        $queue->releaseItem($item);
      }

      watchdog_exception('ami', $e);
      $context['results']['errors'][] = $e->getMessage();

      // Marking the batch job as finished will stop further processing.
      $context['finished'] = 1;
    } catch (\Exception $e) {
      // In case of any other kind of exception, log it and leave the item
      // in the queue to be processed again later.
      watchdog_exception('ami', $e);
      $context['results']['errors'][] = $e->getMessage();
    }
  }

  /**
   * Callback when finishing a batch job.
   *
   * @param $success
   * @param $results
   * @param $operations
   */
  public static function finish($success, $results, $operations) {
    // Display success of no results.
    if (!empty($results['processed'])) {
      \Drupal::messenger()->addMessage(
        \Drupal::translation()->formatPlural(
          count($results['processed']),
          '%queue: One item successfully processed.',
          '%queue: @count items successfully processed.',
          ['%queue' => $results['queue_label']]
        )
      );
    }
    elseif (!isset($results['processed'])) {
      \Drupal::messenger()->addMessage(\Drupal::translation()
        ->translate("Items were not processed. Try to release existing items or add new items to the queues."),
        'warning'
      );
    }

    if (!empty($results['errors'])) {
      \Drupal::messenger()->addError(
        \Drupal::translation()->formatPlural(
          count($results['errors']),
          'Queue %queue error: @errors',
          'Queue %queue errors: <ul><li>@errors</li></ul>',
          [
            '%queue' => $results['queue_label'],
            '@errors' => Markup::create(implode('</li><li>', $results['errors'])),
          ]
        )
      );
    }
    // Cleanup and remove the queue. This is a live batch operation.
    /** @var \Drupal\Core\Queue\QueueFactory $queue_factory */
    $queue_name = $results['queue_name'];
    $queue_factory = \Drupal::service('queue');
    $queue_factory->get($queue_name)->deleteQueue();
  }

}


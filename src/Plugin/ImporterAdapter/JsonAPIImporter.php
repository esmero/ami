<?php

namespace Drupal\ami\Plugin\ImporterAdapter;

use Drupal\Core\Batch\BatchBuilder;
use Drupal\Core\File\FileSystemInterface;
use Drupal\ami\Plugin\ImporterAdapterBase;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Messenger\MessengerTrait;

/**
 * Product importer from a JSON format.
 *
 * @ImporterAdapter(
 *   id = "jsonapi",
 *   label = @Translation("Remote JSON API Importer"),
 *   remote = true
 * )
 */
class JsonAPIImporter extends ImporterAdapterBase {

  use MessengerTrait;
  /**
   * {@inheritdoc}
   */
  public function getConfigurationForm(\Drupal\ami\Entity\ImporterAdapterInterface $importer) {
    $form = [];
    $config = $importer->getPluginConfiguration();
    $form['url'] = [
      '#type' => 'url',
      '#default_value' => isset($config['url']) ? $config['url'] : '',
      '#title' => $this->t('Url'),
      '#description' => $this->t('The URL to the import resource'),
      '#required' => TRUE,
    ];
    return $form;
  }


  /**
   * {@inheritdoc}
   */
  public function import() {
    $data = $this->getData();
    if (!$data) {
      return FALSE;
    }

    if (!isset($data->nodes)) {
      return FALSE;
    }

    $nodes = $data->nodes;

    $batch_builder = (new BatchBuilder())
      ->setTitle($this->t('Importing ADOs'))
      ->setFinishCallback([$this, 'importNodesFinished']);

    $batch_builder->addOperation([$this, 'importNodes'], [$nodes]);
    batch_set($batch_builder->toArray());

    if (PHP_SAPI == 'cli') {
      drush_backend_batch_process();
    }

    return TRUE;
  }


  /**
   * Batch operation to import the products from the JSON file.
   *
   * @param $nodes
   * @param $context
   */
  public function importNodes($nodes, &$context) {
    if (!isset($context['results']['imported'])) {
      $context['results']['imported'] = [];
    }

    if (!$nodes) {
      return;
    }

    $sandbox = &$context['sandbox'];
    if (!$sandbox) {
      $sandbox['progress'] = 0;
      $sandbox['max'] = count($nodes);
      $sandbox['products'] = $nodes;
    }

    $slice = array_splice($sandbox['products'], 0, 3);
    foreach ($slice as $node) {
      $context['message'] = $this->t('Importing product @name', ['@name' => $node->name]);
      $this->persistEntity($node);
      $context['results']['imported'][] = $node->name;
      $sandbox['progress']++;
    }

    $context['finished'] = $sandbox['progress'] / $sandbox['max'];
  }

  /**
   * Callback for when the batch processing completes.
   *
   * @param $success
   * @param $results
   * @param $operations
   */
  public function importNodesFinished($success, $results, $operations) {
    if (!$success) {
      $this->messenger()->addmessage($this->t('There was a problem with the batch'), 'error');
      return;
    }

    $imported = count($results['imported']);
    if ($imported == 0) {
      $this->messenger()->addmessage($this->t('No ADOs found to be imported.'));
    }
    else {
      $this->messenger()->addmessage($this->formatPlural($imported, '1 ADO imported.', '@count ADOs imported.'));
    }
  }

  /**
   * Loads the product data from the remote URL.
   *
   * @return \stdClass
   */
  public function getData(array $config,  $page = 0, $per_page = 20):array {

    /** @var \Drupal\ami\Entity\ImporterAdapterInterface $importer_config */
    $importer_config = $this->configuration['config'];
    $config = $importer_config->getPluginConfiguration();
    $getArguments =  $url = isset($config['getargs']) ? $config['getargs'] : NULL;
    $url = isset($config['url']) ? $config['url'] : NULL;
    if (!$url) {
      return [];
    }
    $default_bundles = $importer_config->getTargetEntityTypes();
    $default_bundle = reset($default_bundles);
    // If we have no default bundle setup do not process anything
    if (!$default_bundle) { return []; };

    // Super naive really.
    $request = $this->httpClient->get($url);
    $json_string = $request->getBody()->getContents();
    //@TODO here is where the Twig template gets applied?
    //OR do we do it on Ingest time? (QueueWorker?)
    $json = json_decode($json_string, TRUE);
    $json_error = json_last_error();
    if ($json_error == JSON_ERROR_NONE) {
      $count = $this->enqueue($json);
    }
    else {
      // ERROR
    }
    return [];
  }
}

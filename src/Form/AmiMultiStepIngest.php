<?php
/**
 * @file
 * Contains \Drupal\ami\Form\AmiMultiStepIngestBaseForm.
 */

namespace Drupal\ami\Form;

use Drupal\Core\Config\Entity\ConfigEntityListBuilder;
use Drupal\Core\Form\FormBase;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Session\AccountInterface;
use Drupal\Core\Session\SessionManagerInterface;
use Drupal\Core\TempStore\PrivateTempStoreFactory;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\Component\Serialization\Json;
use Drupal\node\Entity\Node;

/**
 * Provides a form for that invokes Importer Adapter Plugins and batch imports data
 *
 * @ingroup ami
 */
class AmiMultiStepIngest extends AmiMultiStepIngestBaseForm {

  /**
   * {@inheritdoc}
   */
  public function getFormId() : string {
    return 'ami_multistep_batch_import_form';
  }

  /**
   * {@inheritdoc}
   */
  public function buildForm(array $form, FormStateInterface $form_state) {
    $form = parent::buildForm($form, $form_state);

    dpm(\Drupal\ami\ImporterAdapterListBuilder::load());

    $form['#prefix'] = '<p>This form will import Archipelago Digital Objects</p>';
    $form['actions'] = array(
      '#type' => 'actions',
      'submit' => array(
        '#type' => 'submit',
        '#value' => 'Proceed',
      ),
    );

    return $form;
  }

  /**
   * {@inheritdoc}
   */
  public function submitForm(array &$form, FormStateInterface $form_state) {

    $host = \Drupal::request()->getHost();
    $url = $host . '/' . drupal_get_path('module', 'batch_import_example') . '/docs/animals.json';
    $request = \Drupal::httpClient()->get($url);
    $body = $request->getBody();
    $data = Json::decode($body);
    $total = count($data);

    $batch = [
      'title' => t('Importing animals'),
      'operations' => [],
      'init_message' => t('Import process is starting.'),
      'progress_message' => t('Processed @current out of @total. Estimated time: @estimate.'),
      'error_message' => t('The process has encountered an error.'),
    ];

    foreach($data as $item) {
      $batch['operations'][] = [['\Drupal\batch_import_example\Form\ImportForm', 'importAnimal'], [$item]];
    }

    batch_set($batch);
    \Drupal::messenger()->addMessage('Imported ' . $total . ' animals!');

    $form_state->setRebuild(TRUE);
  }

  /**
   * @param $entity
   * Deletes an entity
   */
  public function importAnimal($item, &$context) {
    $entity = Node::create([
        'type' => 'page',
        'langcode' => 'und',
        'title' => $item['name'],
      ]
    );
    $entity->save();
    $context['results'][] = $item['name'];
    $context['message'] = t('Created @title', array('@title' => $item['name']));
  }

}
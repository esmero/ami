<?php
/**
 * @file
 * Contains \Drupal\ami\Form\AmiMultiStepIngestBaseForm.
 */

namespace Drupal\ami\Form;

use Drupal\ami\Entity\ImporterAdapter;
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
 * This form provides 4 steps
 *  Step 1: select the Importer Plugin to be used
 *  Step 2: Provide the data the Plugin requires or load a saved config
 *  Step 3: Map Columns to Node Entity info
 *  Step 4: Provide Binaries if not remote and ingest
 *  last step can be overridden from the Base Class via $lastStep = int()
 * @ingroup ami
 */
class AmiMultiStepIngest extends AmiMultiStepIngestBaseForm {

  /**
   * {@inheritdoc}
   */
  public function getFormId() : string {
    return 'ami_multistep_import_form';
  }

  /**
   * {@inheritdoc}
   */
  public function buildForm(array $form, FormStateInterface $form_state) {
    $form = parent::buildForm($form, $form_state);
    $form['message-step'] = [
        '#markup' => '<div class="step">' . $this->t('Step @step of @laststep',[
          '@step' => $this->step,
           '@laststep' => $this->lastStep,
          ]) . '</div>',
        ];

    if ($this->step == 1) {
      $pluginValue = $this->store->get('plugin', NULL);
      $definitions = $this->importerManager->getDefinitions();
      $options = [];
      foreach ($definitions as $id => $definition) {
        $options[$id] = $definition['label'];
      }

      $form['plugin'] = [
        '#type' => 'select',
        '#title' => $this->t('Plugin'),
        '#default_value' => $pluginValue, // $importer->getPluginId(),
        '#options' => $options,
        '#description' => $this->t('The plugin to be used to import ADOs.'),
        '#required' => TRUE,
        '#empty_option' => $this->t('Please select a plugin'),
      ];
    }
    if ($this->step == 2) {
      $parents = ['pluginconfig'];
      $form_state->setValue('pluginconfig', $this->store->get('pluginconfig',[]));
      $pluginValue = $this->store->get('plugin', NULL);
      // Only create a new instance if we do not have the PluginInstace around
      /* @var $plugin_instance \Drupal\ami\Plugin\ImporterAdapterInterface | NULL */
      $plugin_instance = $this->store->get('plugininstance');
      if (!$plugin_instance || $plugin_instance->getPluginid() != $pluginValue || $pluginValue == NULL) {
        $configuration = [];
        $configuration['config'] = ImporterAdapter::create();
        $plugin_instance = $this->importerManager->createInstance($pluginValue,$configuration);
        $this->store->set('plugininstance',$plugin_instance);
      }

      $form['pluginconfig'] = $plugin_instance->interactiveForm($parents, $form_state);
      $form['pluginconfig']['#tree'] = TRUE;
    }
    return $form;
  }

  /**
   * {@inheritdoc}
   */
  public function submitForm(array &$form, FormStateInterface $form_state) {
    parent::submitForm($form, $form_state);
    if ($form_state->getValue('plugin', NULL)) {
      $this->store->set('plugin', $form_state->getValue('plugin'));
    }
    if ($form_state->getValue('pluginconfig', [])) {
      $this->store->set('pluginconfig', $form_state->getValue('pluginconfig'));

    }
    if ($this->step == 3) {
      /* @var $plugin_instance \Drupal\ami\Plugin\ImporterAdapterInterface| NULL */
      $plugin_instance = $this->store->get('plugininstance');
      if ($plugin_instance) {
        $data = $plugin_instance->getData($this->store->get('pluginconfig'),0,20);
        dpm($data);
      }
    }


    return;

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

}

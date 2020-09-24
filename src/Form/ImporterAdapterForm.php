<?php

namespace Drupal\ami\Form;

use Drupal\Core\Entity\EntityForm;
use Drupal\Core\Entity\EntityTypeBundleInfoInterface;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Messenger\MessengerInterface;
use Drupal\Core\Url;
use Drupal\ami\Plugin\ImporterAdapterManager;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\strawberryfield\StrawberryfieldUtilityService;

/**
 * Form for creating/editing Importer entities.
 */
class ImporterAdapterForm extends EntityForm {

  /**
   * @var \Drupal\ami\Plugin\ImporterAdapterManager
   */
  protected $importerManager;

  /**
   * @var \Drupal\strawberryfield\StrawberryfieldUtilityService
   */
  protected $strawberryfieldUtilityService;

  /* @var \Drupal\Core\Entity\EntityTypeBundleInfoInterface */
  protected $entityTypeBundleInfo;

  /**
   * ImporterAdapterForm constructor.
   *
   * @param \Drupal\ami\Plugin\ImporterAdapterManager $importerManager
   * @param \Drupal\Core\Messenger\MessengerInterface $messenger
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface $entityTypeManager
   * @param \Drupal\strawberryfield\StrawberryfieldUtilityService $strawberryfieldUtilityService
   */
  public function __construct(ImporterAdapterManager $importerManager, MessengerInterface $messenger, EntityTypeManagerInterface $entityTypeManager, StrawberryfieldUtilityService $strawberryfieldUtilityService, EntityTypeBundleInfoInterface $entityTypeBundleInfo) {
    $this->importerManager = $importerManager;
    $this->messenger = $messenger;
    $this->entityTypeManager = $entityTypeManager;
    $this->entityTypeBundleInfo = $entityTypeManager;
    $this->strawberryfieldUtilityService = $strawberryfieldUtilityService;
    $this->entityTypeBundleInfo = $entityTypeBundleInfo;
  }

  /**
   * {@inheritdoc}
   */
  public static function create(ContainerInterface $container) {
    return new static(
      $container->get('ami.importeradapter_manager'),
      $container->get('messenger'),
      $container->get('entity_type.manager'),
      $container->get('strawberryfield.utility'),
      $container->get('entity_type.bundle.info')

    );
  }

  /**
   * {@inheritdoc}
   */
  public function form(array $form, FormStateInterface $form_state) {
    $form = parent::form($form, $form_state);

    /** @var \Drupal\ami\Entity\ImporterAdapterInterface $importer */
    $importer = $this->entity;

    $bundles = $this->entityTypeBundleInfo->getBundleInfo('node');
    $nodebundleoptions = [];
    foreach ($bundles as $id => $definition) {
      if ($this->strawberryfieldUtilityService->bundleHasStrawberryfield($id)){
        $nodebundleoptions[$id] = $definition['label'];
      }
    }


    $form['label'] = [
      '#type' => 'textfield',
      '#title' => $this->t('Name'),
      '#maxlength' => 255,
      '#default_value' => $importer->label(),
      '#description' => $this->t('Name of the Importer.'),
      '#required' => TRUE,
    ];

    $form['id'] = [
      '#type' => 'machine_name',
      '#default_value' => $importer->id(),
      '#machine_name' => [
        'exists' => '\Drupal\ami\Entity\ImporterAdapter::load',
      ],
      '#disabled' => !$importer->isNew(),
    ];

    $definitions = $this->importerManager->getDefinitions();
    $options = [];
    foreach ($definitions as $id => $definition) {
      $options[$id] = $definition['label'];
    }

    $form['plugin'] = [
      '#type' => 'select',
      '#title' => $this->t('Plugin'),
      '#default_value' => $importer->getPluginId(),
      '#options' => $options,
      '#description' => $this->t('The plugin to be used with this importer.'),
      '#required' => TRUE,
      '#empty_option' => $this->t('Please select a plugin'),
      '#ajax' => array(
        'callback' => [$this, 'pluginConfigAjaxCallback'],
        'wrapper' => 'plugin-configuration-wrapper'
      ),
    ];

    $form['plugin_configuration'] = [
      '#type' => 'hidden',
      '#prefix' => '<div id="plugin-configuration-wrapper">',
      '#suffix' => '</div>',
    ];

    $plugin_id = NULL;
    if ($importer->getPluginId()) {
      $plugin_id = $importer->getPluginId();
    }
    if ($form_state->getValue('plugin') && $plugin_id !== $form_state->getValue('plugin')) {
      $plugin_id = $form_state->getValue('plugin');
    }

    if ($plugin_id) {
      /** @var \Drupal\ami\Plugin\ImporterAdapterInterface $plugin */
      $plugin = $this->importerManager->createInstance($plugin_id, ['config' => $importer]);
      $form['plugin_configuration']['#type'] = 'details';
      $form['plugin_configuration']['#tree'] = TRUE;
      $form['plugin_configuration']['#open'] = TRUE;
      $form['plugin_configuration']['#title'] = $this->t('Plugin configuration for <em>@plugin</em>', ['@plugin' => $plugin->getPluginDefinition()['label']]);
      $form['plugin_configuration']['plugin'] = $plugin->getConfigurationForm($importer);
    }


    $form['update_existing'] = [
      '#type' => 'checkbox',
      '#title' => $this->t('Update existing'),
      '#description' => $this->t('Whether to update existing ADOs if already imported.'),
      '#default_value' => $importer->updateExisting(),
    ];

    $form['target_entity_types'] =  [
      '#type' => 'checkboxes',
      '#options' => $nodebundleoptions,
      '#title' => $this->t('Which Content types will this Importer be allowed to be Ingest?'),
      '#required'=> TRUE,
      '#default_value' => (!$importer->isNew()) ? $importer->getTargetEntityTypes(): [],
    ];

    return $form;
  }

  /**
   * {@inheritdoc}
   */
  public function save(array $form, FormStateInterface $form_state) {
    /** @var \Drupal\ami\Entity\ImporterAdapterInterface $importer */
    $importer = $this->entity;
    $importer->set('plugin_configuration', $importer->getPluginConfiguration()['plugin']);
    $status = $importer->save();

    switch ($status) {
      case SAVED_NEW:
        $this->messenger->addMessage($this->t('Created the %label Importer.', [
          '%label' => $importer->label(),
        ]));
        break;

      default:
        $this->messenger->addMessage($this->t('Saved the %label Importer.', [
          '%label' => $importer->label(),
        ]));
    }

    $form_state->setRedirectUrl($importer->toUrl('collection'));
  }

  /**
   * Ajax callback for the plugin configuration form elements.
   *
   * @param $form
   * @param \Drupal\Core\Form\FormStateInterface $form_state
   *
   * @return array
   */
  public function pluginConfigAjaxCallback($form, FormStateInterface $form_state) {
    return $form['plugin_configuration'];
  }


}

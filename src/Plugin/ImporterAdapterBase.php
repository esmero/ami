<?php

namespace Drupal\ami\Plugin;

use Drupal\Component\Plugin\Exception\PluginException;
use Drupal\Component\Plugin\PluginBase;
use Drupal\Core\DependencyInjection\DependencySerializationTrait;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Messenger\MessengerTrait;
use Drupal\ami\AmiUtilityService;
use Drupal\Core\Plugin\ContainerFactoryPluginInterface;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\ami\Entity\ImporterAdapterInterface;
use Drupal\ami\Plugin\ImporterAdapterInterface as ImporterPluginAdapterInterface ;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\file\Entity\File;


/**
 * Base class for ImporterAdapter plugins.
 */
abstract class ImporterAdapterBase extends PluginBase implements ImporterPluginAdapterInterface, ContainerFactoryPluginInterface {

  use StringTranslationTrait;
  use DependencySerializationTrait;
  /**
   * @var \Drupal\Core\Entity\EntityTypeManager
   */
  protected $entityTypeManager;

  /**
   * @var \GuzzleHttp\ClientInterface
   */
  protected $httpClient;

  /**
   * @var \Drupal\ami\AmiUtilityService
   */
  protected $AmiUtilityService;


  /**
   * ImporterAdapterBase constructor.
   *
   * @param array $configuration
   * @param $plugin_id
   * @param $plugin_definition
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface $entityTypeManager
   * @param \Drupal\ami\AmiUtilityService $ami_utility
   *
   * @throws \Drupal\Component\Plugin\Exception\PluginException
   */
  public function __construct(array $configuration, $plugin_id, $plugin_definition, EntityTypeManagerInterface $entityTypeManager,  AmiUtilityService $ami_utility) {
    parent::__construct($configuration, $plugin_id, $plugin_definition);
    $this->entityTypeManager = $entityTypeManager;
    $this->AmiUtilityService = $ami_utility;

    //@TODO we do not need always a new config.
    // Configs can be empty/unsaved.
    if (!is_array($configuration) && !isset($configuration['config'])) {
      throw new PluginException('Missing AMI ImporterAdapter configuration.');
    }

    if (!$configuration['config'] instanceof ImporterAdapterInterface) {
      throw new PluginException('Wrong AMI ImporterAdapter configuration.');
    }
  }

  /**
   * {@inheritdoc}
   */
  public static function create(ContainerInterface $container, array $configuration, $plugin_id, $plugin_definition) {
    return new static(
      $configuration,
      $plugin_id,
      $plugin_definition,
      $container->get('entity_type.manager'),
      $container->get('ami.utility')
    );
  }

  /**
   * {@inheritdoc}
   */
  public function getConfig() {
    return $this->configuration['config'];
  }

  /**
   * {@inheritdoc}
   */
  public function settingsForm(array $parents, FormStateInterface $form_state): array {
  }

  /**
   * {@inheritdoc}
   */
  public function interactiveForm(array $parents = [], FormStateInterface $form_state): array {
    $form['op'] = [
      '#type' => 'select',
      '#title' => $this->t('Operation'),
      '#options' => [
        'create' => 'Create New ADOs',
        'update' => 'Update existing ADOs',
        //'patch' => 'Patch existing ADOs',
      ],
      '#description' => $this->t('The desired Operation'),
      '#required' => TRUE,
      '#default_value' =>  $form_state->getValue(array_merge($parents , ['op'])),
      '#empty_option' => $this->t('- Please select an Operation -'),
    ];
    return $form;
  }

  /**
   * {@inheritdoc}
   */
  public function getData(array $config,  $page = 0, $per_page = 20): array {
    return [];
  }

  /**
   * {@inheritdoc}
   */
  public function getInfo(array $config, FormStateInterface $form_state, $page = 0, $per_page = 20): array {
    return [];
  }

  /**
   * {@inheritdoc}
   */
  public function getBatch(FormStateInterface $form_state, array $config, \stdClass $amisetdata) {
  }

  /**
   * {@inheritdoc}
   */
  public static function fetchBatch(array $config, ImporterPluginAdapterInterface $plugin_instance, File $file, \stdClass $amisetdata, array &$context):void {
  }

  /**
   * {@inheritdoc}
   */
  public function provideKeys(array $config, array $data): array {
    return $data['headers']?? [];
  }

  /**
   * {@inheritdoc}
   */
  public function provideTypes(array $config, array $data): array {
    $type_column_index = array_search('type', $this->provideKeys($config, $data));
    $alltypes = [];
    if ($type_column_index !== FALSE) {
      $alltypes = $this->AmiUtilityService->getDifferentValuesfromColumn($data,
        $type_column_index);
    }
    return $alltypes;
  }


}

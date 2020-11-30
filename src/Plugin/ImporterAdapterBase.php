<?php

namespace Drupal\ami\Plugin;

use Drupal\Component\Plugin\Exception\PluginException;
use Drupal\Component\Plugin\PluginBase;
use Drupal\Core\DependencyInjection\DependencySerializationTrait;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Messenger\MessengerTrait;
use Drupal\Core\Plugin\ContainerFactoryPluginInterface;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\ami\Entity\ImporterAdapterInterface;
use Drupal\ami\Plugin\ImporterAdapterInterface as ImporterPluginAdapterInterface ;
use GuzzleHttp\ClientInterface;
use Drupal\Core\Form\FormBuilderInterface;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\Core\Queue\QueueFactory;
use Drupal\Core\Queue\QueueWorkerManager;

/**
 * You can use this constant to set how many queued items
 * you want to be processed in one batch operation
 */
define("IMPORT_XML_BATCH_SIZE", 1);

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
   * @var \GuzzleHttp\Client
   */
  protected $httpClient;

  /**
   * {@inheritdoc}
   */
  public function __construct(array $configuration, $plugin_id, $plugin_definition, EntityTypeManagerInterface $entityTypeManager) {
    parent::__construct($configuration, $plugin_id, $plugin_definition);
    $this->entityTypeManager = $entityTypeManager;
    /*$this->httpClient = $httpClient;
    $this->formBuilder = $formBuilder;
    $this->queue_factory = $queue_factory;
    $this->queue_manager = $queue_manager; */
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
      $container->get('entity_type.manager')
     /* $container->get('form_builder'),
      $container->get('queue'),
      $container->get('plugin.manager.queue_worker') */
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
    return [];
  }

  /**
   * {@inheritdoc}
   */
  public function interactiveForm(array $parents, FormStateInterface $form_state): array {
    return [];
  }

  /**
   * {@inheritdoc}
   */
  public function getData(array $config, $page = 0, $per_page = 20): array {
    return [];
  }


}

<?php

namespace Drupal\ami\Plugin;

use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Plugin\DefaultPluginManager;
use Drupal\Core\Cache\CacheBackendInterface;
use Drupal\Core\Extension\ModuleHandlerInterface;

/**
 * Provides the ImporterAdapter plugin manager.
 */
class ImporterAdapterManager extends DefaultPluginManager {

  /**
   * @var \Drupal\Core\Entity\EntityTypeManagerInterface
   */
  protected $entityTypeManager;

  /**
   * ImporterAdapterManager constructor.
   *
   * @param \Traversable $namespaces
   *   An object that implements \Traversable which contains the root paths
   *   keyed by the corresponding namespace to look for plugin implementations.
   * @param \Drupal\Core\Cache\CacheBackendInterface $cache_backend
   *   Cache backend instance to use.
   * @param \Drupal\Core\Extension\ModuleHandlerInterface $module_handler
   *   The module handler to invoke the alter hook with.
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface $entityTypeManager
   */
  public function __construct(\Traversable $namespaces, CacheBackendInterface $cache_backend, ModuleHandlerInterface $module_handler, EntityTypeManagerInterface $entityTypeManager) {
    parent::__construct('Plugin/ImporterAdapter', $namespaces, $module_handler,
      'Drupal\ami\Plugin\ImporterAdapterInterface', 'Drupal\ami\Annotation\ImporterAdapter');

    $this->alterInfo('ami_importeradapter_info');
    $this->setCacheBackend($cache_backend, 'ami_importeradapter_plugins');
    $this->entityTypeManager = $entityTypeManager;
  }

  /**
   * Creates an instance of ImporterAdapterInterface plugin based on the ID of a
   * configuration entity.
   *
   * @param $id
   *   Configuration entity ID
   *
   * @return null|\Drupal\ami\Plugin\ImporterAdapterInterface
   */
  public function createInstanceFromConfig($id) {
    $config = $this->entityTypeManager->getStorage('importeradapter')->load($id);
    if (!$config instanceof \Drupal\ami\Entity\ImporterAdapterInterface) {
      return NULL;
    }

    return $this->createInstance($config->getPluginId(), ['config' => $config]);
  }

  /**
   * Creates an array of importer plugins from all the existing ImporterAdapter
   * configuration entities.
   *
   * @return \Drupal\ami\Plugin\ImporterAdapterInterface[]
   */
  public function createInstanceFromAllConfigs() {
    $configs = $this->entityTypeManager->getStorage('importeradapter')->loadMultiple();
    if (!$configs) {
      return [];
    }
    $plugins = [];
    foreach ($configs as $config) {
      $plugin = $this->createInstanceFromConfig($config->id());
      if (!$plugin) {
        continue;
      }

      $plugins[] = $plugin;
    }

    return $plugins;
  }

}

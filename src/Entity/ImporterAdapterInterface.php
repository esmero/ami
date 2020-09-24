<?php

namespace Drupal\ami\Entity;

use Drupal\Core\Config\Entity\ConfigEntityInterface;
use Drupal\Core\Url;

/**
 * ImporterAdapter configuration entity.
 */
interface ImporterAdapterInterface extends ConfigEntityInterface {

  /**
   * Returns the configuration specific to the chosen plugin.
   *
   * @return array
   */
  public function getPluginConfiguration();

  /**
   * Returns the ImporterAdapter plugin ID to be used by this importer.
   *
   * @return string
   */
  public function getPluginId();

  /**
   * Whether or not to update existing products if they have already been imported.
   *
   * @return bool
   */
  public function updateExisting();

  /**
   * Returns the Product type that needs to be created.
   *
   * @return array
   */
  public function getTargetEntityTypes();

  /**
   * Target Entity Types setter.
   *
   * @param array $target_entity_types
   *   A list of Node Types or Bundle names.
   */
  public function setTargetEntityTypes(array $target_entity_types): void;

  /**
   * Checks if this Config is active.
   *
   * @return bool
   *   True if active.
   */
  public function isActive(): bool;

  /**
   * Sets the active flag.
   *
   * @param bool $active
   *   True to set Active.
   */
  public function setActive(bool $active): void;

}

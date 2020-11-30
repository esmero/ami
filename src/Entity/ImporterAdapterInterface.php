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
  public function getPluginConfiguration(): array;

  /**
   * @param array $pluginconfig
   */
  public function setPluginconfig(array $pluginconfig): void;


  /**
   * Returns the ImporterAdapter plugin ID to be used by this importer.
   *
   * @return string
   */
  public function getPluginId();

  /**
   * Whether or not to update existing ADOs if they have already been imported.
   * @TODO check if this is needed, because we may want a FULL CRUD option
   *
   * @return bool
   */
  public function updateExisting();


  /**
   * Sets the CVS to ADO mapping configs
   *
   * @return void
   */
  public function setAdoMappings(array $mappings): void;

  /**
   * Gets the CVS to ADO mapping configs
   *
   * @return array
   */
  public function getAdoMappings(): array;

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

  /**
   * Returns the ADO types this config can operate on.
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

}

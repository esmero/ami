<?php

namespace Drupal\ami\Plugin;

use Drupal\Component\Plugin\PluginInspectionInterface;

/**
 * Defines an interface for ImporterAdapter plugins.
 */
interface ImporterAdapterInterface extends PluginInspectionInterface {

  /**
   * Performs the import. Returns TRUE if the import was successful or FALSE otherwise.
   *
   * @return bool
   */
  public function import();

  /**
   * Returns the ImporterAdapter configuration entity.
   *
   * @return \Drupal\ami\Entity\ImporterAdapterInterface
   */
  public function getConfig();

  /**
   * Returns the form array for configuring this plugin.
   *
   * @param \Drupal\ami\Entity\ImporterAdapterInterface $importer
   *
   * @return array
   */
  public function getConfigurationForm(\Drupal\ami\Entity\ImporterAdapterInterface $importer);


  /**
   * Saves a SBF bearing entity from the remote data.
   *
   * @param \stdClass $data
   */
  public function persistEntity($data);

}

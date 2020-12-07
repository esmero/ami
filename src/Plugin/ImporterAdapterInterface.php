<?php

namespace Drupal\ami\Plugin;

use Drupal\Component\Plugin\PluginInspectionInterface;
use Drupal\Core\Form\FormStateInterface;

/**
 * Defines an interface for ImporterAdapter plugins.
 *
 * Importer Adapters only Fetch data, clean,preprocess
 * and expose data as CSV back to the module.
 *
 * @see \Drupal\ami\Annotation\ImporterAdapter
 *
 */
interface ImporterAdapterInterface extends PluginInspectionInterface {

  /**
   * Returns the ImporterAdapter configuration entity.
   *
   * @return \Drupal\ami\Entity\ImporterAdapterInterface
   */
  public function getConfig();

  /**
   * Returns the settings form for this ImporterAdapter
   *
   * This form is used when not in interactive mode to prepare a preset.
   *
   * @param array $parents
   * @param \Drupal\Core\Form\FormStateInterface $form_state
   *
   * @return array
   */
  public function settingsForm(array $parents, FormStateInterface $form_state): array;


  /**
   * Returns the form for this ImporterAdapter during Ingest process
   *
   * Each plugin is responsible for using this form during an AMI ingest setup
   *
   * @param array $parents
   * @param \Drupal\Core\Form\FormStateInterface $form_state
   *
   * @return array
   */
  public function interactiveForm(array $parents = [], FormStateInterface $form_state): array;


  /**
   * Get Data from the source
   *
   * @param array $config
   * @param int $page
   *   which page, defaults to 0.
   * @param int $per_page
   *   number of records per page, -1 means all.
   *
   * @return array
   *   array of associative arrays containing header and data as header =>
   *   value pairs
   */
  public function getData(
    array $config,
    $page = 0,
    $per_page = 20
  ):array;

}

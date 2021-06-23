<?php

namespace Drupal\ami\Plugin;

use Drupal\ami\Plugin\ImporterAdapterInterface as ImporterPluginAdapterInterface;
use Drupal\Component\Plugin\PluginInspectionInterface;
use Drupal\Core\Form\FormStateInterface;
use Drupal\file\Entity\File;

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
  public function getData(array $config,  $page = 0, $per_page = 20):array;

  /**
   * Get Info from the source
   *
   * @param array $config
   * @param int $page
   *   which page, defaults to 0.
   * @param int $per_page
   *   number of records per page, -1 means all.
   *
   * @return array
   *   array of associative arrays containing header and data as header =>
   *   value pairs needed to build mapping. May be equal to getData
   */
  public function getInfo(array $config, FormStateInterface $form_state, $page = 0, $per_page = 20):array;

  /**
   * Submits getData via Batch
   *  Only applies to plugins with batch = true annotations.
   *
   * @param \Drupal\Core\Form\FormStateInterface $form_state
   * @param $config
   *
   * @param \stdClass $amisetdata
   *
   * @return mixed
   */
  public function getBatch(FormStateInterface $form_state, array $config, \stdClass $amisetdata);

  /**
   * Fetches getData in increments
   *  Only applies to plugins with batch = true annotations
   *
   * @param array $config
   * @param \Drupal\ami\Plugin\ImporterAdapterInterface $plugin_instance
   * @param \Drupal\file\Entity\File $file
   *    A File ID of an existing CSV to append data to.
   * @param \stdClass $amisetdata
   * @param array $context
   *
   * @return mixed
   */
  public static function fetchBatch(array $config, ImporterPluginAdapterInterface $plugin_instance, File $file, \stdClass $amisetdata, array &$context):void;

}

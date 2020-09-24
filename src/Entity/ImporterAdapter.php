<?php

namespace Drupal\ami\Entity;

use Drupal\Core\Config\Entity\ConfigEntityBase;
use Drupal\Core\Url;

/**
 * Defines the ImporterAdapter Config entity.
 *
 * @ConfigEntityType(
 *   id = "importeradapter",
 *   label = @Translation("AMI Importer Adapter"),
 *   handlers = {
 *     "list_builder" = "Drupal\ami\ImporterAdapterListBuilder",
 *     "form" = {
 *       "add" = "Drupal\ami\Form\ImporterAdapterForm",
 *       "edit" = "Drupal\ami\Form\ImporterAdapterForm",
 *       "delete" = "Drupal\ami\Form\ImporterAdapterDeleteForm"
 *     },
 *     "route_provider" = {
 *       "html" = "Drupal\Core\Entity\Routing\AdminHtmlRouteProvider",
 *     },
 *   },
 *   config_prefix = "importeradapter",
 *   admin_permission = "administer site configuration",
 *   entity_keys = {
 *     "id" = "id",
 *     "label" = "label",
 *     "uuid" = "uuid"
 *   },
 *   links = {
 *     "add-form" = "/admin/structure/importeradapter/add",
 *     "edit-form" = "/admin/structure/importeradapter/{importeradapter}/edit",
 *     "delete-form" = "/admin/structure/importeradapter/{importeradapter}/delete",
 *     "collection" = "/admin/structure/importeradapter"
 *   },
 *   config_export = {
 *     "id",
 *     "label",
 *     "plugin",
 *     "update_existing",
 *     "target_entity_types",
 *     "active",
 *     "plugin_configuration"
 *
 *   }
 * )
 */
class ImporterAdapter extends ConfigEntityBase implements ImporterAdapterInterface {

  /**
   * The ImporterAdapter ID.
   *
   * @var string
   */
  protected $id;

  /**
   * The ImporterAdapter label.
   *
   * @var string
   */
  protected $label;

  /**
   * The plugin ID of the plugin to be used for processing this import.
   *
   * @var string
   */
  protected $plugin;

  /**
   * Whether or not to update existing SBF bearing Entities if they have already been imported.
   *
   * @var bool
   */
  protected $update_existing = TRUE;

  /**
   * The node entity types this configuration entity can target.
   *
   * @var array
   */
  protected $target_entity_types = [];

  /**
   * The configuration specific to the plugin.
   *
   * @var array
   */
  protected $plugin_configuration;

  /**
   * {@inheritdoc}
   */
  public function getPluginId() {
    return $this->plugin;
  }

  /**
   * {@inheritdoc}
   */
  public function updateExisting() {
    return $this->update_existing;
  }


  /**
   * Returns target Entity Types.
   *
   * @return array
   *   The target entity types / Node bundles.
   */
  public function getTargetEntityTypes(): array {
    return $this->target_entity_types;
  }

  /**
   * Target Entity Types setter.
   *
   * @param array $target_entity_types
   *   A list of Node Types or Bundle names.
   */
  public function setTargetEntityTypes(array $target_entity_types): void {
    $this->target_entity_types = $target_entity_types;
  }

  /**
   * {@inheritdoc}
   */
  public function getPluginConfiguration() {
    return $this->plugin_configuration;
  }

  /**
   * Checks if this Config is active.
   *
   * @return bool
   *   True if active.
   */
  public function isActive(): bool {
    return isset($this->active) ? $this->active : FALSE;
  }

  /**
   * Sets the active flag.
   *
   * @param bool $active
   *   True to set Active.
   */
  public function setActive(bool $active): void {
    $this->active = $active;
  }

}

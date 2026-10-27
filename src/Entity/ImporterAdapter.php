<?php

namespace Drupal\ami\Entity;

use Drupal\ami\Form\ImporterAdapterDeleteForm;
use Drupal\ami\Form\ImporterAdapterForm;
use Drupal\ami\ImporterAdapterListBuilder;
use Drupal\Core\Config\Entity\ConfigEntityBase;
use Drupal\Core\Entity\Routing\AdminHtmlRouteProvider;
use Drupal\Core\StringTranslation\TranslatableMarkup;
use Drupal\Core\Url;
use Drupal\Core\Entity\Attribute\ConfigEntityType;

/**
 * Defines the ImporterAdapter Config entity.
 */
#[ConfigEntityType(
  id: 'importeradapter',
  label: new TranslatableMarkup('AMI Importer Adapter'),
  config_prefix: 'importeradapter',
  entity_keys: [
    'id' => 'id',
    'label' => 'label',
    'uuid' => 'uuid'
  ],
  handlers: [
    'list_builder' => ImporterAdapterListBuilder::class,
    'form' => [
      'add' => ImporterAdapterForm::class,
      'edit' => ImporterAdapterForm::class,
      'delete' => ImporterAdapterDeleteForm::class
    ],
    'route_provider' => [
      'html' => AdminHtmlRouteProvider::class
    ]
  ],
  links: [
    'add-form' => '/admin/structure/importeradapter/add',
    'edit-form' => '/admin/structure/importeradapter/{importeradapter}/edit',
    'delete-form' => '/admin/structure/importeradapter/{importeradapter}/delete',
    'collection' => '/admin/structure/importeradapter'
  ],
  admin_permission: 'administer site configuration',
  config_export: [
    'id',
    'label',
    'plugin',
    'update_existing',
    'target_entity_types',
    'active',
    'plugin_configuration'
  ])]
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
  protected $plugin_configuration = [];

  /**
   * The CSV ADO mappings.
   *
   * @var array
   */
  protected $ado_mapping;

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
   * {@inheritdoc}
   */
  public function getTargetEntityTypes(): array {
    return $this->target_entity_types;
  }

  /**
   * {@inheritdoc}
   */
  public function setTargetEntityTypes(array $target_entity_types): void {
    $this->target_entity_types = $target_entity_types;
  }


  /**
   * {@inheritdoc}
   */
  public function getPluginConfiguration():array {
    return $this->plugin_configuration;
  }

  /**
   * {@inheritdoc}
   */
  public function setPluginconfig(array $pluginconfig):void {
    $this->plugin_configuration = $pluginconfig;
  }

  /**
   * Checks if this Config is active.
   *
   * @return bool
   *   True if active.
   */
  public function isActive(): bool {
    return $this->active ?? FALSE;
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

  public function setAdoMappings(array $mappings): void {
    $this->ado_mapping = $mappings;
  }

  public function getAdoMappings(): array {
    return  $this->ado_mapping;
  }


}

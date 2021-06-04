<?php

namespace Drupal\ami\Annotation;

use Drupal\Component\Annotation\Plugin;

/**
 * Defines an AMI ImporterAdapter Plugin annotation object.
 *
 * @see \Drupal\ami\Plugin\ImporterAdapterManager
 *
 * @Annotation
 */
class ImporterAdapter extends Plugin {

  /**
   * The plugin ID.
   *
   * @var string
   */
  public $id;

  /**
   * The label of the plugin.
   *
   * @var \Drupal\Core\Annotation\Translation
   *
   * @ingroup plugin_translatable
   */
  public $label;

  /**
   * If data source is remote
   *
   * @var boolean
   */
  public $remote;

  /**
   * If data fetching should run via batch or directly all
   *
   * @var boolean
   */
  public $batch;

}

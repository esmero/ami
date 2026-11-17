<?php

declare(strict_types=1);

namespace Drupal\ami\Attribute;

use Drupal\Component\Plugin\Attribute\Plugin;
use Drupal\Core\StringTranslation\TranslatableMarkup;

/**
 * Defines an AMI ImporterAdapter Plugin Attribute object.
 *
 *  Class ImporterAdapter
 *
 * @see \Drupal\ami\Plugin\ImporterAdapterManager
 *
 * @ingroup strawberry_field
 */
#[\Attribute(\Attribute::TARGET_CLASS)]
class ImporterAdapter extends Plugin {

  /**
   *  Constructs a ImporterAdapter attribute.
   *
   * @param string $id
   * @param \Drupal\Core\StringTranslation\TranslatableMarkup $label
   * @param bool $remote
   * @param bool $batch
   * @param \Drupal\Core\StringTranslation\TranslatableMarkup|null $description
   */
  public function __construct(
    public readonly string $id,
    public readonly ?TranslatableMarkup $label = NULL,
    public readonly bool $remote = FALSE,
    public readonly bool $batch = FALSE,
    public readonly ?TranslatableMarkup $description = NULL,
  ) {}

}

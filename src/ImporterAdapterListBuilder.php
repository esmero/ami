<?php

namespace Drupal\ami;

use Drupal\Core\Config\Entity\ConfigEntityListBuilder;
use Drupal\Core\Entity\EntityInterface;

/**
 * Provides a listing of Importer Adapter Config entities.
 */
class ImporterAdapterListBuilder extends ConfigEntityListBuilder {

  /**
   * {@inheritdoc}
   */
  public function buildHeader() {
    $header['label'] = $this->t('AMI Importer Adapter');
    $header['id'] = $this->t('Machine name');
    $header['active'] = $this->t('Is active ?');
    return $header + parent::buildHeader();
  }

  /**
   * {@inheritdoc}
   */
  public function buildRow(EntityInterface $entity) {
    $row['label'] = $entity->label();
    $row['id'] = $entity->id();
    $row['active'] = $entity->isActive() ? $this->t('Yes') : $this->t('No');
    return $row + parent::buildRow($entity);
  }
}

<?php
namespace Drupal\ami\Entity\Controller;

use Drupal\ami\amiSetEntityInterface;
use Drupal\Core\Entity\EntityInterface;
use Drupal\Core\Entity\EntityListBuilder;


  /**
   * Provides a list controller for the Ami Set entity.
   *
   * @ingroup ami
   */
class amiSetEntityListBuilder extends EntityListBuilder {

  /**
   * {@inheritdoc}
   *
   * We override ::render() so that we can add our own content above the table.
   * parent::render() is where EntityListBuilder creates the table using our
   * buildHeader() and buildRow() implementations.
   */
  public function render() {
    $build['description'] = [
      '#markup' => $this->t('AMI provides Sets. You can manage the fields on the <a href="@adminlink">AMI provides Sets admin page</a>.', array(
        '@adminlink' => \Drupal::urlGenerator()
          ->generateFromRoute('ami.amisetentity_settings'),
      )),
    ];

    $build += parent::render();
    return $build;
  }

  /**
   * {@inheritdoc}
   *
   * Building the header and content lines for the contact list.
   *
   * Calling the parent::buildHeader() adds a column for the possible actions
   * and inserts the 'edit' and 'delete' links as defined for the entity type.
   */
  public function buildHeader() {
    $header['uuid'] = $this->t('Set UUID');
    $header['id'] = $this->t('Set ID');
    $header['name'] = $this->t('Name');
    $header['last update'] = $this->t('Last update');
    //$header['status'] = $this->t('Status');
    return $header + parent::buildHeader();
  }

  /**
   * {@inheritdoc}
   */
  public function buildRow(EntityInterface $entity) {
    /* @var $entity \Drupal\ami\Entity\amiSetEntity */
    $row['uuid'] = $entity->uuid();
    $row['id'] = $entity->id();
    $row['name'] = $entity->toLink();
    $row['last update'] = \Drupal::service('date.formatter')->format($entity->changed->value, 'custom', 'd/m/Y');
    //$row['status'] = $entity->getStatus();
    return $row + parent::buildRow($entity);
  }

}

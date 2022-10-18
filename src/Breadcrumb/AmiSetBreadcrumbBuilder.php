<?php

namespace Drupal\ami\Breadcrumb;

use Drupal\ami\amiSetEntityInterface;
use Drupal\ami\Entity\amiSetEntity;
use Drupal\Core\Breadcrumb\Breadcrumb;
use Drupal\Core\Breadcrumb\BreadcrumbBuilderInterface;
use Drupal\Core\Link;
use Drupal\Core\Routing\RouteMatchInterface;
use Drupal\Core\StringTranslation\StringTranslationTrait;

/**
 * Provides a breadcrumb for AMI Sets
 */
class AmiSetBreadcrumbBuilder implements BreadcrumbBuilderInterface {

  use StringTranslationTrait;
  /**
   * {@inheritdoc}
   */
  public function applies(RouteMatchInterface $route_match) {
    $amiset = $route_match->getParameter('ami_set_entity');
    return $amiset instanceof amiSetEntityInterface;
  }

  /**
   * {@inheritdoc}
   */
  public function build(RouteMatchInterface $route_match) {
    $breadcrumb = new Breadcrumb();
    $breadcrumb->addCacheContexts(['route']);
    $amiset = $route_match->getParameter('ami_set_entity');
    $links[] = Link::createFromRoute($this->t('Home'), '<front>');

    $links[] = Link::createFromRoute($this->t('Ami Set List'), 'entity.ami_set_entity.collection');
    if ($amiset instanceof amiSetEntityInterface) {
    $links[] = Link::createFromRoute($amiset->label(), 'entity.ami_set_entity.canonical', ['ami_set_entity' =>$amiset->id()]);
    }
    return $breadcrumb->setLinks($links);
  }

}

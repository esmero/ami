<?php
namespace Drupal\ami;

use Drupal\Core\Entity\ContentEntityInterface;
use Drupal\user\EntityOwnerInterface;
use Drupal\Core\Entity\EntityChangedInterface;

  /**
   * Provides an interface defining a AMI Set entity.
   * @ingroup ami
   */
interface amiSetEntityInterface extends ContentEntityInterface, EntityOwnerInterface, EntityChangedInterface {

  /**
   * Gets the current Status
   *
   * @return string
   */
  public function getStatus();


  /**
   * @param $status
   *
   * @return \Drupal\ami\amiSetEntityInterface
   */
  public function setStatus(string $status);


}

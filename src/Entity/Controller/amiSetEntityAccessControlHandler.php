<?php
namespace Drupal\ami\Entity\Controller;

use Drupal\Core\Entity\EntityAccessControlHandler;
use Drupal\Core\Session\AccountInterface;
use Drupal\Core\Entity\EntityInterface;
use Drupal\Core\Access\AccessResult;

class amiSetEntityAccessControlHandler extends EntityAccessControlHandler {

  /**
   * {@inheritdoc}
   *
   * Link the activities to the permissions. checkAccess is called with the
   * $operation as defined in the routing.yml file.
   */
  protected function checkAccess(EntityInterface $entity, $operation, AccountInterface $account) {
    switch ($operation) {
      case 'view':
        return AccessResult::allowedIfHasPermission($account, 'view amiset entity');

      case 'edit':
        return AccessResult::allowedIfHasPermission($account, 'edit amiset entity');

      case 'delete':
        return AccessResult::allowedIfHasPermission($account, 'delete amiset entity');

      case 'process':
        return AccessResult::allowedIfHasPermission($account, 'process amiset entity');

      case 'deleteados':
        return AccessResult::allowedIfHasPermission($account, 'deleteados amiset entity');
    }
    return AccessResult::allowed();
  }

  /**
   * {@inheritdoc}
   *
   * Separate from the checkAccess because the entity does not yet exist, it
   * will be created during the 'add' process.
   */
  protected function checkCreateAccess(AccountInterface $account, array $context, $entity_bundle = NULL) {
    return AccessResult::allowedIfHasPermission($account, 'add amiset entity');
  }

}

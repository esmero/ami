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
    if ($account->hasPermission('administer amiset entity')) {
      return AccessResult::allowed()->cachePerPermissions();
    }
    $is_owner = ($account->id() && $account->id() === $entity->getOwnerId());

    switch ($operation) {
      case 'view':
          $access_result = AccessResult::allowedIf($account->hasPermission('view amiset entity'))
            ->cachePerPermissions()
            ->addCacheableDependency($entity);
          if ($access_result->isForbidden()) {
            $access_result = AccessResult::allowedIf($account->hasPermission('view own amiset entity'))
              ->cachePerPermissions()
              ->addCacheableDependency($entity);
          }
          return $access_result;

      case 'edit':
        if ($account->hasPermission('edit amiset entity')) {
          return AccessResult::allowed()->cachePerPermissions();
        }
        if ($account->hasPermission('edit own amiset entity') && $is_owner) {
          return AccessResult::allowed()->cachePerPermissions()->cachePerUser()->addCacheableDependency($entity);
        }
        else {
          return AccessResult::neutral()
            ->cachePerPermissions()
            ->addCacheableDependency($entity);
        }

      case 'delete':
        if ($account->hasPermission('delete amiset entity')) {
          return AccessResult::allowed()->cachePerPermissions();
        }
        elseif ($account->hasPermission('delete own amiset entity') && $is_owner) {
          return AccessResult::allowed()->cachePerPermissions()->cachePerUser()->addCacheableDependency($entity);
        }
        else {
          return AccessResult::neutral()
            ->cachePerPermissions()
            ->addCacheableDependency($entity);
        }

      case 'process':
        if ($account->hasPermission('process amiset entity')) {
          return AccessResult::allowed()->cachePerPermissions();
        }
        elseif ($account->hasPermission('process own amiset entity') && $is_owner) {
          return AccessResult::allowed()->cachePerPermissions()->cachePerUser()->addCacheableDependency($entity);
        }
        else {
          return AccessResult::neutral()
            ->cachePerPermissions()
            ->addCacheableDependency($entity);
        }

      case 'deleteados':
        if ($account->hasPermission('deleteados amiset entity')) {
          return AccessResult::allowed()->cachePerPermissions();
        }
        elseif ($account->hasPermission('deleteados own amiset entity') && $is_owner) {
          return AccessResult::allowed()->cachePerPermissions()->cachePerUser()->addCacheableDependency($entity);
        }
        else {
          return AccessResult::neutral()
            ->cachePerPermissions()
            ->addCacheableDependency($entity);
        }
      default:
        return AccessResult::neutral()->cachePerPermissions();
    }
  }

  /**
   * {@inheritdoc}
   *
   * Separate from the checkAccess because the entity does not yet exist, it
   * will be created during the 'add' process.
   */
  protected function checkCreateAccess(AccountInterface $account, array $context, $entity_bundle = NULL) {
    $permissions = [
      'administer amiset entity',
      'add amiset entity',
    ];
    return AccessResult::allowedIfHasPermission($account, $permissions, 'OR');
  }

}

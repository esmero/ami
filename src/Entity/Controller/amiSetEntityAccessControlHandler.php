<?php
namespace Drupal\ami\Entity\Controller;

use Drupal\Core\Entity\EntityAccessControlHandler;
use Drupal\Core\Session\AccountInterface;
use Drupal\Core\Entity\EntityInterface;
use Drupal\Core\Access\AccessResult;
use Drupal\ami\AmiUtilityService;

class amiSetEntityAccessControlHandler extends EntityAccessControlHandler {

  /**
   * {@inheritdoc}
   *
   * Link the activities to the permissions. checkAccess is called with the
   * $operation as defined in the routing.yml file.
   */
  protected function checkAccess(EntityInterface $entity, $operation, AccountInterface $account) {

    // Deny access to delete processed ADOs when the AMI set is configured for update rather than create.
    if($operation == 'deleteados') {
      if(!AmiUtilityService::checkAmiSetDeleteAdosAccess($entity)) {
        return AccessResult::forbidden("Deleting processed ADOs from an update AMI set is not supported.");
      }
    }

    if ($account->hasPermission('administer amiset entity')) {
      return AccessResult::allowed()->cachePerPermissions();
    }
    $is_owner = ($account->id() && $account->id() === $entity->getOwnerId());

    switch ($operation) {
      case 'view':
        if ($account->hasPermission('view amiset entity')) {
          return AccessResult::allowed()->cachePerPermissions();
        }
        if ($account->hasPermission('view own amiset entity') && $is_owner) {
          return AccessResult::allowed()->cachePerPermissions()->cachePerUser()->addCacheableDependency($entity);
        }
        else {
          return AccessResult::neutral()
            ->cachePerPermissions()
            ->addCacheableDependency($entity);
        }
      case 'update':
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
        if ($account->hasPermission('delete own amiset entity') && $is_owner) {
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
        if ($account->hasPermission('process own amiset entity') && $is_owner) {
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
        if ($account->hasPermission('deleteados own amiset entity') && $is_owner) {
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
    return AccessResult::allowedIfHasPermissions($account, $permissions, 'OR');
  }

}

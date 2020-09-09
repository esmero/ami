<?php

namespace Drupal\ami\Plugin\Action;

use Drupal\Component\Plugin\ConfigurableInterface;
use Drupal\Core\Access\AccessResult;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Session\AccountInterface;
use Drupal\Core\TempStore\PrivateTempStoreFactory;
use Drupal\strawberryfield\StrawberryfieldUtilityService;
use Drupal\views\ViewExecutable;
use Drupal\views_bulk_operations\Action\ViewsBulkOperationsActionInterface;
use Drupal\Core\Plugin\PluginFormInterface;
use Drupal\strawberryfield\Plugin\Action\StrawberryfieldJsonPatch;
use Swaggest\JsonDiff\Exception as JsonDiffException;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\strawberryfield\Tools\StrawberryfieldJsonHelper;
use Swaggest\JsonDiff\JsonPatch;

/**
 * Provides an action that can Modify Entity attached SBFs via JSON Patch.
 *
 * @Action(
 *   id = "entity:ami_jsonpatch_action",
 *   action_label = @Translation("AMI JSON Patch an ADO"),
 *   category = @Translation("AMI Metadata"),
 *   deriver = "Drupal\Core\Action\Plugin\Action\Derivative\EntityChangedActionDeriver",
 *   type = "node",
 *   confirm = "true"
 * )
 */
class AmiStrawberryfieldJsonPatch extends StrawberryfieldJsonPatch implements ViewsBulkOperationsActionInterface, PluginFormInterface {

  /**
   * Action context.
   *
   * @var array
   *   Contains view data and optionally batch operation context.
   */
  protected $context;

  /**
   * The processed view.
   *
   * @var \Drupal\views\ViewExecutable
   */
  protected $view;

  /**
   * Configuration array.
   *
   * @var array
   */
  protected $configuration;

  /**
   * {@inheritdoc}
   */
  public function setContext(array &$context) {
    $this->context['sandbox'] = &$context['sandbox'];
    foreach ($context as $key => $item) {
      if ($key === 'sandbox') {
        continue;
      }
      $this->context[$key] = $item;
    }
  }

  /**
   * {@inheritdoc}
   */
  public function setView(ViewExecutable $view) {
    $this->view = $view;
  }

  /**
   * {@inheritdoc}
   */
  public function executeMultiple(array $objects) {
    $results = [];
    foreach ($objects as $entity) {
      $results[] = $this->execute($entity);
    }

    return $results;
  }


  /**
   * Default custom access callback.
   *
   * @param \Drupal\Core\Session\AccountInterface $account
   *   The user the access check needs to be preformed against.
   * @param \Drupal\views\ViewExecutable $view
   *   The View Bulk Operations view data.
   *
   * @return bool
   *   Has access.
   */
  public static function customAccess(AccountInterface $account, ViewExecutable $view) {
    return TRUE;
  }




}
<?php

namespace Drupal\ami\Plugin\Action;

use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Session\AccountInterface;
use Drupal\views\ViewExecutable;
use Drupal\views_bulk_operations\Action\ViewsBulkOperationsActionCompletedTrait;
use Drupal\views_bulk_operations\Action\ViewsBulkOperationsActionInterface;
use Drupal\Core\Plugin\PluginFormInterface;
use Drupal\strawberryfield\Plugin\Action\StrawberryfieldJsonPatch;
use Drupal\views_bulk_operations\Action\ViewsBulkOperationsPreconfigurationInterface;

/**
 * Provides an action that can Modify Entity attached SBFs via JSON Patch.
 *
 * @Action(
 *   id = "entity:ami_jsonpatch_action",
 *   action_label = @Translation("JSON Patch Metadata for Archipelago Digital Objects"),
 *   category = @Translation("AMI Metadata"),
 *   deriver = "Drupal\ami\Plugin\Action\Derivative\EntitySbfActionDeriver",
 *   type = "node",
 *   confirm = "true"
 * )
 */
class AmiStrawberryfieldJsonPatch extends StrawberryfieldJsonPatch implements ViewsBulkOperationsActionInterface, ViewsBulkOperationsPreconfigurationInterface, PluginFormInterface {

  use ViewsBulkOperationsActionCompletedTrait;
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
  public function setContext(array &$context):void {
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
  public function setView(ViewExecutable $view):void {
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

  public function buildPreConfigurationForm(array $element, array $values, FormStateInterface $form_state):array {
    return $element;
  }

  public function buildConfigurationForm(array $form, FormStateInterface $form_state) {
    $form = parent::buildConfigurationForm($form, $form_state);
    $form['jsonpatch']['#default_value'] = '[
  { "op": "test", "path": "/description", "value": "Painting" },
  { "op": "replace", "path": "/as:generator/type", "value": "Update" }
]';

    $form['simulate'] = [
      '#title' => $this->t('only simulate and debug affected JSON'),
      '#type' => 'checkbox',
      '#default_value' => ($this->configuration['simulate'] === FALSE) ? FALSE : TRUE,
    ];
    return $form;
  }

  public function submitConfigurationForm(array &$form, FormStateInterface $form_state) {
    parent::submitConfigurationForm($form, $form_state); // TODO: Change the autogenerated stub
    $this->configuration['simulate'] = $form_state->getValue('simulate');
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

  public function getPluginId() {
    return parent::getPluginId(); // TODO: Change the autogenerated stub
  }

  public function getPluginDefinition() {
    return parent::getPluginDefinition(); // TODO: Change the autogenerated stub
  }

  /**
   * {@inheritdoc}
   */
  public function access($object, AccountInterface $account = NULL, $return_as_object = FALSE) {
    return $object->access('update', $account, $return_as_object);
  }

}

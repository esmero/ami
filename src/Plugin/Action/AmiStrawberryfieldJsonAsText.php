<?php

namespace Drupal\ami\Plugin\Action;

use Drupal\Component\Diff\Diff;
use Drupal\Component\Diff\DiffFormatter;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Session\AccountInterface;
use Drupal\strawberryfield\Tools\StrawberryfieldJsonHelper;
use Drupal\views\ViewExecutable;
use Drupal\views_bulk_operations\Action\ViewsBulkOperationsActionInterface;
use Drupal\Core\Plugin\PluginFormInterface;
use Drupal\strawberryfield\Plugin\Action\StrawberryfieldJsonPatch;
use Drupal\views_bulk_operations\Action\ViewsBulkOperationsPreconfigurationInterface;
use Swaggest\JsonDiff\Exception as JsonDiffException;
use Swaggest\JsonDiff\JsonDiff;
use Swaggest\JsonDiff\JsonPatch;

/**
 * Provides an action that can Modify Entity attached SBFs via JSON Patch.
 *
 * @Action(
 *   id = "entity:ami_jsontext_action",
 *   action_label = @Translation("Text based find and replace Metadata for Archipelago Digital Objects"),
 *   category = @Translation("AMI Metadata"),
 *   deriver = "Drupal\Core\Action\Plugin\Action\Derivative\EntityChangedActionDeriver",
 *   type = "node",
 *   confirm = "true"
 * )
 */
class AmiStrawberryfieldJsonAsText extends StrawberryfieldJsonPatch implements ViewsBulkOperationsActionInterface, ViewsBulkOperationsPreconfigurationInterface, PluginFormInterface {

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
   * {@inheritdoc}
   */
  public function execute($entity = NULL) {

    /** @var \Drupal\Core\Entity\EntityInterface $entity */
    $patched = FALSE;
    if ($entity) {
      if ($sbf_fields = $this->strawberryfieldUtility->bearsStrawberryfield(
        $entity
      )) {

        foreach ($sbf_fields as $field_name) {
          /* @var $field \Drupal\Core\Field\FieldItemInterface */
          $field = $entity->get($field_name);
          /* @var \Drupal\strawberryfield\Field\StrawberryFieldItemList $field */
          $entity = $field->getEntity();
          /** @var $field \Drupal\Core\Field\FieldItemList */
          $patched = FALSE;
          foreach ($field->getIterator() as $delta => $itemfield) {
            /** @var $itemfield \Drupal\strawberryfield\Plugin\Field\FieldType\StrawberryFieldItem */
            $main_prop = $itemfield->mainPropertyName();
            $fullvaluesoriginal = $itemfield->provideDecoded(TRUE);
            $stringvalues = $itemfield->{$main_prop};
            // As simple as this
            $count = 0;
            $fullvaluesjson = [];
            $fullvalues = $stringvalues;
            $fullvalues = str_replace($this->configuration['jsonfind'], $this->configuration['jsonreplace'], $stringvalues, $count);
            // Now try to decode fullvalues
            $fullvaluesjson = json_decode($fullvalues, TRUE, 50);
            $json_error = json_last_error();
            if ($json_error != JSON_ERROR_NONE) {
              $visualjsondiff = new Diff(explode(PHP_EOL,$stringvalues), explode(PHP_EOL,$fullvalues));
              $formatter = new DiffFormatter();
              $output = $formatter->format($visualjsondiff);
              //$this->messenger()->addMessage($output);
              $this->messenger()->addError(
                $this->t(
                  'We could not safely find and replace metadata for @entity. Your result after the replacement may not be a valid JSON.',
                  [
                    '@entity' => $entity->label()
                  ]
                ));
              $this->messenger()->addMessage($output);
              return $patched;
            }
            try {
              if ($this->configuration['simulate']) {
                $this->messenger()->addMessage('In simulation Mode');
                if ($fullvalues == $stringvalues) {
                  $patched = FALSE;
                  $this->messenger()->addStatus($this->t(
                    'No Match for @entity, so skipping',
                    [
                      '@entity' => $entity->label()
                    ]
                  ));
                  return $patched;
                }
                $r = new JsonDiff(
                  $fullvaluesoriginal,
                  $fullvaluesjson,
                  JsonDiff::REARRANGE_ARRAYS + JsonDiff::SKIP_JSON_MERGE_PATCH + JsonDiff::COLLECT_MODIFIED_DIFF
                );
                 // We just keep track of the changes. If none! Then we do not set
                // the formstate flag.
                $message = $this->formatPlural($r->getDiffCnt(),
                  'Simulated patch: Digital Object @label would get one modification',
                  'Simulated patch: Digital Object @label would get @count modifications',
                  ['@label' => $entity->label()]);

                $this->messenger()->addMessage($message);
                /*$modified_diff = $r->getModifiedDiff();
                foreach ($modified_diff as $modifiedPathDiff) {
                  $this->messenger()->addMessage($modifiedPathDiff->path);
                  $this->messenger()->addMessage($modifiedPathDiff->original);
                  $this->messenger()->addMessage($modifiedPathDiff->new);
                }*/

              } else {
                if ($fullvalues == $stringvalues) {
                  $patched = FALSE;
                  $this->messenger()->addStatus($this->t(
                    'No change for @entity, so skipping',
                    [
                      '@entity' => $entity->label()
                    ]
                  ));
                  return $patched;
                }
                $patched = TRUE;
                if (!$itemfield->setMainValueFromArray((array) $fullvaluesjson)) {
                  $this->messenger()->addError(
                    $this->t(
                      'We could not persist the metadata for @entity. Your result after the replacement may not be a valid JSON. Please contact your Site Admin.',
                      [
                        '@entity' => $entity->label()
                      ]
                    )
                  );
                  $patched = FALSE;
                };
              }
            } catch (JsonDiffException $exception) {
              $patched = FALSE;
              $this->messenger()->addWarning(
                $this->t(
                  'Patch could not be applied for @entity',
                  [
                    '@entity' => $entity->label()
                  ]
                )
              );
            }
          }
        }
        if ($patched) {
          $this->logger->notice('%label had the following find: @jsonsearch and replace:@jsonreplace applied', [
            '%label' => $entity->label(),
            '@jsonsearch' => '<pre><code>'.$this->configuration['jsonfind'].'</code></pre>',
            '@jsonreplace' => '<pre><code>'.$this->configuration['jsonreplace'].'</code></pre>',

          ]);
          if (!$this->configuration['simulate']) {
            $entity->save();
          }
        }
      return $patched;
      }
    }
  }


  public function buildPreConfigurationForm(array $element, array $values, FormStateInterface $form_state) {
  }

  public function buildConfigurationForm(array $form, FormStateInterface $form_state) {
    $form['jsonfind'] = [
      '#type' => 'textfield',
      '#title' => t('JSON Search String'),
      '#default_value' => $this->configuration['jsonfind'],
      '#size' => '40',
      '#description' => t('A string you want to find inside your JSON.'),
    ];
    $form['jsonreplace'] = [
      '#type' => 'textfield',
      '#title' => t('JSON Replacement String'),
      '#default_value' => $this->configuration['jsonreplace'],
      '#size' => '40',
      '#description' => t('Replacement string for the matched search'),
    ];

    $form['simulate'] = [
      '#title' => $this->t('only simulate and debug affected JSON'),
      '#type' => 'checkbox',
      '#default_value' => ($this->configuration['simulate'] === FALSE) ? FALSE : TRUE,
    ];
    return $form;
  }

  public function submitConfigurationForm(array &$form, FormStateInterface $form_state) {
    $this->configuration['jsonfind'] = $form_state->getValue('jsonfind');
    $this->configuration['jsonreplace'] = $form_state->getValue('jsonreplace');
    $this->configuration['simulate'] = $form_state->getValue('simulate');
  }

  /**
   * {@inheritdoc}
   */
  public function validateConfigurationForm(array &$form, FormStateInterface $form_state) {

  }


  /**
   * {@inheritdoc}
   */
  public function defaultConfiguration() {
    return [
      'jsonfind' => '',
      'jsonreplace' => '',
      'simulate' => TRUE,
    ];
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

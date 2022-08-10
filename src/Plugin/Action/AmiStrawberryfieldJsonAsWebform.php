<?php

namespace Drupal\ami\Plugin\Action;

use Drupal\Component\Diff\Diff;
use Drupal\Component\Diff\DiffFormatter;
use Drupal\Core\Ajax\AjaxResponse;
use Drupal\Core\Ajax\HtmlCommand;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Session\AccountInterface;
use Drupal\Core\TempStore\PrivateTempStoreFactory;
use Drupal\views\ViewExecutable;
use Drupal\views_bulk_operations\Action\ViewsBulkOperationsActionCompletedTrait;
use Drupal\webform\Plugin\WebformElement\WebformManagedFileBase;
use Drupal\webform\Plugin\WebformElementEntityReferenceInterface;
use Swaggest\JsonDiff\Exception as JsonDiffException;
use Swaggest\JsonDiff\JsonDiff;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\strawberryfield\Tools\StrawberryfieldJsonHelper;
use Symfony\Component\HttpFoundation\RedirectResponse;


/**
 * Provides an action that can Modify Entity attached SBFs via JSON Patch.
 *
 * @Action(
 *   id = "entity:ami_jsonwebform_action",
 *   action_label = @Translation("Webform find-and-replace Metadata for Archipelago Digital Objects"),
 *   category = @Translation("AMI Metadata"),
 *   deriver = "Drupal\ami\Plugin\Action\Derivative\EntitySbfActionDeriver",
 *   type = "node",
 *   confirm = "true"
 * )
 */
class AmiStrawberryfieldJsonAsWebform extends AmiStrawberryfieldJsonAsText {

  use ViewsBulkOperationsActionCompletedTrait;
  /**
   * @var \Drupal\ami\AmiUtilityService
   */
  protected $AmiUtilityService;

  /**
   * The webform element manager.
   *
   * @var \Drupal\webform\Plugin\WebformElementManagerInterface
   */
  protected $webformElementManager;

  public static function create(ContainerInterface $container, array $configuration, $plugin_id, $plugin_definition) {
    $instance = parent::create($container, $configuration, $plugin_id, $plugin_definition);
    $instance->AmiUtilityService = $container->get('ami.utility');
    $instance->webformElementManager = $container->get('plugin.manager.webform.element');
    return $instance;

  }

  public function buildPreConfigurationForm(array $element, array $values, FormStateInterface $form_state) {

  }

  public function buildConfigurationForm(array $form, FormStateInterface $form_state) {
    $form_state->setAlwaysProcess(TRUE);
    $webform = $this->AmiUtilityService->getWebforms();
    $form['#tree'] = TRUE;
    $form['webform'] =[
      '#type' => 'select',
      '#title' => $this->t('Select which Webform you want to use'),
      '#options' => $webform,
      '#default_value' => $form_state->getValue('webform')? $form_state->getValue('webform') : NULL,
      '#empty_option' => $this->t('- Please select a webform -'),
      '#ajax' => array(
        'callback' => [$this, 'webformAjaxCallback'],
        'wrapper' => 'webform-elements-wrapper',
        'event' => 'change',
      ),
    ];

    $form['webform_elements'] = [
      '#tree' => TRUE,
      '#type' => 'container',
      '#prefix' => '<div id="webform-elements-wrapper">',
      '#suffix' => '</div>',
    ];
    $form['elements_rendered'] = [
      '#tree' => TRUE,
      '#type' => 'container',
      '#prefix' => '<div id="webform-elements-render-wrapper">',
      '#suffix' => '</div>',
    ];

    $webform_element_options = [];
    $webform_entity = NULL;
    foreach($form_state->getStorage() as $prop => $value) {
      $form_state->set($prop, $value);
    }
    $webform_id = $form_state->getValue('webform');
       if (!$webform_id) {
         $input = $form_state->getUserInput();
         $webform_id = $input['webform'] ?? NULL;
       }

    if ($webform_id) {
      /* @var \Drupal\webform\Entity\Webform $webform_entity */
      $webform_entity = $this->entityTypeManager->getStorage('webform')->load($webform_id);
      $anyelement = $webform_entity->getElementsInitializedAndFlattened('update');
      foreach ($anyelement as $elementkey => $element) {
        $element_plugin = $this->webformElementManager->getElementInstance($element);
        if (($element_plugin->getTypeName() != 'webform_wizard_page') &&
          !($element_plugin instanceof WebformManagedFileBase) &&
          ($element_plugin->getTypeName() != 'webform_metadata_nominatim') &&
          ($element_plugin->getTypeName() != 'webform_metadata_multiagent') &&
          ($element_plugin->getTypeName() != 'webform_metadata_panoramatour') &&
          $element_plugin->isInput($element)) {
          $webform_element_options[$elementkey] = ($element['#title'] ?? ' Unamed ') . $this->t('(@elementkey JSON key )',[
              '@elementkey' => $elementkey
            ]);
        }
      }
    }
    $form['webform_elements']['elements_for_this_form'] = [
      '#type' => 'select',
      // If not #validated, dynamically populated select options don't work.
      '#validated' => TRUE,
      '#title' => $this->t('Select which Form Element you want to use'),
      '#options' => count($webform_element_options) ? $webform_element_options:  [],
      '#default_value' => NULL,
      '#empty_option' => $this->t('- Please select an element -'),
      '#ajax' => array(
        'callback' => [$this,'webformElementAjaxCallback'],
        'wrapper' => 'webform-elements-render-wrapper',
        'event' => 'change',
      ),
    ];
    $chosen_element = $form_state->getValue(['webform_elements','elements_for_this_form'], NULL);
    if (!$chosen_element) {
      $input = $form_state->getUserInput();
      $chosen_element = $input['webform_elements']['elements_for_this_form'] ?? NULL;
    }

    if ($webform_entity && $chosen_element) {
      $myelement = $webform_entity->getElementDecoded($chosen_element);
      $libraries = $webform_entity->getSubmissionForm()['#attached']['library'] ?? [];
      $form['#attached']['library'] = ($form['#attached']['library'] ?? []) + $libraries;
      $cleanelement = [];
      foreach($myelement as $key => $value) {
        if (strpos($key, '#webform') === FALSE && strpos($key, '#access_') === FALSE) {
          $cleanelement[$key] = $value;
        }
      }
      $cleanelement['#required'] = FALSE;
      $cleanelement['#validated'] = FALSE;
      $form['elements_rendered']['jsonfind_element_'.$chosen_element]= $cleanelement;

      $form['elements_rendered']['jsonfind_element_'.$chosen_element]['#title'] = $this->t('Value to Search for in <em>@elementkey</em> JSON key', [ '@elementkey' => $chosen_element]);
      $form['elements_rendered']['jsonreplace_element_'.$chosen_element]= $cleanelement;
      $form['elements_rendered']['jsonreplace_element_'.$chosen_element]['#title'] = $this->t('Value to replace with in <em>@elementkey</em> JSON key', [ '@elementkey' => $chosen_element]);
    }


    $form['simulate'] = [
      '#title' => $this->t('only simulate and debug affected JSON'),
      '#type' => 'checkbox',
      '#default_value' => ($this->configuration['simulate'] === FALSE) ? FALSE : TRUE,
    ];
    return $form;
  }

  public function webformAjaxCallback(array $form, FormStateInterface $form_state) {
    return $form['webform_elements'];
  }

  public function webformElementAjaxCallback(array $form, FormStateInterface $form_state) {
    return $form['elements_rendered'];
  }

  public function submitConfigurationForm(array &$form, FormStateInterface $form_state) {
    // Hacky but its the way we can do this dynamically
    $chosen_element = $form_state->getValue(['webform_elements','elements_for_this_form'], NULL);
    if ($chosen_element) {
      $jsonfind = $form_state->getValue(['elements_rendered','jsonfind_element_'.$chosen_element], NULL) ?? ($form_state->getUserInput()['jsonfind_element_'.$chosen_element] ?? []);
      $jsonreplace = $form_state->getValue(['elements_rendered','jsonreplace_element_'.$chosen_element], NULL) ?? ($form_state->getUserInput()['jsonreplace_element_'.$chosen_element] ?? []);
      $jsonfind_ready[$chosen_element] = $jsonfind;
      $jsonreplace_ready[$chosen_element] = $jsonreplace;
      $this->configuration['jsonfind'] = json_encode($jsonfind_ready) ?? '{}';
      $this->configuration['jsonreplace'] = json_encode($jsonreplace_ready) ?? '{}';
      $this->configuration['simulate'] = $form_state->getValue('simulate');
    }
    else {
      $form_state->setRebuild(TRUE);
    }
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
            $fullvaluesmodified = $fullvaluesoriginal;
            $count = 0;
            $fullvaluesjson = [];
            // This is how it goes.
            // First we get the original key from $this->configuration['jsonfind']
            // Then we search inside the original data and see if its single valued
            // or multivalued
            // If we find the match (which means for each property of the jsonfind there
            // needs to be a match in the original
            // we replace the existing value with the following condition
            // - If jsonreplace is empty, we delete the original
            // - If not we replace the found one
            $decoded_jsonfind = json_decode($this->configuration['jsonfind'], TRUE);
            $decoded_jsonreplace = json_decode($this->configuration['jsonreplace'], TRUE);
            $key = reset(array_keys($decoded_jsonfind));
            if ($key) {
              $isAssociativeOriginal = FALSE;
              if (!empty($fullvaluesmodified[$key])) {
                if (is_array($fullvaluesmodified[$key])) {
                  $isAssociativeOriginal = StrawberryfieldJsonHelper::arrayIsMultiSimple($fullvaluesmodified[$key]);
                }
                // If none are arrays we treat them like objects 1:1 comparisson.
                if (!is_array($fullvaluesmodified[$key]) && !is_array($decoded_jsonfind[$key])) {
                  $isAssociativeOriginal = TRUE;
                }
              }
              if (!$isAssociativeOriginal) {
                // We have a few things to catch here
                // Before trying to iterate over each member trying to replace a value
                // We will check IF the $decoded_jsonfind[$key] is actually == $fullvaluesmodified[$key]
                // e.g ismemberof: [] and decoded_jsonreplace[$key] == []
                if ($fullvaluesmodified[$key] == $decoded_jsonfind[$key]) {
                  $fullvaluesmodified[$key] = $decoded_jsonreplace[$key];
                  $patched = TRUE;
                }
                else {
                  // Now if original is indexed we do not know if the replacement is a value or also indexed
                  // So try first like original is indexed but each member is an array and compare...
                  foreach ($fullvaluesmodified[$key] as &$item) {
                    if ($item == $decoded_jsonfind[$key]) {
                      // Exact Array to Array 1:1 match
                      $item = $decoded_jsonreplace[$key];
                      $patched = TRUE;
                    }
                  }
                  // We are not going to do selective a few versus another?
                  // We can!
                  if (!$patched && is_array($decoded_jsonfind[$key]) && !StrawberryfieldJsonHelper::arrayIsMultiSimple($decoded_jsonfind[$key])) {
                    // Still we need to be sure ALL things to be searched for exist. A single difference means no patching
                    // So we traverse differently here
                    // Only if all needles are in the haystack
                    // we do the actual replacement.
                    $all_found = [];
                    $fullvaluesmodified_for_key_stashed =  $fullvaluesmodified[$key];
                    foreach ($decoded_jsonfind[$key] as $item) {
                      // And to be sure we make a STRICT comparison
                      $found = array_search($item, $fullvaluesmodified[$key], TRUE);
                      if ($found !== FALSE) {
                        // Since we do not know if the Match/search for are more items than the replacements we will delete the found and add any new ones at the end.
                        unset($fullvaluesmodified[$key][$found]);
                        $patched = TRUE;
                      }
                      else {
                        $patched = FALSE;
                        break;
                      }
                    }
                    // If after this we decide we could not patch
                    // We return the original value.
                    if (!$patched) {
                      $fullvaluesmodified[$key] = $fullvaluesmodified_for_key_stashed;
                    }
                    else {
                      // Here we merge the already stripped from the Match pattern source with the replacements
                      $fullvaluesmodified[$key] = array_values(array_merge($fullvaluesmodified[$key], $decoded_jsonreplace[$key]));
                    }
                  }
                }
              }
              else {
                // Means we have a single Object not a list in the source.
                if ($fullvaluesmodified[$key] == $decoded_jsonfind[$key]) {
                  $fullvaluesmodified[$key] = $decoded_jsonreplace[$key];
                  $patched = TRUE;
                }
              }
            }
            // Now try to decode fullvalues
            $fullvaluesmodified_string = json_encode($fullvaluesmodified);
            $fullvaluesoriginal_string = json_encode($fullvaluesoriginal);
            if ($json_error != JSON_ERROR_NONE) {
              $visualjsondiff = new Diff(explode(PHP_EOL, $fullvaluesmodified_string), explode(PHP_EOL,$fullvaluesoriginal_string));
              $formatter = new DiffFormatter();
              $output = $formatter->format($visualjsondiff);
              $this->messenger()->addError(
                $this->t(
                  'We could not safely find and replace metadata for @entity. Your result after the replacement may not be a valid JSON.',
                  [
                    '@entity' => $entity->label()
                  ]
                ));
              $this->messenger()->addError($output);
              return $patched;
            }
            try {
              if ($this->configuration['simulate']) {
                $this->messenger()->addMessage('In simulation Mode');
                if ($fullvaluesoriginal_string == $fullvaluesmodified_string) {
                  $patched = FALSE;
                  $this->messenger()->addStatus($this->t(
                    'No Match for search:@jsonsearch and replace:@jsonreplace on @entity, so skipping',
                    [
                      '@entity' => $entity->label(),
                      '@jsonsearch' => '<pre><code>'.$this->configuration['jsonfind'].'</code></pre>',
                      '@jsonreplace' => '<pre><code>'.$this->configuration['jsonreplace'].'</code></pre>',

                    ]
                  ));
                  return $patched;
                }
                $r = new JsonDiff(
                  $fullvaluesoriginal,
                  $fullvaluesmodified,
                  JsonDiff::REARRANGE_ARRAYS + JsonDiff::SKIP_JSON_MERGE_PATCH + JsonDiff::COLLECT_MODIFIED_DIFF
                );
                // We just keep track of the changes. If none! Then we do not set
                // the formstate flag.
                $message = $this->formatPlural($r->getDiffCnt(),
                  'Simulated patch: Digital Object @label would get one modification',
                  'Simulated patch: Digital Object @label would get @count modifications',
                  ['@label' => $entity->label()]);

                $this->messenger()->addMessage($message);
                $modified_diff = $r->getModifiedDiff();
                foreach ($modified_diff as $modifiedPathDiff) {
                  $this->messenger()->addMessage($modifiedPathDiff->path);
                  $this->messenger()->addMessage($modifiedPathDiff->original);
                  $this->messenger()->addMessage($modifiedPathDiff->new);
                }
              }
              else {
                if ($fullvaluesoriginal_string == $fullvaluesmodified_string) {
                  $patched = FALSE;
                  $this->messenger()->addStatus($this->t(
                    'No change for @entity, skipping.',
                    [
                      '@entity' => $entity->label()
                    ]
                  ));
                  return $patched;
                }

                if ($patched) {
                  if (!$itemfield->setMainValueFromArray((array) $fullvaluesmodified)) {
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
              }
            }
            catch (JsonDiffException $exception) {
              $patched = FALSE;
              $this->messenger()->addWarning(
                $this->t(
                  'Patch could not be applied for @entity',
                  [
                    '@entity' => $entity->label()
                  ]
                )
              );
            return $patched;
            }
          }
        }
        if ($patched) {
          if (!$this->configuration['simulate']) {
            // In case after saving the Label changes we keep the original one here
            // For reporting/messaging
            $label = $entity->label();
            $this->logger->notice('%label had the following find: @jsonsearch and replace:@jsonreplace applied', [
              '%label' => $label,
              '@jsonsearch' => '<pre><code>'.$this->configuration['jsonfind'].'</code></pre>',
              '@jsonreplace' => '<pre><code>'.$this->configuration['jsonreplace'].'</code></pre>',
            ]);
            if ($entity->getEntityType()->isRevisionable()) {
              // Forces a New Revision for Not-create Operations.
              $entity->setNewRevision(TRUE);
              $entity->setRevisionCreationTime(\Drupal::time()->getRequestTime());
              // Set data for the revision
              $entity->setRevisionLogMessage('ADO modified via Webform Search And Replace with search token:' . $this->configuration['jsonfind'] .' and replace token:' .$this->configuration['jsonreplace']);
              $entity->setRevisionUserId($this->currentUser->id());
            }
            $entity->save();
            $link = $entity->toUrl()->toString();
            $this->messenger()->addStatus($this->t('ADO <a href=":link" target="_blank">%title</a> was successfully patched.',[
              ':link' => $link,
              '%title' => $label,
            ]));
          }
        }
      }
    }
    return $patched;
  }



  /**
   * {@inheritdoc}
   */
  public function validateConfigurationForm(array &$form, FormStateInterface $form_state) {
    foreach($form_state->getStorage() as $prop => $value) {
      $form_state->set($prop, $value);
    }
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

  /**
   * {@inheritdoc}
   */
  public function __sleep() {
    $obj_vars = get_object_vars($this);
    $vars = parent::__sleep();
    // Well why? Because of loggers include Request Stack and this fails
    // @see https://www.drupal.org/project/drupal/issues/3055287
    // Not the same but close.
    $unserializable[] = 'logger';
    $vars = array_diff($vars, $unserializable);
    return $vars;
  }
}

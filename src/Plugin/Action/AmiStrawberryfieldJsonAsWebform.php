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
use Drupal\webform\Plugin\WebformElement\WebformManagedFileBase;
use Drupal\webform\Plugin\WebformElementEntityReferenceInterface;
use Swaggest\JsonDiff\Exception as JsonDiffException;
use Swaggest\JsonDiff\JsonDiff;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\strawberryfield\Tools\StrawberryfieldJsonHelper;


/**
 * Provides an action that can Modify Entity attached SBFs via JSON Patch.
 *
 * @Action(
 *   id = "entity:ami_jsonwebform_action",
 *   action_label = @Translation("Webform based find and replace Metadata for Archipelago Digital Objects"),
 *   category = @Translation("AMI Metadata"),
 *   deriver = "Drupal\ami\Plugin\Action\Derivative\EntitySbfActionDeriver",
 *   type = "node",
 *   confirm = "true"
 * )
 */
class AmiStrawberryfieldJsonAsWebform extends AmiStrawberryfieldJsonAsText {

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
    $webform = $this->AmiUtilityService->getWebforms();
    $form_state->disableCache();
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
      '#type' => 'fieldset',
      '#prefix' => '<div id="webform-elements-render-wrapper">',
      '#suffix' => '</div>',
    ];

    $webform_element_options = [];
    $webform_entity = NULL;
    foreach($form_state->getStorage() as $prop => $value) {
      $form_state->set($prop, $value);
    }
    if (!empty($form_state->getValues()) && !empty($form_state->getValue('webform'))) {

      /* @var \Drupal\webform\Entity\Webform $webform_entity */
      $webform_entity = $this->entityTypeManager->getStorage('webform')->load($form_state->getValue('webform'));
      $anyelement = $webform_entity->getElementsInitializedAndFlattened();
      foreach ($anyelement as $elementkey => $element) {
        $element_plugin = $this->webformElementManager->getElementInstance($element);
        if (($element_plugin->getTypeName() != 'webform_wizard_page') && !($element_plugin instanceof WebformManagedFileBase) && $element_plugin->isInput($element)) {
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
    if ($webform_entity && $chosen_element) {
      //$myelement1 = $webform_entity->getElementsDecodedAndFlattened();
      $myelement = $webform_entity->getElementDecoded($form_state->getValue(['webform_elements','elements_for_this_form']));
      //$myelement2 = \Drupal::service('plugin.manager.webform.element')->processElements($myelement);
      $libraries = $webform_entity->getSubmissionForm()['#attached']['library'] ?? [];
      $form['#attached']['library'] = ($form['#attached']['library'] ?? []) + $libraries;
      $cleanelement = [];
      foreach($myelement as $key => $value) {
        if (strpos($key, '#webform') === FALSE && strpos($key, '#access_') === FALSE) {
          $cleanelement[$key] = $value;
        }
      }
      //$cleanelement['#element_validate'][] = [$this,'elementDynamicValidate'];
      $cleanelement['#required'] = FALSE;
      $cleanelement['#validated'] = FALSE;
      //$cleanelement['#default_value'] = NULL;
      $form['elements_rendered']['jsonfind_element']= $cleanelement;

      $form['elements_rendered']['jsonfind_element']['#title'] = $this->t('Value to Search for in <em>@elementkey</em> JSON key', [ '@elementkey' => $chosen_element]);
      $form['elements_rendered']['jsonreplace_element']= $cleanelement;
      $form['elements_rendered']['jsonreplace_element']['#title'] = $this->t('Value to replace with in <em>@elementkey</em> JSON key', [ '@elementkey' => $chosen_element]);
    }

    $form['simulate'] = [
      '#title' => $this->t('only simulate and debug affected JSON'),
      '#type' => 'checkbox',
      '#default_value' => ($this->configuration['simulate'] === FALSE) ? FALSE : TRUE,
    ];
    $form['actions']['submit']['#ajax'] = [
      'callback' => 'configureActionAjaxCallback',
    ];
    return $form;
  }

  public function elementDynamicValidate(&$element, FormStateInterface $form_state) {
    $form_state->set('holi','chao');
  }

  public function configureActionAjaxCallback(array $form, FormStateInterface $form_state) {
    //$form_state->setRebuild(TRUE);
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
    $jsonfind = $form_state->getUserInput()['elements_rendered']['jsonfind_element'] ?? [];
    $jsonreplace = $form_state->getUserInput()['elements_rendered']['jsonreplace_element'] ?? [];
    $chosen_element = $form_state->getValue(['webform_elements','elements_for_this_form'], NULL);
    // $form_state->setRebuild(TRUE);
    if ($chosen_element) {
      $jsonfind_ready[$chosen_element] = $jsonfind;
      $jsonreplace_ready[$chosen_element] = $jsonreplace;
      $this->configuration['jsonfind'] = json_encode($jsonfind_ready) ?? '{}';
      $this->configuration['jsonreplace'] = json_encode($jsonreplace_ready) ?? '{}';
      $this->configuration['simulate'] = $form_state->getValue('simulate');
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
            $key = reset(array_keys($decoded_jsonfind));
            if ($key) {
              if (!empty($fullvaluesoriginal[$key])) {
                $isAssociativeOriginal = StrawberryfieldJsonHelper::arrayIsMultiSimple($fullvaluesoriginal[$key]);
                if (!$isAssociative) {
                  foreach($fullvaluesoriginal[$key] as &$item) {
                    if ($item == $decoded_jsonfind[$key]) {
                      // Exact Array to Array 1:1 match
                      $item = $decoded_jsonfind[$key];
                      $patched = TRUE;
                    }
                  }
                }
                else {
                  // Means we have a single Object not a list in the source.
                  if ($fullvaluesoriginal[$key] == $decoded_jsonfind[$key]) {
                    $fullvaluesoriginal[$key] = $decoded_jsonfind[$key];
                    $patched = TRUE;
                  }
                }
              }
            }





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

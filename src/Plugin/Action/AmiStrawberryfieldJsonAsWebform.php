<?php

namespace Drupal\ami\Plugin\Action;

use Drupal\Core\Ajax\AjaxResponse;
use Drupal\Core\Ajax\HtmlCommand;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Session\AccountInterface;
use Drupal\Core\TempStore\PrivateTempStoreFactory;
use Drupal\views\ViewExecutable;
use Drupal\webform\Plugin\WebformElement\WebformManagedFileBase;
use Drupal\webform\Plugin\WebformElementEntityReferenceInterface;
use Symfony\Component\DependencyInjection\ContainerInterface;


/**
 * Provides an action that can Modify Entity attached SBFs via JSON Patch.
 *
 * @Action(
 *   id = "entity:ami_jsonwebform_action",
 *   action_label = @Translation("Webform based find and replace Metadata for Archipelago Digital Objects"),
 *   category = @Translation("AMI Metadata"),
 *   deriver = "Drupal\Core\Action\Plugin\Action\Derivative\EntityChangedActionDeriver",
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
    dpm($form_state->getValues());
    $webform_entity = NULL;
    if (!empty($form_state->getValues()) && !empty($form_state->getValue('webform'))) {
      /* @var \Drupal\webform\Entity\Webform $webform_entity */
      $webform_entity = $this->entityTypeManager->getStorage('webform')->load($form_state->getValue('webform'));
      $anyelement = $webform_entity->getElementsInitializedAndFlattened();
      foreach ($anyelement as $elementkey => $element) {
        $element_plugin = $this->webformElementManager->getElementInstance($element);
        if (($element_plugin->getTypeName() != 'webform_wizard_page') && !($element_plugin instanceof WebformManagedFileBase)) {
          $webform_element_options[$elementkey] = $element['#title'];
        }
      }
      //dpm($webform_entity->getElementsDecodedAndFlattened());
        //dpm($webform_entity->getElementInitialized($form_state->getValue('elements')));
        //$form['webform_elements']['elements_render']['jsonfind2']= $webform_entity->getElementInitialized($form_state->getValue('elements'));
    }
    $form['webform_elements']['elements_for_this_form'] = [
      '#type' => 'select',
      // If not #validated, dynamically populated select options don't work.
      '#validated' => TRUE,
      '#title' => $this->t('Select which Form Element you want to use'),
      '#options' => count($webform_element_options) ? $webform_element_options:  [],
      '#default_value' => NULL,
      '#submit' => [[$this, 'field_submit']],
      '#executes_submit_callback' => TRUE,
      '#empty_option' => $this->t('- Please select an element -'),
      '#ajax' => array(
        'callback' => [get_class($this),'webformElementAjaxCallback'],
        'wrapper' => 'webform-elements-render-wrapper',
        'event' => 'change',
      ),
    ];
    dpm($form_state->getValues());
    dpm($form_state->getValue(['webform_elements,elements_for_this_form']));
    if ($webform_entity && $form_state->getValue(['webform_elements','elements_for_this_form'])){
       $myelement = $webform_entity->getElement($form_state->getValue(['webform_elements','elements_for_this_form']));
       $cleanelement = [];
       foreach($myelement as $key => $value) {
          if (strpos($key, '#webform') === FALSE && strpos($key, '#access_') === FALSE) {
            $cleanelement[$key] = $value;
          }
       }
       $cleanelement['#element_validate'][] = [get_class($this),'elementDynamicValidate'];
       $cleanelement['#required'] = FALSE;
       $cleanelement['#validated'] = FALSE;
       $cleanelement['#default_value'] = NULL;
       $cleanelement['#submit'] = [[$this, 'dynamic_field_submit']];
       $cleanelement['#executes_submit_callback'] = TRUE;
       $form['elements_rendered']['jsonfind_element']= $cleanelement;
       dpm($cleanelement);
       dpm($form['elements_rendered']['jsonfind_element']);
    }

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

  /**
   * Submission handler for condition changes in
   */
  public function field_submit(array &$form, FormStateInterface $form_state) {

    if (empty($form_state->getValue('webform'))) {
      $form_state->unsetValue(['webform_elements','elements_for_this_form']);
    }
    if (empty($form_state->getValue(['webform_elements','elements_for_this_form']))) {

    }
    // We have to unset this is a form element's needed value may not match
    // a new one's need. E.g textfield v/s entity autocomplete
    //$form_state->unsetValue(['elements_rendered','jsonfind_element']);
    $form_state->setRebuild(TRUE);
  }
  /**
   * Submission handler for condition changes in
   */
  public function dynamic_field_submit(array &$form, FormStateInterface $form_state) {
  }

  public static function elementDynamicValidate(&$element, FormStateInterface $form_state) {

    //$element['#value'] = array_filter($element['#value']);
    error_log(var_export($element,true));
    error_log(var_export($form_state->getValue('elements_rendered'),true));
    $form_state->set('holi','chao');
   // $form_state->setValueForElement($element, $element['#value']);
  }

  public function webformAjaxCallback(array $form, FormStateInterface $form_state) {
    return $form['webform_elements'];
  }
  public static function webformElementAjaxCallback(array $form, FormStateInterface $form_state) {
    return $form['elements_rendered'];
    /*$item = [
      '#type' => 'item',
      '#title' => $this->t('Ajax value'),
      '#markup' => microtime(),
    ];
    $response = new AjaxResponse();
    $response->addCommand(new HtmlCommand('#form-elements-render-wrapper', $item));
    return $response;*/
  }

  public function submitConfigurationForm(array &$form, FormStateInterface $form_state) {
    dpm($form_state->getValues());
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

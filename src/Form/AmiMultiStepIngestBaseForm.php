<?php
/**
 * @file
 * Contains \Drupal\ami\Form\AmiMultiStepIngestBaseForm.
 */

namespace Drupal\ami\Form;

use Drupal\ami\AmiUtilityService;
use Drupal\ami\Plugin\ImporterAdapterManager;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Form\FormBase;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Language\LanguageInterface;
use Drupal\Core\Session\AccountInterface;
use Drupal\Core\Session\SessionManagerInterface;
use Drupal\Component\Transliteration\TransliterationInterface;
use Drupal\Core\TempStore\PrivateTempStoreFactory;
use Symfony\Component\DependencyInjection\ContainerInterface;

class AmiMultiStepIngestBaseForm extends FormBase {

  /**
   * The current step
   *
   * @var int
   */
  protected $step = 1;

  /**
   * @var int
   */
  protected $lastStep = 6;

  /**
   * @var \Drupal\Core\Session\SessionManagerInterface
   */
  private $sessionManager;

  /**
   * @var \Drupal\Core\Session\AccountInterface
   */
  private $currentUser;

  /**
   * @var \Drupal\user\PrivateTempStore
   */
  protected $store;

  /**
   * @var \Drupal\ami\Plugin\ImporterAdapterManager
   */
  protected $importerManager;

  /**
   * The entity manager service.
   *
   * @var \Drupal\Core\Entity\EntityTypeManagerInterface
   */
  protected $entityTypeManager;

  /**
   * @var \Drupal\ami\AmiUtilityService
   */
  protected $AmiUtilityService;

  /**
   * The transliteration service.
   *
   * @var \Drupal\Component\Transliteration\TransliterationInterface
   */
  protected $transliteration;

  /**
   * Constructs a AmiMultiStepIngestBaseForm.
   *
   * @param \Drupal\Core\TempStore\PrivateTempStoreFactory $temp_store_factory
   * @param \Drupal\Core\Session\SessionManagerInterface $session_manager
   * @param \Drupal\Core\Session\AccountInterface $current_user
   * @param \Drupal\ami\Plugin\ImporterAdapterManager $importerManager
   * @param \Drupal\ami\AmiUtilityService $ami_utility
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface $entity_type_manager
   * @param \Drupal\Component\Transliteration\TransliterationInterface $transliteration
   */
  public function __construct(PrivateTempStoreFactory $temp_store_factory, SessionManagerInterface $session_manager, AccountInterface $current_user, ImporterAdapterManager $importerManager, AmiUtilityService $ami_utility,  EntityTypeManagerInterface $entity_type_manager, TransliterationInterface $transliteration) {
    $this->sessionManager = $session_manager;
    $this->currentUser = $current_user;
    $this->store = $temp_store_factory->get('ami_multistep_data');
    $this->importerManager = $importerManager;
    $this->entityTypeManager = $entity_type_manager;
    $this->AmiUtilityService = $ami_utility;
    $this->transliteration = $transliteration;

  }

  /**
   * {@inheritdoc}
   */
  public static function create(ContainerInterface $container) {
    return new static(
      $container->get('tempstore.private'),
      $container->get('session_manager'),
      $container->get('current_user'),
      $container->get('ami.importeradapter_manager'),
      $container->get('ami.utility'),
      $container->get('entity_type.manager'),
      $container->get('transliteration')
    );
  }

  /**
   * {@inheritdoc}
   */
  public function getFormId() : string {
    return 'ami_multistep_import_base_form';
  }

  /**
   * {@inheritdoc}.
   */
  public function buildForm(array $form, FormStateInterface $form_state) {

    $form = array();
    $form['#prefix'] = '<div id="ami_multistep_form">';
    $form['#suffix'] = '</div>';
    $form['actions']['#type'] = 'actions';

    if ($this->step > 1 && $this->step != $this->lastStep) {
      $form['actions']['prev'] = array(
        '#type' => 'submit',
        '#name' => 'prev',
        '#value' => t('Back'),
        '#limit_validation_errors' => [],
        '#submit' => ['::submitForm'],
        '#ajax' => array(
          // We pass in the wrapper we created at the start of the form
          'wrapper' => 'ami_multistep_form',
          // We pass a callback function we will use later to render the form for the user
          'callback' => '::ami_multistep_form_ajax_callback',
          'event' => 'click',
        ),
      );
    }
    if ($this->step != $this->lastStep) {
      $form['actions']['next'] = array(
        '#type' => 'submit',
        '#name' => 'next',
        '#value' => t('Next'),
        '#ajax' => array(
          // We pass in the wrapper we created at the start of the form
          'wrapper' => 'ami_multistep_form',
          // We pass a callback function we will use later to render the form for the user
          'callback' => '::ami_multistep_form_ajax_callback',
          'event' => 'click',
        ),
      );
    }
    if ($this->step +1 == $this->lastStep) {
      $form['actions']['next'] = [
        '#type' => 'submit',
        '#name' => 'next',
        '#value' => t('Press to Create Set'),
      ];
    }


    if ($this->step == $this->lastStep) {
      return $form;
      // @TODO see if we want to process something here.
      // Leaving the Button around in case we want that?
    }
    return $form;
  }

  /**
   * {@inheritdoc}
   */
  public function submitForm(array &$form, FormStateInterface $form_state) {
    if ($form_state->getTriggeringElement()['#name'] == 'prev') {
      $this->step--;
    } else {
      $this->step++;
    }
    $form_state->setRebuild();
  }

  /**
   * {@inheritdoc}
   */
  public function validateForm(array &$form, FormStateInterface $form_state) {

    if ($form_state->getTriggeringElement()['#name'] == 'prev') {
      // No validation my friends.
    } else {
      //@TODO each step has its own validation.
      return parent::validateForm($form, $form_state);
    }
  }
  /**
   * Saves the data from the multistep form.
   */
  protected function saveData() {
    // Logic for saving data goes here...
    $this->deleteStore();
  }

  public function ami_multistep_form_ajax_callback(array &$form, FormStateInterface $form_state) {
    return $form;
  }

  /**
   * Helper method that removes all the keys from the store collection used for
   * the multistep form.
   */
  protected function deleteStore() {
    $keys = ['name', 'email', 'age', 'location'];
    foreach ($keys as $key) {
      $this->store->delete($key);
    }
  }
  /**
   * {@inheritdoc}
   */
  protected function getMachineNameSuggestion(string $string) {
    // @TODO maybe move into ami.service?
    // This is basically the same as what is done in
    // \Drupal\system\MachineNameController::transliterate()
    $transliterated = $this->transliteration->transliterate($string, LanguageInterface::LANGCODE_DEFAULT, '_');
    $transliterated = mb_strtolower($transliterated);

    $transliterated = preg_replace('@[^a-z0-9_.]+@', '', $transliterated);

    return $transliterated;
  }
}

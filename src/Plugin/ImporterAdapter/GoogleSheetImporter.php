<?php

namespace Drupal\ami\Plugin\Importer;
use Drupal\google_api_client\Service\GoogleApiClientService;
use Google_Service_Sheets;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\StreamWrapper\StreamWrapperManagerInterface;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\ami\Plugin\ImporterAdapterBase;
use GuzzleHttp\ClientInterface;
use Symfony\Component\DependencyInjection\ContainerInterface;

/**
 * Product importer from a CSV format.
 *
 * @ImporterAdapter(
 *   id = "googlesheet",
 *   label = @Translation("Google Sheets Importer"),
 *   remote = false
 * )
 */
class GoogleSheetImporter extends SpreadsheetImporter {

  /**
   * @var \Drupal\Core\StreamWrapper\StreamWrapperManagerInterface
   */
  protected $streamWrapperManager;


  /**
   * @var \Drupal\google_api_client\Service\GoogleApiClientService
   */
  protected $googleApiClientService;

  /**
   * {@inheritdoc}
   */
  public function __construct(array $configuration, $plugin_id, $plugin_definition, EntityTypeManagerInterface $entityTypeManager, ClientInterface $httpClient, StreamWrapperManagerInterface $streamWrapperManager, GoogleApiClientService $google_api_client_service) {
    parent::__construct($configuration, $plugin_id, $plugin_definition, $entityTypeManager, $httpClient);
    $this->streamWrapperManager = $streamWrapperManager;
    $this->googleApiClientService = $google_api_client_service;
  }

  /**
   * {@inheritdoc}
   */
  public static function create(ContainerInterface $container, array $configuration, $plugin_id, $plugin_definition) {
    return new static(
      $configuration,
      $plugin_id,
      $plugin_definition,
      $container->get('entity_type.manager'),
      $container->get('http_client'),
      $container->get('stream_wrapper_manager'),
      $container->get('google_api_client.client')
    );
  }


  use StringTranslationTrait;

  /**
   * {@inheritdoc}
   */
  public function getConfigurationForm(\Drupal\ami\Entity\ImporterAdapterInterface $importer) {
    $form = [];
    $config = $importer->getPluginConfiguration();
    $stored_defaults = array('spreadsheet_id' => '', 'spreadsheet_range' => 'Sheet1!A1:B10');

    if (isset($form_state['storage']['values']['step'.$form_state['storage']['step']])) {
      $stored_defaults['spreadsheet_id'] = $form_state['storage']['values']['step'.$form_state['storage']['step']]['google_api']['spreadsheet_id'];
      $stored_defaults['spreadsheet_range'] = $form_state['storage']['values']['step'.$form_state['storage']['step']]['google_api']['spreadsheet_range'];
    }

   //$form = \Drupal::formBuilder()->getForm('Drupal\custom_module\Form\CustomModuleForm',$parameter);

    $form['google_api']= array(
      '#prefix' => '<div id="ami-googleapi">',
      '#suffix' => '</div>',
      '#tree' => TRUE,
      'spreadsheet_id' => array(
        '#type' => 'textfield',
        '#required' => TRUE,
        '#title' => $this->t('ID of your Google Sheet'),
        '#description' => 'Example: https://docs.google.com/spreadsheets/d/aaBAccEEFfC_aBC-aBc0d1EF/edit, use aaBAccEEFfC_aBC-aBc0d1EF portion as the ID or full URL',
        '#default_value' => $stored_defaults['spreadsheet_id'],
      ),
      'spreadsheet_range' => array(
        '#type' => 'textfield',
        '#required' => TRUE,
        '#title' =>  $this->t('Cell Range'),
        '#description' => t('Cell Range for your Google Sheet, in the form of SheetName!A1:B10'),
        '#default_value' => $stored_defaults['spreadsheet_range'],
      ),
    );
    /*
    if (
      !preg_match('#https?://docs.google.com/spreadsheets/d/(.+)/edit(\#gid=(\d+))?#', $form_state->getValue('spreadsheet_url'), $matches)
    ) {
      $form_state->setErrorByName('googlesheets][spreadsheet_url', $this->t('Please provide a valid Google Sheet URL.'));
    }
    else {
      $form_state->setValue('spreadsheet_id', $matches[1]);
      $form_state->setValue('spreadsheet_sheet_id', !empty($matches[3]) ? $matches[3] : 0);
    }
  }
*/
    return $form;
  }


  /**
   * {@inheritdoc}
   */
  public function import() {
    $entities = $this->getData();
    if (!$entities) {
      return FALSE;
    }

    foreach ($entities as $entity) {
      $this->persistEntity($entity);
    }

    return TRUE;
  }

  /**
   * Loads the product data from the remote URL.
   *
   * @return array
   */
  private function getData() {
    /** @var \Drupal\ami\Entity\ImporterAdapterInterface $importer_config */
    $importer_config = $this->configuration['config'];
    $config = $importer_config->getPluginConfiguration();


    return $entity;
  }


  /**
   * Saves a Node entity from the remote data.
   *
   * @param \stdClass $data
   */
  public function persistEntity($data) {
    /** @var \Drupal\ami\Entity\ImporterAdapterInterface $config */
    $config = $this->configuration['config'];

    $existing = $this->entityTypeManager->getStorage('product')->loadByProperties(['remote_id' => $data->id]);
    if (!$existing) {
      $values = [
        'remote_id' => $data->id,
        'type' => $config->getBundle(),
      ];
      /** @var \Drupal\ami\Entity\ProductInterface $product */
      $product = $this->entityTypeManager->getStorage('product')->create($values);
      $product->setName($data->name);
      $product->setProductNumber($data->number);
      $product->save();
      return;
    }

    if (!$config->updateExisting()) {
      return;
    }

    /** @var \Drupal\ami\Entity\ProductInterface $product */
    $product = reset($existing);
    $product->setName($data->name);
    $product->setProductNumber($data->number);
    $product->save();
  }
}

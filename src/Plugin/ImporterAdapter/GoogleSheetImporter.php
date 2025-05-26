<?php

namespace Drupal\ami\Plugin\ImporterAdapter;

use Drupal\ami\Plugin\ImporterAdapter\SpreadsheetImporter;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Messenger\MessengerInterface;
use Drupal\Core\Messenger\MessengerTrait;
use Drupal\google_api_client\GoogleApiClientInterface;
use Drupal\google_api_client\Service\GoogleApiClientService;
use Google_Service_Sheets;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\StreamWrapper\StreamWrapperManagerInterface;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use GuzzleHttp\ClientInterface;
use PhpOffice\PhpSpreadsheet\IOFactory;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\ami\AmiUtilityService;
use Google_Service_Exception;

/**
 * ADO importer from a remote Google Spreadsheet.
 *
 * @ImporterAdapter(
 *   id = "googlesheet",
 *   label = @Translation("Google Sheets Importer"),
 *   remote = false,
 *   batch = false,
 * )
 */
class GoogleSheetImporter extends SpreadsheetImporter {

  use StringTranslationTrait;
  use MessengerTrait;

  /**
   * @var \Drupal\Core\StreamWrapper\StreamWrapperManagerInterface
   */
  protected StreamWrapperManagerInterface $streamWrapperManager;

  /**
   * @var \GuzzleHttp\ClientInterface
   */
  protected $httpClient;

  /**
   * @var \Drupal\google_api_client\Service\GoogleApiClientService
   */
  protected $googleApiClientService;

  /**
   * GoogleSheetImporter constructor.
   *
   * @param array $configuration
   * @param $plugin_id
   * @param $plugin_definition
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface $entityTypeManager
   * @param \GuzzleHttp\ClientInterface $httpClient
   * @param \Drupal\Core\StreamWrapper\StreamWrapperManagerInterface $streamWrapperManager
   * @param \Drupal\google_api_client\Service\GoogleApiClientService $google_api_client_service
   * @param \Drupal\ami\AmiUtilityService $ami_utility
   */
  public function __construct(array $configuration, $plugin_id, $plugin_definition, EntityTypeManagerInterface $entityTypeManager, ClientInterface $httpClient, StreamWrapperManagerInterface $streamWrapperManager, GoogleApiClientService $google_api_client_service, AmiUtilityService $ami_utility) {
    parent::__construct($configuration, $plugin_id, $plugin_definition, $entityTypeManager, $streamWrapperManager, $ami_utility);
    $this->streamWrapperManager = $streamWrapperManager;
    $this->googleApiClientService = $google_api_client_service;
    $this->httpClient = $httpClient;
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
      $container->get('google_api_client.client'),
      $container->get('ami.utility')
    );
  }

  /**
   * {@inheritdoc}
   */
  public function interactiveForm(array $parents, FormStateInterface $form_state):array {
    // None of the interactive Form elements should be persisted as Config elements
    // Here.
    // Maybe we should have some annotation that says which ones for other plugins?
    //$form = parent::interactiveForm($parents,$form_state);
    $form['op'] = [
      '#type' => 'select',
      '#title' => $this->t('Operation'),
      '#options' => [
        'create' => 'Create New ADOs',
        'update' => 'Update existing ADOs',
        //'patch' => 'Patch existing ADOs',
      ],
      '#description' => $this->t('The desired Operation'),
      '#required' => TRUE,
      '#default_value' =>  $form_state->getValue(array_merge($parents , ['op'])),
      '#empty_option' => $this->t('- Please select an Operation -'),
    ];
    $form['google_api']= array(
      '#prefix' => '<div id="ami-googleapi">',
      '#suffix' => '</div>',
      '#tree' => TRUE,
      'spreadsheet_id' => array(
        '#type' => 'textfield',
        '#required' => TRUE,
        '#title' => $this->t('ID of your Google Sheet'),
        '#description' => 'Example: https://docs.google.com/spreadsheets/d/aaBAccEEFfC_aBC-aBc0d1EF/edit, use that same full URL',
        '#default_value' => $form_state->getValue(array_merge($parents , ['google_api','spreadsheet_id'])),
        '#element_validate' => [[get_class($this), 'validateSpreadsheetId']]
      ),
      'spreadsheet_range' => array(
        '#type' => 'textfield',
        '#required' => TRUE,
        '#title' =>  $this->t('Cell Range'),
        '#description' => t('Cell Range for your Google Sheet, in the form of SheetName!A1:B10'),
        '#default_value' => $form_state->getValue(array_merge($parents , ['google_api','spreadsheet_range'])),
        '#element_validate' => [[get_class($this), 'validateRange']]
      ),
    );

    return $form;
  }

  public static function validateSpreadsheetId($element, FormStateInterface $form_state) {
    //@TODO Google Sheet validation by calling Google and checking if we can read it
    if (
    !preg_match('#https?://docs.google.com/spreadsheets/d/(.+)/edit(\#gid=(\d+))?#', $form_state->getValue($element['#parents']), $matches)
    ) {
      $form_state->setError($element, t('Please provide a valid Google Sheet URL.'));
    }
  }


  public static function validateRange($element, FormStateInterface $form_state, array $form) {
    //@TODO implement range Validation
  }

  /**
   * {@inheritdoc}
   */
  public function getData(array $config,  $page = 0, $per_page = 20):array {
    $spreadsheetId = $config['google_api']['spreadsheet_id'];
    $range = $config['google_api']['spreadsheet_range'];
    $range = trim(
      $range
    );
      // Parse the ID from the URL if a full URL was provided.
      // @author of following chunk is Mark Mcfate @McFateM!
      if ($parsed = parse_url($spreadsheetId)) {
        if (isset($parsed['scheme'])) {
          $parts = explode('/', $parsed['path']);
          $spreadsheetId = $parts[3];
        }
      }

    $tabdata = ['headers' => [], 'data' => [], 'totalrows' => 0];

    // Load the account
    $offset = $page * $per_page;
    /* @var $google_api_clients \Drupal\google_api_client\GoogleApiClientInterface[] | NULL */
    $google_api_clients = $this->entityTypeManager->getStorage('google_api_client')->loadByProperties(['name'=> 'AMI']);
    foreach ($google_api_clients as $google_api_client) {
      if ($google_api_client->getAuthenticated()) {
        $scopes =  $google_api_client->getScopes();
        if (in_array('https://www.googleapis.com/auth/spreadsheets.readonly',$scopes )) {
          $chosen_google_api_client = $google_api_client;
        }
      }
    }
    if ($chosen_google_api_client) {
      // Get the service.
      // Apply the account to the service
      $this->googleApiClientService->setGoogleApiClient($chosen_google_api_client);

      // Fetch Client
      $client = $this->googleApiClientService->googleClient;

      // Establish a connection first
      try {
        $service = new Google_Service_Sheets($client);
        $response = $service->spreadsheets_values->get($spreadsheetId, $range);
        $sp_data = $response->getValues();
        // Empty value? just return
        if (($sp_data == NULL) or empty($sp_data)) {
          $this->messenger()->addMessage(
            t('Nothing to read, check your Data source content'),
            MessengerInterface::TYPE_ERROR
          );
          return $tabdata;
        }
      } catch (Google_Service_Exception $e) {
        $this->messenger()->addMessage(
          t('Google API Error: @e', ['@e' => $e->getMessage()]),
          MessengerInterface::TYPE_ERROR
        );
        return $tabdata;
      }
      $table = [];
      $headers = [];
      $maxRow = 0;
      $highestRow = count($sp_data);

      $rowHeaders = $sp_data[0];

      // Well, if someone passed a null into the rowHeaders we are doomed in PHP 8.1+
      // So do we make nulls empty strings?
      $cleanStrings = function ($array_item) {
        $array_item = is_string($array_item) ? stripslashes($array_item) : $array_item;
        $array_item = empty($array_item) ? '' : $array_item;
        return $array_item;
      };

      $rowHeaders_utf8 = array_map($cleanStrings, $rowHeaders ?? []);
      foreach($rowHeaders_utf8 as &$header) {
        $header = function_exists('mb_convert_encoding') ? mb_convert_encoding($header, 'UTF-8', mb_list_encodings()) : utf8_encode($header);
      }
      $rowHeaders_utf8 = array_map('strtolower', $rowHeaders_utf8);
      $rowHeaders_utf8 = array_map('trim', $rowHeaders_utf8);

      $headercount = count($rowHeaders);

      if (($highestRow) >= 1) {
        // Returns Row Headers.

        $maxRow = 1; // at least until here.
        foreach ($sp_data as $rowindex => $row) {

          // Google Spreadsheets start with Index 0. But PHPSPREADSHEET
          // public function does with 1.
          // To keep both public function responses in sync using the same params, i will compensate offsets here:
          if (($rowindex >= 1) && ($rowindex > ($offset - 1)) && (($rowindex <= ($offset + $per_page)) || $per_page == -1)) {

            $flat = trim(implode('', $row));
            //check for empty row...if found stop there.
            if (strlen($flat) == 0) {
              $maxRow = $rowindex;
              break;
            }
            $row = $this->AmiUtilityService->arrayEquallySeize(
              $headercount,
              $row
            );
            $table[$rowindex] = $row;
          }
          $maxRow = $rowindex;
        }
      }
      $tabdata = [
        'headers' => $rowHeaders_utf8,
        'data' => $table,
        'totalrows' => $maxRow,
      ];
    } else {
      $this->messenger()->addMessage(
        t('Your Google API Client is not properly setup. Please make sure it is labeled AMI, it is authenticated and it has spreadsheets.readonly as permission setup'),
        MessengerInterface::TYPE_ERROR
      );
    }
    return $tabdata;
  }

  public function getInfo(array $config, FormStateInterface $form_state, $page = 0, $per_page = 20): array {
    return $this->getData($config, $page, $per_page);
  }


}

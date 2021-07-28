<?php
/**
 * @file
 * src/AmiUtilityService.php
 *
 * Contains Parsing/Processing utilities
 * @author Diego Pino Navarro
 */

namespace Drupal\ami;

use Drupal\Component\Transliteration\TransliterationInterface;
use Drupal\Core\Archiver\ArchiverManager;
use Drupal\Core\Config\ConfigFactoryInterface;
use Drupal\Core\Entity\EntityTypeBundleInfoInterface;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use \Drupal\Core\Entity\EntityFieldManagerInterface;
use Drupal\Core\Extension\ModuleHandlerInterface;
use Drupal\Core\File\FileSystemInterface;
use Drupal\Core\KeyValueStore\KeyValueFactoryInterface;
use Drupal\Core\Language\LanguageManagerInterface;
use Drupal\Core\Logger\LoggerChannelFactoryInterface;
use Drupal\Core\Messenger\MessengerTrait;
use Drupal\Core\Session\AccountInterface;
use Drupal\Core\StreamWrapper\StreamWrapperManagerInterface;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\Core\Url;
use Drupal\file\Entity\File;
use Drupal\file\FileUsage\FileUsageInterface;
use GuzzleHttp\ClientInterface;
use Drupal\strawberryfield\StrawberryfieldUtilityService;
use GuzzleHttp\Cookie\CookieJar;

class AmiLoDService {

  use StringTranslationTrait;
  use MessengerTrait;
  use StringTranslationTrait;

  /**
   * @var array
   */
  private $parameters = [];

  /**
   * File system service.
   *
   * @var \Drupal\Core\File\FileSystemInterface
   */
  protected $fileSystem;

  /**
   * The 'file.usage' service.
   *
   * @var \Drupal\file\FileUsage\FileUsageInterface
   */
  protected $fileUsage;

  /**
   * The entity manager service.
   *
   * @var \Drupal\Core\Entity\EntityTypeManagerInterface
   */
  protected $entityTypeManager;

  /**
   * The stream wrapper manager.
   *
   * @var \Drupal\Core\StreamWrapper\StreamWrapperManagerInterface
   */
  protected $streamWrapperManager;

  /**
   * The archiver manager.
   *
   * @var \Drupal\Core\Archiver\ArchiverManager
   */
  protected $archiverManager;

  /**
   * The Storage Destination Scheme.
   *
   * @var string;
   */
  protected $destinationScheme = NULL;

  /**
   * The Module Handler.
   *
   * @var \Drupal\Core\Extension\ModuleHandlerInterface;
   */
  protected $moduleHandler;

  /**
   * The language Manager
   *
   * @var \Drupal\Core\Language\LanguageManagerInterface
   */
  protected $languageManager;

  /**
   * The Transliteration
   *
   * @var \Drupal\Component\Transliteration\TransliterationInterface
   */
  protected $transliteration;

  /**
   * The  Configuration settings.
   *
   * @var \Drupal\Core\Config\ImmutableConfig
   */
  protected $config;

  /**
   * The logger factory.
   *
   * @var \Drupal\Core\Logger\LoggerChannelFactoryInterface
   */
  protected $loggerFactory;

  /**
   * The Strawberry Field Utility Service.
   *
   * @var \Drupal\strawberryfield\StrawberryfieldUtilityService
   */
  protected $strawberryfieldUtility;

  /**
   * @var \Drupal\Core\Session\AccountInterface
   */
  protected $currentUser;

  /**
   * @var \GuzzleHttp\ClientInterface
   */
  protected $httpClient;

  /**
   * @var \Drupal\ami\AmiUtilityService
   */
  protected $AmiUtilityService;

  /**
   * Key value service.
   *
   * @var \Drupal\Core\KeyValueStore\KeyValueFactoryInterface
   */
  protected $keyValue;


  /**
   * AmiLoDService constructor.
   *
   * @param \Drupal\Core\File\FileSystemInterface $file_system
   * @param \Drupal\file\FileUsage\FileUsageInterface $file_usage
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface $entity_type_manager
   * @param \Drupal\Core\StreamWrapper\StreamWrapperManagerInterface $stream_wrapper_manager
   * @param \Drupal\Core\Archiver\ArchiverManager $archiver_manager
   * @param \Drupal\Core\Config\ConfigFactoryInterface $config_factory
   * @param \Drupal\Core\Session\AccountInterface $current_user
   * @param \Drupal\Core\Language\LanguageManagerInterface $language_manager
   * @param \Drupal\Component\Transliteration\TransliterationInterface $transliteration
   * @param \Drupal\Core\Extension\ModuleHandlerInterface $module_handler
   * @param \Drupal\Core\Logger\LoggerChannelFactoryInterface $logger_factory
   * @param \Drupal\strawberryfield\StrawberryfieldUtilityService $strawberryfield_utility_service
   * @param \GuzzleHttp\ClientInterface $http_client
   * @param \Drupal\ami\AmiUtilityService $ami_utility
   * @param \Drupal\Core\KeyValueStore\KeyValueFactoryInterface $key_value
   */
  public function __construct(
    FileSystemInterface $file_system,
    FileUsageInterface $file_usage,
    EntityTypeManagerInterface $entity_type_manager,
    StreamWrapperManagerInterface $stream_wrapper_manager,
    ArchiverManager $archiver_manager,
    ConfigFactoryInterface $config_factory,
    AccountInterface $current_user,
    LanguageManagerInterface $language_manager,
    TransliterationInterface $transliteration,
    ModuleHandlerInterface $module_handler,
    LoggerChannelFactoryInterface $logger_factory,
    StrawberryfieldUtilityService $strawberryfield_utility_service,
    ClientInterface $http_client,
    AmiUtilityService $ami_utility,
    KeyValueFactoryInterface $key_value
  ) {
    $this->fileSystem = $file_system;
    $this->fileUsage = $file_usage;
    $this->entityTypeManager = $entity_type_manager;
    $this->streamWrapperManager = $stream_wrapper_manager;
    $this->archiverManager = $archiver_manager;
    //@TODO evaluate creating a ServiceFactory instead of reading this on construct.
    $this->destinationScheme = $config_factory->get(
      'strawberryfield.storage_settings'
    )->get('file_scheme');
    $this->config = $config_factory->get(
      'strawberryfield.filepersister_service_settings'
    );
    $this->languageManager = $language_manager;
    $this->transliteration = $transliteration;
    $this->moduleHandler = $module_handler;
    $this->loggerFactory = $logger_factory;
    $this->strawberryfieldUtility = $strawberryfield_utility_service;
    $this->currentUser = $current_user;
    $this->httpClient = $http_client;
    $this->AmiUtilityService = $ami_utility;
    $this->keyValue = $key_value;

  }


  public function invokeLoDRoute(string $domain, string $query, string $auth_type, $vocab = 'subjects', $rdftype = 'thing', $lang = 'en' , $count = 5):array {

    $current_laguage = $lang ?? \Drupal::languageManager()
        ->getCurrentLanguage()
        ->getId();

    switch ($auth_type) {
      case 'nominatim':
        $controller_url = Url::fromRoute(
          'webform_strawberryfield.nominatim',
          ['api_type' => 'search', 'count' => $count, 'lang' => $current_laguage]);
        break;
      default:
        $controller_url = Url::fromRoute(
          'webform_strawberryfield.auth_autocomplete',
          ['auth_type' => $auth_type, 'vocab' => $vocab, 'rdftype' => $rdftype, 'count' => $count]
        );
    }
    // When using this on localhost:8001/Docker the cookie domain won't match with the called one.
    // That is expected and webform_strawberryfield will use instead the X-CSRF-TOKEN.
    if ($domain == 'http://localhost:8001') {
      $domain = $_SERVER['REQUEST_SCHEME'].'://'.$_SERVER['SERVER_ADDR'].':'.$_SERVER['SERVER_PORT'];
    }
    $cookieJar = CookieJar::fromArray($_COOKIE, $domain);

    $controller_path = $controller_url->setAbsolute()->toString(TRUE)->getGeneratedUrl();
    $csrf_token = \Drupal::csrfToken()->get($controller_url->setAbsolute(FALSE)->toString(TRUE)->getGeneratedUrl());
    $options = [
      'headers' =>  [
        'Content-Type' => 'application/json',
        'X-CSRF-Token' => $csrf_token,
      ],
      'cookies' => $cookieJar,
    ];
    // When o docker and running a local instance the server domain is localhost:8001 (normally in our ensemble)
    // But localhost does not resolve internally to the right IP.
    // @TODO make this configurable since we can also use esmero-web, but that won't work for multisites
    // OR SSL certs. So better this way. We could also check if IP actually matches localhost? (127.0.0.1 or 0.0.0.0)
    if (substr($controller_path, 0, 21 ) === "http://localhost:8001") {
      $controller_path = str_replace("http://localhost:8001", $_SERVER['REQUEST_SCHEME'].'://'.$_SERVER['SERVER_ADDR'].':'.$_SERVER['SERVER_PORT'], $controller_path);
    }

    $options = array_merge_recursive(['query' => ['_format' => 'json', 'q' => $query]], $options);
    $response = $this->httpClient->request('GET', $controller_path, $options);
    $sucessfull =  $response->getStatusCode() >= 200 && $response->getStatusCode() < 300;
    $response_encoded = $sucessfull ? json_decode($response->getBody()->getContents()) : [];
    // Removes desc , changes value for uri to make it SBF webform element compliant
    $response_cleaned = [];
    foreach ($response_encoded as $key => $entry) {
      $response_cleaned[$key]['uri'] = $entry->value ?? '';
      $response_cleaned[$key]['label'] = !empty($entry->desc) ? substr($entry->label ?? '', 0, -strlen($entry->desc)) : $entry->label ?? '';
    }
    return $response_cleaned;
  }

  /**
   * From a given CSV files returns different values for a list of columns
   *
   * @param \Drupal\file\Entity\File $file
   * @param array $columns
   *
   * @return array
   *   An Associative Array keyed by Column name
   */
  public function provideLoDColumnValues(File $file, array $columns):array {
    $data = $this->AmiUtilityService->csv_read($file);
    $column_keys = $data['headers'] ?? [];
    $alldifferent = [];
    foreach ($columns as $column) {
      $column_index = array_search($column, $column_keys);
      if ($column_index !== FALSE) {
        $alldifferent[$column] = $this->AmiUtilityService->getDifferentValuesfromColumnSplit($data,
          $column_index);
      }
    }
    return $alldifferent;
  }

  /**
   * Checks if a string is valid JSON
   *
   * @param $string
   *
   * @return bool
   */
  public function isJson($string) {
    json_decode($string);
    return json_last_error() === JSON_ERROR_NONE;
  }

  /**
   * Helper function that negates ::isJson.
   * @param $string
   *
   * @return bool
   */
  public function isNotJson($string) {
    return !$this->isJson($string);
  }
}

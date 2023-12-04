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
use Drupal\Core\Entity\EntityTypeManagerInterface;
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
use Drupal\file\FileUsage\FileUsageInterface;
use Drupal\webform_strawberryfield\Controller\NominatimController;
use GuzzleHttp\ClientInterface;
use Drupal\strawberryfield\StrawberryfieldUtilityService;
use GuzzleHttp\Cookie\CookieJar;
use Symfony\Component\HttpFoundation\Request;

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
   * Key value service.
   *
   * @var \Drupal\Core\KeyValueStore\KeyValueFactoryInterface
   */
  protected $keyValue;

  public CONST AMI_FORM_EXPOSED_LOD_SOURCES =  [
    'loc;subjects;thing' => 'LoC subjects(LCSH)',
    'loc;names;thing' => 'LoC Name Authority File (LCNAF)',
    'loc;genreForms;thing' => 'LoC Genre/Form Terms (LCGFT)',
    'loc;graphicMaterials;thing' => 'LoC Thesaurus of Graphic Materials (TGN)',
    'loc;geographicAreas;thing' => 'LoC MARC List for Geographic Areas',
    'loc;relators;thing' => 'LoC Relators Vocabulary (Roles)',
    'loc;rdftype;CorporateName' => 'LoC MADS RDF by type: Corporate Name',
    'loc;rdftype;PersonalName' => 'LoC MADS RDF by type: Personal Name',
    'loc;rdftype;FamilyName' => 'LoC MADS RDF by type: Family Name',
    'loc;rdftype;Topic' => 'LoC MADS RDF by type: Topic',
    'loc;rdftype;GenreForm' =>  'LoC MADS RDF by type: Genre Form',
    'loc;rdftype;Geographic' => 'LoC MADS RDF by type: Geographic',
    'loc;rdftype;Temporal' =>  'LoC MADS RDF by type: Temporal',
    'loc;rdftype;ExtraterrestrialArea' => 'LoC MADS RDF by type: Extraterrestrial Area',
    'viaf;subjects;thing' => 'Viaf',
    'getty;aat;fuzzy' => 'Getty aat Fuzzy',
    'getty;aat;terms' => 'Getty aat Terms',
    'getty;aat;exact' => 'Getty aat Exact Label Match',
    'wikidata;subjects;thing' => 'Wikidata Q Items',
    'mesh;term;startswith' => 'MeSH (starts with Term)',
    'mesh;term;contains' => 'MeSH (contains Term)',
    'mesh;term;exact' => 'MeSH (exact Term)',
    'mesh;descriptor;startswith' => 'MeSH (starts with Descriptor)',
    'mesh;descriptor;contains' => 'MeSH (contains Descriptor)',
    'mesh;descriptor;exact' => 'MeSH (exact Descriptor)'
  ];

  public CONST LOD_COLUMN_TO_ARGUMENTS = [
    'loc_subjects_thing' => 'loc;subjects;thing',
    'loc_names_thing' => 'loc;names;thing',
    'loc_genreforms_thing' => 'loc;genreForms;thing',
    'loc_graphicmaterials_thing' => 'loc;graphicMaterials;thing',
    'loc_geographicareas_thing' => 'loc;geographicAreas;thing',
    'loc_relators_thing' => 'loc;relators;thing',
    'loc_rdftype_corporatename' => 'loc;rdftype;CorporateName',
    'loc_rdftype_personalname' =>  'loc;rdftype;PersonalName',
    'loc_rdftype_familyname' => 'loc;rdftype;FamilyName',
    'loc_rdftype_topic' => 'loc;rdftype;Topic',
    'loc_rdftype_genreform' =>  'loc;rdftype;GenreForm',
    'loc_rdftype_geographic' => 'loc;rdftype;Geographic',
    'loc_rdftype_temporal' =>  'loc;rdftype;Temporal',
    'loc_rdftype_extraterrestrialarea' => 'loc;rdftype;ExtraterrestrialArea',
    'viaf_subjects_thing' => 'viaf;subjects;thing',
    'getty_aat_fuzzy' => 'getty;aat;fuzzy',
    'getty_aat_terms' => 'getty;aat;terms',
    'getty_aat_exact' => 'getty;aat;exact',
    'wikidata_subjects_thing' => 'wikidata;subjects;thing',
    'mesh_term_startswith' => 'mesh;term;startswith',
    'mesh_term_contains' => 'mesh;term;contains',
    'mesh_term_exact' => 'mesh;term;exact',
    'mesh_descriptor_startswith' => 'mesh;descriptor;startswith',
    'mesh_descriptor_contains' => 'mesh;descriptor;contains',
    'mesh_descriptor_exact' => 'mesh;descriptor;exact'
  ];

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
    $this->keyValue = $key_value;
  }

  /**
   * Deletes All LoD KeyValues for a given AMI Set ID.
   *
   * @param $set_id
   */
  public function cleanKeyValuesPerAmiSet($set_id) {
    $keyvalue_collection = 'ami_lod_temp_' . $set_id;
    $this->keyValue->get($keyvalue_collection)->deleteAll();
    $keyvalue_collection_mappings = 'ami_lod_temp_mappings';
    $this->keyValue->get($keyvalue_collection_mappings)->delete($set_id);
  }

  /**
   * Inserts a new LoD KeyValue for a given Label/AMI Set ID pair.
   *
   * @param $label
   * @param $data
   * @param $set_id
   */
  public function setKeyValuePerAmiSet($label, $data, $set_id) {
    // Too much trouble dealing with encodings/UTF-8 and MYSQL
    // And drupal here. Simpler if the label is md5-ed
    $label = md5($label);
    $keyvalue_collection = 'ami_lod_temp_'. $set_id;
    $this->keyValue->get($keyvalue_collection)
      ->set($label, $data);
  }

  /**
   * Sets the JSON Key Mappings (original) for a given AMI Set ID.
   *
   * @param $data
   * @param $set_id
   */
  public function setKeyValueMappingsPerAmiSet($data, $set_id) {
    $keyvalue_collection = 'ami_lod_temp_mappings';
    $this->keyValue->get($keyvalue_collection)
      ->set($set_id, $data);
  }

  /**
   * Gets the LoD KeyValue for a given Label/AMI Set ID pair.
   *
   * @param $label
   * @param $set_id
   *
   * @return mixed
   */
  public function getKeyValuePerAmiSet($label, $set_id) {
    $label = md5($label);
    $keyvalue_collection = 'ami_lod_temp_'. $set_id;
    return $this->keyValue->get($keyvalue_collection)
      ->get($label, NULL);
  }

  /**
   * Gets all LoD KeyValue for a given AMI Set ID.
   *
   * @param $set_id
   *
   * @return array
   *    Each Entry is keyed by the MD5 of the label.
   */
  public function getAllKeyValuesPerAmiSet($set_id) {
    $keyvalue_collection = 'ami_lod_temp_'. $set_id;
    return $this->keyValue->get($keyvalue_collection)
      ->getAll();
  }

  /**
   * Gets the JSON Key Mappings (original) for a given AMI Set ID.
   *
   * @param $set_id
   *
   * @return mixed
   */
  public function getKeyValueMappingsPerAmiSet($set_id) {
    $keyvalue_collection = 'ami_lod_temp_mappings';
    return $this->keyValue->get($keyvalue_collection)
      ->get($set_id, NULL);
  }

  public function invokeLoDRoute(string $domain, string $query, string $auth_type, $vocab = 'subjects', $rdftype = 'thing', $lang = 'en' , $count = 5):array {
    $response_cleaned = [];
    $current_laguage = $lang ?? \Drupal::languageManager()
        ->getCurrentLanguage()
        ->getId();

    if ($auth_type != 'nominatim') {
      $controller_url = Url::fromRoute(
        'webform_strawberryfield.auth_autocomplete',
        ['auth_type' => $auth_type, 'vocab' => $vocab, 'rdftype' => $rdftype, 'count' => $count]
      );
      // When using this on localhost:8001/Docker the cookie domain won't match with the called one.
      // That is expected and webform_strawberryfield will use instead the X-CSRF-TOKEN.
      if ($domain == 'http://localhost:8001') {
        $domain = $_SERVER['REQUEST_SCHEME'] . '://' . $_SERVER['SERVER_ADDR'] . ':' . $_SERVER['SERVER_PORT'];
      }
      $cookieJar = CookieJar::fromArray($_COOKIE, $domain);

      $controller_path = $controller_url->setAbsolute()
        ->toString(TRUE)
        ->getGeneratedUrl();
      $csrf_token = \Drupal::csrfToken()
        ->get($controller_url->setAbsolute(FALSE)
          ->toString(TRUE)
          ->getGeneratedUrl());
      $options = [
        'headers' => [
          'Content-Type' => 'application/json',
          'X-CSRF-Token' => $csrf_token,
        ],
        'cookies' => $cookieJar,
      ];
      // When o docker and running a local instance the server domain is localhost:8001 (normally in our ensemble)
      // But localhost does not resolve internally to the right IP.
      // @TODO make this configurable since we can also use esmero-web, but that won't work for multisites
      // OR SSL certs. So better this way. We could also check if IP actually matches localhost? (127.0.0.1 or 0.0.0.0)
      if (substr($controller_path, 0, 21) === "http://localhost:8001") {
        $controller_path = str_replace("http://localhost:8001",
          $_SERVER['REQUEST_SCHEME'] . '://' . $_SERVER['SERVER_ADDR'] . ':' . $_SERVER['SERVER_PORT'],
          $controller_path);
      }

      $options = array_merge_recursive([
        'query' => [
          '_format' => 'json',
          'q' => $query
        ]
      ], $options);
      $response = $this->httpClient->request('GET', $controller_path, $options);
      $sucessfull = $response->getStatusCode() >= 200 && $response->getStatusCode() < 300;
      $response_encoded = $sucessfull ? json_decode($response->getBody()
        ->getContents()) : [];
      // Removes desc , changes value for uri to make it SBF webform element compliant
      foreach ($response_encoded as $key => $entry) {
        $response_cleaned[$key]['uri'] = $entry->value ?? '';
        $response_cleaned[$key]['label'] = !empty($entry->desc) ? substr($entry->label ?? '', 0, -strlen($entry->desc)) : $entry->label ?? '';
      }
    }
    else {
      //@TODO refactor all this in a reusable method inside webform_strawberrfield.
      $response_cleaned = [];
      $controller_nominatim = new NominatimController($this->httpClient);
      // tricky? Our request has the arguments + the method takes the same
      // Just for compatibility since route collection manager
      // Does that automatically on a real public request.
      if (in_array($rdftype, ['thing', 'search'])){
        $controller_url = Url::fromRoute(
          'webform_strawberryfield.nominatim',
          ['api_type' => 'search', 'count' => 1, 'lang' => $current_laguage],
          ['query' => ['q' => $query]]
        );
      }
      elseif ($rdftype == 'reverse') {
        [$lat, $long] = explode(",", $query, 2);
        $controller_url = Url::fromRoute(
          'webform_strawberryfield.nominatim',
          ['api_type' => 'reverse', 'count' => 1, 'lang' => $current_laguage],
          ['query' => ['lat' => $lat, 'lon' => $long]]
        );
      }
      else {
        return [];
      }

      $json_response = $controller_nominatim->handleRequest(
        Request::create($controller_url->toString(), 'GET'),
        $rdftype,
        1,
        $current_laguage
      );
      $nomitanim_response_encoded = $json_response->isSuccessful() ? $json_response->getContent() : [];
      if (!$json_response->isEmpty()) {
        $response_encoded = json_decode(
          $nomitanim_response_encoded,
          FALSE
        );
        //Just return if not value present
        // Just a failsafe, we are already checking for an empty json_response.
        if (empty($response_encoded[0]->value)) {
          return $response_cleaned;
        }
        $address = $response_encoded[0]->value->properties->address ?? [];

        foreach ($address as $properties => $value)
          switch ($properties) {
            case 'hamlet':
            case 'village':
            case 'locality':
            case 'croft' :
              $normalized_address['locality'] = $value;
              break;
            case 'city':
            case 'town':
            case 'municipality':
              $normalized_address['city'] = $value;
              break;
            case 'neighbourhood':
            case 'suburb':
            case 'city_district':
            case 'district':
            case 'quarter':
            case 'houses':
            case 'subdivision':
              $normalized_address['neighbourhood'] = $value;
              break;
            case 'county' :
            case 'local_administrative_area' :
            case 'county_code':
              $normalized_address['county'] = $value;
              break;
            case 'state_district':
              $normalized_address['state_district'] = $value;
              break;
            case 'state':
            case 'province':
            case 'state_code':
              $normalized_address['state'] = $value;
              break;
            case 'country':
              $normalized_address['country'] = $value;
              break;
            case 'country_code':
              $normalized_address['country_code'] = $value;
              break;
            case 'postcode':
              $normalized_address['postcode'] = $value;
              break;
          }

        // Take what we got from nominatim and put into our location keys.
        // Lat and Long are in http://en.wikipedia.org/wiki/en:WGS-84
        $response_cleaned = [
          'value' => $response_encoded[0]->label ?? NULL,
          'lat' => $response_encoded[0]->value->geometry->coordinates[1] ?? NULL,
          'lng' => $response_encoded[0]->value->geometry->coordinates[0] ?? NULL,
          'category' => $response_encoded[0]->value->properties->category ?? NULL,
          'display_name' => $response_encoded[0]->label ?? NULL,
          'osm_id' => $response_encoded[0]->value->properties->osm_id ?? NULL,
          'osm_type' => $response_encoded[0]->value->properties->osm_type ?? NULL,
          'neighbourhood' => isset($normalized_address['neighbourhood']) ? $normalized_address['neighbourhood'] : '',
          'locality' => isset($normalized_address['locality']) ? $normalized_address['locality'] : '',
          'city' => isset($normalized_address['city']) ? $normalized_address['city'] : '',
          'county' => isset($normalized_address['county']) ? $normalized_address['county'] : '',
          'state_district' => isset($normalized_address['state_district']) ? $normalized_address['state_district'] : '',
          'state' => isset($normalized_address['state']) ? $normalized_address['state'] : '',
          'postcode' => isset($normalized_address['postcode']) ? $normalized_address['postcode'] : '',
          'country' => isset($normalized_address['country']) ? $normalized_address['country'] : '',
          'country_code' => isset($normalized_address['country_code']) ? $normalized_address['country_code'] : '',
        ];
      }
    }
    return $response_cleaned;
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

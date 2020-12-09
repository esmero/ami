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
use Drupal\Core\Language\LanguageManagerInterface;
use Drupal\Core\Logger\LoggerChannelFactoryInterface;
use Drupal\Core\Messenger\MessengerInterface;
use Drupal\Core\Messenger\MessengerTrait;
use Drupal\Core\Session\AccountInterface;
use Drupal\Core\StreamWrapper\StreamWrapperManagerInterface;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\file\FileUsage\FileUsageInterface;
use Drupal\strawberryfield\StrawberryfieldUtilityService;
use Drupal\webform_strawberryfield\Element\WebformMetadataCsvFile;
use Symfony\Component\HttpFoundation\File\MimeType\ExtensionGuesser;
use Ramsey\Uuid\Uuid;
use Symfony\Component\Translation\Dumper\CsvFileDumper;

class AmiUtilityService {

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
   * The Entity field manager service
   *
   * @var \Drupal\Core\Entity\EntityFieldManagerInterface $entityFieldManager;

   */
  protected $entityFieldManager;

  /**
   * @var \Drupal\Core\Entity\EntityTypeBundleInfoInterface
   */
  protected $entityTypeBundleInfo;

  /**
   * @var \Drupal\Core\Session\AccountInterface
   */
  protected $currentUser;

  /**
   * StrawberryfieldFilePersisterService constructor.
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
   * @param StrawberryfieldUtilityService $strawberryfield_utility_service ,
   * @param \Drupal\Core\Entity\EntityFieldManagerInterface $entity_field_manager
   * @param \Drupal\Core\Entity\EntityTypeBundleInfoInterface $entity_type_bundle_info
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
    EntityFieldManagerInterface $entity_field_manager,
    EntityTypeBundleInfoInterface $entity_type_bundle_info
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
    $this->entityFieldManager = $entity_field_manager;
    $this->entityTypeBundleInfo = $entity_type_bundle_info;
    $this->currentUser = $current_user;
  }


  /**
   * Checks if value is prefixed entity or an UUID.
   *
   * @param mixed $val
   *
   * @return bool
   */
  public function isEntityId($val) {
    return (!is_int($val) && is_numeric(
        $this->getIdfromPrefixedEntityNode($val)
      ) || \Drupal\Component\Uuid\Uuid::isValid(
        $val
      ));
  }

  /**
   * Array value callback. True if value is not an array.
   *
   * @param mixed $val
   *
   * @return bool
   */
  private function isNotArray($val) {
    return !is_array($val);
  }

  /**
   * Array value callback. True if $key starts with Entity
   *
   * @param mixed $val
   *
   * @return bool
   */
  public function getIdfromPrefixedEntityNode($key) {
    if (strpos($key, 'entity:node:', 0) !== FALSE) {
      return substr($key, strlen("entity:node:"));
    }
    return FALSE;
  }


  public function getIngestInfo($file_path, $config) {
    error_log('Running  AMI getIngestInfo');

    $file_data_all = $this->read_filedata(
      $file_path,
      -1,
      $offset = 0
    );
    $config['data']['headers'] = $file_data_all['headers'];

    $namespace_hash = [];
    // Keeps track of all parents and child that don't have a PID assigned.
    $parent_hash = [];
    $namespace_count = [];
    $info = [];
    // Keeps track of invalid rows.
    $invalid = [];
    foreach ($file_data_all['data'] as $index => $row) {
      // Each row will be an object.
      $objectInfo = [];
      $objectInfo['type'] = trim(
        $row[$this->parameters['type_source_field_index']]
      );
      // Lets start by grouping by parents, namespaces and generate uuids
      // namespaces are inherited, so we just need to find collection
      // objects in parent uuid column.
      $objectInfo['parent'] = trim(
        $row[$this->parameters['object_maping']['parentmap']]
      );
      $possiblePID = "";

      $objectInfo['data'] = $row;


      $possibleUUID = trim($row[$this->parameters['object_maping']['uuidmap']]);

      if (\Drupal\Component\Uuid\Uuid::isValid($possibleUUID)) {
        $objectInfo['uuid'] = $possibleUUID;
        // Now be more strict for action = update
        if (in_array(
          $row[$this->parameters['object_maping']['crudmap']],
          ['create', 'update', 'delete']
        )) {
          $existing_object = $this->entityTypeManager->getStorage('node')
            ->loadByProperties(['uuid' => $objectInfo['uuid']]);
          //@TODO field_descriptive_metadata  is passed from the Configuration
          if (!$existing_object) {
            unset($objectInfo);
            $invalid = $invalid + [$index => $index];
          }
        }
      }
      if (!isset($objectInfo['uuid'])) {
        unset($objectInfo);
        $invalid = $invalid + [$index => $index];
      }

      if (isset($objectInfo)) {
        if (\Drupal\Component\Uuid\Uuid::isValid($objectInfo['parent'])) {
          // If valid PID, let's try to fetch a valid namespace for current type
          // we will store all this stuff in a temp hash to avoid hitting
          // this again and again.
          $objectInfo['parent_type'] = $this->getParentType(
            $objectInfo['parent']
          );
          if ($objectInfo['parent_type']) {
            if (!isset($objectInfo['uuid'])) { //Only do this if no UUID assigned yet
              $objectInfo['namespace'] = 'genericnamespace';
            }
            else {
              // we have a PID but i still want my objectInfo['namespace']
              // NO worries about checking if uuidparts is in fact lenght of 2
              // PID was checked for sanity a little bit earlier
              $objectInfo['namespace'] ='genericnamespace';
            }
          }
          else {
            // No parent type, no object, can't create.
            unset($objectInfo);
            $invalid = $invalid + [$index => $index];
          }
        }
        else {
          // Means our parent object is a ROW index
          // (referencing another row in the spreadsheet)
          // So a different strategy is needed. We will need recurse
          // until we find a non numeric parent or none! Because
          // in Archipelago we allow the none option for sure!
          $notUUID = TRUE;
          $parent = $objectInfo['parent'];
          $parent_hash[$parent][$index] = $index;
          $parentchilds = [];
          // Lets check if the index actually exists before going crazy.

          if (!isset($file_data_all['data'][$parent])) {
            $invalid[$parent] = $parent;
            $invalid[$index] = $index;
          }

          if ((!isset($invalid[$index])) && (!isset($invalid[$parent]))) {
            // Only traverse if we don't have this index or the parent one
            // in the invalid register.
            $objectInfo['parent_type'] = $file_data_all['data'][$parent][$this->parameters['type_source_field_index']];
            $parentchilds = [];
            $i = 0;
            while ($notUUID) {
              $parentup = $file_data_all['data'][$parent][$this->parameters['object_maping']['parentmap_row']['parentmap']];

              // The Simplest approach for breaking a knot /infinite loop,
              // is invalidating the whole parentship chain for good.
              $inaloop = isset($parentchilds[$parentup]);
              // If $inaloop === true means we already traversed this branch
              // so we are in a loop and all our original child and it's
              // parent objects are invalid.
              if ($inaloop) {
                $invalid = $invalid + $parentchilds;
                unset($objectInfo);
                $notUUID = FALSE;
                break;
              }

              $parentchilds[$parentup] = $parentup;
              if (\Drupal\Component\Uuid\Uuid::isValid(trim($parentup))) {
                if (!isset($objectInfo['uuid'])) { //Only do this if no PID assigned yet
                  $namespace = 'genericnamespace';
                  $objectInfo['namespace'] = $namespace;
                }
                else {
                  $objectInfo['namespace'] = 'genericnamespace';
                }

                $notUUID = FALSE;
                break;
              }
              elseif (empty(trim($parent))) {

                // We can't continue here
                // means there must be an error
                // This will fail for any child object that is
                // child of any of these parents.
                $invalid = $invalid + $parentchilds + [$objectInfo['parent'] => $objectInfo['parent']];
                unset($objectInfo);
                $notUUID = FALSE;
              }
              else {
                // This a simple accumulator, means all is well,
                // parent is still an index.
                $parent_hash[$parentup][$parent] = $parent;
              }
              $parent = $parentup;
            }
          }
          else {
            unset($objectInfo);
          }
        }
      }
      if (isset($objectInfo) and !empty($objectInfo)) {
        $info[$index] = $objectInfo;
      }
    }
    // Ok, maybe this is expensive, so let's try it first so.
    // TODO: optimize maybe?
    /*Uuid::uuid5(
      Uuid::NAMESPACE_URL,
      'https://www.php.net'
    );*/
    // Using UUID5 we can make sure that given a certain NameSpace URL (which would
    // be a distributeable UUID amongst many repos and a passed URL, we get always
    // the same UUID.
    //e.g if the source is a remote URL or we get a HANDLE URL per record
    // WE can always generate the SAME URL and that way avoid
    // Duplicated ingests!

    // New first pass: ensure parents have always a PID first
    // since rows/parent/child order could be arbitrary
    foreach ($parent_hash as $parent => $children) {
      if (isset($info[$parent])) {
        $namespace = $info[$parent]['namespace'];
        $info[$parent]['uuid'] = isset($info[$parent]['uuid']) ? $info[$parent]['uuid'] : Uuid::uuid4();
      }
    }

    // Now the real pass, iterate over every row.
    foreach ($info as $index => &$objectInfo) {
      $namespace = $objectInfo['namespace'];
      $objectInfo['uuid'] = isset($objectInfo['uuid']) ? $objectInfo['uuid'] : Uuid::uuid4();

      // Is this object parent of someone?
      if (isset($parent_hash[$objectInfo['parent']])) {
        $objectInfo['parent'] = $info[$objectInfo['parent']]['uuid'];
      }
    }
    // Keep track of what could be processed and which ones not.
    $this->processedObjectsInfo = [
      'success' => array_keys($info),
      MessengerInterface::TYPE_ERROR => array_keys($invalid),
      'fatal' => [],
    ];

    return $info;
  }

  public function getParentType($uuid) {
    // This is the same as format_strawberry \format_strawberryfield_entity_view_mode_alter
    // @TODO refactor into a reusable method inside strawberryfieldUtility!
    $entity = $this->entityTypeManager->getStorage('node')
      ->loadByProperties(['uuid' =>$uuid]);
    if ($sbf_fields = $this->strawberryfieldUtility->bearsStrawberryfield($entity)) {
      foreach ($sbf_fields as $field_name) {
        /* @var $field StrawberryFieldItem */
        $field = $entity->get($field_name);
        if (!$field->isEmpty()) {
          foreach ($field->getIterator() as $delta => $itemfield) {
            /** @var $itemfield \Drupal\strawberryfield\Plugin\Field\FieldType\StrawberryFieldItem */
            $flatvalues = (array) $itemfield->provideFlatten();
            if (isset($flatvalues['type'])) {
              $adotype = array_merge($adotype, (array) $flatvalues['type']);
            }
          }
        }
      }
    }
    if (!empty($adotype)) {
      return reset($adotype);
    }
    //@TODO refactor into a CONST
    return 'thing';
  }


  /**
   * Checks if an URI from spreadsheet is remote or local and returns a file
   *
   * @param string $url
   *   The URL of the file to grab.
   *
   * @return mixed
   *   One of these possibilities:
   *   - If remote and exists a Drupal file object
   *   - If not remote and exists a stripped file object
   *   - If does not exist boolean FALSE
   */
  public function remote_file_get($url) {
    $parsed_url = parse_url($url);
    $remote_schemes = ['http', 'https', 'feed'];
    if (!isset($parsed_url['scheme']) || (isset($parsed_url['scheme']) && !in_array(
          $parsed_url['scheme'],
          $remote_schemes
        ))) {
      // If local file, engage any hook_remote_file_get and return the real path.
      $path = [];
      /*$path = module_invoke_all(
        'remote_file_get',
        $url
      );*/
      // get only the first path.
      if (!empty($path)) {
        if ($path[0]) {
          return $path[0];
        }
      }

      // if local file, try the path.
      $localfile = $this->fileSystem->realpath($url);
      if (!file_exists($localfile)) {
        return FALSE;
      }
      return $localfile;
    }

    // Simulate what could be the final path of a remote download.
    // to avoid redownloading.
    $localfile = file_build_uri(
      $this->fileSystem->basename($parsed_url['path'])
    );
    if (!file_exists($localfile)) {
      // Actual remote heavy lifting only if not present.
      $destination = "temporary://ami/";
      $localfile = $this->retrieve_remote_file(
        $url,
        $destination,
        FileSystemInterface::EXISTS_RENAME
      );
      return $localfile;
    }
    else {
      return $localfile;
    }
    return FALSE;
  }

  /**
   * Attempts to get a file using drupal_http_request and to store it locally.
   *
   * @param string $url
   *   The URL of the file to grab.
   * @param string $destination
   *   Stream wrapper URI specifying where the file should be placed. If a
   *   directory path is provided, the file is saved into that directory under
   *   its original name. If the path contains a filename as well, that one will
   *   be used instead.
   *   If this value is omitted, the site's default files scheme will be used,
   *   usually "public://".
   * @param int $replace
   *   Replace behavior when the destination file already exists:
   *   - FILE_EXISTS_REPLACE: Replace the existing file.
   *   - FILE_EXISTS_RENAME: Append _{incrementing number} until the filename is
   *     unique.
   *   - FILE_EXISTS_ERROR: Do nothing and return FALSE.
   *
   * @return mixed
   *   One of these possibilities:
   *   - If it succeeds an managed file object
   *   - If it fails, FALSE.
   */
  public function retrieve_remote_file(
    $url,
    $destination = NULL,
    $replace = FileSystemInterface::EXISTS_RENAME
  ) {
    // pre set a failure
    $localfile = FALSE;
    $parsed_url = parse_url($url);
    $mime = 'application/octet-stream';
    if (!isset($destination)) {
      $path = file_build_uri($this->fileSystem->basename($parsed_url['path']));
    }
    else {
      if (is_dir($this->fileSystem->realpath($destination))) {

        // Prevent URIs with triple slashes when glueing parts together.
        $path = str_replace(
            '///',
            '//',
            "{$destination}/"
          ) . $this->fileSystem->basename(
            $parsed_url['path']
          );
      }
      else {
        $path = $destination;
      }
    }
    $result = drupal_http_request($url);
    if ($result->code != 200) {
      $this->messenger()->addMessage(
        t(
          'HTTP error @errorcode occurred when trying to fetch @remote.',
          [
            '@errorcode' => $result->code,
            '@remote' => $url,
          ]
        ),
        MessengerInterface::TYPE_ERROR
      );
      return FALSE;
    }

    // It would be more optimal to run this after saving
    // but i really need the mime in case no extension is present
    $mimefromextension = \Drupal::service('file.mime_type.guesser')->guess(
      $path
    );

    if (($mimefromextension == "application/octet-stream") &&
      isset($result->headers['Content-Type'])) {
      $mimetype = $result->headers['Content-Type'];
      $extension = ExtensionGuesser::getInstance()->guess($mimetype);
      $info = pathinfo($path);
      if (($extension != "bin") && ($info['extension'] != $extension)) {
        $path = $path . "." . $extension;
      }
    }
    // File is being made managed and permanent here, will be marked as
    // temporary once it is processed AND/OR associated with a SET
    $localfile = file_save_data($result->data, $path, $replace);
    if (!$localfile) {
      $this->messenger()->addError(
        $this->t(
          '@remote could not be saved to @path.',
          [
            '@remote' => $url,
            '@path' => $path,
          ]
        ),
        MessengerInterface::TYPE_ERROR
      );
    }

    return $localfile;
  }


  public function temp_directory($create = TRUE) {
    $directory = &drupal_static(__FUNCTION__, '');
    if (empty($directory)) {
      $directory = 'temporary://ami';
      if ($create && !file_exists($directory)) {
        mkdir($directory);
      }
    }
    return $directory;
  }

  public function metadatadisplay_process(array $twig_input = []) {
    if (count($twig_input) == 0) {
      return;
    }
    $loader = new Twig_Loader_Array(
      [
        $twig_input['name'] => $twig_input['template'],
      ]
    );

    $twig = new \Twig_Environment(
      $loader, [
        'cache' => $this->fileSystem->realpath('private://'),
      ]
    );

    //We won't validate here. We are here because our form did that
    $output = $twig->render($twig_input['name'], $twig_input['data']);
    //@todo catch anyway any twig error to avoid the worker to fail bad.

    return $output;
  }

  /**
   * Creates an CSV from array and returns file.
   *
   * @param array $data
   *   Same as import form handles, to be dumped to CSV.
   *
   * @return file
   */
  public function csv_save(array $data, $uuid_key = 'node_uuid') {

    //$temporary_directory = $this->fileSystem->getTempDirectory();
    // We should be allowing downloads for this from temp
    // But drupal refuses to serve files that are not referenced by an entity?
    // Read this and be astonished!!
    // https://api.drupal.org/api/drupal/core%21modules%21file%21src%21FileAccessControlHandler.php/class/FileAccessControlHandler/9.1.x
    // We just get a lot of access denied in temporary:// and in private://
    // Solution either attach to an entity SOFORT so permissions can be
    // inherited or create a custom endpoint like '\Drupal\format_strawberryfield\Controller\IiifBinaryController::servetempfile'
    $path = 'public://ami/csv';
    $filename = $this->currentUser->id() . '-' . uniqid() . '.csv';
    // Ensure the directory
    if (!$this->fileSystem->prepareDirectory(
      $path,
      FileSystemInterface::CREATE_DIRECTORY
    )) {
      $this->messenger()->addError(
        $this->t('Unable to create directory for CSV file. Verify permissions please')
      );
      return;
    }
    // Ensure the file
    $file = file_save_data('', $path .'/'. $filename, FileSystemInterface::EXISTS_REPLACE);
    if (!$file) {
      $this->messenger()->addError(
        $this->t('Unable to create AMI CSV file. Verify permissions please.')
      );
      return;
    }
    $realpath = $this->fileSystem->realpath($file->getFileUri());
    $fh = new \SplFileObject($realpath, 'w');
    if (!$fh) {
      $this->messenger()->addError(
        $this->t('Error reading back the just written file!.')
      );
      return;
    }
    array_walk($data['headers'], 'htmlspecialchars');
    // How we want to get the key number that contains the $uuid_key
    $haskey = array_search($uuid_key, $data['headers']);
    if ($haskey === FALSE) {
      array_unshift($data['headers'], $uuid_key);
    }



    $fh->fputcsv($data['headers']);

    foreach ($data['data'] as $row) {
      if ($haskey === FALSE) {
        array_unshift($data['headers'], $uuid_key);
        $row[0] = Uuid::uuid4();
      } else {
        if (empty(trim($row[$haskey])) || !Uuid::isValid(trim($row[$haskey]))) {
          $row[$haskey] = Uuid::uuid4();
        }
      }

      array_walk($row, 'htmlspecialchars');
      $fh->fputcsv($row);
    }
    // PHP Bug! This should happen automatically
    clearstatcache(TRUE, $realpath);
    $size = $fh->getSize();
    // This is how you close a \SplFileObject
    $fh =  NULL;
    // Notify the filesystem of the size change
    $file->setSize($size);
    $file->setPermanent();
    $file->save();

    // Tell the user where we have it.
    $this->messenger()->addMessage(
      $this->t(
        'Your source data was saved and is available as CSV at. <a href="@url">@filename</a>.',
        [
          '@url' => file_create_url($file->getFileUri()),
          '@filename' => $file->getFilename(),
        ]
      )
    );

    return $file->id();
  }

  /**
   * Deal with different sized arrays for combining
   *
   * @param array $header
   *   a CSV header row
   * @param array $row
   *   a CSV data row
   *
   * @return array
   * combined array
   */
  public function ami_array_combine_special(
    $header,
    $row
  ) {
    $headercount = count($header);
    $rowcount = count($row);
    if ($headercount > $rowcount) {
      $more = $headercount - $rowcount;
      for ($i = 0; $i < $more; $i++) {
        $row[] = "";
      }
    }
    else {
      if ($headercount < $rowcount) {
        // more fields than headers
        // Header wins always
        $row = array_slice($row, 0, $headercount);
      }
    }

    return array_combine($header, $row);
  }


  /**
   * Match different sized arrays.
   *
   * @param integer $headercount
   *   an array length to check against.
   * @param array $row
   *   a CSV data row
   *
   * @return array
   *  a resized to header size data row
   */
  public function array_equallyseize($headercount, $row = []) {

    $rowcount = count($row);
    if ($headercount > $rowcount) {
      $more = $headercount - $rowcount;
      for ($i = 0; $i < $more; $i++) {
        $row[] = "";
      }

    }
    else {
      if ($headercount < $rowcount) {
        // more fields than headers
        // Header wins always
        $row = array_slice($row, 0, $headercount);
      }
    }

    return $row;
  }

  /**
   * For a given Numeric Column index, get all different normalized values
   *
   * @param array $data
   * @param int $key
   *
   * @return array
   */
  public function getDifferentValuesfromColumn(array $data, int $key):array {
    $unique = [];
    $all = array_column($data['data'], $key);
    $unique = array_map('trim', $all);
    $unique = array_unique($unique, SORT_STRING);
    return $unique;
  }

  public function getMetadataDisplays() {

    $result = [];
    $query = $this->entityTypeManager->getStorage('metadatadisplay_entity')->getQuery();

    $metadatadisplay_ids = $query
      ->condition("mimetype" , "application/json")
      ->sort('name', 'ASC')
      ->execute();
    if (count($metadatadisplay_ids)) {
      $metadatadisplays = $this->entityTypeManager->getStorage('metadatadisplay_entity')->loadMultiple($metadatadisplay_ids);
      foreach($metadatadisplays as $id => $metadatadisplay) {
        $result[$id] = $metadatadisplay->label();
      }
    }

    return $result;
  }

  public function getWebforms() {

    $result = [];
    $query = $this->entityTypeManager->getStorage('webform')->getQuery();

    $webform_ids = $query
      ->condition("status" , "open")
      ->sort('title', 'ASC')
      ->execute();
    if (count($webform_ids)) {
      $webforms= $this->entityTypeManager->getStorage('webform')->loadMultiple($webform_ids);
      foreach($webforms as $id => $webform) {
        /* @var \Drupal\webform\Entity\Webform $webform */
        $handlercollection = $webform->getHandlers('strawberryField_webform_handler',TRUE);
        if ($handlercollection->count()) {
          $result[$id] = $webform->label();
        }
        // We check if \Drupal\webform_strawberryfield\Plugin\WebformHandler\strawberryFieldharvester is being user
        // In the future this may change?
        // We can also check for dependencies and see if the form has one against Webform Strawberryfield
        // That would work now but not for sure in the future if we add specialty handles
      }
    }

    return $result;
  }


  public function getBundlesAndFields() {
    $bundle_options = [];
    //Only node bundles with a strawberry field are allowed
    /**************************************************************************/
    // Node types with Strawberry fields
    /**************************************************************************/
    $access_handler = $this->entityTypeManager->getAccessControlHandler('node');
    $access = FALSE;

    $bundles = $this->entityTypeBundleInfo->getBundleInfo('node');
    foreach ($bundles as $bundle => $bundle_info) {
      if ($this->strawberryfieldUtility->bundleHasStrawberryfield($bundle)) {
        dpm($bundle);
        $access = $this->checkNodeAccess($bundle);
        if ($access && $access->isAllowed()) {
          foreach($this->checkFieldAccess($bundle) as $key => $value) {
            $bundle_options[$key] = $value. ' for '. $bundle_info['label'];
          }
        }
      }
    }
    return $bundle_options;
  }

  /**
   * Checks if a User can Create a Given Bundle type
   *
   * @param string $bundle
   * @param \Drupal\Core\Session\AccountInterface|null $account
   * @param null $entity_id
   *
   * @return \Drupal\Core\Access\AccessResultInterface|bool
   * @throws \Drupal\Component\Plugin\Exception\InvalidPluginDefinitionException
   * @throws \Drupal\Component\Plugin\Exception\PluginNotFoundException
   */
  public function checkNodeAccess(string $bundle, AccountInterface $account = NULL, $entity_id = NULL) {
    try {
      $access_handler = $this->entityTypeManager->getAccessControlHandler('node');
      /*$storage = $this->entityTypeManager->getStorage('node');
      /*if (!empty($parameters['revision_id'])) {
        $entity = $storage->loadRevision($parameters['revision_id']);
        $entity_access = $access_handler->access($entity, 'update', $account, TRUE);
      }

      elseif ($parameters['entity_id']) {
        $entity = $storage->load($parameters['entity_id']);
        $entity_access = $access_handler->access($entity, 'update', $account, TRUE);
      }
      */

      return $access_handler->createAccess($bundle, $account, [], TRUE);
      }
    catch (\Exception $exception) {
      // Means the Bundles does not exist?
      // User is wrong?
      // etc. Simply no access is easier to return
      return FALSE;
    }

  }

  /**
   * Checks if User can edit a given SBF field
   *
   * @TODO content of a field could override access. Add a per entity check.
   *
   * @param string $entity_type_id
   * @param \Drupal\Core\Session\AccountInterface|null $account
   * @param $bundle
   *
   * @return bool
   */
  public function checkFieldAccess($bundle, AccountInterface $account = NULL) {
    $fields = [];
    $access_handler = $this->entityTypeManager->getAccessControlHandler('node');
    $field_definitions = $this->strawberryfieldUtility->getStrawberryfieldDefinitionsForBundle($bundle);

    foreach ($field_definitions as $field_definition) {
        $field_access = $access_handler->fieldAccess('edit', $field_definition, $account, NULL, TRUE);
        if($field_access->isAllowed()) {
          $fields[$bundle.':'.$field_definition->getName()] = $field_definition->getLabel();
        }
    }
    return $fields;
  }


  public function processMetadataDisplay($data) {


  }

  public function processWebform($data) {


  }


  public function ingestAdo($data) {

  }





  public function updateAdo($data) {

  }

  public function patchAdo($data) {

  }

  public function deleteAdo($data) {

  }



}

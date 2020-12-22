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
use Drupal\file\Entity\File;
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
   * @var \Drupal\Core\Entity\EntityFieldManagerInterface $entityFieldManager ;
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

  public function getParentType($uuid) {
    // This is the same as format_strawberry \format_strawberryfield_entity_view_mode_alter
    // @TODO refactor into a reusable method inside strawberryfieldUtility!
    $entity = $this->entityTypeManager->getStorage('node')
      ->loadByProperties(['uuid' => $uuid]);
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
    $file = file_save_data('', $path . '/' . $filename, FileSystemInterface::EXISTS_REPLACE);
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
        array_unshift($row, $uuid_key);
        $row[0] = Uuid::uuid4();
      }
      else {
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
    $fh = NULL;
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


  public function csv_read(File $file) {

    $wrapper = $this->streamWrapperManager->getViaUri($file->getFileUri());
    if (!$wrapper) {
      return NULL;
    }

    $url = $wrapper->realpath();
    $spl = new \SplFileObject($url, 'r');
    $data = [];
    while (!$spl->eof()) {
      $data[] = $spl->fgetcsv();
    }

    $table = [];
    $maxRow = 0;

    $highestRow = count($data);

    $rowHeaders = $data[0];
    $rowHeaders_utf8 = array_map('stripslashes', $rowHeaders);
    $rowHeaders_utf8 = array_map('utf8_encode', $rowHeaders_utf8);
    $rowHeaders_utf8 = array_map('strtolower', $rowHeaders_utf8);
    $rowHeaders_utf8 = array_map('trim', $rowHeaders_utf8);

    $headercount = count($rowHeaders);

    if (($highestRow) >= 1) {
      // Returns Row Headers.

      $maxRow = 1; // at least until here.
      foreach ($data as $rowindex => $row) {
        if ($rowindex == 0) {
          // Skip header
          continue;
        }
        $flat = trim(implode('', $row));
        //check for empty row...if found stop there.
        if (strlen($flat) == 0) {
          $maxRow = $rowindex;
          break;
        }
        // This was done already by the Import Plugin but since users
        // Could eventually reupload the spreadsheet better so
        $row = $this->array_equallyseize(
          $headercount,
          $row
        );
        // Offsetting all rows by 1. That way we do not need to remap numeric parents
        $table[$rowindex+1] = $row;
      }
      $maxRow = $rowindex;
    }

    $tabdata = [
      'headers' => $rowHeaders_utf8,
      'data' => $table,
      'totalrows' => $maxRow,
    ];

    return $tabdata;

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

  public function createAmiSet(\stdClass $data) {
    // See \Drupal\ami\Entity\amiSetEntity
    $current_user_name = $this->currentUser->getDisplayName();
    $set = [
      'mapping' => $data->mapping,
      'adomapping' => $data->adomapping,
      'zip' => $data->zip,
      'csv' => $data->csv,
      'plugin' =>  $data->plugin,
      'pluginconfig' =>  $data->pluginconfig,
      'column_keys' => $data->column_keys,
      'total_rows' => $data->total_rows,
    ];
    $jsonvalue = json_encode($set, JSON_PRETTY_PRINT);
    /* @var \Drupal\ami\Entity\amiSetEntity $entity */
    $entity = $this->entityTypeManager->getStorage('ami_set_entity')->create(['name' => 'AMI Set of '.$current_user_name]);
    $entity->set('set', $jsonvalue);
    $entity->set('source_data', [$data->csv]);
    $entity->set('zip_file', [$data->zip]);
    $entity->set('status' , 'ready');
    try {
      $result = $entity->save();
    }
    catch (\Exception $exception) {
      dpm($exception);
      $this->messenger()->addError(t('Ami Set entity Failed to be persisted because of @message', ['@message' => $exception->getMessage()]));
      return NULL;
    }
    if ($result == SAVED_NEW) {
      return $entity->id();
    }

  }

  public function preprocessAmiSet(File $file, \stdClass $data) {
    dpm('Running preprocessAmiSet');

    $file_data_all = $this->csv_read($file);
    dpm($file_data_all);
    // we may want to check if saved metadata headers == csv ones first.
    // $data->column_keys
    $config['data']['headers'] = $file_data_all['headers'];
    // In old times we totally depended on position, now we are going to do something different, we will combine
    // Headers and keys.
    $data->mapping->type_key = isset($data->mapping->type_key) ? $data->mapping->type_key : 'type';

    // Keeps track of all parents and child that don't have a PID assigned.
    $parent_hash = [];
    $info = [];
    // Keeps track of invalid rows.
    $invalid = [];

    foreach ($file_data_all['data'] as $index => $keyedrow) {
      // This makes tracking of values more consistent and easier for the actual processing via
      // twig templates, webforms or direct
      $row = array_combine($config['data']['headers'], $keyedrow);
      dpm($row);
      // Each row will be an object.
      $ado = [];
      $ado['type'] = trim(
        $row[$data->mapping->type_key]
      );
      dpm($ado);
      // Lets start by grouping by parents, namespaces and generate uuids
      // namespaces are inherited, so we just need to find collection
      // objects in parent uuid column.

      // We may have multiple parents
      // @dmer deal with files as parents of a node in a next iteration.
      // So, we will here simply track also any parent
      foreach($data->adomapping->parents as $parent_key) {
        // Used to access parent columns using numerical indexes for when looking back inside $file_data_all
        $parent_to_index[$parent_key] = array_search($parent_key, $config['data']['headers']);

        dpm('parent keys');
        dpm($parent_key);
        $ado['parent'][$parent_key] = trim(
          $row[$parent_key]
        );
        $ado['anyparent'][] = $row[$parent_key];
      }
      dpm($ado['parent']);
      dpm($ado['anyparent']);

      $ado['data'] = $row;
      dpm($ado);

      // UUIDs are already assigned by this time
      dpm($data->adomapping->uuid->uuid);
      $possibleUUID = trim($row[$data->adomapping->uuid->uuid]);
      dpm($possibleUUID);
      // Double check? User may be tricking us!
      if (Uuid::isValid($possibleUUID)) {
        $ado['uuid'] = $possibleUUID;
        // Now be more strict for action = update
        if ($data->pluginconfig->op ==! "create")
        {
          $existing_object = $this->entityTypeManager->getStorage('node')
            ->loadByProperties(['uuid' => $ado['uuid']]);
          //@TODO field_descriptive_metadata  is passed from the Configuration
          if (!$existing_object) {
            unset($ado);
            $invalid = $invalid + [$index => $index];
          }
        }
      }
      dpm($ado);
      if (!isset($ado['uuid'])) {
        dpm('NO UUID! removing'.$index);
        unset($ado);
        $invalid = $invalid + [$index => $index];
      }

      if (isset($ado)) {
        // CHECK. Different to IMI. we have multiple relationships
        foreach($ado['parent'] as $parent_key => $parent_ado) {
          // Discard further processing of empty parents
          if (strlen(trim($parent_ado)) ==0 ) {
            dpm('empty parent');
            continue;
          }

          if (!Uuid::isValid($parent_ado) && (intval(trim($parent_ado)) > 1)) {
            dpm('its a row');
            dpm(intval(trim($parent_ado)));

            // Means our parent object is a ROW index
            // (referencing another row in the spreadsheet)
            // So a different strategy is needed. We will need recurse
            // until we find a non numeric parent or none! Because
            // in Archipelago we allow the none option for sure!
            $rootfound = FALSE;
            // SUPER IMPORTANT. SINCE PEOPLE ARE LOOKING AT A SPREADSHEET THEIR PARENT NUMBER WILL INCLUDE THE HEADER
            // SO WE ARE OFFSET by 1, substract 1
            $parent_numeric = intval(trim($parent_ado));
            $parent_hash[$parent_key][$parent_numeric][$index] = $index;
            $parentchilds = [];
            // Lets check if the index actually exists before going crazy.

            // If parent is empty that is OK here. WE are Ok with no membership!
            if (!isset($file_data_all['data'][$parent_numeric])) {
              dpm('parent row does not exist!');
              $invalid[$parent_numeric] = $parent_numeric;
              $invalid[$index] = $index;
            }

            if ((!isset($invalid[$index])) && (!isset($invalid[$parent_numeric]))) {
              // Only traverse if we don't have this index or the parent one
              // in the invalid register.
              $parentchilds = [];
              $i = 0;
              while (!$rootfound) {
                dpm('while');
                dpm($parent_numeric);
                dpm($parent_to_index[$parent_key]);

                $parentup = $file_data_all['data'][$parent_numeric][$parent_to_index[$parent_key]];
                if ($this->isRootParent($parentup)) {
                  $rootfound = TRUE;
                  break;
                }
                // If $parentup
                // The Simplest approach for breaking a knot /infinite loop,
                // is invalidating the whole parentship chain for good.
                $inaloop = isset($parentchilds[$parentup]);
                // If $inaloop === true means we already traversed this branch
                // so we are in a loop and all our original child and it's
                // parent objects are invalid.
                if ($inaloop) {
                  dpm('we are in a loop!');
                  $invalid = $invalid + $parentchilds;
                  unset($ado);
                  $rootfound = TRUE;
                  // Means this object is already doomed. We break any attempt
                  // to get relationships for this one.
                  break 2;
                }

                $parentchilds[$parentup] = $parentup;
                // If this parent is either a UUID or empty means we reached the root

                // This a simple accumulator, means all is well,
                // parent is still an index.
                $parent_hash[$parent_key][$parentup][$parent_numeric] = $parent_numeric;

                $parent_numeric = $parentup;
              }
            }
            else {
              unset($ado);
            }
          }
        }
      }
      if (isset($ado) and !empty($ado)) {
        $info[$index] = $ado;
      }
    }


    // New first pass: ensure parents have always a PID first
    // since rows/parent/child order could be arbitrary
    dpm($info);
    dpm($parent_hash);
    // Now the real pass, iterate over every row.
    foreach ($info as $index => &$ado) {
      //$ado['uuid'] = isset($ado['uuid']) ? $ado['uuid'] : Uuid::uuid4();
      foreach($data->adomapping->parents as $parent_key) {
        // Is this object parent of someone?
        if (isset($parent_hash[$parent_key][$ado['parent'][$parent_key]])) {
          $ado['parent'][$parent_key] = $info[$ado['parent'][$parent_key]]['uuid'];
        }
      }
    }

    dpm($info);
    return $info;
  }

  /**
   * Super simple callback to check if in a CSV our parent is the actual root element
   *
   * @param string $parent
   *
   * @return bool
   */
  protected function isRootParent(string $parent) {
    return (Uuid::isValid(trim($parent)) || strlen(trim($parent)) == 0);
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

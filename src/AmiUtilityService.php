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
use Drupal\Core\Render\RenderContext;
use Drupal\Core\Session\AccountInterface;
use Drupal\Core\StreamWrapper\StreamWrapperManagerInterface;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\file\Entity\File;
use Drupal\file\FileUsage\FileUsageInterface;
use Drupal\strawberryfield\StrawberryfieldFileMetadataService;
use Drupal\strawberryfield\Tools\StrawberryfieldJsonHelper;
use GuzzleHttp\ClientInterface;
use Drupal\strawberryfield\StrawberryfieldUtilityService;
use Ramsey\Uuid\Uuid;
use Drupal\Core\File\Exception\FileException;
use SplFileObject;

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
   * @var \GuzzleHttp\ClientInterface
   */
  protected $httpClient;

  /**
   * Key value service.
   *
   * @var \Drupal\Core\KeyValueStore\KeyValueFactoryInterface
   */
  protected $keyValue;

  /**
   * @var \Drupal\ami\AmiLoDService
   */
  protected $AmiLoDService;

  /**
   * The Strawberry Field File Metadata Service.
   *
   * @var \Drupal\strawberryfield\StrawberryfieldFileMetadataService
   */
  protected $strawberryfieldFileMetadataService;

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
   * @param StrawberryfieldUtilityService $strawberryfield_utility_service
   * @param \Drupal\Core\Entity\EntityFieldManagerInterface $entity_field_manager
   * @param \Drupal\Core\Entity\EntityTypeBundleInfoInterface $entity_type_bundle_info
   * @param \GuzzleHttp\ClientInterface $http_client
   * @param \Drupal\Core\KeyValueStore\KeyValueFactoryInterface $key_value
   * @param \Drupal\strawberryfield\StrawberryfieldFileMetadataService $strawberryfield_file_metadata_service
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
    EntityTypeBundleInfoInterface $entity_type_bundle_info,
    ClientInterface $http_client,
    AmiLoDService $ami_lod,
    KeyValueFactoryInterface $key_value,
    StrawberryfieldFileMetadataService $strawberryfield_file_metadata_service
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
    $this->httpClient = $http_client;
    $this->AmiLoDService = $ami_lod;
    $this->keyValue = $key_value;
    $this->strawberryfieldFileMetadataService = $strawberryfield_file_metadata_service;
  }


  /**
   * Checks if value is prefixed entity or an UUID.
   *
   * @param mixed $val
   *
   * @return bool
   */
  public function isEntityId($val) {
    return (!is_int($val)
      && is_numeric(
        $this->getIdfromPrefixedEntityNode($val)
      )
      || \Drupal\Component\Uuid\Uuid::isValid(
        $val
      ));
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

  /**
   * Checks if URI is remote/local/zip/folder and returns a file if found.
   *
   * @param string $uri
   *   The URL of the file to grab.
   *
   * @param \Drupal\file\Entity\File|NULL $zip_file
   *
   * @return File|FALSE
   *   One of these possibilities:
   *   - If remote and exists a Drupal file object
   *   - If not remote and exists a Drupal file object
   *   - If does not exist boolean FALSE
   */
  public function file_get($uri, File $zip_file = NULL) {
    $parsed_url = parse_url($uri);
    $remote_schemes = ['http', 'https', 'feed'];
    $finaluri = FALSE;
    $ami_temp_folder = 'ami/setfiles/';
    $destination = "temporary://" . $ami_temp_folder;
    if (!isset($parsed_url['scheme'])
      || (isset($parsed_url['scheme'])
        && !in_array(
          $parsed_url['scheme'],
          $remote_schemes
        ))
    ) {
      // Now that we know its not remote, try with our registered schemas
      // means its either private/public/s3, etc

      $scheme = $this->streamWrapperManager->getScheme($uri);
      if ($scheme) {
        if (!file_exists($uri)) {
          return FALSE;
        }
        $finaluri = $uri;
      }
      else {
        // Means it may be local to the accessible file storage, eg. a path inside
        // the server or inside a provided ZIP file
        //@TODO check if we should copy here or just deal with it.
        $localfile = $this->fileSystem->realpath($uri);
        if (!file_exists($localfile) && !$zip_file) {
          return FALSE;
        }
        elseif ($zip_file) {
          // Try with the ZIP file in case there is a ZIP and local failed
          // Use the Zip file uuid to prefix the destination.
          $localfile = file_build_uri(
            $this->fileSystem->basename($ami_temp_folder . $zip_file->uuid() . '/' . urldecode($parsed_url['path']))
          );
          if (!file_exists($localfile)) {
            $destination_zip = $destination . $zip_file->uuid() . '/';
            if (!$this->fileSystem->prepareDirectory(
              $destination_zip,
              FileSystemInterface::CREATE_DIRECTORY
              | FileSystemInterface::MODIFY_PERMISSIONS
            )
            ) {
              $this->messenger()->addError(
                $this->t(
                  'Unable to create directory where to extract from ZIP @zip file @uri. Verify permissions please',
                  [
                    '@uri' => $uri,
                    '@zip' => $zip_file->getFileUri(),
                  ]
                )
              );
              return FALSE;
            }
            $localfile = $this->retrieve_fromzip_file($uri, $destination_zip,
              FileSystemInterface::EXISTS_REPLACE, $zip_file);
          }
        }
        $finaluri = $localfile;
      }
    }
    else {
      // This may be remote!
      // Simulate what could be the final path of a remote download.
      // to avoid re downloading.
      $localfile = file_build_uri(
        $this->fileSystem->basename(urldecode($parsed_url['path']))
      );
      $md5uri = md5($uri);
      $destination = $destination . $md5uri . '/' ;
      $path = str_replace(
          '///',
          '//',
          "{$destination}/"
        ) . $this->fileSystem->basename(urldecode($parsed_url['path']))
        );
      if ($isthere = glob($this->fileSystem->realpath($path) . '.*')) {
        // Ups its here
        if (count($isthere) == 1) {
          $localfile = $isthere[0];
        }
      }
      // Actual remote heavy lifting only if not present.
      if (!file_exists($localfile)) {
        if (!$this->fileSystem->prepareDirectory(
          $destination,
          FileSystemInterface::CREATE_DIRECTORY
          | FileSystemInterface::MODIFY_PERMISSIONS
        )
        ) {
          $this->messenger()->addError(
            $this->t(
              'Unable to create directory where to download remote file @uri. Verify permissions please',
              [
                '@uri' => $uri,
              ]
            )
          );
          return FALSE;
        }

        $localfile = $this->retrieve_remote_file(
          $uri,
          $destination,
          FileSystemInterface::EXISTS_RENAME
        );
        $finaluri = $localfile;
      }
      else {
        $finaluri = $localfile;
      }
    }
    // This is the actual file creation independently of the source.
    if ($finaluri) {
      $file = $this->create_file_from_uri($finaluri);
      return $file;
    }
    return FALSE;
  }

  /**
   * Attempts to get a file using drupal_http_request and to store it locally.
   *
   * @param string $url
   *     The URL of the file to grab.
   * @param string $destination
   *     Stream wrapper URI specifying where the file should be placed. If a
   *     directory path is provided, the file is saved into that directory
   *     under
   *     its original name. If the path contains a filename as well, that one
   *     will be used instead. If this value is omitted, the site's default
   *     files scheme will be used, usually "public://".
   * @param int $replace
   *     Replace behavior when the destination file already exists:
   *     - FILE_EXISTS_REPLACE: Replace the existing file.
   *     - FILE_EXISTS_RENAME: Append _{incrementing number} until the filename
   *     is unique.
   *     - FILE_EXISTS_ERROR: Do nothing and return FALSE.
   *
   * @return mixed
   *   One of these possibilities:
   *   - If it succeeds an managed file object
   *   - If it fails, FALSE.
   */
  public function retrieve_remote_file(
    $uri,
    $destination = NULL,
    $replace = FileSystemInterface::EXISTS_RENAME
  ) {
    // pre set a failure
    $localfile = FALSE;
    $md5uri = md5($uri);
    $parsed_url = parse_url($uri);
    if (!isset($destination)) {
      $path = file_build_uri($this->fileSystem->basename(urldecode($parsed_url['path'])));
    }
    else {
      if (is_dir($this->fileSystem->realpath($destination))) {
        // Prevent URIs with triple slashes when glueing parts together.
        $path = str_replace(
            '///',
            '//',
            "{$destination}/"
          ) . $this->fileSystem->basename(urldecode($parsed_url['path']));
      }
      else {
        $path = $destination;
      }
    }
    /* @var \Psr\Http\Message\ResponseInterface $response */
    try {
      $realpath = $this->fileSystem->realpath($path);
      $response = $this->httpClient->get($uri, ['sink' => $realpath]);
    } catch (\Exception $exception) {
      $this->messenger()->addError(
        $this->t(
          'Unable to download remote file from @uri to local @path with error: @error. Verify URL exists, its openly accessible and destination is writable.',
          [
            '@uri' => $uri,
            '@path' => $path,
            '@error' => $exception->getMessage(),
          ]
        )
      );
      return FALSE;
    }

    // It would be more optimal to run this after saving
    // but i really need the mime in case no extension is present
    $mimefromextension = \Drupal::service('strawberryfield.mime_type.guesser.mime')
      ->guess($path);
    if (($mimefromextension == "application/octet-stream")
      && !empty($response->getHeader('Content-Type'))
    ) {
      $mimetype = $response->getHeader('Content-Type');
      if (count($mimetype) > 0) {
        $extension = \Drupal::service('strawberryfield.mime_type.guesser.mime')
          ->inverseguess($mimetype[0]);
      }
      else {
        $extension = '';
      }
      $info = pathinfo($realpath);
      if (!isset($info['extension']) || $info['extension'] != $extension) {
        $newpath = $realpath . "." . $extension;
        $status = @rename($realpath, $newpath);
        if ($status === FALSE && !file_exists($newpath)) {
          $this->messenger()->addError(
            $this->t(
              'Unable to rename downloaded file from @realpath to local @newpath. Verify if destination is writable.',
              [
                '@realpath' => $realpath,
                '@newpath' => $newpath,
              ]
            )
          );
          return FALSE;
        }
        $localfile = $newpath;
      }
      else {
        $localfile = $realpath;
      }
    }
    else {
      // Means we got an mimetype that is not and octet,derived from the
      // extension and we will roll with the original download path.
      if (file_exists($realpath)) {
        $localfile = $realpath;
      }
    }
    return $localfile;
  }

  /**
   * Attempts to get a file from a ZIP file store it locally.
   *
   * @param string $uri
   * @param string|null $destination
   *     Stream wrapper URI specifying where the file should be placed. If a
   *     directory path is provided, the file is saved into that directory
   *     under
   *     its original name. If the path contains a filename as well, that one
   *     will be used instead. If this value is omitted, the site's default
   *     files scheme will be used, usually "public://".
   * @param int $replace
   *     Replace behavior when the destination file already exists:
   *     - FILE_EXISTS_REPLACE: Replace the existing file.
   *     - FILE_EXISTS_RENAME: Append _{incrementing number} until the filename
   *     is unique.
   *     - FILE_EXISTS_ERROR: Do nothing and return FALSE.
   *
   * @param \Drupal\file\Entity\File $zip_file
   *     A Zip file with that may contain the $uri
   *
   * @return mixed
   *   One of these possibilities:
   *   - If it succeeds an managed file object
   *   - If it fails, FALSE.
   */
  public function retrieve_fromzip_file($uri, $destination = NULL, $replace = FileSystemInterface::EXISTS_RENAME, File $zip_file) {
    $zip_realpath = NULL;
    $md5uri = md5($uri);
    $parsed_url = parse_url($uri);
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
          ) . $md5uri . '_' . $this->fileSystem->basename(
            $parsed_url['path']
          );
      }
      else {
        $path = $destination;
      }
    }
    try {
      $realpath = $this->fileSystem->realpath($path);
      $zip_realpath = $this->fileSystem->realpath($zip_file->getFileUri());
      // Means Mr. Zip is in S3 or who knows where
      // And ZipArchive (Why!!) can not stream from remote
      // @TODO write once for all a remote ZIP file streamer DIEGO
      if (!$zip_realpath) {
        // This will add a delay once...
        $zip_realpath = $this->strawberryfieldFileMetadataService->ensureFileAvailability($zip_file, NULL);
      }
      $z = new \ZipArchive();
      $contents = NULL;
      if ($z->open($zip_realpath)) {
        $fp = $z->getStream($uri);
        if (!$fp) {
          return FALSE;
        }
        while (!feof($fp)) {
          $contents .= fread($fp, 2);
        }
        fclose($fp);
        if ($contents && file_put_contents($realpath, $contents)) {
          return $realpath;
        }
      }
      else {
        // Opening the ZIP file failed.
        return FALSE;
      }
    }
    catch (\Exception $exception) {
      $this->messenger()->addError(
        $this->t(
          'Unable to extract file @uri from ZIP @zip to local @path. Verify ZIP exists, its readable and destination is writable.',
          [
            '@uri' => $uri,
            '@zip' => $zip_realpath,
            '@path' => $path,
          ]
        )
      );
    }
    return FALSE;
  }

  /**
   * Creates File from a local accessible Path/URI.
   *
   * @param $localpath
   *
   * @return \Drupal\Core\Entity\EntityInterface|false
   * @throws \Drupal\Component\Plugin\Exception\InvalidPluginDefinitionException
   * @throws \Drupal\Component\Plugin\Exception\PluginNotFoundException
   * @throws \Drupal\Core\Entity\EntityStorageException
   */
  public function create_file_from_uri($localpath) {
    try {
      $file = $this->entityTypeManager->getStorage('file')->create(
        [
          'uri' => $localpath,
          'uid' => $this->currentUser->id(),
          'status' => FILE_STATUS_PERMANENT,
        ]
      );
      // If we are replacing an existing file re-use its database record.
      // @todo Do not create a new entity in order to update it. See
      //   https://www.drupal.org/node/2241865.
      // Check if File with same URI already exists.
      $existing_files = \Drupal::entityTypeManager()
        ->getStorage('file')
        ->loadByProperties(['uri' => $localpath]);
      if (count($existing_files)) {
        $existing = reset($existing_files);
        $file->fid = $existing->id();
        $file->setOriginalId($existing->id());
        $file->setFilename($existing->getFilename());
      }

      $file->save();
      return $file;
    }
    catch (FileException $e) {
      return FALSE;
    }
  }

  /**
   * Creates an empty CSV returns file.
   *
   * @param string|null $filename
   *    If given it will use that, if null will create a new one
   *
   * @return int|string|null
   * @throws \Drupal\Core\Entity\EntityStorageException
   */
  public function csv_touch(string $filename = NULL) {
    $path = 'public://ami/csv';
    $filename = $filename ?? $this->currentUser->id() . '-' . uniqid() . '.csv';
    // Ensure the directory
    if (!$this->fileSystem->prepareDirectory(
      $path,
      FileSystemInterface::CREATE_DIRECTORY
      | FileSystemInterface::MODIFY_PERMISSIONS
    )
    ) {
      $this->messenger()->addError(
        $this->t(
          'Unable to create directory for CSV file. Verify permissions please'
        )
      );
      return NULL;
    }
    // Ensure the file
    $file = file_save_data(
      '', $path . '/' . $filename, FileSystemInterface::EXISTS_REPLACE
    );
    if (!$file) {
      $this->messenger()->addError(
        $this->t('Unable to create AMI CSV file. Verify permissions please.')
      );
      return NULL;
    }
    $file->setPermanent();
    $file->save();
    return $file->id();
  }


  /**
   * Creates an CSV from array and returns file.
   *
   * @param array $data
   *   Same as import form handles, to be dumped to CSV.
   *   $data should contain two keys, 'headers' and 'data'
   *   'data' will be rows and may/not be associative.
   *
   * @param string $uuid_key
   *
   * @return int|string|null
   * @throws \Drupal\Core\Entity\EntityStorageException
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
      | FileSystemInterface::MODIFY_PERMISSIONS
    )
    ) {
      $this->messenger()->addError(
        $this->t(
          'Unable to create directory for CSV file. Verify permissions please'
        )
      );
      return NULL;
    }
    // Ensure the file
    $file = file_save_data(
      '', $path . '/' . $filename, FileSystemInterface::EXISTS_REPLACE
    );
    if (!$file) {
      $this->messenger()->addError(
        $this->t('Unable to create AMI CSV file. Verify permissions please.')
      );
      return NULL;
    }
    $wrapper = $this->streamWrapperManager->getViaUri($file->getFileUri());
    if (!$wrapper) {
      return NULL;
    }
    $url = $wrapper->getUri();
    $fh = new SplFileObject($url, 'w');
    if (!$fh) {
      $this->messenger()->addError(
        $this->t('Error reading back the just written file!.')
      );
      return NULL;
    }
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
        // In case Data is passed as an associative Array
        if (StrawberryfieldJsonHelper::arrayIsMultiSimple($row)) {
          if (!isset($row[$uuid_key]) || empty(trim($row[$uuid_key])) || !Uuid::isValid(trim($row[$uuid_key]))) {
            $row[$uuid_key] = Uuid::uuid4();
          }
        }
        else {
          if (empty(trim($row[$haskey])) || !Uuid::isValid(trim($row[$haskey]))) {
            $row[$haskey] = Uuid::uuid4();
          }
        }
      }

      $fh->fputcsv($row);
    }
    // PHP Bug! This should happen automatically
    clearstatcache(TRUE, $url);
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


  /**
   * Appends CSV from array and returns file.
   *
   * @param array $data
   *   Same as import form handles, to be dumped to CSV.
   *
   * @param \Drupal\file\Entity\File $file
   *
   * @param string|null $uuid_key
   *    IF NULL then no attempt of using UUIDS will be made.
   *    Needed for LoD Reconciling CSVs
   * @param bool $append_header
   *
   * @return int|string|null
   * @throws \Drupal\Core\Entity\EntityStorageException
   */
  public function csv_append(array $data, File $file, $uuid_key = 'node_uuid', bool $append_header = TRUE) {

    $wrapper = $this->streamWrapperManager->getViaUri($file->getFileUri());
    if (!$wrapper) {
      return NULL;
    }
    $url = $wrapper->getUri();
    $fh = new \SplFileObject($url, 'a');
    if (!$fh) {
      $this->messenger()->addError(
        $this->t('Error reading the CSV file!.')
      );
      return NULL;
    }
    // How we want to get the key number that contains the $uuid_key
    if ($uuid_key) {
      $haskey = array_search($uuid_key, $data['headers']);
      if ($haskey === FALSE) {
        array_unshift($data['headers'], $uuid_key);
      }
    }
    if ($append_header) {
      $fh->fputcsv($data['headers']);
    }

    foreach ($data['data'] as $row) {
      if ($uuid_key) {
        if ($haskey === FALSE) {
          array_unshift($row, $uuid_key);
          $row[0] = Uuid::uuid4();
        }
        else {
          // In case Data is passed as an associative Array
          if (StrawberryfieldJsonHelper::arrayIsMultiSimple($row)) {
            if (!isset($row[$uuid_key]) || empty(trim($row[$uuid_key])) || !Uuid::isValid(trim($row[$uuid_key]))) {
              $row[$uuid_key] = Uuid::uuid4();
            }
          }
          else {
            if (empty(trim($row[$haskey])) || !Uuid::isValid(trim($row[$haskey]))) {
              $row[$haskey] = Uuid::uuid4();
            }
          }
        }
      }

      $fh->fputcsv($row);
    }
    // PHP Bug! This should happen automatically
    clearstatcache(TRUE, $url);
    $size = $fh->getSize();
    // This is how you close a \SplFileObject
    $fh = NULL;
    // Notify the filesystem of the size change
    $file->setSize($size);
    $file->save();
    return $file->id();
  }


  /**
   * @param \Drupal\file\Entity\File $file
   * @param int $offset
   *    Where to start to read the file, starting from 0.
   * @param int $count
   *    Number of results, 0 will fetch all
   * @param bool $always_include_header
   *    Always return header even with an offset.
   *
   * @return array|null
   *   Returning array will be in this form:
   *    'headers' => $rowHeaders_utf8 or [] if $always_include_header == FALSE
   *    'data' => $table,
   *    'totalrows' => $maxRow,
   */
  public function csv_read(File $file, int $offset = 0, int $count = 0, bool $always_include_header = TRUE) {

    $wrapper = $this->streamWrapperManager->getViaUri($file->getFileUri());
    if (!$wrapper) {
      return NULL;
    }

    $url = $wrapper->getUri();
    $spl = new \SplFileObject($url, 'r');
    if ($offset > 0) {
      // We only set this flags when an offset is present.
      // Because if not fgetcsv is already dealing with multi line CSV rows.
      $spl->setFlags(
        SplFileObject::READ_CSV |
        SplFileObject::READ_AHEAD |
        SplFileObject::SKIP_EMPTY |
        SplFileObject::DROP_NEW_LINE
      );
    }

    if ($offset > 0 && !$always_include_header) {
      // If header needs to be included then we offset later on
      $spl->seek($offset);
    }
    $data = [];
    $seek_to_offset = ($offset > 0 && $always_include_header);
    while (!$spl->eof() && ($count == 0 || ($spl->key() < ($offset + $count)))) {
      $data[] = $spl->fgetcsv();
      if ($seek_to_offset) {
        $spl->seek($offset);
        // So we do not process this again.
        $seek_to_offset = FALSE;
      }
    }

    $table = [];
    $maxRow = 0;

    $highestRow = count($data);
    if ($always_include_header) {
      $rowHeaders = $data[0];
      $rowHeaders_utf8 = array_map('stripslashes', $rowHeaders);
      $rowHeaders_utf8 = array_map('utf8_encode', $rowHeaders_utf8);
      $rowHeaders_utf8 = array_map('strtolower', $rowHeaders_utf8);
      $rowHeaders_utf8 = array_map('trim', $rowHeaders_utf8);
      $headercount = count($rowHeaders);
    }
    else {
      $rowHeaders = $rowHeaders_utf8 = [];
      $not_a_header = $data[0] ?? [];
      $headercount = count($not_a_header);
    }


    if (($highestRow) >= 1) {
      // Returns Row Headers.

      $maxRow = 1; // at least until here.
      $rowindex = 0;
      foreach ($data as $rowindex => $row) {
        if ($rowindex == 0) {
          // Skip header
          continue;
        }
        // Ensure row is always an array.
        $row = $row ?? [];
        $flat = trim(implode('', $row));
        //check for empty row...if found stop there.
        if (strlen($flat) == 0) {
          $maxRow = $rowindex;
          break;
        }
        // This was done already by the Import Plugin but since users
        // Could eventually re upload the spreadsheet better so
        $row = $this->arrayEquallySeize(
          $headercount,
          $row
        );
        // Offsetting all rows by 1. That way we do not need to remap numeric parents
        $table[$rowindex + 1] = $row;
      }
      $maxRow = $maxRow ?? $rowindex;
    }

    return  [
      'headers' => $rowHeaders_utf8,
      'data' => $table,
      'totalrows' => $maxRow,
    ];

  }

  /**
   * Removes columns from an existing CSV
   *
   * @param \Drupal\file\Entity\File $file
   *
   * @param array $headerwithdata
   *
   * @return array|null
   */
  public function csv_clean(File $file, array $headerwithdata = []) {
    $wrapper = $this->streamWrapperManager->getViaUri($file->getFileUri());
    if (!$wrapper) {
      return NULL;
    }
    $url = $wrapper->getUri();
    // New temp file for the output
    $path = 'public://ami/csv';
    $filenametemp = $this->currentUser->id() . '-' . uniqid() . '_clean.csv';
    // Ensure the directory
    if (!$this->fileSystem->prepareDirectory(
      $path,
      FileSystemInterface::CREATE_DIRECTORY
      | FileSystemInterface::MODIFY_PERMISSIONS
    )) {
      $this->messenger()->addError(
        $this->t(
          'Unable to create directory for Temporary CSV file. Verify permissions please'
        )
      );
      return NULL;
    }
    // Ensure the file
    $tempurl = $this->fileSystem->saveData( '', $path . '/' . $filenametemp, FileSystemInterface::EXISTS_REPLACE);

    if (!$tempurl) {
      return NULL;
    }
    $keys = [];

    $spl = new \SplFileObject($url, 'r');
    $spltmp = new \SplFileObject($tempurl, 'a');
    $i = 1;
    $data = [];
    while (!$spl->eof()) {
      $data = $spl->fgetcsv();
      if ($i == 1) {
        $removecolumns = array_diff($data, $headerwithdata);
        foreach ($removecolumns as $column) {
          $keys[] = array_search($column, $data);
        }
        array_filter($keys);
      }
      foreach ($keys as $key) {
        unset($data[$key]);
      }
      $data = array_values($data);
      $spltmp->fputcsv($data);
      $i++;
    }
    $size = $spltmp->getSize();
    $spltmp = NULL;
    $spl = NULL;
    clearstatcache(TRUE, $tempurl);
    $file->setFileUri($tempurl);
    $file->setFilename($filenametemp);
    $file->setSize($size);
    $file->save();
    return $file->id();
  }

  /**
   * @param \Drupal\file\Entity\File $file
   *
   * @return int
   */
  public function csv_count(File $file) {
    $wrapper = $this->streamWrapperManager->getViaUri($file->getFileUri());
    if (!$wrapper) {
      return NULL;
    }

    $url = $wrapper->getUri();
    $spl = new \SplFileObject($url, 'r');
    $spl->setFlags(
      SplFileObject::READ_CSV |
      SplFileObject::READ_AHEAD |
      SplFileObject::SKIP_EMPTY |
      SplFileObject::DROP_NEW_LINE
    );
    $spl->seek(PHP_INT_MAX);
    $key = $spl->key();
    $spl = NULL;
    return $key;
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
  public function amiArrayCombineSpecial($header, $row) {
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
  public function arrayEquallySeize($headercount, $row = []) {

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
  public function getDifferentValuesfromColumn(array $data, int $key): array {
    $unique = [];
    $all = array_column($data['data'], $key);
    $unique = array_map('trim', $all);
    $unique = array_unique($unique, SORT_STRING);
    return $unique;
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
  public function provideDifferentColumnValuesFromCSV(File $file, array $columns):array {
    $data = $this->csv_read($file);
    $column_keys = $data['headers'] ?? [];
    $alldifferent = [];
    foreach ($columns as $column) {
      $column_index = array_search($column, $column_keys);
      if ($column_index !== FALSE) {
        $alldifferent[$column] = $this->getDifferentValuesfromColumnSplit($data,
          $column_index);
      }
    }
    return $alldifferent;
  }



  /**
   * Returns a list Metadata Displays.
   *
   * @return array
   * @throws \Drupal\Component\Plugin\Exception\InvalidPluginDefinitionException
   * @throws \Drupal\Component\Plugin\Exception\PluginNotFoundException
   */
  public function getMetadataDisplays() {

    $result = [];
    $query = $this->entityTypeManager->getStorage('metadatadisplay_entity')
      ->getQuery();

    $metadatadisplay_ids = $query
      ->condition("mimetype", "application/json")
      ->sort('name', 'ASC')
      ->execute();
    if (count($metadatadisplay_ids)) {
      $metadatadisplays = $this->entityTypeManager->getStorage(
        'metadatadisplay_entity'
      )->loadMultiple($metadatadisplay_ids);
      foreach ($metadatadisplays as $id => $metadatadisplay) {
        $result[$id] = $metadatadisplay->label();
      }
    }

    return $result;
  }

  /**
   * Returns WebformOptions marked as Archipelago
   *
   * @return array
   */
  public function getWebformOptions():array {
    try {
    /** @var \Drupal\webform\WebformOptionsInterface[] $webform_options */
    $webform_options  = $this->entityTypeManager->getStorage('webform_options')->loadByProperties(['category' => 'archipelago']);
    $options = [];
    foreach($webform_options as $webform_option) {
      $options = array_merge($options, $webform_option->getOptions());
    }
    }
    catch (\Exception $e) {
      // Return some basic defaults in case there are no Options.
      // @TODO tell the user to create a few.
      $options = ['Document' => 'Document', 'Photograph' => 'Photograph', 'Book' => 'Book', 'Article' => 'Article', 'Thing' => 'Thing', 'Video' => 'Video', 'Audio' => 'Audio'];
    }
    return array_unique($options);
  }



  /**
   * Returns a list of Webforms.
   *
   * @return array
   * @throws \Drupal\Component\Plugin\Exception\InvalidPluginDefinitionException
   * @throws \Drupal\Component\Plugin\Exception\PluginNotFoundException
   */
  public function getWebforms() {

    $result = [];
    $query = $this->entityTypeManager->getStorage('webform')->getQuery();

    $webform_ids = $query
      ->condition("status", "open")
      ->sort('title', 'ASC')
      ->execute();
    if (count($webform_ids)) {
      $webforms = $this->entityTypeManager->getStorage('webform')->loadMultiple(
        $webform_ids
      );
      foreach ($webforms as $id => $webform) {
        /* @var \Drupal\webform\Entity\Webform $webform */
        $handlercollection = $webform->getHandlers(
          'strawberryField_webform_handler', TRUE
        );
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

    $bundles = $this->entityTypeBundleInfo->getBundleInfo('node');
    foreach ($bundles as $bundle => $bundle_info) {
      if ($this->strawberryfieldUtility->bundleHasStrawberryfield($bundle)) {
        $access = $this->checkBundleAccess($bundle);
        if ($access && $access->isAllowed()) {
          foreach ($this->checkFieldAccess($bundle) as $key => $value) {
            $bundle_options[$key] = $value . ' for ' . $bundle_info['label'];
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
   *
   * @return \Drupal\Core\Access\AccessResultInterface|bool
   */
  public function checkBundleAccess(string $bundle, AccountInterface $account = NULL) {
    try {
      $access_handler = $this->entityTypeManager->getAccessControlHandler(
        'node'
      );
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
   * @param $bundle
   * @param \Drupal\Core\Session\AccountInterface|null $account
   *
   * @return array
   *    A list of Fields where Edit access is allowed.
   */
  public function checkFieldAccess($bundle, AccountInterface $account = NULL) {
    $fields = [];
    $access_handler = $this->entityTypeManager->getAccessControlHandler('node');
    $field_definitions = $this->strawberryfieldUtility->getStrawberryfieldDefinitionsForBundle($bundle);

    foreach ($field_definitions as $field_definition) {
      $field_access = $access_handler->fieldAccess(
        'edit', $field_definition, $account, NULL, TRUE
      );
      if ($field_access->isAllowed()) {
        $fields[$bundle . ':' . $field_definition->getName()] = $field_definition->getLabel();
      }
    }
    return $fields;
  }

  /**
   * Creates an AMI Set using a stdClass Object
   *
   * @param \stdClass $data
   *
   * @return int|mixed|string|null
   * @throws \Drupal\Component\Plugin\Exception\InvalidPluginDefinitionException
   * @throws \Drupal\Component\Plugin\Exception\PluginNotFoundException
   */
  public function createAmiSet(\stdClass $data) {
    // See \Drupal\ami\Entity\amiSetEntity
    $current_user_name = $this->currentUser->getDisplayName();
    $set = [
      'mapping' => $data->mapping,
      'adomapping' => $data->adomapping,
      'zip' => $data->zip,
      'csv' => $data->csv,
      'plugin' => $data->plugin,
      'pluginconfig' => $data->pluginconfig,
      'column_keys' => $data->column_keys,
      'total_rows' => $data->total_rows,
    ];
    $zipfail = FALSE;
    $name = $data->name ?? 'AMI Set of ' . $current_user_name;
    $jsonvalue = json_encode($set, JSON_PRETTY_PRINT);
    /* @var \Drupal\ami\Entity\amiSetEntity $entity */
    $entity = $this->entityTypeManager->getStorage('ami_set_entity')->create(
      ['name' => $name]
    );
    $entity->set('set', $jsonvalue);
    $entity->set('source_data', [$data->csv]);
    $entity->set('zip_file', [$data->zip]);
    $entity->set('status', 'ready');
    try {
      $result = $entity->save();
      // Now ensure we move the Zip file if any to private
      if ($this->streamWrapperManager->isValidScheme('private') && $data->zip) {
        $target_directory = 'private://ami/zip';
        // Ensure the directory
        if (!$this->fileSystem->prepareDirectory(
          $target_directory,
          FileSystemInterface::CREATE_DIRECTORY
          | FileSystemInterface::MODIFY_PERMISSIONS
        )) {
          $zipfail = TRUE;
        }
        else {
          $zipfile = $this->entityTypeManager->getStorage('file')
            ->load($data->zip);
          if (!$zipfile) {
            $zipfail = TRUE;
          } else {
            $zipfile = file_move($zipfile, $target_directory, FileSystemInterface::EXISTS_REPLACE);
            if (!$zipfile) {
              $zipfail = TRUE;
            }
          }
        }
      }
      if ($zipfail) {
        $this->messenger()->addError(
          $this->t(
            'ZIP file attached to Ami Set entity could not be moved to temporary storage. Please check with your system admin if you have permissions.'
          ));
      }
    }
    catch (\Exception $exception) {
      $this->messenger()->addError(
        $this->t(
          'Ami Set entity Failed to be persisted because of @message',
          ['@message' => $exception->getMessage()]
        )
      );
      return NULL;
    }
    if ($result == SAVED_NEW) {
      return $entity->id();
    }
  }

  /**
   * Processes rows and assigns correct parents and UUIDs
   *
   * @param \Drupal\file\Entity\File $file
   *   A CSV
   * @param \stdClass $data
   *     The AMI Set Config data
   * @param array $invalid
   *    Keeps track of invalid rows.
   * @param bool $strict
   *    TRUE means Set Config and CSV will be strictly validated,
   *    FALSE means it will just validated for the needed elements
   *
   * @return array
   * @throws \Drupal\Component\Plugin\Exception\InvalidPluginDefinitionException
   * @throws \Drupal\Component\Plugin\Exception\PluginNotFoundException
   */
  public function preprocessAmiSet(File $file, \stdClass $data, array &$invalid = [], $strict = FALSE): array {

    $file_data_all = $this->csv_read($file);
    // We want to validate here if the found Headers match at least the
    // Mapped ones during AMI setup. If not we will return an empty array
    // And send a Message to the user.
    if (!$this->validateAmiSet($file_data_all, $data, $strict)) {
      return [];
    }

    $config['data']['headers'] = $file_data_all['headers'];
    // In old times we totally depended on position, now we are going to do something different, we will combine
    // Headers and keys.
    $data->mapping->type_key = isset($data->mapping->type_key)
      ? $data->mapping->type_key : 'type';

    // Keeps track of all parents and child that don't have a PID assigned.
    $parent_hash = [];
    $info = [];

    foreach ($file_data_all['data'] as $index => $keyedrow) {
      // This makes tracking of values more consistent and easier for the actual processing via
      // twig templates, webforms or direct
      $row = array_combine($config['data']['headers'], $keyedrow);
      // Each row will be an object.
      $ado = [];
      $ado['type'] = trim(
        $row[$data->mapping->type_key] ?? 'Thing'
      );
      // Lets start by grouping by parents, namespaces and generate uuids
      // namespaces are inherited, so we just need to find collection
      // objects in parent uuid column.

      // We may have multiple parents
      // @dmer deal with files as parents of a node in a next iteration.
      // So, we will here simply track also any parent
      // Initialize in case the Mapping provides no parents
      $ado['anyparent'] = [];
      $ado['parent'] = [];
      foreach ($data->adomapping->parents as $parent_key) {
        // Used to access parent columns using numerical indexes for when looking back inside $file_data_all
        $parent_to_index[$parent_key] = array_search(
          $parent_key, $config['data']['headers']
        );

        $ado['parent'][$parent_key] = trim(
          $row[$parent_key]
        );
        $ado['anyparent'][] = $row[$parent_key];
      }

      $ado['data'] = $row;

      // UUIDs should be already assigned by this time
      $possibleUUID = $row[$data->adomapping->uuid->uuid] ?? NULL;
      $possibleUUID = $possibleUUID ? trim($possibleUUID) : $possibleUUID;
      // Double check? User may be tricking us!
      if ($possibleUUID && Uuid::isValid($possibleUUID)) {
        $ado['uuid'] = $possibleUUID;
        // Now be more strict for action = update/patch
        if ($data->pluginconfig->op !== 'create') {
          $existing_objects = $this->entityTypeManager->getStorage('node')
            ->loadByProperties(['uuid' => $ado['uuid']]);
          // Do access control here, will be done again during the atomic operation
          // In case access changes later of course
          // Processors do NOT delete. So we only check for Update.
          $existing_object = $existing_objects && count($existing_objects) == 1 ? reset($existing_objects) : NULL;
          if (!$existing_object || !$existing_object->access('update')) {
            unset($ado);
            $invalid = $invalid + [$index => $index];
          }
        }
      }
      if (!isset($ado['uuid'])) {
        unset($ado);
        $invalid = $invalid + [$index => $index];
      }

      if (isset($ado)) {
        // CHECK. Different to IMI. we have multiple relationships
        foreach ($ado['parent'] as $parent_key => $parent_ado) {
          // Discard further processing of empty parents
          if (strlen(trim($parent_ado)) == 0) {
            // Empty parent;
            continue;
          }

          if (!Uuid::isValid($parent_ado) && (intval(trim($parent_ado)) > 1)) {
            // Its a row
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

            // Lets check if the index actually exists before going crazy.

            // If parent is empty that is OK here. WE are Ok with no membership!
            if (!isset($file_data_all['data'][$parent_numeric])) {
              // Parent row does not exist
              $invalid[$parent_numeric] = $parent_numeric;
              $invalid[$index] = $index;
            }

            if ((!isset($invalid[$index])) && (!isset($invalid[$parent_numeric]))) {
              // Only traverse if we don't have this index or the parent one
              // in the invalid register.
              $parentchilds = [];
              while (!$rootfound) {
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
                  // In a loop
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


    // Now the real pass, iterate over every row.

    foreach ($info as $index => &$ado) {
      foreach ($data->adomapping->parents as $parent_key) {
        // Is this object parent of someone?
        if (isset($parent_hash[$parent_key][$ado['parent'][$parent_key]])) {
          $ado['parent'][$parent_key] = $info[$ado['parent'][$parent_key]]['uuid'];
        }
      }
      // Since we are reodering we may want to keep the original row_id around
      // To help users debug which row has issues in case of ingest errors
      $ado['row_id'] = $index;
    }
    // Now reoder, add parents first then the rest.
    $newinfo = [];

    // parent hash contains keys with all the properties and then keys with parent
    //rows and child arrays with their children
    /* E.g
    array:2 [▼
      "ismemberof" => array:2 [▼
        3 => array:3 [▼
          4 => 4
          6 => 6
          7 => 7
        ]
        7 => array:3 [▼
          8 => 8
          9 => 9
         10 => 10
        ]
      ]
      "partof" => array:1 [▼
        10 => array:2 [▼
          11 => 11
          12 => 12
        ]
      ]
    ]
     */
    // This way the move parent Objects first and leave children to the end.
    foreach ($parent_hash as $parent_tree) {
      foreach ($parent_tree as $row_id => $children) {
        // There could be a reference to a non existing index.
        if (isset($info[$row_id])) {
          $newinfo[] = $info[$row_id];
          unset($info[$row_id]);
        }
        else {
          // Unset Invalid index if the row never existed
          unset($invalid[$row_id]);
        }
      }
    }
    $newinfo = array_merge($newinfo, $info);
    unset($info);
    // @TODO Should we do a final check here? Alert the user the rows are less/equal to the desired?
    return $newinfo;
  }


  /**
   * Validates an AMI Set Config and its data extracted from the CSV
   *
   * @param array $file_data_all
   *    The data present in the CSV as an array.
   * @param \stdClass $data
   *    The AMI config data as passed by the AMI set
   * @param bool $strict
   *    Strict means the CSV headers need to match 1:1 with the config,
   *    Not only the mappings. This will be used for unattended ingests.
   *
   * @return bool
   *    FALSE it important header elements, mappings are missing and or
   *    data empty.
   */
  protected function validateAmiSet(array $file_data_all, \stdClass $data, $strict = FALSE ):bool {
    $valid = is_object($data->adomapping->base);
    $valid = $valid && is_object($data->adomapping->uuid);
    // Parents may be empty. Not required?
    $valid = $valid && (is_object($data->adomapping->parents) || is_array($data->adomapping->parents));
    $valid = $valid && isset($data->pluginconfig->op) && is_string($data->pluginconfig->op);
    $valid = $valid && $file_data_all && count($file_data_all['headers']);
    $valid = $valid && (!$strict || (is_array($data->column_keys) && count($data->column_keys)));
    $valid = $valid && in_array($data->pluginconfig->op, ['create', 'update', 'patch']);
    if ($valid) {
      $required_headers = array_values((array)$data->adomapping->base);
      $required_headers = array_merge($required_headers, array_values((array)$data->adomapping->uuid));
      $required_headers = array_merge($required_headers, array_values((array)$data->adomapping->parents));
      if ($strict) {
        // Normally column_keys will always contain also the ones in adomapping
        // But safer to check both in case someone manually edited the set.
        $required_headers = array_merge($required_headers, array_values((array)$data->column_keys));
      }
      // We use internally Lower case Headers.
      $required_headers = array_map('strtolower', $required_headers);
      $headers_missing = array_diff(array_unique($required_headers), $file_data_all['headers']);
      if (count($headers_missing)) {
        $message = $this->t(
          'Your CSV has the following important header (first row) column names missing: <em>@keys</em>. Please correct. Cancelling Processing.',
          [
            '@keys' => implode(',', $headers_missing)
          ]
        );
        $this->messenger()->addError($message);
        return FALSE;
      }
    }
    else {
      $message = $this->t(
        'Your AMI Set has invalid/missing/incomplete settings or CSV data. Please check, correct or create a new one via the "Create AMI Set" Form. Cancelling Processing.'
      );
      $this->messenger()->addError($message);
      return FALSE;
    }
    return TRUE;
  }


  /**
   * Returns UUIDs for AMI data the user has permissions to operate on.
   *
   * @param \Drupal\file\Entity\File $file
   * @param \stdClass $data
   *
   * @param null|string $op
   *
   * @return mixed
   * @throws \Drupal\Component\Plugin\Exception\InvalidPluginDefinitionException
   * @throws \Drupal\Component\Plugin\Exception\PluginNotFoundException
   */
  public function getProcessedAmiSetNodeUUids(File $file, \stdClass $data, $op = NULL) {

    $file_data_all = $this->csv_read($file);
    // we may want to check if saved metadata headers == csv ones first.
    // $data->column_keys
    $config['data']['headers'] = $file_data_all['headers'];
    $uuids = [];
    foreach ($file_data_all['data'] as $index => $keyedrow) {
      // This makes tracking of values more consistent and easier for the actual processing via
      // twig templates, webforms or direct
      $row = array_combine($config['data']['headers'], $keyedrow);
      $possibleUUID = $row[$data->adomapping->uuid->uuid] ?? NULL;
      $possibleUUID = $possibleUUID ? trim($possibleUUID) : $possibleUUID;
      // Double check? User may be tricking us!
      if ($possibleUUID && Uuid::isValid($possibleUUID)) {
        if ($op !== 'create' && $op !== NULL) {
          $existing_objects = $this->entityTypeManager->getStorage('node')
            ->loadByProperties(['uuid' => $possibleUUID]);
          // Do access control here, will be done again during the atomic operation
          // In case access changes later of course
          // This does NOT delete. So we only check for Update.
          $existing_object = $existing_objects && count($existing_objects) == 1 ? reset($existing_objects) : NULL;
          if ($existing_object && $existing_object->access($op)) {
            $uuids[] = $possibleUUID;
          }
        }
        else {
          $uuids[] = $possibleUUID;
        }
      }
    }
    return $uuids;
  }


  /**
   * Super simple callback to check if in a CSV our parent is the actual root
   * element
   *
   * @param string $parent
   *
   * @return bool
   */
  protected function isRootParent(string $parent) {
    return (Uuid::isValid(trim($parent)) || strlen(trim($parent)) == 0);
  }


  /**
   * This function tries to decode any string that may be a valid JSON.
   *
   * @param array $row
   *
   * @return array
   */
  public function expandJson(array $row) {
    foreach ($row as $colum => &$value) {
      $expanded = json_decode($value, TRUE);
      $json_error = json_last_error();
      // WE ignore JSON errors since simple strings or arrays are not valid
      // JSON STRINGs and its OK if we do not decode them.
      if ($json_error == JSON_ERROR_NONE) {
        $value = $expanded;
      }
      else {
        // Check if this may even be a JSON someone messed up

        if (is_string($value) &&
          (
            (strpos(ltrim($value), '{') === 0) ||
            (strpos(ltrim($value), '[') === 0)
          )
        ) {
          // Ok, it actually starts with a {}
          // try to clean up.
          $quotes = array(
            "\xC2\xAB"     => '"', // « (U+00AB) in UTF-8
            "\xC2\xBB"     => '"', // » (U+00BB) in UTF-8
            "\xE2\x80\x98" => "'", // ‘ (U+2018) in UTF-8
            "\xE2\x80\x99" => "'", // ’ (U+2019) in UTF-8
            "\xE2\x80\x9A" => "'", // ‚ (U+201A) in UTF-8
            "\xE2\x80\x9B" => "'", // ‛ (U+201B) in UTF-8
            "\xE2\x80\x9C" => '"', // “ (U+201C) in UTF-8
            "\xE2\x80\x9D" => '"', // ” (U+201D) in UTF-8
            "\xE2\x80\x9E" => '"', // „ (U+201E) in UTF-8
            "\xE2\x80\x9F" => '"', // ‟ (U+201F) in UTF-8
            "\xE2\x80\xB9" => "'", // ‹ (U+2039) in UTF-8
            "\xE2\x80\xBA" => "'", // › (U+203A) in UTF-8
          );
          $possiblejson  = strtr($value, $quotes);
          $expanded = json_decode($possiblejson, TRUE);
          $json_error = json_last_error();
          // Last chance to be JSON. Allison says e.g EDTF may start with []
          // So do not nullify. Simply keep the mess.
          if ($json_error == JSON_ERROR_NONE) {
            $value = $expanded;
          }
          elseif (substr( $colum, 0, 3 ) === "as:" ||
            substr( $colum, 0, 3 ) === "ap:"
          ) {
            // We can not allow wrong JSON to permeate into controlled
            // by us properties
            // @TODO apply a JSON Schema validator at the end.
            $value = NULL;
          }
        }
      }
    }
    return $row;
  }

  /**
   * Processes Data through a Metadata Display Entity.
   *
   * @param \stdClass $data
   *      $data->info['row']
   *      has the following format
   *      [
   *      "type" => "Book"
   *      "parent" =>  [
   *      "partof" => ""
   *      "ismemberof" => "1808b621-0831-4dd3-8126-19085fefcd77"
   *      ],
   *      "anyparent" =>
   *      "data" => [
   *      "node_uuid" => "5d4f8ed7-7471-4115-beed-39dc1e625180"
   *      "type" => "Book"
   *      "label" => "Batch Ingested Book parent of existing collection"
   *      "subject_loc" => "Public Libraries"
   *      "audios" => ""
   *      "videos" => ""
   *      "images" => ""
   *      "documents" => ""
   *      ],
   *      "uuid" => "5d4f8ed7-7471-4115-beed-39dc1e625180"
   *      ]
   *
   * @param array $additional_context
   *    Any additional $context that may be passed. This is appended to the
   *    Twig context but will never replace/override the one provided
   *    by this method.
   *
   * @return string|NULL
   *    Either a valid JSON String or NULL if casting via Twig template failed
   *
   * @throws \Drupal\Component\Plugin\Exception\InvalidPluginDefinitionException
   * @throws \Drupal\Component\Plugin\Exception\PluginNotFoundException
   */
  public function processMetadataDisplay(\stdClass $data, array $additional_context = []) {
    $op = $data->pluginconfig->op;
    $ophuman = [
      'create' => 'created',
      'update' => 'updated',
      'patch' => 'patched',
    ];

    $row_id = $data->info['row']['row_id'];
    $set_id = $data->info['set_id'];
    $setURL = $data->info['set_url'];
    // Should never happen but better stop processing here
    if (!$data->info['row']['data']) {
      $message = $this->t(
        'Empty or Null Data Row. Skipping for AMI Set ID @setid, Row @row, future ADO with UUID @uuid.',
        [
          '@uuid' => $data->info['row']['uuid'],
          '@row' => $row_id,
          '@setid' => $set_id,
        ]
      );
      $this->loggerFactory->get('ami')->error($message);
      return NULL;
    }
    $jsonstring = NULL;
    if ($data->mapping->globalmapping == "custom") {
      $metadatadisplay_id = $data->mapping->custommapping_settings->{$data->info['row']['type']}->metadata_config->template ?? NULL;
    }
    else {
      $metadatadisplay_id = $data->mapping->globalmapping_settings->metadata_config->template ?? NULL;
    }
    if (!$metadatadisplay_id) {
      if (!$data->info['row']['data']) {
        $message = $this->t(
          'Ups. No template mapping for type @type. Skipping for AMI Set ID @setid, Row @row, future ADO with UUID @uuid.',
          [
            '@uuid' => $data->info['row']['uuid'],
            '@row' => $row_id,
            '@setid' => $set_id,
            '@type' => $data->info['row']['type'],
          ]
        );
        $this->loggerFactory->get('ami')->error($message);
        return NULL;
      }
    }


    $metadatadisplay_entity = $this->entityTypeManager->getStorage('metadatadisplay_entity')
      ->load($metadatadisplay_id);
    if ($metadatadisplay_entity) {
      $node = NULL;
      $original_value = NULL;
      // Deal with passing current to be updated data as context to the template.
      if ($op == 'update' || $op == 'patch') {
        /** @var \Drupal\Core\Entity\ContentEntityInterface[] $existing */
        $existing = $this->entityTypeManager->getStorage('node')
          ->loadByProperties(
            ['uuid' => $data->info['row']['uuid']]
          );

        if (!count($existing) == 1) {
          $this->messenger()->addError($this->t('Sorry, the ADO with UUID @uuid you requested to be @ophuman via Set @setid does not exist. Skipping',
            [
              '@uuid' => $data->info['row']['uuid'],
              '@setid' => $set_id,
              '@ophuman' => $ophuman[$op],
            ]));
          return NULL;
        }

        $account = $data->info['uid'] == \Drupal::currentUser()
          ->id() ? \Drupal::currentUser() : $this->entityTypeManager->getStorage('user')
          ->load($data->info['uid']);

        if ($account) {
          $existing_object = reset($existing);
          if (!$existing_object->access('update', $account)) {
            $this->messenger()->addError($this->t('Sorry you have no system permission to @ophuman ADO with UUID @uuid via Set @setid. Skipping',
              [
                '@uuid' => $data->info['row']['uuid'],
                '@setid' => $set_id,
                '@ophuman' => $ophuman[$op],
              ]));
            return NULL;
          }

          $vid = $this->entityTypeManager
            ->getStorage('node')
            ->getLatestRevisionId($existing_object->id());

          $node = $vid ? $this->entityTypeManager->getStorage('node')
            ->loadRevision($vid) : $existing[0];

          if ($data->mapping->globalmapping == "custom") {
            $property_path = $data->mapping->custommapping_settings->{$data->info['row']['type']}->bundle ?? NULL;
          }
          else {
            $property_path = $data->mapping->globalmapping_settings->bundle ?? NULL;
          }
          $property_path_split = explode(':', $property_path);
          if (!$property_path_split || count($property_path_split) < 2) {
            $this->messenger()->addError($this->t('Sorry, your Bundle/Fields set for the requested an ADO with @uuid on Set @setid are wrong. You may have made a larger change in your repo and deleted a Content Type. Aborting.',
              [
                '@uuid' => $data->info['row']['uuid'],
                '@setid' => $data->info['set_id']
              ]));
            return NULL;
          }

          $field_name = $property_path_split[1];
          // @TODO make this configurable.
          // This allows us not to pass an offset if the SBF is multivalued.
          // WE do not do this, Why would you want that? Who knows but possible.
          $field_name_offset = $property_path_split[2] ?? 0;
          /** @var \Drupal\Core\Field\FieldItemInterface $field */
          $field = $node->get($field_name);
          /** @var \Drupal\strawberryfield\Field\StrawberryFieldItemList $field */
          if (!$field->isEmpty()) {
            /** @var $field \Drupal\Core\Field\FieldItemList */
            foreach ($field->getIterator() as $delta => $itemfield) {
              /** @var \Drupal\strawberryfield\Plugin\Field\FieldType\StrawberryFieldItem $itemfield */
              if ($field_name_offset == $delta) {
                $original_value = $itemfield->provideDecoded(TRUE);
                break;
              }
            }
          }
        }
      }

      $context['data'] = $this->expandJson($data->info['row']['data']);
      $context_lod = [];
      // get the mappings for this set if any
      // @TODO Refactor into a Method?
      $lod_mappings = $this->AmiLoDService->getKeyValueMappingsPerAmiSet($set_id);
      if ($lod_mappings) {
        foreach($lod_mappings as $source_column => $destination) {
          if (isset($context['data'][$source_column])) {
            // sad here. Ok, this is a work around for our normally
            // Strange CSV data structure
            $data_to_clean['data'][0] = [$context['data'][$source_column]];
            $labels = $this->getDifferentValuesfromColumnSplit($data_to_clean,
              0);
            foreach($labels as $label) {
              $lod_for_label = $this->AmiLoDService->getKeyValuePerAmiSet($label, $set_id);
              if (is_array($lod_for_label) && count($lod_for_label) > 0) {
                foreach ($lod_for_label as $approach => $lod) {
                  if (isset($lod['lod'])) {
                    $context_lod[$source_column][$approach] = array_merge($context_lod[$source_column][$approach] ?? [], $lod['lod']);
                  }
                }
              }
            }
          }
        }
      }

      $context['data_lod'] = $context_lod;
      $context['dataOriginal'] = $original_value;
      $context['setURL'] = $setURL;
      $context['setId'] = $set_id;
      $context['rowId'] = $row_id;
      $context['setOp'] = ucfirst($op);

      $context['node'] = $node;
      // Add any extras passed to the caller.
      $context = $context + $additional_context;
      $original_context = $context;
      // Allow other modules to provide extra Context!
      // Call modules that implement the hook, and let them add items.
      \Drupal::moduleHandler()
        ->alter('format_strawberryfield_twigcontext', $context);
      $context = $context + $original_context;
      $cacheabledata = [];
      // @see https://www.drupal.org/node/2638686 to understand
      // What cacheable, Bubbleable metadata and early rendering means.
      $cacheabledata = \Drupal::service('renderer')->executeInRenderContext(
        new RenderContext(),
        function () use ($context, $metadatadisplay_entity) {
          return $metadatadisplay_entity->renderNative($context);
        }
      );
      if (count($cacheabledata)) {
        $jsonstring = $cacheabledata->__toString();
        $jsondata = json_decode($jsonstring, TRUE);
        $json_error = json_last_error();
        // Just because i like to clean up memory.
        unset($jsondata);
        if ($json_error != JSON_ERROR_NONE) {
          $message = $this->t(
            'We could not generate JSON via Metadata Display with ID @metadatadisplayid for AMI Set ID @setid, Row @row, future ADO with UUID @uuid. This is the Template %output',
            [
              '@metadatadisplayid' => $metadatadisplay_id,
              '@uuid' => $data->info['row']['uuid'],
              '@row' => $row_id,
              '@setid' => $set_id,
              '%output' => $jsonstring,
            ]
          );
          $this->loggerFactory->get('ami')->error($message);
          return NULL;
        }
      }
    }
    else {
      $message = $this->t(
        'Metadata Display with ID @metadatadisplayid could not be found for AMI Set ID @setid, Row @row for a future node with UUID @uuid.',
        [
          '@metadatadisplayid' => $metadatadisplay_id,
          '@uuid' => $data->info['row']['uuid'],
          '@row' => $row_id,
          '@setid' => $set_id,
        ]
      );
      $this->loggerFactory->get('ami')->error($message);
      return NULL;
    }
    return $jsonstring;
  }

  /**
   * For a given Numeric Column index, get different/non json, split values
   *
   * @param array $data
   * @param int $key
   *
   * @param array $delimiters
   *
   * @return array
   */
  public function getDifferentValuesfromColumnSplit(array $data, int $key, array $delimiters = ['|@|', ';'] ): array {
    $unique = [];
    $all = array_column($data['data'], $key);
    $all_notJson = array_filter($all,  array($this, 'isNotJson'));
    $all_entries = [];
    // The difficulty. In case of multiple delimiters we need to see which one
    // works/works better. But if none, assume it may be also right since a single
    // Value is valid. So we need to accumulate, count and discern
    foreach ($all_notJson as $entries) {
      $current_entries = [];
      foreach ($delimiters as $delimiter) {
        $split_entries = explode($delimiter, $entries) ?? [];
        $current_entries[$delimiter] = (array) $split_entries;
      }
      $chosen_entries = [];
      foreach ($current_entries as $delimiter => $current_entry) {
        $chosen_entries = $current_entry;
        if (count($chosen_entries) > 1) {
          break;
        }
      }
      foreach ($chosen_entries as $chosen_entry) {
        $all_entries[] = $chosen_entry;
      }
    }
    $unique = array_map('trim', $all_entries);
    $unique = array_unique(array_values($unique), SORT_STRING);
    return $unique;
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

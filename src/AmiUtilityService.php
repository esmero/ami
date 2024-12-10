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
use Drupal\Core\Entity\EntityInterface;
use Drupal\Core\Entity\EntityTypeBundleInfoInterface;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use \Drupal\Core\Entity\EntityFieldManagerInterface;
use Drupal\Core\Extension\ModuleHandlerInterface;
use Drupal\Core\File\Exception\FileWriteException;
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
use Drupal\file\FileInterface;
use Drupal\file\FileUsage\FileUsageInterface;
use Drupal\strawberryfield\StrawberryfieldFileMetadataService;
use Drupal\strawberryfield\StrawberryfieldFilePersisterService;
use Drupal\strawberryfield\Tools\StrawberryfieldJsonHelper;
use GuzzleHttp\ClientInterface;
use Drupal\strawberryfield\StrawberryfieldUtilityService;
use Ramsey\Uuid\Uuid;
use Drupal\Core\File\Exception\FileException;
use SplFileObject;
use Drupal\Core\File\Exception\InvalidStreamWrapperException;
use Drupal\Core\File\FileExists;

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
   * @var \Drupal\Core\Config\ConfigFactoryInterface
   */
  private ConfigFactoryInterface $config_factory;

  /**
   * @var \Drupal\strawberryfield\StrawberryfieldFilePersisterService
   */
  private StrawberryfieldFilePersisterService $strawberryfieldFilePersisterService;

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
   * @param \Drupal\ami\AmiLoDService $ami_lod
   * @param \Drupal\Core\KeyValueStore\KeyValueFactoryInterface $key_value
   * @param \Drupal\strawberryfield\StrawberryfieldFileMetadataService $strawberryfield_file_metadata_service
   * @param \Drupal\strawberryfield\StrawberryfieldFilePersisterService $strawberryfield_file_persister_service
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
    StrawberryfieldFileMetadataService $strawberryfield_file_metadata_service,
    StrawberryfieldFilePersisterService $strawberryfield_file_persister_service
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
    $this->config_factory = $config_factory;
    $this->strawberryfieldFilePersisterService = $strawberryfield_file_persister_service;
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
  public function file_get($uri, File $zip_file = NULL, $force = FALSE) {
    $uri = trim($uri);

    $parsed_url = parse_url($uri);
    $remote_schemes = ['http', 'https', 'feed'];
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
      // normalize targetx

      $scheme = $this->streamWrapperManager->getScheme($uri);
      if ($scheme) {
        // Try also with our internal S3 check-if-its-there-function
        if (!file_exists($uri) && !$this->strawberryfieldFilePersisterService->fileS3Exists($uri)) {
          return FALSE;
        }
        $finaluri = $uri;
      }
      else {
        // Means it may be local to the accessible file storage, eg. a path inside
        // the server or inside a provided ZIP file
        $localfile = $this->fileSystem->realpath($uri);

        if (!$localfile && !$zip_file) {
          return FALSE;
        }
        elseif ($localfile) {
          // We can not allow DIRS. C'mon
          if (is_dir($localfile)) {
            return FALSE;
          }
          // Means the file is there already locally. Just assign.
          $finaluri = $localfile;
        }
        elseif (!$localfile && $zip_file) {
          // Means no local file but we can check inside a ZIP.
          // Try with the ZIP file in case there is a ZIP and local failed
          // Use the Zip file uuid to prefix the destination.
          $localfile = $this->streamWrapperManager->normalizeUri(
            $destination . $zip_file->uuid() . '/' . urldecode($parsed_url['path'])
          );
          if (!file_exists($localfile) || $force) {
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
              FileExists::Replace, $zip_file);
          }
        }
        $finaluri = $localfile;
      }
    }
    else {
      // This may be remote!
      // Simulate what could be the final path of a remote download.
      // to avoid re downloading.

      $md5uri = md5($uri);
      $destination = $destination . $md5uri . '/' ;
      $path = str_replace(
          '///',
          '//',
          "{$destination}"
        ) . $this->fileSystem->basename(urldecode($parsed_url['path']));
      $localfile = $this->streamWrapperManager->normalizeUri($path);
      $escaped_path = str_replace(' ', '\ ', $path);
      // This is very naive since the remote file might be different than
      // the last part of the actual URL (header given name).
      $isthere = glob($this->fileSystem->realpath($escaped_path) . '.*');
      $isthere = is_array($isthere) && (count($isthere) == 1) ? $isthere : glob($this->fileSystem->realpath($escaped_path));

      if (is_array($isthere) && count($isthere) == 1) {
        // Ups its here
        // Use path here instead of the first entry to keep the streamwrapper
        // around for future use
        $localfile = $path;
      }
      // Actual remote heavy lifting only if not present.
      if (!file_exists($localfile) || $force) {
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
          $destination
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
   *
   * @return mixed
   *   One of these possibilities:
   *   - If it succeeds, a managed file object
   *   - If it fails, FALSE.
   */
  public function retrieve_remote_file(
    $uri,
    $destination = NULL
  ) {
    // pre set a failure
    $parsed_url = parse_url($uri);
    $basename = $this->fileSystem->basename(urldecode($parsed_url['path']));
    if (!isset($destination)) {
      $basename = \Drupal::config('system.file')->get('default_scheme') . '://' . $basename;
      $path = $this->streamWrapperManager->normalizeUri($basename);
    }
    else {
      if (is_dir($this->fileSystem->realpath($destination))) {
        // Prevent URIs with triple slashes when glue-ing parts together.
        $path = str_replace(
            '///',
            '//',
            "{$destination}"
          ) . $basename;
      }
      else {
        $path = $destination;
        $basename = $this->fileSystem->basename($destination);
      }
    }
    /* @var \Psr\Http\Message\ResponseInterface $response */
    $max_time = (int) ini_get('max_execution_time') * 0.75;
    try {
      // @TODO make 75% or any percentage a global config
      // @TODO edge case someone set max execution time to 0, but timeout on client
      // Might need to be a number .. not 0.
      // @TODO add to global config
      if ($max_time == 0) {
        $max_time = 720.00;
      }
      // Do a HEAD request first. Be sure we don't have anything in the 4XX or 5xx range
      $head = $this->httpClient->head($uri, ['timeout' => round($max_time,2)]);
      // Note. This will never run per se. Because the client is setup to throw an exception, but...
      // If Drupal changes the client base setup in the future we won't know and we won't catch it
      // So keeping it.
      if ($head->getStatusCode() >= 400) {
        return FALSE;
      }
      $response = $this->httpClient->get($uri, ['sink' => $path, 'timeout' => round($max_time,2)]);
      // Edge case... in a fraction of time, someone closes the file from the remote source. We can still cancel
      // Same as with the HEAD. In this current setup this won't run and that is ok, the catch deals with it.
      if ($response->getStatusCode() >= 400) {
        if (file_exists($path)) {
          @unlink($path);
        }
        return FALSE;
      }

      $filename_from_remote = $basename;
      $filename_from_remote_without_extension = pathinfo($filename_from_remote, PATHINFO_FILENAME);
      $extensions_from_remote = pathinfo($filename_from_remote, PATHINFO_EXTENSION);
      $extension_from_mime = NULL;
      $extension = NULL;
      $content_disposition = $response->getHeader('Content-Disposition');
      if (!empty($content_disposition)) {
        $filename_from_remote = $this->getFilenameFromDisposition($content_disposition[0]);
        if ($filename_from_remote) {
          // we want the name without extension, we do not trust the extension
          // See remote sources with double extension!
          $filename_from_remote_without_extension = pathinfo($filename_from_remote, PATHINFO_FILENAME);
          $extensions_from_remote = pathinfo($filename_from_remote, PATHINFO_EXTENSION);
        }
      }
      $extensions_from_remote = !empty($extensions_from_remote) ? $extensions_from_remote : NULL;
      $filename_from_remote_without_extension = !empty($filename_from_remote_without_extension) ? $filename_from_remote_without_extension : NULL;
      // remove any leading dots from here. The original Path (because it is calculated based on the URL) will not contain these
      if ($filename_from_remote_without_extension) {
        // remove any spaces from the start and end
        $filename_from_remote_without_extension = trim($filename_from_remote_without_extension);
        // now remove and leading dots + spaces that might come after the dots
        $filename_from_remote_without_extension = preg_replace('/^(\.|\h)*/m', '', $filename_from_remote_without_extension);
        // if the regular expression fails OR the filename was just dots (like i have seen it all - the song -) use the original sink path
        $filename_from_remote_without_extension = !empty($filename_from_remote_without_extension)
        && (strlen($filename_from_remote_without_extension) > 0) ? $filename_from_remote_without_extension : NULL;
      }
    }
    catch (\Exception $exception) {
      // Deals with 4xx and 5xx too.
      $message_vars = [
        '@uri' => $uri,
        '@path' => $path,
        '@error' => $exception->getMessage(),
        '@time' => $max_time,
        '@code' => $exception->getCode()
      ];
      $message = 'Unable to download remote file from @uri to local @path with HTTP code @code and error: @error. Verify URL exists, file can be downloaded in @time seconds, its openly accessible and destination is writable.';
      $this->loggerFactory->get('ami')->error($message, $message_vars);
      // in case the sink did download the file/we delete it here.
      if (file_exists($path)) {
        @unlink($path);
      }
      return FALSE;
    }

    // First try with Remote Headers to get a valid extension
    if (!empty($response->getHeader('Content-Type'))) {
      $mimetype = $response->getHeader('Content-Type');
      if (count($mimetype) > 0) {
        // Happens that text files can have ALSO a charset, so mime can be
        // joined by a; like text/vtt;charset=UTF-8
        $mimetype_array = explode(";", $mimetype[0]);
        if ($mimetype_array) {
          //Exceptions for "some" remote sources that might provide/non canonical mimetypes
          if ($mimetype_array[0] == 'image/jpg') {
            $mimetype_array[0] = 'image/jpeg';
          }
          if ($mimetype_array[0] == 'image/tif') {
            $mimetype_array[0] = 'image/tiff';
          }
          if ($mimetype_array[0] == "audio/vnd.wave") {
            $mimetype_array[0] = "audio/x-wave";
          }

          $mimefromextension = NULL;
          if ($extensions_from_remote) {
            $mimefromextension = \Drupal::service(
              'strawberryfield.mime_type.guesser.mime'
            )
              ->guessMimeType($filename_from_remote ?? $path);
          }

          if (count($mimetype_array) && ($mimefromextension == NULL || $mimefromextension != $mimetype_array[0]) && ($mimetype_array[0] != 'application/octet-stream')) {
            $extension = \Drupal::service(
              'strawberryfield.mime_type.guesser.mime'
            )
              ->inverseguess($mimetype_array[0]);
          }
          else {
            $extension = $extensions_from_remote;
          }
        }
      }
    }
    // If none try with the filename either from remote (if set) of from the download path
    if (!$extension || $extension == 'bin'){
      $mimefromextension = \Drupal::service('strawberryfield.mime_type.guesser.mime')
        ->guessMimeType($filename_from_remote ?? $path);
      if (($mimefromextension !== "application/octet-stream")) {
        $extension = $extensions_from_remote ?? 'bin';
      }
    }
    if ($filename_from_remote_without_extension) {
      $newpath = $this->fileSystem->dirname($path) . '/' . $filename_from_remote_without_extension . '.' . $extension;
      if ($newpath != $path) {
        $status = @rename($path, $newpath);
        if ($status === FALSE && !file_exists($newpath)) {
          $message_vars =  [
            '@path' => $path,
            '@newpath' => $newpath,
          ];
          $message = 'Unable to rename downloaded file from @path to local @newpath. Verify if destination is writable.';
          $this->messenger()->addError($this->t($message, $message_vars));
          $this->loggerFactory->get('ami')->error($message, $message_vars);
          return FALSE;
        }
        $localfile = $newpath;
      }
      else {
        $localfile = $path;
      }
    }
    else {
      $localfile = $path;
    }

    if (file_exists($localfile)) {
      return $localfile;
    }
    else {
      return FALSE;
    }
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
   *   - If it fails or NULL, FALSE.
   */
  public function retrieve_fromzip_file($uri, $destination = NULL, $replace = FileExists::Rename, File $zip_file = NULL) {
    if (!$zip_file) {
      return FALSE;
    }
    $zip_realpath = NULL;
    $parsed_url = parse_url($uri);
    if (!isset($destination)) {
      $basename = $this->fileSystem->basename($parsed_url['path']);
      $basename = \Drupal::config('system.file')->get('default_scheme') . '://' . $basename;
      $path = $this->streamWrapperManager->normalizeUri($basename);
    }
    else {
      if (is_dir($this->fileSystem->realpath($destination))) {
        // Prevent URIs with triple slashes when glueing parts together.
        $path = str_replace(
            '///',
            '//',
            "{$destination}"
          ) . $this->fileSystem->basename(
            $parsed_url['path']
          );
      }
      else {
        $path = $destination;
      }
    }
    try {
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
        if ($contents && file_put_contents($path, $contents)) {
          return $path;
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
   * @param \Drupal\file\Entity\File $zip_file
   * @param null $extension
   *      If passed, we will only return files with that extension.
   *
   * @return array
   *    All the names of the ZIP file. We won't yet extract them there.
   */
  public function listZipFileContent(File $zip_file, $extension = NULL): array {
    $files = [];
    $zip_realpath = $this->fileSystem->realpath($zip_file->getFileUri());
    if (!$zip_realpath) {
      // This will add a delay once...
      $zip_realpath = $this->strawberryfieldFileMetadataService->ensureFileAvailability($zip_file, NULL);
    }
    if ($zip_realpath) {
      $z = new \ZipArchive();
      $z->open($zip_realpath);
      $files = [];
      if ($z) {
        for ($i = 0; $i < $z->numFiles; $i++) {
          $file_name = $z->getNameIndex($i);
          if ($extension) {
            if (strpos($file_name, '.' . $extension) !== FALSE || strpos($file_name, '.' . strtoupper($extension) !== FALSE)) {
              $files[] = $file_name;
            }
            else {
              $files[] = $file_name;
            }
          }
        }
        $z->close();
      }
    }
    return $files;
  }

  /**
   * @param \Drupal\file\Entity\File $zip_file
   * @param $file_path
   *    This is only safe if $file_path is a text,xml,yml (text type)
   *    Up to the caller to handle Binary if they decide to use it like that.
   * @return string|null
   */
  public function getZipFileContent(File $zip_file, $file_path): ?string {
    $content = NULL;
    $zip_realpath = $this->fileSystem->realpath($zip_file->getFileUri());
    if (!$zip_realpath) {
      // This will add a delay once...
      $zip_realpath = $this->strawberryfieldFileMetadataService->ensureFileAvailability($zip_file, NULL);
    }
    if ($zip_realpath) {
      $z = new \ZipArchive();
      $z->open($zip_realpath);
      if ($z) {
        $stream = $z->getStream($file_path);
        if (FALSE !== $stream) {
          $content = stream_get_contents($stream);
        }
        $z->close();
      }
    }
    return $content;
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
      // Sadly File URIs can not be longer than 255 characters. We have no other way
      // Because of Drupal's DB Fixed schema and a Override on this might
      // Not be great/sustainable.
      // Because this is not a very common thing
      // I will incur in the penalty of moving a file here
      // Just to keep code isolated but also to have the chance to preserve
      // the original name!
      if (strlen($localpath) > 255) {
        $path_length = strlen($localpath);
        $filename = $this->fileSystem->basename($localpath);
        $prefix_and_wrapper_length = ($path_length - strlen($filename));
        $max_file_length = 255 - $prefix_and_wrapper_length;
        $max_part_length = floor($max_file_length / 2);
        $first_part = substr($localpath, $prefix_and_wrapper_length, $max_part_length);
        // -4 because 'we' are cute and will add this in between " -_- "
        $second_part = substr($localpath, -1 * ($max_part_length - 4));
        $new_uri = substr($localpath, 0, $prefix_and_wrapper_length) . $first_part .' -_- '.$second_part;
        if (strlen($filename > 255)) {
          // For consistency i cut again.
          $filename  = substr($filename, 0, 127) .' -_- '. substr($filename, -1 * (123));
        }
        try {
          $moved_file = $this->fileSystem->move(
            $localpath, $new_uri, FileSystemInterface::EXISTS_REPLACE
          );
          $message = 'File generated during Ami Set Processing with temporary URI @longuri was longer than 255 characters (Drupal field limit) so had to be renamed to shorter @path';
          $this->loggerFactory->get('ami')->warning($message, [
            '@longuri' =>$localpath,
            '@path' => $new_uri,
          ]);
        }
        catch (FileWriteException $writeException) {
          $message = 'Unable to move file from longer than 255 characters @longuri to shorter @path with error: @error.';
          $this->loggerFactory->get('ami')->error($message, [
            '@error' => $writeException->getMessage(),
            '@longuri' =>$localpath,
            '@path' => $new_uri,
          ]);
          return FALSE;
        }
        $localpath = $new_uri;
      }
      else {
        $filename = $this->fileSystem->basename($localpath);
      }
      /** @var File $file */
      $file = $this->entityTypeManager->getStorage('file')->create(
        [
          'uri' => $localpath,
          'uid' => $this->currentUser->id(),
          'status' => FileInterface::STATUS_PERMANENT,
          'filename' => $filename,
        ]
      );

      // If we are replacing an existing file re-use its database record.
      // @todo Do not create a new entity in order to update it. See
      //   https://www.drupal.org/node/2241865.
      // Check if File with same URI already exists.
      // @TODO: Should we check for AMI also if the current user can update the file?
      $existing_files = \Drupal::entityTypeManager()
        ->getStorage('file')
        ->loadByProperties(['uri' => $localpath]);
      if (count($existing_files)) {
        $existing = reset($existing_files);
        $file->fid = $existing->id();
        $file->uuid = $existing->uuid();
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
   * Returns the filename from a Content-Disposition header string.
   *
   * @param string $value
   *
   * @return string|null
   *    Returns NULL if could not be parsed/empty
   */
  protected function getFilenameFromDisposition(string $value) {
    $value = trim($value);

    if (strpos($value, ';') === false) {
      return NULL;
    }

    [$type, $attr_parts] = explode(';', $value, 2);

    $attr_parts = explode(';', $attr_parts);
    $attributes = [];

    foreach ($attr_parts as $part) {
      if (strpos($part, '=') === false) {
        continue;
      }

      [$key, $value] = explode('=', $part, 2);

      $attributes[trim($key)] = trim($value);
    }

    $attrNames = ['filename*' => true, 'filename' => false];
    $filename = NULL;
    $isUtf8 = false;
    foreach ($attrNames as $attrName => $utf8) {
      if (!empty($attributes[$attrName])) {
        $filename = trim($attributes[$attrName]);
        $isUtf8 = $utf8;
        break;
      }
    }
    if (empty($filename)) {
      return NULL;
    }

    if ($isUtf8 && strpos($filename, "utf-8''") === 0 && $filename = substr($filename, strlen("utf-8''"))) {
      return rawurldecode($filename);
    }
    if (substr($filename, 0, 1) === '"' && substr($filename, -1, 1) === '"') {
      $filename = substr($filename, 1, -1) ?? NULL;
    }

    return $filename;
  }

  /**
   * Creates an empty CSV and returns a file.
   *
   * @param string|null $filename
   *    If given it will use that, if null will create a new one.
   *    If filename is the full uri to an existing file it will update that one
   *    and its entity too.
   * @param string|null $subpath
   *    If set, it should be a folder structure without a leading slash. eg. set1/anothersubfolder/
   *
   * @return int|string|null
   */
  public function csv_touch(string $filename = NULL, ?string $subpath = NULL) {
    if (!$subpath) {
      $path = 'public://ami/csv';
    }
    else {
      $path = 'public://ami/csv/'.$subpath;
    }
    // Check if dealing with an existing file first
    if ($filename && is_file($filename) && $this->streamWrapperManager->isValidUri($filename)) {
      $uri = $filename;
    }
    else {
      // if not there go with our standard naming convention.
      $filename = $filename ?? $this->currentUser->id() . '-' . uniqid() . '.csv';
      $uri = $path . '/' . $filename;
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
    }

    $file = \Drupal::service('file.repository')->writeData('', $uri, FileExists::Replace);

    if (!$file) {
      $this->messenger()->addError(
        $this->t('Unable to create AMI CSV file. Verify permissions please.')
      );
      return NULL;
    }
    clearstatcache(TRUE, $file->getFileUri());
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
   *    The header key can has/should have the UUIDs of new/existing ADOs.
   * @param boolean $auto_uuid
   *    Defines if we are going to generate UUIDs when not valid/not present
   *    Or leave the $uuid_key field as it is and let this fail/if later.
   *
   * @return int|string|null
   * @throws \Drupal\Core\Entity\EntityStorageException
   */
  public function csv_save(array $data, $uuid_key = 'node_uuid', $auto_uuid = TRUE) {

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

    $file = \Drupal::service('file.repository')->writeData('',  $path . '/' . $filename, FileSystemInterface::EXISTS_REPLACE);
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
          '@url' => \Drupal::service('file_url_generator')->generateAbsoluteString($file->getFileUri()),
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
   * @param bool $escape_characters
   *    Defaults to internal PHP mechanism for escaping characters (a "/")
   *    Set to FALSE if you are passing JSON encoded strings into cells.
   *    NOTE: Make sure you also disable it IF reading back from files generated through this
   *
   * @return int|string|null
   * @throws \Drupal\Core\Entity\EntityStorageException
   */
  public function csv_append(array $data, File $file, $uuid_key = 'node_uuid', bool $append_header = TRUE, $escape_characters = TRUE, $auto_uuid = TRUE) {

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
      if ($escape_characters) {
        $fh->fputcsv($row);
      }
      else {
        $fh->fputcsv($row, ',', '"', "");
      }
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
   * @param bool $escape_characters
   *
   * @return array|null
   *   Returning array will be in this form:
   *    'headers' => $rowHeaders_utf8 or [] if $always_include_header == FALSE
   *    'data' => $table,
   *    'totalrows' => $maxRow,
   */
  public function csv_read(File $file, int $offset = 0, int $count = 0, bool $always_include_header = TRUE, $escape_characters = TRUE) {

    $wrapper = $this->streamWrapperManager->getViaUri($file->getFileUri());
    if (!$wrapper) {
      return NULL;
    }

    $url = $wrapper->getUri();
    $uri = $this->streamWrapperManager->normalizeUri($url);
    if (!is_file($uri)) {
      $message = $this->t(
        'CSV File referenced in AMI set for processing at @uri is no longer present. Check your composting times. Skipping',
        [
          '@uri' => $uri,
        ]
      );
      $this->loggerFactory->get('ami')->error($message);
      return NULL;
    }

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
      if (!$escape_characters) {
        $spl->setCsvControl(',', '"', "");
      }
    }

    if ($offset > 0 && !$always_include_header) {
      // If header needs to be included then we offset later on
      // PHP 8.0.16 IS STILL BUGGY with SEEK.
      //$spl->seek($offset) does not work here.
      for ($i = 0; $i < $offset; $i++) {
        $spl->next();
      }

    }
    $data = [];
    $seek_to_offset = ($offset > 0 && $always_include_header);
    while (!$spl->eof() && ($count == 0 || ($spl->key() < ($offset + $count)))) {
      if (!$escape_characters) {
        $data[] = $spl->fgetcsv( ',', '"', "");
      }
      else {
        $data[] = $spl->fgetcsv();
      }
      if ($seek_to_offset) {
        for ($i = 0; $i < $offset; $i++) {
          $spl->next();
        }
        // PHP 8.0.16 IS STILL BUGGY with SEEK.
        //$spl->seek($offset); doe snot work here
        // So we do not process this again.
        $seek_to_offset = FALSE;
      }
    }

    $table = [];
    $maxRow = 0;

    $highestRow = count($data);
    if ($always_include_header) {
      $rowHeaders = $data[0] ?? [];
      $rowHeaders_utf8 = array_map(function($value) {
        $value = $value ?? '';
        $value = stripslashes($value);
        $value = function_exists('mb_convert_encoding') ? mb_convert_encoding($value, 'UTF-8', 'ISO-8859-1') : utf8_encode($value);
        $value = strtolower($value);
        $value = trim($value);
        return $value;
      }, $rowHeaders);
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
        $maxRow = $rowindex;
        if (strlen($flat) == 0) {
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
   *  Removes columns from an existing CSV.
   *
   * @param \Drupal\file\Entity\File $file
   * @param array $headerwithdata
   *
   * @return int|mixed|string|null
   * @throws \Drupal\Core\Entity\EntityStorageException
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
   * @param bool $escape_characters
   *
   * @return int
   */
  public function csv_count(File $file, $escape_characters = TRUE) {
    $wrapper = $this->streamWrapperManager->getViaUri($file->getFileUri());
    if (!$wrapper) {
      return NULL;
    }
    $key = 0;

    $url = $wrapper->getUri();
    $spl = new \SplFileObject($url, 'r');
    $spl->setFlags(
      SplFileObject::READ_CSV |
      SplFileObject::READ_AHEAD |
      SplFileObject::SKIP_EMPTY |
      SplFileObject::DROP_NEW_LINE
    );
    while (!$spl->eof()) {
      if (!$escape_characters) {
        $spl->fgetcsv( ',', '"', "");
      }
      else {
        $spl->fgetcsv();
      }
      $key = $spl->key();
    }

    /*
    Some PHP 8 versions fail on seeking on lines with JSON content and either returns
    0 lines (with the flags) or way more without the flags
    $spl->seek(PHP_INT_MAX);
    $key = $spl->key();
    */

    $spl = NULL;
    // $key is always offset by 1.
    return $key + 1;
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
    $unique = array_map(function($value) {
      $value = $value ?? '';
      return trim($value);
    }, $all);
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
      ->accessCheck(TRUE)
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
      ->accessCheck(TRUE)
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
          /* @var FileInterface|null $zipfile */
          $zipfile = $this->entityTypeManager->getStorage('file')
            ->load($data->zip);
          if (!$zipfile) {
            $zipfail = TRUE;
          }
          else {
            /** @var \Drupal\file\FileRepositoryInterface $file_repository */
            $file_repository = \Drupal::service('file.repository');
            try {
              $zipfile = $file_repository->move($zipfile, $target_directory, FileExists::Rename);
            }
            catch (InvalidStreamWrapperException $e) {
              $zipfail = TRUE;
            }
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

    // Use the AMI set user ID for checking access to entity operations.
    $uid = $data->info['uid'] ?? \Drupal::currentUser()->id();
    $account = $uid == \Drupal::currentUser()->id() ? \Drupal::currentUser() : $this->entityTypeManager->getStorage('user')->load($uid);

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
    // Keps track of UUIDs found and their original Row Index. Adds sadly another Foreach
    // But we need this so we can also sort CSVs where parents are all UUIDs.
    // To do so we will count the level/deepness of a tree, and sort by shortest.
    $uuid_to_row_index_hash = [];
    $info = [];
    // First pass to accumulate UUIDs and their CSV order of appearance.
    foreach ($file_data_all['data'] as $index => $keyedrow) {
      $row = array_combine($config['data']['headers'], $keyedrow);
      // UUIDs should be already assigned by this time
      $possibleUUID = $row[$data->adomapping->uuid->uuid] ?? NULL;
      $possibleUUID = $possibleUUID ? trim($possibleUUID) : $possibleUUID;
      // Double check? User may be tricking us!
      if ($possibleUUID && Uuid::isValid($possibleUUID)) {
        $uuid_to_row_index_hash[$possibleUUID] = $index;
      }
    }

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
        $parent_ados_toexpand = (array) trim(
          $row[$parent_key]
        );
        $parent_ados_array = [];
        $parent_ados_expanded = $this->expandJson($parent_ados_toexpand);
        $parent_ados_expanded = $parent_ados_expanded[0] ?? NULL;
        if (is_array($parent_ados_expanded)) {
          $parent_ados_array = $parent_ados_expanded;
        }
        elseif (is_string($parent_ados_expanded) || is_integer($parent_ados_expanded)) {
          // This allows single value and or ; and trims. Neat?
          $parent_ados_array = array_map(function($value) {
            $value = $value ?? '';
            return trim($value);
          }, explode(';', $parent_ados_expanded));
        }

        $ado['parent'][$parent_key] = $parent_ados_array;
        $ado['anyparent'] = array_unique(array_merge($ado['anyparent'], $ado['parent'][$parent_key]));
      }

      $ado['data'] = $row;

      // UUIDs should be already assigned by this time
      $possibleUUID = $row[$data->adomapping->uuid->uuid] ?? NULL;
      $possibleUUID = $possibleUUID ? trim($possibleUUID) : $possibleUUID;
      if ($possibleUUID && isset($uuid_to_row_index_hash[$possibleUUID])) {
        $ado['uuid'] = $possibleUUID;
        // Now be more strict for action = update/patch
        if ($data->pluginconfig->op !== 'create') {
          $existing_objects = $this->entityTypeManager->getStorage('node')
            ->loadByProperties(['uuid' => $ado['uuid']]);
          // Do access control here, will be done again during the atomic operation
          // In case access changes later of course
          // Processors do NOT delete. So we only check for Update.
          $existing_object = $existing_objects && count($existing_objects) == 1 ? reset($existing_objects) : NULL;
          if (!$existing_object || !$existing_object->access('update', $account)) {
            unset($ado);
            $invalid = $invalid + [$index => $index];
          }
        }
      }
      else {
        unset($ado);
        $invalid = $invalid + [$index => $index];
      }

      if (isset($ado)) {
        //We might have multiple relationships/or none at all.
        foreach ($ado['parent'] as $parent_key => &$parent_ados) {
          foreach ($parent_ados as $parent_ado) {
            $rootfound = FALSE;
            $parent_numeric = $this->getParentRowId($parent_ado, $uuid_to_row_index_hash);
            if ($parent_numeric === FALSE) {
              $invalid[$parent_numeric] = $parent_numeric;
              $invalid[$index] = $index;
              unset($ado);
              continue;
            }
            elseif ($parent_numeric === NULL) {
              // This will also be true if there was no parent value.
              $rootfound = TRUE;
              continue;
            }
            else {
              $parent_hash[$parent_key][$parent_numeric][$index] = $index;
            }
            $parentchilds = [];
            $parent_numeric_loop = $parent_numeric;
            while (!$rootfound) {
              // $parentup gets the same treatment as $ado['parent']
              $parentup_toexpand = [trim(
                $file_data_all['data'][$parent_numeric_loop][$parent_to_index[$parent_key]]
              )];
              $parentup_array = [];
              $parentup_expanded = $this->expandJson($parentup_toexpand);
              $parentup_expanded = $parentup_expanded[0] ?? NULL;
              if (is_array($parentup_expanded)) {
                $parentup_array = $parent_ados_expanded;
              }
              elseif (is_string($parentup_expanded)
                || is_integer(
                  $parentup_expanded
                )
              ) {
                // This allows single value and or ; and trims. Neat?
                $parentup_array = array_map(function($value) {
                  $value = $value ?? '';
                  return trim($value);
                }, explode(';', $parentup_expanded));
              }

              foreach ($parentup_array as $parentup) {
                $parentup_numeric = $this->getParentRowId($parentup, $uuid_to_row_index_hash);
                if ($parentup_numeric === FALSE) {
                  $invalid[$parentup_numeric] = $parentup_numeric;
                  $invalid[$index] = $index;
                  unset($ado);
                  $rootfound = TRUE;
                  break;
                }
                elseif ($parentup_numeric === NULL) {
                  $rootfound = TRUE;
                  break;
                }

                // If $parentup
                // The Simplest approach for breaking a knot /infinite loop,
                // is invalidating the whole parentship chain for good.
                $inaloop = isset($parentchilds[$parentup_numeric]);
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

                $parentchilds[$parentup_numeric] = $parentup_numeric;
                // If this parent is either a UUID or empty means we reached the root
                // This a simple accumulator, means all is well,
                // parent is still an index.
                $parent_hash[$parent_key][$parentup_numeric][$parent_numeric_loop]
                  = $parent_numeric_loop;
                $parent_numeric_loop = $parentup_numeric;
              }
            }
          }
        }
      }
      if (isset($ado) and !empty($ado)) {
        $info[$index] = $ado;
      }
    }

    // Now the real pass, iterate over every row.
    $parent_hash_flat = [];
    foreach ($info as $index => &$ado) {
      foreach ($data->adomapping->parents as $parent_key) {
        // Is this object parent of someone?
        // at this stage $ado['parent'][$parent_key] SHOULD BE AN ARRAY IF VALID
        if (is_array($ado['parent'][$parent_key])) {
          foreach ($ado['parent'][$parent_key] ?? [] as $index_rel => $parentnumeric) {
            // This will only match if the original value is a row, if not the existing UUID will be preserved?
            if (!empty($parentnumeric) && isset($parent_hash[$parent_key][$parentnumeric])) {
              $ado['parent'][$parent_key][$index_rel] = $info[$parentnumeric]['uuid'];
            }
          }
          $ado['parent'][$parent_key] = array_filter(array_unique($ado['parent'][$parent_key]));
        }
        if (isset($parent_hash[$parent_key][$index])) {
          $parent_hash_flat[$index] = array_unique(array_merge($parent_hash_flat[$index] ?? [], $parent_hash[$parent_key][$index]));
        }
      }
      // Since we are reodering we may want to keep the original row_id around
      // To help users debug which row has issues in case of ingest errors
      $ado['row_id'] = $index;
    }

    // Before attempting a re-order. Give the user the chance to be right.
    // We will validate the given order. If not, or already having an invalid we will sort.
    $seen = [];
    $needs_sorting = FALSE;
    if (empty($invalid)) {
      foreach ($info as $entry) {
        $seen[$entry['uuid']] = $entry['uuid'];
        foreach(($entry['parent'] ?? [] ) as $rel => $uuids) {
          $uuids = array_filter($uuids);
          foreach ($uuids as $parent_uuid) {
            // Means the UUID is pointing to the same CSV but we have not seen the parent yet
            if ((string)$parent_uuid !='' && isset($uuid_to_row_index_hash[$parent_uuid]) && !isset($seen[$parent_uuid])) {
                $needs_sorting = TRUE;
                break 3;
            }
          }
        }
      }
    }
    else {
      $needs_sorting = TRUE;
    }
    unset($seen);

    if ($needs_sorting) {
      // Now reoder, add parents first then the rest.
      $newinfo = [];
      // parent hash flat contains keys with all the properties and then a numeric array with their children.
      $added = [];
      foreach ($parent_hash_flat as $row_id => $all_children) {
        $this->sortTreeByChildren($row_id, $parent_hash_flat, $added, []);
      }
      foreach ($added as $order => $row_id) {
        if (isset($info[$row_id])) {
          $newinfo[] = $info[$row_id];
          unset($info[$row_id]);
        }
      }
      // In theory $info will only contain Objects that have no Children and are no child of others.
      $info = array_merge($newinfo, array_values($info));
      unset($newinfo);
      unset($added);
    }

    unset($parent_hash_flat);
    unset($parent_hash);
    unset($uuid_to_row_index_hash);

    return $info;
  }

  protected function sortTreeByChildren($row_id, $tree, &$ordered, $ordered_completetree) {
    if (isset($tree[$row_id]) && !in_array($row_id, $ordered)) {
      $subtree[] = $row_id;
      $offset = NULL;
      foreach ($tree[$row_id] as $child_id) {
        if (in_array($child_id, $ordered)) {
          // When a child is found, we don't add it again to the main order.
          // Maybe we should delete it?
          $child_offset = array_search($child_id, $ordered, TRUE);
          // When multiple offsets are present we take the one that inserts the subtree earlier.
          if ($offset !== NULL) {
            $offset = min($offset, $child_offset);
          }
          else {
            $offset = $child_offset;
          }
        }
        else {
          // Only add of course if not there already.
          $subtree[] = $child_id;
        }
      }
      if ($offset !== NULL) {
        array_splice($ordered, $offset,0, $subtree);
      }
      else {
        $ordered = array_merge($ordered, $subtree);
      }
    }
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
      $required_headers = array_map(function($value) {
        $value = $value ?? '';
        return strtolower($value);
      }, $required_headers);

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
   * Returns UUIDs for AMI data the user has permissions (if op passed) to operate on.
   *
   * @param \Drupal\file\Entity\File $file
   * @param \stdClass $data
   *
   * @param null|string $op
   *
   * @return mixed
   *  UUIDs will be in the keys, possible child CSVs (array) in the values.
   *
   * @throws \Drupal\Component\Plugin\Exception\InvalidPluginDefinitionException
   * @throws \Drupal\Component\Plugin\Exception\PluginNotFoundException
   */
  public function getProcessedAmiSetNodeUUids(File $file, \stdClass $data, $op = NULL) {

    // Use the AMI set user ID for checking access to entity operations.
    $account =  $data->info['uid'] == \Drupal::currentUser()->id() ? \Drupal::currentUser() : $this->entityTypeManager->getStorage('user')->load($data->info['uid']);

    $file_data_all = $this->csv_read($file);
    if (!$file_data_all) {
      return [];
    }
    // we may want to check if saved metadata headers == csv ones first.
    // $data->column_keys
    $config['data']['headers'] = $file_data_all['headers'];
    $uuids = [];
    // We want to get the per ROW CSVs if any here too
    $file_csv_columns = [];


    foreach ($file_data_all['data'] as $index => $keyedrow) {
      // This makes tracking of values more consistent and easier for the actual processing via
      // twig templates, webforms or direct
      $row = array_combine($config['data']['headers'], $keyedrow);

      if ($data->mapping->globalmapping == "custom") {
        if ($row['type'] ?? NULL ) {
          $csv_file_object = $data->mapping->custommapping_settings->{$row['type']}->files_csv ?? NULL;
        }
        else {
          $csv_file_object = NULL;
        }
      } else {
        $csv_file_object = $data->mapping->globalmapping_settings->files_csv ?? NULL;
      }
      if ($csv_file_object && is_object($csv_file_object)) {
        $file_csv_columns = array_values(get_object_vars($csv_file_object));
      }

      $possibleUUID = $row[$data->adomapping->uuid->uuid] ?? NULL;
      $possibleUUID = $possibleUUID ? trim($possibleUUID) : $possibleUUID;
      $possibleCSV = [];
      // Check now for the CSV file column/file name
      if (count($file_csv_columns)) {
        foreach($file_csv_columns as $file_csv_column) {
          $possibleCSV[] = $row[$file_csv_column] ?? NULL;
        }
      }
      $possibleCSV = array_filter($possibleCSV);
      // Double check? User may be tricking us!
      if ($possibleUUID && Uuid::isValid($possibleUUID)) {
        if ($op !== 'create' && $op !== NULL) {
          $existing_objects = $this->entityTypeManager->getStorage('node')
            ->loadByProperties(['uuid' => $possibleUUID]);
          // Do access control here, will be done again during the atomic operation
          // In case access changes later of course
          // This does NOT delete. So we only check for Update.
          $existing_object = $existing_objects && count($existing_objects) == 1 ? reset($existing_objects) : NULL;

          if ($existing_object && $existing_object->access($op, $account)) {
            $uuids[$possibleUUID] = $possibleCSV;
          }
        }
        else {
          $uuids[$possibleUUID] = $possibleCSV;
        }
      }
      else {
       $message = $this->t(
          'Invalid UUID @uuid found. Skipping for AMI Set ID @setid, Row @row',
          [
            '@uuid' => $possibleUUID,
            '@row' => $index,
            '@setid' => 1,
          ]
        );
        $this->loggerFactory->get('ami')->warning($message);
      }
    }
    return $uuids;
  }

  /**
   * Callback to get the Index number in the CSV of a parent numeric id
   * or uuid
   *
   * @param string $parent
   *
   * @return int|null|bool
   *    FALSE if not present at all
   *    NULL if a UUID but not in this CSV or empty (so no parent)
   */
  protected function getParentRowId(string $parent_ado, $uuid_to_row_index_hash) {
    $parent_numeric = FALSE;
    if (empty($parent_ado) || strlen(trim($parent_ado)) == 0) {
      $parent_numeric = NULL;
    }
    elseif (!Uuid::isValid(trim($parent_ado))
      && is_scalar($parent_ado)
      && (intval($parent_ado) > 1 && intval($parent_ado) <= count($uuid_to_row_index_hash)+1 )
    ) {
      $parent_numeric = intval(trim($parent_ado));
    }
    elseif (Uuid::isValid(trim($parent_ado)))  {
      // fetch the actual ROW id using the
      $parent_numeric = isset($uuid_to_row_index_hash[trim($parent_ado)]) ? $uuid_to_row_index_hash[trim($parent_ado)] : NULL;
    }
    return  $parent_numeric;
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
      $context_lod_contextual = [];
      // get the mappings for this set if any
      // @TODO Refactor into a Method?
      $lod_mappings = $this->AmiLoDService->getKeyValueMappingsPerAmiSet($set_id);
      /* @TODO refactor into a reusable method */
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
                    $serialized = array_map('serialize', $context_lod[$source_column][$approach]);
                    $unique = array_unique($serialized);
                    $context_lod[$source_column][$approach] = array_intersect_key($context_lod[$source_column][$approach], $unique);
                    $context_lod_contextual[$source_column][$label][$approach] = array_merge($context_lod_contextual[$source_column][$label][$approach] ?? [], $lod['lod']);
                  }
                }
              }
            }
          }
        }
      }

      $context['data_lod'] = $context_lod;
      $context['data_lod_contextual'] = $context_lod_contextual;
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
      try {
        $cacheabledata = \Drupal::service('renderer')->executeInRenderContext(
          new RenderContext(),
          function () use ($context, $metadatadisplay_entity) {
            return $metadatadisplay_entity->renderNative($context);
          }
        );
      }
      catch (\Exception $error) {
          $message = $this->t(
            'Twig could not render the Metadata Display ID @metadatadisplayid for AMI Set ID @setid, with Row @row, future ADO with UUID @uuid. The Twig internal renderer error is: %output. Please check your template against that AMI row and make sure you are handling values, arrays and filters correctly.',
            [
              '@metadatadisplayid' => $metadatadisplay_id,
              '@uuid' => $data->info['row']['uuid'],
              '@row' => $row_id,
              '@setid' => $set_id,
              '%output' => $error->getMessage(),
            ]
          );
          $this->loggerFactory->get('ami')->error($message);
          return NULL;
      }
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
    $all_isstring= array_filter($all, 'is_string');
    $all_notJson = array_filter($all_isstring,  array($this, 'isNotJson'));
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
    $unique = array_map(function($value) {
      $value = $value ?? '';
      return trim($value);
    }, $all_entries);

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

  /**
   * Move file to local to if needed process.
   *
   * @param \Drupal\file\FileInterface $file
   *   The File URI to look at.
   *
   * @return string|bool
   *   The temp local file path or.
   *   False if we could not acquire location
   *   TRUE if its already local so we can use existing path.
   *
   * @see \Drupal\strawberryfield\StrawberryfieldFileMetadataService::ensureFileAvailability
   *    This is wrapper to expose publicly an injected dependency method.
   */

  public function ensureFileAvailability(FileInterface $file, $checksum = NULL) {
    return $this->strawberryfieldFileMetadataService->ensureFileAvailability($file,
      $checksum);
  }

  /**
   * Removes an non managed file and temp generated by this module.
   *
   * @param string $templocation
   *
   * @return bool
   *
   * @see \Drupal\strawberryfield\StrawberryfieldFileMetadataService::cleanUpTemp
   *   This is wrapper to expose publicly an injected dependency method.
   */
  public function cleanUpTemp(string $templocation) {
    return $this->strawberryfieldFileMetadataService->cleanUpTemp($templocation);
  }

  /**
   * Checks the AMISet operation to determine if it's permissible to delete processed ADOs.
   *
   * True if the AMISet operation is not 'update' or 'patch'.
   *
   * @param  \Drupal\Core\Entity\EntityInterface  $entity
   *
   * @return bool
   */
  public static function checkAmiSetDeleteAdosAccess(EntityInterface $entity): bool {
    // Deny access to delete processed ADOs when the AMI set is configured for update rather than create.
    $cache_id = 'amiset_deleteados_access_' . $entity->id();
    $deleteados_access = &drupal_static(__CLASS__ . __METHOD__ . $cache_id);
    if (!isset($deleteados_access)) {
      if ($deleteados_access_cache = \Drupal::cache()->get($cache_id)) {
        return $deleteados_access_cache->data;
      }
      else {
        $set_field = $entity->get('set');
        if ($set_field instanceof \Drupal\strawberryfield\Field\StrawberryFieldItemList) {
          $set = json_decode($entity->get('set')->getString(), TRUE);
          if (json_last_error() == JSON_ERROR_NONE) {
            $deleteados_access = (empty($set['pluginconfig']['op']) || !in_array($set['pluginconfig']['op'], ['update', 'patch', 'sync']));
            \Drupal::cache()->set($cache_id, $deleteados_access);
          }
        }
      }
    }
    return $deleteados_access;
  }

  /**
   * Invalidates amiset delete ADOs cache for a given AMISet entity.
   *
   * @param  \Drupal\Core\Entity\EntityInterface  $entity
   */
  public static function invalidateAmiSetDeleteAdosAccessCache(EntityInterface $entity) {
    $cache_id = 'amiset_deleteados_access_' . $entity->id();
    \Drupal::cache()->invalidate($cache_id);
  }

}

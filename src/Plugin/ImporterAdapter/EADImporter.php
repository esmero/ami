<?php

namespace Drupal\ami\Plugin\ImporterAdapter;

use Drupal\ami\AmiUtilityService;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Messenger\MessengerTrait;
use Drupal\Core\StreamWrapper\StreamWrapperManagerInterface;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\ami\Plugin\ImporterAdapterBase;
use PhpOffice\PhpSpreadsheet\IOFactory;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\file\Entity\File;

/**
 * ADO importer for EADs from Spreadsheet format and XML too.
 *
 * @ImporterAdapter(
 *   id = "ead",
 *   label = @Translation("EAD Importer"),
 *   remote = false,
 *   batch = true,
 * )
 */
class EADImporter extends SpreadsheetImporter {


  use StringTranslationTrait;
  use MessengerTrait;

  /**
   * @var \Drupal\Core\StreamWrapper\StreamWrapperManagerInterface
   */
  protected StreamWrapperManagerInterface $streamWrapperManager;

  /**
   * @var string|null
   */
  protected ?string $tempFile;

  /**
   * SpreadsheetImporter constructor.
   *
   * @param array $configuration
   * @param $plugin_id
   * @param $plugin_definition
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface $entityTypeManager
   * @param \Drupal\Core\StreamWrapper\StreamWrapperManagerInterface $streamWrapperManager
   * @param \Drupal\ami\AmiUtilityService $ami_utility
   *
   * @throws \Drupal\Component\Plugin\Exception\PluginException
   */
  public function __construct(array $configuration, $plugin_id, $plugin_definition, EntityTypeManagerInterface $entityTypeManager,  StreamWrapperManagerInterface $streamWrapperManager, AmiUtilityService $ami_utility) {
    parent::__construct($configuration, $plugin_id, $plugin_definition, $entityTypeManager, $ami_utility);
    $this->streamWrapperManager = $streamWrapperManager;
    $this->tempFile = NULL;
    register_shutdown_function([$this, 'shutdown']);
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
      $container->get('stream_wrapper_manager'),
      $container->get('ami.utility')
    );
  }


  use StringTranslationTrait;

  /**
   * {@inheritdoc}
   */
  public function interactiveForm(array $parents, FormStateInterface $form_state):array {
    $form = [];
    $form = parent::interactiveForm($parents,$form_state);
    $form['file'] = [
      '#type' => 'managed_file',
      '#default_value' => $form_state->getValue(array_merge($parents , ['file'])),
      '#title' => $this->t('Upload your file'),
      '#description' => $this->t('The CSV file containing your EAD records.'),
      '#required' => TRUE,
      '#upload_location' => 'private://',
      '#upload_validators' => [
        'file_validate_extensions' => ['csv'],
      ],
    ];

    return $form;
  }


  /**
   * {@inheritdoc}
   */
  public function getData(array $config,  $page = 0, $per_page = 20): array {
    $data = parent::getData($config, $page, $per_page);
    $offset = $page * $per_page;
    $tabdata = ['headers' => [], 'data' => $data, 'totalrows' => 0];

    /* @var File $file */
    $file = $this->entityTypeManager->getStorage('file')
      ->load($config['file'][0]);
    if (!$file) {
      $this->messenger()->addMessage(
        $this->t(
          'Could not load the file. Please check your Drupal logs or contact your Repository Admin'
        )
      );
      return $tabdata;
    }

    $response = $this->AmiUtilityService->ensureFileAvailability($file);

    if ($response === TRUE) {
      $file_path = $this->streamWrapperManager->getViaUri($file->getFileUri())->realpath();
      $this->streamWrapperManager->getViaUri($file->getFileUri())->getUri();
    }
    elseif ($response === FALSE) {
      $this->messenger()->addMessage(
        $this->t(
          'Could not copy source file to a local location. Please check your Filesystem Permissions, Drupal logs or contact your Repository Admin'
        )
      );
      return $tabdata;
    }
    else {
      $this->tempFile = $response;
      $file_path = $response;
    }
    try {
      $inputFileType = IOFactory::identify($file_path);
      // Because of \PhpOffice\PhpSpreadsheet\Cell\DataType::checkString we can
      // Not use this library for CSVs that contain large JSONs
      // Since we do not know if they contain that, we will
      // assume so (maybe a user choice in the future)
      return $this->AmiUtilityService->csv_read($file, 0, 0, TRUE) ?? $tabdata;
    }
    catch (\Exception $e) {
      $this->messenger()->addMessage(
        $this->t(
          'Could not parse file with error: @error',
          ['@error' => $e->getMessage()]
        )
      );
      return $tabdata;
    }
  }

  public function getInfo(array $config, FormStateInterface $form_state, $page = 0, $per_page = 20): array {
    return $this->getData($config, $page, $per_page);
  }

  /**
   * Shutdown that "should" clean temp file if one was generated
   */
  public function shutdown() {
    // on PHP-FPM there will be never output of this one..
    if ($this->tempFile !== NULL) {
      $this->AmiUtilityService->cleanUpTemp($this->tempFile);
    }
  }
}
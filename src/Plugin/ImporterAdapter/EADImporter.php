<?php

namespace Drupal\ami\Plugin\ImporterAdapter;

use Drupal\ami\AmiUtilityService;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Messenger\MessengerTrait;
use Drupal\Core\StreamWrapper\StreamWrapperManagerInterface;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\ami\Plugin\ImporterAdapterBase;
use Drupal\Core\TempStore\PrivateTempStore;
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

  public function provideTypes(array $config, array $data): array
  {
    // These are our discussed types. No flexibility here.
    return [
      'ArchiveContainer' =>  'ArchiveContainer',
      'ArchiveComponent' => 'ArchiveComponent',
    ];
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

  public function stepFormAlter(&$form, FormStateInterface $form_state, PrivateTempStore $store, $step): void
  {
    if ($step == 3) {
      $form['ingestsetup']['globalmapping'] = [
        '#type' => 'select',
        '#title' => $this->t('Select the data transformation approach'),
        '#default_value' => 'custom',
        '#options' => ['custom' => 'Custom (Expert Mode)'],
        '#description' => $this->t('How your source data will be transformed into EADs Metadata.'),
        '#required' => TRUE,
      ];
      foreach ($form['ingestsetup']['custommapping'] ?? [] as $key => &$settings) {
        if (strpos($key,'#') !== 0 && is_array($settings)) {
          if ($settings['metadata']['#default_value'] ?? NULL) {
            $form['ingestsetup']['custommapping'][$key]['metadata']['#default_value'] = 'template';
            $form['ingestsetup']['custommapping'][$key]['metadata']['#options'] = ['template' => 'Template'];
          }
        }
      }
    }
    $form = $form;
  }
}

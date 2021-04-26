<?php

namespace Drupal\ami\Plugin\ImporterAdapter;

use Drupal\ami\AmiUtilityService;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\StreamWrapper\StreamWrapperManagerInterface;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\ami\Plugin\ImporterAdapterBase;
use PhpOffice\PhpSpreadsheet\IOFactory;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\file\Entity\File;

/**
 * ADO importer from a Spreadsheet format.
 *
 * @ImporterAdapter(
 *   id = "spreadsheet",
 *   label = @Translation("Spreadsheet Importer"),
 *   remote = false,
 *   batch = false,
 * )
 */
class SpreadsheetImporter extends ImporterAdapterBase {

  /**
   * @var \Drupal\Core\StreamWrapper\StreamWrapperManagerInterface
   */
  protected $streamWrapperManager;

  /**
   * @var \Drupal\ami\AmiUtilityService
   */
  protected $AmiUtilityService;


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
    parent::__construct($configuration, $plugin_id, $plugin_definition, $entityTypeManager);
    $this->streamWrapperManager = $streamWrapperManager;
    $this->AmiUtilityService = $ami_utility;
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
  public function interactiveForm(array $parents = [], FormStateInterface $form_state):array {
    $form = [];
    $form = parent::interactiveForm($parents,$form_state);
    $form['file'] = [
      '#type' => 'managed_file',
      '#default_value' => $form_state->getValue(array_merge($parents , ['file'])),
      '#title' => $this->t('Upload your file'),
      '#description' => $this->t('The Spreadsheet file containing your ADO records.'),
      '#required' => TRUE,
      '#upload_location' => 'public://',
      '#upload_validators' => [
        'file_validate_extensions' => ['csv xls xlsx xlst tsv'],
      ],
    ];

    return $form;
  }


  /**
   * {@inheritdoc}
   */
  public function getData(array $config,  $page = 0, $per_page = 20): array {
    $data = parent::getData($config, $page, $per_page);
    /* @var File $file */
    $file = $this->entityTypeManager->getStorage('file')
      ->load($config['file'][0]);
    $file_path = $this->streamWrapperManager->getViaUri($file->getFileUri())->realpath();
    $offset = $page * $per_page;

      $tabdata = ['headers' => [], 'data' => [], 'totalrows' => 0];

      try {
        $inputFileType = IOFactory::identify($file_path);
        $objReader = IOFactory::createReader($inputFileType);
        $objReader->setReadDataOnly(TRUE);
        $objPHPExcel = $objReader->load($file_path);
      } catch (Exception $e) {
        $this->messenger()->addMessage(
          t(
            'Could not parse file with error: @error',
            ['@error' => $e->getMessage()]
          )
        );
        return $tabdata;
      }

      $table = [];
      $headers = [];
      $maxRow = 0;
      $worksheet = $objPHPExcel->getActiveSheet();
      $highestRow = $worksheet->getHighestRow();
      $highestColumn = $worksheet->getHighestDataColumn(1);

      if (($highestRow) > 1) {
        // Returns Row Headers.
        $rowHeaders = $worksheet->rangeToArray(
          'A1:' . $highestColumn . '1',
          NULL,
          TRUE,
          TRUE,
          FALSE
        );
        $rowHeaders_utf8 = array_map('stripslashes', $rowHeaders[0]);
        $rowHeaders_utf8 = array_map('utf8_encode', $rowHeaders_utf8);
        $rowHeaders_utf8 = array_map('strtolower', $rowHeaders_utf8);
        $rowHeaders_utf8 = array_map('trim', $rowHeaders_utf8);
        $rowHeaders_utf8 = array_filter($rowHeaders_utf8);

        $headercount = count($rowHeaders_utf8);
        foreach ($worksheet->getRowIterator() as $row) {
          $rowindex = $row->getRowIndex();
          if (($rowindex > 1) && ($rowindex > ($offset)) && (($rowindex <= ($offset + $per_page + 1)) || $per_page == -1)) {
            $rowdata = [];
            // gets one row data
            $datarow = $worksheet->rangeToArray(
              "A{$rowindex}:" . $highestColumn . $rowindex,
              NULL,
              TRUE,
              TRUE,
              FALSE
            );
            $flat = trim(implode('', $datarow[0]));
            //check for empty row...if found stop there.
            if (strlen($flat) == 0) {
              $maxRow = $rowindex;
              break;
            }

            $row = $this->AmiUtilityService->arrayEquallySeize(
              $headercount,
              $datarow[0]
            );

            $table[$rowindex] = $datarow[0];
          }
          $maxRow = $rowindex;
        }
      }
      $tabdata = [
        'headers' => $rowHeaders_utf8,
        'data' => $table,
        'totalrows' => $maxRow,
      ];
      $objPHPExcel->disconnectWorksheets();
      return $tabdata;
  }

  public function getInfo(array $config, FormStateInterface $form_state, $page = 0, $per_page = 20): array {
    return $this->getData($config, $form_state, $page,
      $per_page);
  }

}

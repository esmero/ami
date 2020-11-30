<?php

namespace Drupal\ami\Plugin\ImporterAdapter;

use Drupal\google_api_client\Service\GoogleApiClientService;
use Google_Service_Sheets;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\StreamWrapper\StreamWrapperManagerInterface;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\ami\Plugin\ImporterAdapterBase;
use GuzzleHttp\ClientInterface;
use PhpOffice\PhpSpreadsheet\IOFactory;
use Symfony\Component\DependencyInjection\ContainerInterface;

/**
 * Product importer from a CSV format.
 *
 * @ImporterAdapter(
 *   id = "spreadsheet",
 *   label = @Translation("Spreadsheet Importer"),
 *   remote = false
 * )
 */
class SpreadsheetImporter extends ImporterAdapterBase {

  /**
   * @var \Drupal\Core\StreamWrapper\StreamWrapperManagerInterface
   */
  protected $streamWrapperManager;


  /**
   * @var \Drupal\google_api_client\Service\GoogleApiClientService
   */
  protected $googleApiClientService;

  /**
   * {@inheritdoc}
   */
  public function __construct(array $configuration, $plugin_id, $plugin_definition, EntityTypeManagerInterface $entityTypeManager,  StreamWrapperManagerInterface $streamWrapperManager) {
    parent::__construct($configuration, $plugin_id, $plugin_definition, $entityTypeManager);
    $this->streamWrapperManager = $streamWrapperManager;
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
      $container->get('stream_wrapper_manager')
    );
  }


  use StringTranslationTrait;

  /**
   * {@inheritdoc}
   */
  public function getConfigurationForm(\Drupal\ami\Entity\ImporterAdapterInterface $importer) {
    $form = [];
    $config = $importer->getPluginConfiguration();
    $form['file'] = [
      '#type' => 'managed_file',
      '#default_value' => isset($config['file']) ? $config['file'] : '',
      '#title' => $this->t('File'),
      '#description' => $this->t('The CSV file containing your ADO records.'),
      '#required' => TRUE,
      '#upload_location' => 'public://',
      '#upload_validators' => [
        'file_validate_extensions' => ['csv xls xlst tsv'],
      ],
    ];

    return $form;
  }


  /**
   * {@inheritdoc}
   */
  public function getData(array $config, $page = 0, $per_page = 20): array {
    $data = parent::getData($config,$page, $per_page);
    $file_path = $config['file_path'];
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
      $highestColumn = $worksheet->getHighestColumn();
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
            );//Devuelve los titulos de cada columna
            $flat = trim(implode('', $datarow[0]));
            //check for empty row...if found stop there.
            if (strlen($flat) == 0) {
              $maxRow = $rowindex;
              break;
            }
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

    //@TODO we are going to use SplFileObject when actually ingesting.
    /** @var \Drupal\ami\Entity\ImporterAdapterInterface $importer_config */
    $importer_config = $this->configuration['config'];
    $config = $importer_config->getPluginConfiguration();
    $fids = isset($config['file']) ? $config['file'] : [];
    if (!$fids) {
      return [];
    }

    $fid = reset($fids);
    /** @var \Drupal\file\FileInterface $file */
    $file = $this->entityTypeManager->getStorage('file')->load($fid);
    $wrapper = $this->streamWrapperManager->getViaUri($file->getFileUri());
    if (!$wrapper) {
      return [];
    }

    $url = $wrapper->realpath();
    $spl = new \SplFileObject($url, 'r');
    $data = [];
    while (!$spl->eof()) {
      $data[] = $spl->fgetcsv();
    }

    $products = [];
    $header = [];
    foreach ($data as $key => $row) {
      if ($key == 0) {
        $header = $row;
        continue;
      }

      if ($row[0] == "") {
        continue;
      }

      $product = new \stdClass();
      foreach ($header as $header_key => $label) {
        $product->{$label} = $row[$header_key];
      }
      $products[] = $product;
    }

    return $products;
  }


  /**
   * Saves an ADO.
   *
   * @param \stdClass $data
   */
  public function persistEntity($data) {
    /** @var \Drupal\ami\Entity\ImporterAdapterInterface $config */
    $config = $this->configuration['config'];

    $existing = $this->entityTypeManager->getStorage('product')->loadByProperties(['remote_id' => $data->id, 'source' => $config->getSource()]);
    if (!$existing) {
      $values = [
        'remote_id' => $data->id,
        'source' => $config->getSource(),
        'type' => $config->getBundle(),
      ];
      /** @var \Drupal\ami\Entity\ProductInterface $product */
      $product = $this->entityTypeManager->getStorage('product')->create($values);
      $product->setName($data->name);
      $product->setProductNumber($data->number);
      $product->save();
      return;
    }

    if (!$config->updateExisting()) {
      return;
    }

    /** @var \Drupal\ami\Entity\ProductInterface $product */
    $product = reset($existing);
    $product->setName($data->name);
    $product->setProductNumber($data->number);
    $product->save();
  }
}

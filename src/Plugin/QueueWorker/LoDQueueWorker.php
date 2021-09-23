<?php

namespace Drupal\ami\Plugin\QueueWorker;

use Drupal\ami\AmiLoDService;
use Drupal\ami\AmiUtilityService;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Logger\LoggerChannelFactoryInterface;
use Drupal\Core\Messenger\MessengerInterface;
use Drupal\Core\Plugin\ContainerFactoryPluginInterface;
use Drupal\Core\Queue\QueueWorkerBase;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\strawberryfield\StrawberryfieldUtilityService;
use Symfony\Component\DependencyInjection\ContainerInterface;

/**
 * Process each passed label against a set of LoD Urls.
 *
 * @QueueWorker(
 *   id = "ami_lod_ado",
 *   title = @Translation("AMI LoD Reconciling Queue Worker")
 * )
 */
class LoDQueueWorker extends QueueWorkerBase implements ContainerFactoryPluginInterface {

  use StringTranslationTrait;

  /**
   * Drupal\Core\Entity\EntityTypeManager definition.
   *
   * @var \Drupal\Core\Entity\EntityTypeManager
   */
  protected $entityTypeManager;

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
   * @var \Drupal\ami\AmiUtilityService
   */
  protected $AmiUtilityService;

  /**
   * @var \Drupal\ami\AmiLoDService
   */
  protected $AmiLoDService;

  /**
   * The messenger.
   *
   * @var \Drupal\Core\Messenger\MessengerInterface
   */
  protected $messenger;

  /**
   * Constructor.
   *
   * @param array $configuration
   * @param string $plugin_id
   * @param mixed $plugin_definition
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface $entity_type_manager
   * @param \Drupal\Core\Logger\LoggerChannelFactoryInterface $logger_factory
   * @param \Drupal\strawberryfield\StrawberryfieldUtilityService $strawberryfield_utility_service
   * @param \Drupal\ami\AmiUtilityService $ami_utility
   * @param \Drupal\ami\AmiLoDService $ami_lod
   * @param \Drupal\Core\Messenger\MessengerInterface $messenger
   */
  public function __construct(
    array $configuration,
    $plugin_id,
    $plugin_definition,
    EntityTypeManagerInterface $entity_type_manager,
    LoggerChannelFactoryInterface $logger_factory,
    StrawberryfieldUtilityService $strawberryfield_utility_service,
    AmiUtilityService $ami_utility,
    AmiLoDService $ami_lod,
    MessengerInterface $messenger
  ) {
    parent::__construct($configuration, $plugin_id, $plugin_definition);
    $this->entityTypeManager = $entity_type_manager;
    $this->loggerFactory = $logger_factory;
    $this->strawberryfieldUtility = $strawberryfield_utility_service;
    $this->AmiUtilityService = $ami_utility;
    $this->AmiLoDService = $ami_lod;
    $this->messenger = $messenger;
  }

  /**
   * Implementation of the container interface to allow dependency injection.
   *
   * @param \Symfony\Component\DependencyInjection\ContainerInterface $container
   * @param array $configuration
   * @param string $plugin_id
   * @param mixed $plugin_definition
   *
   * @return static
   */
  public static function create(
    ContainerInterface $container,
    array $configuration,
    $plugin_id,
    $plugin_definition
  ) {
    return new static(
      empty($configuration) ? [] : $configuration,
      $plugin_id,
      $plugin_definition,
      $container->get('entity_type.manager'),
      $container->get('logger.factory'),
      $container->get('strawberryfield.utility'),
      $container->get('ami.utility'),
      $container->get('ami.lod'),
      $container->get('messenger')
    );
  }

  /**
   * {@inheritdoc}
   */
  public function processItem($data) {
    /* Data info has this structure
     $data->info = [
            'label' => The label passed to the Reconciling URL,
            'domain' => This Server's Domain name
            'headers' => All headers (LoD Routes) as key => value pairs
            'normalized_mappings' => an array with source columns and where to find the results
             like
              array:2 [▼
                "mods_name_personal_namepart" => array:2 [▼
                    0 => "loc_names_thing"
                    1 => "loc_rdftype_personalname"
              ]
             "mods_genre" => array:3 [▼
                0 => "loc_rdftype_genreform"
                1 => "getty_aat_fuzzy"
                2 => "wikidata_subjects_thing"
      ]
    ]
            'lodconfig' => an array of LoD URL Route arguments separated by comma in the form of,
               0 => "loc;subjects;thing"
               1 => "loc;rdftype;GenreForm"
               2 => "getty;aat;exact"
            'set_id' =>  The Set id
            'csv' => The ID of the CSV file that will hold the results
            'uid' => The User ID that processed the Set
            'set_url' => A direct URL to the set.
            'attempt' => The number of attempts to process. We always start with a 1
          ];
    */
    // Load the CSV
    /** @var \Drupal\file\Entity\File $file_lod */
    $file_lod = $this->entityTypeManager->getStorage('file')->load(
      $data->info['csv']);

    if (empty($data->info['label']) || empty($data->info['domain']) || empty ($data->info['lodconfig'])) {
      // Exception, means we have no label, no domain or empty lodconfig
      return;
    }
    $newdata['headers'] = $data->info['headers'];
    $newdata['data'][0] = array_fill_keys($newdata['headers'], '');
    $context_data = [];
    if (isset($data->info['lodconfig']) && is_array($data->info['lodconfig']) && $file_lod) {
      $lod_route_arguments = $data->info['lodconfig'];
      foreach ($lod_route_arguments as $lod_route_argument) {
        $lod_route_argument_list = explode(';', $lod_route_argument);
        //@TODO allow the number of results to be set on the \Drupal\ami\Form\amiSetEntityReconcileForm
        // And passed as an argument. Same with Language? Not all LoD Routes can make use or more languages.
        $lod_route_column_name = strtolower(implode('_', $lod_route_argument_list));
        $lod = $this->AmiLoDService->invokeLoDRoute($data->info['domain'],
        $data->info['label'], $lod_route_argument_list[0],
        $lod_route_argument_list[1], $lod_route_argument_list[2], 'en', 1);

        $newdata['data'][0][$lod_route_column_name] = json_encode($lod, JSON_PRETTY_PRINT|JSON_UNESCAPED_SLASHES|JSON_UNESCAPED_UNICODE) ?? '';
        $newdata['data'][0]['original'] = (string) $data->info['label'];
        $newdata['data'][0]['csv_columns'] = json_encode((array)$data->info['csv_columns']) ?? '';
        // Adds a "Checked" column used to mark manually reconciliated elements.
        $newdata['data'][0]['checked'] = FALSE;
        // Context data is simpler
        $context_data[$lod_route_column_name]['lod'] = $lod;
        $context_data[$lod_route_column_name]['columns'] = $data->info['csv_columns'];
      }

      $this->AmiUtilityService->csv_append($newdata, $file_lod,NULL, FALSE);
      // Sets the same data, per label (as key) into keystore so we can fetch it as Twig Context when needed.
      //@TODO also do similar if going for a "direct" in that case we replace the columns found in the original data
      $this->AmiUtilityService->setKeyValuePerAmiSet($data->info['label'], $context_data, $data->info['set_id']);
    }
  }

}

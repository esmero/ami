<?php

namespace Drupal\ami\Plugin\Action;

use Drupal\Core\Render\RendererInterface;
use Drupal\Core\StreamWrapper\StreamWrapperManagerInterface;
use Drupal\Core\Url;
use Drupal\Core\Link;
use Drupal\Core\Access\AccessResult;
use Drupal\Core\Action\ConfigurableActionBase;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Plugin\ContainerFactoryPluginInterface;
use Drupal\Core\Plugin\PluginFormInterface;
use Drupal\Core\Session\AccountInterface;
use Drupal\Core\TempStore\PrivateTempStoreFactory;
use Drupal\Component\Plugin\DependentPluginInterface;
use Drupal\ami\AmiUtilityService;
use Drupal\strawberryfield\StrawberryfieldUtilityService;
use Drupal\strawberryfield\Tools\StrawberryfieldJsonHelper;
use Drupal\views\ViewExecutable;
use Drupal\views_bulk_operations\Action\ViewsBulkOperationsActionCompletedTrait;
use Drupal\views_bulk_operations\Action\ViewsBulkOperationsActionInterface;
use Drupal\views_bulk_operations\Action\ViewsBulkOperationsPreconfigurationInterface;
use Psr\Log\LoggerInterface;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\HttpFoundation\RedirectResponse;

/**
 * Provides an action that export SBFs to CSV.
 *
 * @Action(
 *   id = "entity:ami_csvexport_action",
 *   action_label = @Translation("Export Archipelago Digital Objects to CSV"),
 *   category = @Translation("AMI Metadata"),
 *   deriver = "Drupal\ami\Plugin\Action\Derivative\EntitySbfActionDeriver",
 *   type = "node",
 *   pass_context = TRUE,
 *   pass_view = TRUE,
 *   confirm = "true"
 * )
 */
class AmiStrawberryfieldCSVexport extends ConfigurableActionBase implements DependentPluginInterface, ContainerFactoryPluginInterface, ViewsBulkOperationsActionInterface, ViewsBulkOperationsPreconfigurationInterface, PluginFormInterface {

  use ViewsBulkOperationsActionCompletedTrait;
  const FILE_EXTENSION = 'csv';

  /**
   * Action context.
   *
   * @var array
   *   Contains view data and optionally batch operation context.
   */
  protected $context;

  /**
   * @var \Drupal\ami\AmiUtilityService
   */
  protected $AmiUtilityService;

  /**
   * The processed view.
   *
   * @var \Drupal\views\ViewExecutable
   */
  protected $view;

  /**
   * Configuration array.
   *
   * @var array
   */
  protected $configuration;


  /**
   * The entity type manager.
   *
   * @var \Drupal\Core\Entity\EntityTypeManagerInterface
   */
  protected $entityTypeManager;

  /**
   * The tempstore object.
   *
   * @var \Drupal\Core\TempStore\SharedTempStore
   */
  protected $tempStore;

  /**
   * The current user.
   *
   * @var \Drupal\Core\Session\AccountInterface
   */
  protected $currentUser;
  /**
   * The Strawberry Field Utility Service.
   *
   * @var \Drupal\strawberryfield\StrawberryfieldUtilityService
   */
  protected $strawberryfieldUtility;


  /**
   * @var \Psr\Log\LoggerInterface
   */
  protected $logger;

  /**
   * The streamWrapperManager
   *
   * @var \Drupal\Core\StreamWrapper\StreamWrapperManagerInterface
   */
  private $streamWrapperManager;

  /**
   * The renderer
   *
   * @var \Drupal\Core\Render\RendererInterface
   */
  private $renderer;

  /**
   * AmiStrawberryfieldCSVexport constructor.
   *
   * @param array $configuration
   * @param $plugin_id
   * @param $plugin_definition
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface $entity_type_manager
   * @param \Drupal\Core\TempStore\PrivateTempStoreFactory $temp_store_factory
   * @param \Drupal\Core\Session\AccountInterface $current_user
   * @param \Drupal\strawberryfield\StrawberryfieldUtilityService $strawberryfield_utility_service
   * @param \Psr\Log\LoggerInterface $logger
   * @param \Drupal\Core\Render\RendererInterface $renderer
   * @param \Drupal\Core\StreamWrapper\StreamWrapperManagerInterface $streamWrapperManager
   * @param \Drupal\ami\AmiUtilityService $ami_utility
   *
   */
  public function __construct(array $configuration, $plugin_id, $plugin_definition, EntityTypeManagerInterface $entity_type_manager, PrivateTempStoreFactory $temp_store_factory, AccountInterface $current_user, StrawberryfieldUtilityService $strawberryfield_utility_service, LoggerInterface $logger,  RendererInterface $renderer, StreamWrapperManagerInterface $streamWrapperManager, AmiUtilityService $ami_utility) {
    parent::__construct($configuration, $plugin_id, $plugin_definition);
    $this->entityTypeManager = $entity_type_manager;
    $this->currentUser = $current_user;
    $this->tempStore = $temp_store_factory->get('amiaction_csv_export');
    $this->strawberryfieldUtility = $strawberryfield_utility_service;
    $this->logger = $logger;
    $this->renderer = $renderer;
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
      $container->get('tempstore.private'),
      $container->get('current_user'),
      $container->get('strawberryfield.utility'),
      $container->get('logger.factory')->get('action'),
      $container->get('renderer'),
      $container->get('stream_wrapper_manager'),
      $container->get('ami.utility')
    );
  }


  /**
   * {@inheritdoc}
   */
  public function setContext(array &$context) {
    $this->context['sandbox'] = &$context['sandbox'];
    foreach ($context as $key => $item) {
      if ($key === 'sandbox') {
        continue;
      }
      $this->context[$key] = $item;
    }
  }

  /**
   * {@inheritdoc}
   */
  public function setView(ViewExecutable $view) {
    $this->view = $view;
  }

  /**
   * {@inheritdoc}
   */
  public function executeMultiple(array $objects) {
    $results = $response = $errors = [];
    foreach ($objects as $entity) {
      $result = $this->execute($entity);
      if ($result) {
        $this->processHeader(array_keys($result));
      } else {
        $errors[] = $this->t("Errors on: item @label, could not be exported<br>",
          ['@label' => $entity->label()]);
      }
      $results[] = $result;
      //$this->context['sandbox']['processed']++;
    }
    $this->saveRows($results);
    $response[] =  $this->t("@total successfully processed items in batch @batch",
      [
        '@total' => count(array_filter($results)),
        '@batch' => $this->context['sandbox']['current_batch']
      ]);
    // Generate the output file if the last row has been processed.
    if (!isset($this->context['sandbox']['total']) || ($this->context['sandbox']['processed'] + $this->context['sandbox']['batch_size']) >= $this->context['sandbox']['total']) {
      $output = $this->generateOutput();
      $this->sendToFile($output);
      $response[] =  $this->t("CSV export done: @total items processed.",
        ['@total' => $this->context['sandbox']['total']]);
    }
    return array_merge($response, $errors);
  }

  /**
   * {@inheritdoc}
   */
  public function execute($entity = NULL) {

    /** @var \Drupal\Core\Entity\EntityInterface $entity */
    $row = [];

    if ($entity) {
      if ($sbf_fields = $this->strawberryfieldUtility->bearsStrawberryfield(
        $entity
      )) {
        foreach ($sbf_fields as $field_name) {
          /* @var $field \Drupal\Core\Field\FieldItemInterface */
          $field = $entity->get($field_name);
          /* @var \Drupal\strawberryfield\Field\StrawberryFieldItemList $field */
          $entity = $field->getEntity();
          /** @var $field \Drupal\Core\Field\FieldItemList */
          foreach ($field->getIterator() as $delta => $itemfield) {
            /** @var $itemfield \Drupal\strawberryfield\Plugin\Field\FieldType\StrawberryFieldItem */
            $fullvalues = $itemfield->provideDecoded(TRUE);
            $row['node_uuid'] = $entity->uuid();
            if ($this->configuration['expand_nodes_to_uuids']) {
              // UUID-dify the mappings here
              if (isset($fullvalues['ap:entitymapping']['entity:node']) && is_array($fullvalues['ap:entitymapping']['entity:node'])) {
                foreach ($fullvalues['ap:entitymapping']['entity:node'] as $jsonkey_with_nodeids) {
                  if ($this->configuration['create_ami_set']) {
                    $this->context['sandbox']['parent_columns'] = $this->context['sandbox']['parent_columns'] ?? [];
                    $this->context['sandbox']['parent_columns'][] = $jsonkey_with_nodeids;
                    $this->context['sandbox']['parent_columns'] = array_unique($this->context['sandbox']['parent_columns']);
                  }
                  if (isset($fullvalues[$jsonkey_with_nodeids])) {
                    if (is_array($fullvalues[$jsonkey_with_nodeids])) {
                      foreach ($fullvalues[$jsonkey_with_nodeids] as $key => $nodeid) {
                        $related_entity = $this->entityTypeManager->getStorage('node')
                          ->load($nodeid);
                        if ($related_entity) {
                          $fullvalues[$jsonkey_with_nodeids][$key] = $related_entity->uuid();
                        }
                      }
                      // This will string-ify multiple related NODES to a single ; separated list of UUIDs.
                      $fullvalues[$jsonkey_with_nodeids] = implode(";",
                        $fullvalues[$jsonkey_with_nodeids]);
                    }
                    else {
                      $related_entity = $this->entityTypeManager->getStorage('node')
                        ->load($fullvalues[$jsonkey_with_nodeids]);
                      if ($related_entity) {
                        $fullvalues[$jsonkey_with_nodeids] = $related_entity->uuid();
                      }
                    }
                  }
                }
              }
            }
            // If two types have different bundles only one will win. Do not do that ok?
            if ($this->configuration['create_ami_set']) {
              $this->context['sandbox']['type_bundle'] = $this->context['sandbox']['type_bundle'] ?? [];
              $this->context['sandbox']['type_bundle'][$fullvalues['type']] = $entity->bundle().':'.$field_name;
            }

            if ($this->configuration['no_media']) {
              // Remove all as:type keys and keys with files
              if (isset($fullvalues['ap:entitymapping']['entity:file']) && is_array($fullvalues['ap:entitymapping']['entity:file'])) {
                $fullvalues = array_diff_key($fullvalues, array_flip(array_merge($fullvalues['ap:entitymapping']['entity:file'], StrawberryfieldJsonHelper::AS_FILE_TYPE)));
              }
            }
            if ($this->configuration['migrate_media'] && !$this->configuration['no_media']) {
              $ordersubkey = 'sequence';
              if (isset($fullvalues['ap:entitymapping']['entity:file']) && is_array($fullvalues['ap:entitymapping']['entity:file'])) {
                // Clear the original Keys (with File ids out first)
                $fullvalues = array_diff_key($fullvalues, array_flip(array_merge($fullvalues['ap:entitymapping']['entity:file'])));
                foreach (StrawberryfieldJsonHelper::AS_FILE_TYPE as $as_key) {
                  if (isset($fullvalues[$as_key]) && is_array($fullvalues[$as_key])) {
                    StrawberryfieldJsonHelper::orderSequence($fullvalues, $as_key,
                      $ordersubkey);
                    foreach ($fullvalues[$as_key] as $mediaentry) {
                      if  ($mediaentry['dr:uuid'] && $mediaentry['name'] && $mediaentry['dr:for']) {
                        $link = Url::fromRoute('format_strawberryfield.iiifbinary',
                          [
                            'node' => $entity->id(),
                            'uuid' => $mediaentry['dr:uuid'],
                            'format' => $mediaentry['name']
                          ],
                          ['absolute' => TRUE]
                        )->toString();
                        $fullvalues[$mediaentry['dr:for']] =  empty($fullvalues[$mediaentry['dr:for']]) ? $link : $fullvalues[$mediaentry['dr:for']] . ';' . $link;
                        if ($this->configuration['create_ami_set']) {
                          // set context for all accumulated file columns
                          $this->context['sandbox']['file_columns'] = $this->context['sandbox']['file_columns'] ?? [];
                          $this->context['sandbox']['file_columns'][] = $mediaentry['dr:for'];
                          $this->context['sandbox']['file_columns'] = array_unique($this->context['sandbox']['file_columns']);
                        }
                      }
                    }
                  }
                }
                // Now remove technical metadata
                $fullvalues = array_diff_key($fullvalues, array_flip(StrawberryfieldJsonHelper::AS_FILE_TYPE));
              }
            }
            foreach($fullvalues as $key => $fullvalue) {
              $row[$key] = is_array($fullvalue) ? json_encode($fullvalue, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_HEX_QUOT ,512) : $fullvalue;
            }
          }
        }
      }
    }
    return $row;
  }


  protected function generateOutput() {
    $rows = [];
    for ($i = 1; $i <= $this->context['sandbox']['current_batch']; $i++) {
      $chunk = $this->tempStore->get($this->context['sandbox']['cid_prefix'] . $i);
      if ($chunk) {
        $keys = $this->context['sandbox']['headers'];
        $template = array_fill_keys($keys, NULL);
        $new_chunk = array_map(function($item) use ($template) {
          return array_merge($template, $item);
        }, $chunk);
        $rows = array_merge($rows, $new_chunk);
        $this->tempStore->delete($this->context['sandbox']['cid_prefix'] . $i);
      }
    }
    return $rows;
  }



  /**
   * Output generated string to file. Message user.
   *
   * @param string $output
   *   The string that will be saved to a file.
   */
  protected function sendToFile($output) {
    if (!empty($output)) {
      $data['data'] = $output;
      $data['headers'] = $this->context['sandbox']['headers'];
      $file_id = $this->AmiUtilityService->csv_save($data, 'node_uuid');
      if ($file_id && $this->configuration['create_ami_set']) {
        $amisetdata = new \stdClass();
        $amisetdata->plugin = 'spreadsheet';
        /* start definitions to make php8 happy */
        $amisetdata->pluginconfig = new \stdClass();
        $amisetdata->adomapping = new \stdClass();
        $amisetdata->mapping = new \stdClass();
        $amisetdata->adomapping->base = new \stdClass();
        $amisetdata->adomapping->uuid = new \stdClass();
        $amisetdata->mapping->custommapping_settings = new \stdClass();
        $amisetdata->pluginconfig->op = 'update';
        $amisetdata->pluginconfig->file = [$file_id];
        $amisetdata->adomapping->base->label = "label";
        $amisetdata->adomapping->uuid->uuid = "node_uuid";
        $amisetdata->adomapping->parents = $this->context['sandbox']['parent_columns'] ?? [];
        $amisetdata->adomapping->autouuid = 0;
        $amisetdata->mapping->globalmapping = "custom";
        // Set by ::execute()
        foreach ($this->context['sandbox']['type_bundle'] as $type => $bundle_field) {
          {$type} = new \stdClass();
          $amisetdata->mapping->custommapping_settings->{$type}->files = $this->context['sandbox']['file_columns'] ?? [];
          $amisetdata->mapping->custommapping_settings->{$type}->bundle = $bundle_field;
          $amisetdata->mapping->custommapping_settings->{$type}->metadata = "direct";
        }
        $amisetdata->csv = $file_id;
        $amisetdata->column_keys = $this->context['sandbox']['headers'] ?? [];
        $amisetdata->total_rows = count($output);
        $amisetdata->zip = null;
        $amisetdata->name = trim($this->configuration['create_ami_set_name']);
        $amisetdata->name = strlen($amisetdata->name) > 0 ? $amisetdata->name : NULL;
        $amiset_id = $this->AmiUtilityService->createAmiSet($amisetdata);
        if ($amiset_id) {
          $url = Url::fromRoute('entity.ami_set_entity.canonical',
            ['ami_set_entity' => $amiset_id]);
          $this->messenger()
            ->addStatus($this->t('Well Done! New AMI Set was created and you can <a href="@url">see it here</a>',
              ['@url' => $url->toString()]));
        }
      }
    }
  }


  public function buildPreConfigurationForm(array $element, array $values, FormStateInterface $form_state) {
  }

  public function buildConfigurationForm(array $form, FormStateInterface $form_state) {
    $form['expand_nodes_to_uuids'] = [
      '#type' => 'checkbox',
      '#title' => $this->t('Expand related ADOs to UUIDs'),
      '#default_value' => ($this->configuration['expand_nodes_to_uuids'] === FALSE) ? FALSE : TRUE,
      '#size' => '40',
      '#description' => t('When enabled all related ADOs (ismemberof, etc) are going to be expandad to their UUIDs. This allows changes to parentship to be made on the CSV.'),
    ];
    $form['no_media'] = [
      '#type' => 'checkbox',
      '#title' => $this->t('Do not export Media/Files'),
      '#default_value' => ($this->configuration['no_media'] === FALSE) ? FALSE : TRUE,
      '#size' => '40',
      '#description' => t('When enabled File references and their associated technical Metadata (<em>as:filetype</em> JSON keys, e.g <em>as:image</em>) will be skipped. This allows pure Descriptive Metadata to be exported to CSV'),
    ];

    $form['migrate_media'] = [
      '#title' => $this->t('Convert Media to Portable Absolute URLs.'),
      '#description' => $this->t('When enabled all File references will be converted to absolute URLs and their associated technical Metadata (<em>as:filetype</em> JSON keys, e.g <em>as:image</em>) will be skipped. This allows CSVs to be used to ingest new ADOs in other repositories.'),
      '#type' => 'checkbox',
      '#default_value' => ($this->configuration['migrate_media'] === FALSE) ? FALSE : TRUE,
    ];
    $form['create_ami_set'] = [
      '#title' => $this->t('Attach CSV to a new AMI Set.'),
      '#description' => $this->t('When checked a new AMI set with the exported data will be created and configured for "Updating" existing ADOs.'),
      '#type' => 'checkbox',
      '#default_value' => ($this->configuration['create_ami_set'] === FALSE) ? FALSE : TRUE,
    ];
    $form['create_ami_set_name'] = [
      '#title' => $this->t('Please Give your AMI Set a name. If empty we will create a (quite) generic one for you'),
      '#type' => 'textfield',
      '#size' => '40',
      '#default_value' => $this->configuration['create_ami_set_name']
    ];
    return $form;
  }

  public function submitConfigurationForm(array &$form, FormStateInterface $form_state) {
    $this->configuration['expand_nodes_to_uuids'] = $form_state->getValue('expand_nodes_to_uuids');
    $this->configuration['no_media'] = $form_state->getValue('no_media');
    $this->configuration['migrate_media'] = $form_state->getValue('migrate_media');
    $this->configuration['create_ami_set'] = $form_state->getValue('create_ami_set');
    $this->configuration['create_ami_set_name'] = $form_state->getValue('create_ami_set_name');
  }

  /**
   * {@inheritdoc}
   */
  public function validateConfigurationForm(array &$form, FormStateInterface $form_state) {

  }

  /**
   * {@inheritdoc}
   */
  public function defaultConfiguration() {
    return [
      'expand_nodes_to_uuids' => FALSE,
      'no_media' => FALSE,
      'migrate_media' => FALSE,
      'create_ami_set' => FALSE,
      'create_ami_set_name' => 'CSV Export/Import AMI Set',
    ];
  }

  /**
   * Default custom access callback.
   *
   * @param \Drupal\Core\Session\AccountInterface $account
   *   The user the access check needs to be preformed against.
   * @param \Drupal\views\ViewExecutable $view
   *   The View Bulk Operations view data.
   *
   * @return bool
   *   Has access.
   */
  public static function customAccess(AccountInterface $account, ViewExecutable $view) {
    return TRUE;
  }

  /**
   * {@inheritdoc}
   */
  public function access($object, AccountInterface $account = NULL, $return_as_object = FALSE) {

    /** @var \Drupal\Core\Entity\EntityInterface $object */
    $result = $object->access('view', $account, TRUE)
      ->andIf(AccessResult::allowedIfHasPermission($account, 'CSV Export Archipelago Digital Objects'));
    return $return_as_object ? $result : $result->isAllowed();
  }

  /**
   * {@inheritdoc}
   */
  public function calculateDependencies() {
    $module_name = $this->entityTypeManager
      ->getDefinition($this->getPluginDefinition()['type'])
      ->getProvider();
    return ['module' => [$module_name]];
  }

  /**
   * Saves batch data into Private storage.
   *
   * @param array $rows
   *   Rows from batch.
   */
  protected function saveRows(array &$rows) {
    $this->tempStore->set($this->getCid(), $rows);
    unset($rows);
  }
  /**
   * Saves combined header data into the batch context
   *
   * @param array $rows
   *   Rows from batch.
   */
  protected function processHeader(array $header) {
    if (!isset($this->context['sandbox']['headers'])) {
      $this->context['sandbox']['headers'] = $header;
    }
    else {
      $this->context['sandbox']['headers'] = array_unique(array_merge($this->context['sandbox']['headers'], $header));
    }
  }

  /**
   * Gets Cache ID for current batch.
   *
   * @return string
   *   Cache unique ID for Temporary storage.
   */
  protected function getCid() {
    if (!isset($this->context['sandbox']['cid_prefix'])) {
      $this->context['sandbox']['cid_prefix'] = $this->context['view_id'] . ':'
        . $this->context['display_id'] . ':' . $this->context['action_id'] . ':'
        . md5(serialize(array_keys($this->context['list']))) . ':';
    }

    return $this->context['sandbox']['cid_prefix'] . $this->context['sandbox']['current_batch'];
  }

  /**
   * Prepares sandbox data (header and cache ID).
   *
   * @return array
   *   Table header.
   */
  protected function getHeader() {
    // Build output header array.
    $header = &$this->context['sandbox']['header'];
    if (!empty($header)) {
      return $header;
    }
    return $this->setHeader();
  }

  public function getConfiguration() {
    return parent::getConfiguration(); // TODO: Change the autogenerated stub
  }

  public function setConfiguration(array $configuration) {
    parent::setConfiguration(
      $configuration
    ); // TODO: Change the autogenerated stub
  }


}

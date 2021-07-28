<?php
/**
 * @file
 * Contains \Drupal\ami\Form\AmiMultiStepIngestBaseForm.
 */

namespace Drupal\ami\Form;

use Drupal\ami\AmiUtilityService;
use Drupal\ami\Entity\ImporterAdapter;
use Drupal\ami\Plugin\ImporterAdapterManager;
use Drupal\Component\Transliteration\TransliterationInterface;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Session\AccountInterface;
use Drupal\Core\Session\SessionManagerInterface;
use Drupal\Core\TempStore\PrivateTempStoreFactory;
use Drupal\Core\Url;

/**
 * Provides a form for that invokes Importer Adapter Plugins and batch imports data
 *
 * This form provides 6 steps
 *  Step 1: select the Importer Plugin to be used
 *  Step 2: Provide the data the Plugin requires or load a saved config
 *  Step 3: Map Columns to Metadata info
 *  Step 4: Map Columns to Node entity Info
 *  Step 5: Provide Binaries if not remote and ingest
 *  last step can be overridden from the Base Class via $lastStep = int()
 * @ingroup ami
 */
class AmiMultiStepIngest extends AmiMultiStepIngestBaseForm {

  protected $lastStep = 6;

  /**
   * Holds a ready select options array with usable metadata displays
   *
   * @var array
   */
  protected $metadatadisplays = [];

  /**
   * Holds a ready select options array with usable webforms
   *
   * @var array
   */
  protected $webforms = [];


  /**
   * Holds a ready select options array with usable webforms
   *
   * @var array
   */
  protected $bundlesAndFields = [];

  /**
   * {@inheritdoc}
   */
  public function getFormId() : string {
    return 'ami_multistep_import_form';
  }

  public function __construct(PrivateTempStoreFactory $temp_store_factory, SessionManagerInterface $session_manager, AccountInterface $current_user, ImporterAdapterManager $importerManager, AmiUtilityService $ami_utility,  EntityTypeManagerInterface $entity_type_manager, TransliterationInterface $transliteration) {
    parent::__construct($temp_store_factory, $session_manager, $current_user, $importerManager, $ami_utility,  $entity_type_manager, $transliteration);
    $this->metadatadisplays = $this->AmiUtilityService->getMetadataDisplays();
    $this->webforms = $this->AmiUtilityService->getWebforms();
    $this->bundlesAndFields = $this->AmiUtilityService->getBundlesAndFields();
  }

  /**
   * {@inheritdoc}
   */
  public function buildForm(array $form, FormStateInterface $form_state) {
    $form = parent::buildForm($form, $form_state);
    $form['message-step'] = [
      '#markup' => '<div class="step">' . $this->t('AMI step @step of @laststep',[
          '@step' => $this->step,
          '@laststep' => $this->lastStep,
        ]) . '</div>',
    ];
    if ($this->step == 1) {
      $pluginValue = $this->store->get('plugin', NULL);
      $definitions = $this->importerManager->getDefinitions();
      $options = [];
      foreach ($definitions as $id => $definition) {
        $options[$id] = $definition['label'];
      }

      $form['plugin'] = [
        '#type' => 'select',
        '#title' => $this->t('Plugin'),
        '#default_value' => $pluginValue, // $importer->getPluginId(),
        '#options' => $options,
        '#description' => $this->t('The plugin to be used to import ADOs.'),
        '#required' => TRUE,
        '#empty_option' => $this->t('- Please select a plugin -'),
      ];
    }
    if ($this->step == 2) {
      $parents = ['pluginconfig'];
      $form_state->setValue('pluginconfig', $this->store->get('pluginconfig',[]));
      $pluginValue = $this->store->get('plugin', NULL);
      // Only create a new instance if we do not have the PluginInstace around
      /* @var $plugin_instance \Drupal\ami\Plugin\ImporterAdapterInterface | NULL */
      $plugin_instance = $this->store->get('plugininstance');
      if (!$plugin_instance || $plugin_instance->getPluginid() != $pluginValue || $pluginValue == NULL) {
        $configuration = [];
        $configuration['config'] = ImporterAdapter::create();
        $plugin_instance = $this->importerManager->createInstance($pluginValue,$configuration);
        $this->store->set('plugininstance',$plugin_instance);
      }
      $form['pluginconfig'] = $plugin_instance->interactiveForm($parents, $form_state);
      $form['pluginconfig']['#tree'] = TRUE;
    }
    // TO keep this discrete and easier to edit maybe move to it's own method?
    if ($this->step == 3) {
      // We should never reach this point if data is not enough. Submit handler
      // will back to Step 2 if so.
      $data = $this->store->get('data');
      $pluginconfig = $this->store->get('pluginconfig');
      $plugin_instance = $this->store->get('plugininstance');
      $op = $pluginconfig['op'];
      $column_keys = $plugin_instance->provideKeys($pluginconfig, $data);
      $mapping = $this->store->get('mapping');
      $metadata = [
        'direct' => 'Direct ',
        'template' => 'Template',
        //'webform' => 'Webform',
      ];
      $template = $this->getMetadatadisplays();
      $webform = $this->getWebforms();
      $bundle = $this->getBundlesAndFields();


      $global_metadata_options = $metadata + ['custom' => 'Custom (Expert Mode)'];
      //Each row (based on its type column) can have its own approach setup(expert mode)
      $element_conditional = [];
      $element = [];
      $element['bundle'] =[
        '#type' => 'select',
        '#title' => $this->t('Fields and Bundles'),
        '#options' => $bundle,
        '#description' => $this->t('Destination Field/Bundle for New ADOs'),
      ];

      $element_conditional['template'] = [
        '#type' => 'select',
        '#title' => $this->t('Template'),
        '#options' => $template,
        '#description' => $this->t('Columns will be casted to ADO metadata (JSON) using a Twig template setup for JSON output'),
      ];

      $element_conditional['webform'] =[
        '#type' => 'select',
        '#title' => $this->t('Webform'),
        '#options' => $webform,
        '#description' => $this->t('Columns are casted to ADO metadata (JSON) by passing/validating Data through an existing Webform'),
      ];

      $form['ingestsetup']['globalmapping'] = [
        '#type' => 'select',
        '#title' => $this->t('Select the data transformation approach'),
        '#default_value' => isset($mapping['globalmapping']) && !empty($mapping['globalmapping']) ? $mapping['globalmapping'] : reset($global_metadata_options),
        '#options' => $global_metadata_options,
        '#description' => $this->t('How your source data will be transformed into ADOs Metadata.'),
        '#required' => TRUE,
      ];
      $newelements_global = $element_conditional;
      foreach ($newelements_global as $key => &$subelement) {
        $subelement['#default_value'] = isset($mapping['globalmapping_settings']['metadata_config'][$key]) ? $mapping['globalmapping_settings']['metadata_config'][$key]: reset(${$key});
        $subelement['#states'] = [
          'visible' => [
            ':input[name*="globalmapping"]' => ['value' => $key],
          ],
        ];
      }
      $form['ingestsetup']['metadata_config'] = $newelements_global;

      $form['ingestsetup']['files'] = [
        '#type' => 'select',
        '#title' => $this->t('Select which columns contain filenames, entities or URLs where we can fetch files'),
        '#default_value' => isset($mapping['globalmapping_settings']['files']) ? $mapping['globalmapping_settings']['files'] : [],
        '#options' => array_combine($column_keys, $column_keys),
        '#size' => count($column_keys),
        '#multiple' => TRUE,
        '#description' => $this->t('From where your files will be fetched to be uploaded and attached to an ADOs and described in the Metadata.'),
        '#empty_option' => $this->t('- Please select columns -'),
        '#states' => [
          'visible' => [
            ':input[name*="globalmapping"]' => ['!value' => 'custom'],
          ],
        ]
      ];

      $form['ingestsetup']['bundle'] = $element['bundle'];
      $form['ingestsetup']['bundle']['#default_value'] = isset($mapping['globalmapping_settings']['bundle']) ? $mapping['globalmapping_settings']['bundle'] : reset($bundle);
      $form['ingestsetup']['bundle']['#states'] = [
        'visible' => [
          ':input[name*="globalmapping"]' => ['!value' => 'custom'],
        ],
      ];

      // Get all headers and check for a 'type' key first, if not allow the user to select one?
      // Wonder if we can be strict about this and simply require always a "type"?
      // @TODO WE need to check for 'type' always. Maybe even in the submit handler?
      $alltypes = $plugin_instance->provideTypes($pluginconfig, $data);
      if (!empty($alltypes)) {
        $form['ingestsetup']['custommapping'] = [
          '#type' => 'fieldset',
          '#tree' => TRUE,
          '#title' => t('Please select your custom data transformation and mapping options'),
          '#states' => [
            'visible' => [
              ':input[name*="globalmapping"]' => ['value' => 'custom'],
            ],
          ]
        ];
        foreach ($alltypes as $column_index => $type) {
          // Transliterate $types we can use them as machine names
          $machine_type = $this->getMachineNameSuggestion($type);
          $form['ingestsetup']['custommapping'][$type] = [
            '#type' => 'details',
            '#title' => t('For @type', ['@type' => $type]),
            '#description' => t('Choose your transformation option'),
            '#open' => TRUE, // Controls the HTML5 'open' attribute. Defaults to FALSE.
          ];
          $form['ingestsetup']['custommapping'][$type]['metadata'] = [
            //'#name' => 'metadata_'.$machine_type,
            '#type' => 'select',
            '#title' => $this->t('Select the data transformation approach for @type', ['@type' => $type]),
            '#default_value' => isset($mapping['custommapping_settings'][$type]['metadata']) ? $mapping['custommapping_settings'][$type]['metadata'] : reset($metadata),
            '#options' => $metadata,
            '#description' => $this->t('How your source data will be transformed into ADOs (JSON) Metadata.'),
            '#required' => TRUE,
            '#attributes' =>  [
              'data-adotype' => 'metadata_'.$machine_type
            ],
          ];
          // We need to reassign or if not circular references mess with the render array
          $newelements = $element_conditional;
          foreach ($newelements as $key => &$subelement) {
            $subelement['#default_value'] = isset($mapping['custommapping_settings'][$type]['metadata_config'][$key]) ? $mapping['custommapping_settings'][$type]['metadata_config'][$key] : reset(${$key});
            $subelement['#states'] = [
              'visible' => [
                ':input[data-adotype="metadata_'.$machine_type.'"]' => ['value' => $key],
              ],
              'required' => [
                ':input[data-adotype="metadata_'.$machine_type.'"]' => ['value' => $key],
              ]
            ];
          }

          $form['ingestsetup']['custommapping'][$type]['metadata_config'] = $newelements;
          $form['ingestsetup']['custommapping'][$type]['bundle'] = $element['bundle'];

          $form['ingestsetup']['custommapping'][$type]['bundle']['#default_value'] = isset($mapping['custommapping_settings'][$type]['bundle']) ? $mapping['custommapping_settings'][$type]['bundle'] : reset($bundle);

          if ($op == 'update' || $op == 'patch') {
            $files_title = $this->t('Select which columns contain filenames or URLs where we can fetch the files for @type replacing/clearing existing ones if there is already data in the same key in your ADO.', ['@type' => $type]);
            $files_description = $this->t('<b>WARNING:</b> If you want to keep existing files for an existing column, Do <em>not</em> select it. <br/> If you do so those will be replaced by the new ones provided in your data set or deleted if the value under that column is EMPTY.<br/> AMI uses semicolons ";" to separate multiple files or URLs inside a single cell.');
          }
          else {
            $files_title = $this->t('Select which columns contain filenames or URLs where we can fetch the files for @type', ['@type' => $type]);
            $files_description = $this->t('From where your files will be fetched to be uploaded and attached to an ADOs and described in the Metadata. <br/> AMI uses semicolons ";" to separate multiple files or URLs inside a single cell.');
          }

          $form['ingestsetup']['custommapping'][$type]['files'] = [
            '#type' => 'select',
            '#title' => $files_title,
            '#default_value' => isset($mapping['custommapping_settings'][$type]['files']) ? $mapping['custommapping_settings'][$type]['files'] : [],
            '#options' => array_combine($column_keys, $column_keys),
            '#size' => count($column_keys),
            '#multiple' => TRUE,
            '#description' => $files_description,
            '#empty_option' => $this->t('- Please select columns for @type -', ['@type' => $type]),
          ];
        }
      }
      else {
        $form['message-error'] = [
          '#markup' => '<div class="error">' . $this->t('Your data needs to provide a "type" column and at least one ADO type value under that column. None found ') . '</div>',
        ];
      }
    }

    if ($this->step == 4) {
      $data = $this->store->get('data');
      $pluginconfig = $this->store->get('pluginconfig');
      $op = $pluginconfig['op'];
      $plugin_instance = $this->store->get('plugininstance');
      $column_keys = $plugin_instance->provideKeys($pluginconfig, $data);
      $column_options = array_combine($column_keys, $column_keys);
      $mapping = $this->store->get('mapping');
      $adomapping = $this->store->get('adomapping');
      $required_maps = [
        'label' => 'Ado Label',
      ];
      $form['ingestsetup']['adomapping'] = [
        '#type' => 'fieldset',
        '#tree' => TRUE,
        '#title' => t('Please select your Global ADO mappings'),
      ];

      if ($op == 'update' || $op == 'patch') {
        $node_description = $this->t('Columns that hold either other row <b>numbers</b> or <b>UUIDs</b>(an existing ADO) connecting ADOs between each other (e.g "ismemberof"). You can choose multiple. <br/><b>WARNING:</b> If you want to keep existing relationships (e.g Collection Membership) for an existing column, Do <em>not</em> select it. If you do so relationships will be replaced by the new ones provided in your data set or deleted if the value under that column is EMPTY.');
      }
      else {
        $node_description = $this->t('Columns that hold either other row <b>numbers</b> or <b>UUIDs</b>(an existing ADO) connecting ADOs between each other (e.g "ismemberof"). You can choose multiple.');
      }



      $form['ingestsetup']['adomapping']['parents'] = [
        '#type' => 'select',
        '#title' => $this->t('ADO Parent Columns'),
        '#default_value' => isset($adomapping['parents']) ? $adomapping['parents'] : [],
        '#options' => array_combine($column_keys, $column_keys),
        '#size' => count($column_keys),
        '#multiple' => TRUE,
        '#required' => FALSE,
        '#description' => $node_description,
        '#empty_option' => $this->t('- Please select columns -'),
      ];
      if  ($op == 'update' || $op == 'patch') {
        $form['ingestsetup']['adomapping']['autouuid'] = [
          '#type' => 'checkbox',
          '#title' => $this->t('No automatically assign UUID possible'),
          '#description' => $this->t(
            'For an Update Operation you have to provide the UUIDs.'
          ),
          '#required' => FALSE,
          '#disabled' => TRUE,
          '#default_value' => FALSE,
        ];
      }
      else {
        $form['ingestsetup']['adomapping']['autouuid'] = [
          '#type' => 'checkbox',
          '#title' => $this->t('Automatically assign UUID'),
          '#description' => $this->t(
            'Check this to automatically Assign UUIDs to each ADO. <br/><b>Important</b>: AMI will generate those under a <b>node_uuid</b> column.<br/>If you data already contains a <b>node_uuid</b> column with UUIDs inside, existing values will be used.'
          ),
          '#required' => FALSE,
          '#default_value' => isset($adomapping['autouuid']) ? $adomapping['autouuid'] : TRUE,
        ];
      }
      $form['ingestsetup']['adomapping']['uuid'] = [
        '#type' => 'webform_mapping',
        '#title' => $this->t('UUID assignment'),
        '#description' => $this->t(
          'Please select how your ADO UUID will be assigned'
        ),
        '#description_display' => 'before',
        '#empty_option' =>  $this->t('- Let AMI decide -'),
        '#empty_value' =>  NULL,
        '#default_value' =>  isset($adomapping['uuid']) ? $adomapping['uuid'] : [],
        '#required' => FALSE,
        '#source' => [ 'uuid' => 'ADO UUID'],
        '#source__title' => $this->t('ADO mappings'),
        '#destination__title' => $this->t('Data Columns'),
        '#destination' => $column_options,
        '#states' => [
          'visible' => [
            ':input[name*="autouuid"]' => ['checked' => FALSE],
          ],
          'required' => [
            ':input[name*="autouuid"]' => ['checked' => FALSE],
          ]
        ]
      ];
      if ($op == 'update' || $op == 'patch') {
        unset($form['ingestsetup']['adomapping']['uuid']['#states']);
        $form['ingestsetup']['adomapping']['uuid']['#required'] = TRUE;
      }

        $form['ingestsetup']['adomapping']['base'] = [
        '#type' => 'webform_mapping',
        '#title' => $this->t('Required ADO mappings'),
        '#format' => 'list',
        '#description_display' => 'before',
        '#empty_option' =>  $this->t('- Let AMI decide -'),
        '#empty_value' =>  NULL,
        '#default_value' =>  isset($adomapping['base']) ? $adomapping['base'] : [],
        '#required' => true,
        '#source' => $required_maps,
        '#source__title' => $this->t('Base ADO mappings'),
        '#destination__title' => $this->t('columns'),
        '#destination' => $column_options
      ];
    }
    if ($this->step == 5) {
      $fileid = $this->store->get('zip');
      $form['zip'] = [
        '#type' => 'managed_file',
        '#title' => $this->t('Provide an ZIP file'),
        '#required' => false,
        '#multiple' => false,
        '#default_value' => isset($fileid) ? [$fileid] : NULL,
        '#description' => $this->t('Provide an optional ZIP file containing your assets.'),
        '#upload_location' => 'temporary://ami',
        '#upload_validators' => [
          'file_validate_extensions' => ['zip'],
        ],
      ];
    }
    return $form;
  }

  /**
   * {@inheritdoc}
   */
  public function submitForm(array &$form, FormStateInterface $form_state) {
    parent::submitForm($form, $form_state);
    if ($form_state->getValue('plugin', NULL)) {

      if ($this->store->get('plugin') != $form_state->getValue('plugin', NULL)) {
        $this->store->set('pluginconfig',[]);
      }
      $this->store->set('plugin', $form_state->getValue('plugin'));

    }
    if ($form_state->getValue('pluginconfig', [])) {
      $this->store->set('pluginconfig', $form_state->getValue('pluginconfig'));
    }
    // First data fetch step
    if ($this->step == 3) {
      $this->store->delete('data');
      /* @var $plugin_instance \Drupal\ami\Plugin\ImporterAdapterInterface| NULL */
      $plugin_instance = $this->store->get('plugininstance');
      if ($plugin_instance) {
        // We may want to run a batch here?
        // @TODO investigate how to run a batch and end in the same form different step?
        // Idea is batch is only needed if there is a certain max number, e.g 5000 rows?
        $data = $plugin_instance->getInfo($this->store->get('pluginconfig'), $form_state,0,-1);
        // Check if the Plugin is ready processing or needs more data
        $ready = $form_state->getValue('pluginconfig')['ready'] ?? TRUE;
        if (!$ready) {
          // Back yo Step 2 until the Plugin is ready doing its thing.
          $this->step = 2;
          $form_state->setRebuild();
        }
        else {
          // Why 3? At least title, a type and a parent even if empty
          // Total rows contains data without headers So a single one is good enough.
          if (is_array($data) && !empty($data) and isset($data['headers']) && count($data['headers']) >= 3 && isset($data['totalrows']) && $data['totalrows'] >= 1) {
            $this->store->set('data', $data);
          }
          else {
            // Not the data we are looking for? Back to Step 2.
            $this->step = 2;
            $form_state->setRebuild();
            // @TODO show how its lacking?
            $this->messenger()
              ->addError($this->t('Sorry. Your Source data is not enough for Processing. We need at least a header column, 3 columns and a data column. Please adjust your Plugin Configuration and try again.'));
          }
        }
      }
    }
    if ($this->step == 4) {
      if ($form_state->getTriggeringElement()['#name'] !== 'prev') {
        $globalmapping = $form_state->getValue('globalmapping');
        $custommapping = $form_state->getValue('custommapping');
        $files =  $form_state->getValue('files');
        $bundle =  $form_state->getValue('bundle');
        $template = $form_state->getValue('template');
        $webform = $form_state->getValue('webform');
        $this->store->set('mapping', [
          'globalmapping' => $globalmapping,
          'custommapping_settings' => $custommapping,
          'globalmapping_settings' => [
            'files' => $files,
            'bundle' => $bundle,
            'metadata_config' => [
              'template' => $template,
              'webform' => $webform
            ]
          ]
        ]);
      }
    }
    if ($this->step == 5) {
      if ($form_state->getTriggeringElement()['#name'] !== 'prev') {
        $adomapping = $form_state->getValue('adomapping');
        $this->store->set('adomapping', $adomapping);
      }
    }
    if ($this->step == 6) {
      if ($form_state->getValue('zip', NULL)) {
        $file = $this->entityTypeManager->getStorage('file')
          ->load($form_state->getValue('zip')[0]); // Just FYI. The file id will be stored as an array
        // And you can access every field you need via standard method
        if ($file) {
          $this->store->set('zip', $file->id());
        }
        else {
          $this->store->set('zip', NULL);
        }
      } else {
        $this->store->set('zip', NULL);
      }

      $amisetdata = new \stdClass();

      $amisetdata->plugin = $this->store->get('plugin');
      $amisetdata->pluginconfig = $this->store->get('pluginconfig');
      $amisetdata->mapping = $this->store->get('mapping');
      $amisetdata->adomapping = $this->store->get('adomapping');
      $amisetdata->zip = $this->store->get('zip');

      /* @var $plugin_instance \Drupal\ami\Plugin\ImporterAdapterInterface| NULL */
      $plugin_instance = $this->store->get('plugininstance');
      if ($plugin_instance) {
        if (!$plugin_instance->getPluginDefinition()['batch']) {
          $data = $plugin_instance->getData($this->store->get('pluginconfig'),
            0, -1);
          $amisetdata->column_keys = $data['headers'];
          $amisetdata->total_rows = $data['totalrows'];
        }

        // We should probably add the UUIDs here right now.
        $uuid_key = isset($amisetdata->adomapping['uuid']['uuid']) && !empty($amisetdata->adomapping['uuid']['uuid']) ? $amisetdata->adomapping['uuid']['uuid'] : 'node_uuid';
        // We want to reset this value now
        $amisetdata->adomapping['uuid']['uuid'] = $uuid_key;
        if (!$plugin_instance->getPluginDefinition()['batch']) {
          $fileid = $this->AmiUtilityService->csv_save($data, $uuid_key);
        } else {
          $fileid = $this->AmiUtilityService->csv_touch();
        }
        $batch = [];
        if (isset($fileid)) {
          $amisetdata->csv = $fileid;
          if ($plugin_instance->getPluginDefinition()['batch']) {
            $data = $this->store->get('data');
            $amisetdata->column_keys = [];
            $amisetdata->total_rows = NULL; // because we do not know yet
            $id = $this->AmiUtilityService->createAmiSet($amisetdata);
            $batch = $plugin_instance->getBatch($form_state, $this->store->get('pluginconfig'), $amisetdata);
            if ($id) {
              $url = Url::fromRoute('entity.ami_set_entity.canonical',
                ['ami_set_entity' => $id]);
              $this->messenger()->addStatus($this->t('Well Done! New AMI Set was created'));
              $this->store->delete('data');
              $form_state->setRebuild(FALSE);
              $form_state->setRedirect('entity.ami_set_entity.canonical',
                ['ami_set_entity' => $id]);
            }
          }
          else {
            $id = $this->AmiUtilityService->createAmiSet($amisetdata);
            if ($id) {
              $url = Url::fromRoute('entity.ami_set_entity.canonical',
                ['ami_set_entity' => $id]);
              $this->messenger()
                ->addStatus($this->t('Well Done! New AMI Set was created and you can <a href="@url">see it here</a>',
                  ['@url' => $url->toString()]));
              $this->store->delete('data');
              $form_state->setRebuild(FALSE);
              $form_state->setRedirect('entity.ami_set_entity.canonical',
                ['ami_set_entity' => $id]);
            }
          }
        }
        else {
          $this->messenger()
            ->addError('Ups. Something went wrong when generating your full source data as CSV. Please retry and/or contact your site admin.');
        }
      }
      else {
        // Explain why
        $this->messenger()->addError('Ups. Something went wrong and we could not get your data because we could not load the importer plugin. Please contact your site admin.');
      }
    }

    // Parent already sets rebuild but better to not trust our own base classes
    // In case they change.
    if ($this->step < $this->lastStep) {
      $form_state->setRebuild(TRUE);
    } else {
      if (!empty($batch)) {
        batch_set($batch);
      }
    }
  }

  /**
   * @return array
   */
  public function getMetadatadisplays(): array {
    return $this->metadatadisplays;
  }

  /**
   * @param array $metadatadisplays
   */
  public function setMetadatadisplays(array $metadatadisplays): void {
    $this->metadatadisplays = $metadatadisplays;
  }

  /**
   * @return array
   */
  public function getWebforms(): array {
    return $this->webforms;
  }

  /**
   * @param array $webforms
   */
  public function setWebforms(array $webforms): void {
    $this->webforms = $webforms;
  }

  /**
   * @return array
   */
  public function getBundlesAndFields(): array {
    return $this->bundlesAndFields;
  }

  /**
   * @param array $bundlesAndFields
   */
  public function setBundlesAndFields(array $bundlesAndFields): void {
    $this->bundlesAndFields = $bundlesAndFields;
  }

}

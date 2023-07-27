<?php


namespace Drupal\ami\Controller;

use Drupal\ami\AmiUtilityService;
use Drupal\Core\Ajax\AjaxResponse;
use Drupal\Core\Ajax\OpenOffCanvasDialogCommand;
use Drupal\Core\Ajax\ReplaceCommand;
use Drupal\Core\Controller\ControllerBase;
use Drupal\Core\Entity\Element\EntityAutocomplete;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Messenger\MessengerInterface;
use Drupal\Core\Session\AccountInterface;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;
use Drupal\Component\Utility\Xss;
use Drupal\ami\Entity\amiSetEntity;
use Twig\Error\Error as TwigError;
use Drupal\format_strawberryfield\Form\MetadataDisplayForm;

/**
 * Defines a route controller for watches autocomplete form elements.
 */
class AmiRowAutocompleteHandler extends ControllerBase {

  /**
   * @var \Drupal\Core\Session\AccountInterface
   */
  protected $currentUser;

  /**
   * The entity manager service.
   *
   * @var \Drupal\Core\Entity\EntityTypeManagerInterface
   */
  protected $entityTypeManager;

  /**
   * @var \Drupal\ami\AmiUtilityService
   */
  protected $AmiUtilityService;

  /**
   * Constructs a AmiMultiStepIngestBaseForm.
   *
   * @param \Drupal\Core\Session\AccountInterface $current_user
   * @param \Drupal\ami\AmiUtilityService $ami_utility
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface $entity_type_manager
   */
  public function __construct(AccountInterface $current_user, AmiUtilityService $ami_utility,  EntityTypeManagerInterface $entity_type_manager) {
    $this->currentUser = $current_user;
    $this->entityTypeManager = $entity_type_manager;
    $this->AmiUtilityService = $ami_utility;
  }

  /**
   * {@inheritdoc}
   */
  public static function create(ContainerInterface $container) {
    return new static(
      $container->get('current_user'),
      $container->get('ami.utility'),
      $container->get('entity_type.manager')
    );
  }
  /**
   * Handler for AMI Set CSV autocomplete request.
   *
   * Filters against Labels
   *
   */
  public function handleAutocomplete(Request $request, amiSetEntity $ami_set_entity) {
    $results = [];
    $input = $request->query->get('q');

    // Get the typed string from the URL, if it exists.
    if (!$input) {
      return new JsonResponse($results);
    }

    if (!$ami_set_entity) {
      return new JsonResponse($results);
    }
    $input = Xss::filter($input);
    $csv_file_reference = $ami_set_entity->get('source_data')->getValue();
    if (isset($csv_file_reference[0]['target_id'])) {
      /** @var \Drupal\file\Entity\File $file */
      $file = $this->entityTypeManager->getStorage('file')->load(
        $csv_file_reference[0]['target_id']
      );
      $data = new \stdClass();
      foreach ($ami_set_entity->get('set') as $item) {
        /** @var \Drupal\strawberryfield\Plugin\Field\FieldType\StrawberryFieldItem $item */
        $data = $item->provideDecoded(FALSE);
      }
      $label_column = $data->adomapping->base->label ?? 'label';
      $uuid_column = $data->adomapping->uuid->uuid ?? 'node_uuid';
      $file_data_all = $this->AmiUtilityService->csv_read($file, 0, 0, TRUE);
      $column_keys = $file_data_all['headers'] ?? [];
      $label_original_index = array_search($label_column, $column_keys);
      $uuid_original_index = array_search($uuid_column, $column_keys);
      $i = 0;
      if ($label_original_index !== FALSE) {
        foreach ($file_data_all['data'] as $id => &$row) {
          if (isset($row[$label_original_index]) && stripos($row[$label_original_index], $input) === 0) {
            $i++;
            $label = [
              $row[$label_original_index],
              '<small>(' . $id . ')</small>',
              $row[$uuid_original_index] ?? 'NO UUID Assigned',
            ];

            $results[] = [
              'value' => $id,
              'label' => implode(' ', $label),
            ];
            if ($i == 10) {
              break;
            }
          }
        }
      }
    }

    return new JsonResponse($results);
  }

  /**
   * AJAX callback.
   */
  public static function ajaxPreviewAmiSet($form, FormStateInterface $form_state) {
    $response = new AjaxResponse();

    /** @var \Drupal\format_strawberryfield\MetadataDisplayInterface $entity */
    $entity = $form_state->getFormObject()->getEntity();


    // Attach the library necessary for using the OpenOffCanvasDialogCommand and
    // set the attachments for this Ajax response.
    $form['#attached']['library'][] = 'core/drupal.dialog.off_canvas';
    $form['#attached']['library'][] = 'codemirror_editor/editor';
    $form['#attached']['library'][] = 'core/jquery';
    $form['#attached']['library'][] = 'core/jquery.form';
    $form['#attached']['library'][] = 'core/drupal.dialog.ajax';
    $form['#attached']['library'][] = 'core/drupal.ajax';
    $response->setAttachments($form['#attached']);
    $row = $form_state->getValues()['ado_amiset_row_context_preview'] ?? 1;
    $row = (int) $row;
    if (!empty($form_state->getValues()['ado_amiset_preview'])) {
      $form_state->setValue('ado_amiset_row_context_preview', $row);
      $id = $form_state->getValues()['ado_amiset_preview'] ?? NULL;
      $id = $id ?? EntityAutocomplete::extractEntityIdFromAutocompleteInput($form_state->getUserInput()['ado_amiset_preview']);
      $form_state->setValue('ado_amiset_preview', $id);
      /** @var \Drupal\node\NodeInterface $preview_node */
      $preview_ami_set = \Drupal::entityTypeManager()
        ->getStorage('ami_set_entity')
        ->load($id);
      if (empty($preview_ami_set)) {
        return $response;
      }
      // Now get the row, if not passed we will get the first because we are
      // weird.
      $csv_file_reference = $preview_ami_set->get('source_data')->getValue();
      if (isset($csv_file_reference[0]['target_id'])) {
        /** @var \Drupal\file\Entity\File $file */
        $file = \Drupal::entityTypeManager()->getStorage('file')->load(
          $csv_file_reference[0]['target_id']
        );
        if (PHP_VERSION_ID < 81000) {
          //@TODO fgetcsv has a bug when called after a seek, offsets on 1 always.
          // We are trying to skip the header too (but get it)
          // Tested on 80016 and error is still in place.
          $row = $row - 2;
        }
        $file_data_all = NULL;
        if ($file) {
          $file_data_all = \Drupal::service('ami.utility')
            ->csv_read($file, $row, 1, TRUE);
        }
        if ($file_data_all !== NULL && !empty($file_data_all['data']) && is_array($file_data_all['data'])) {

          $jsondata = array_combine($file_data_all['headers'],
            reset($file_data_all['data']));
          $jsondata = \Drupal::service('ami.utility')->expandJson($jsondata);
          // Check if render native is requested and get mimetype
          $mimetype = $form_state->getValue('mimetype');
          $mimetype = !empty($mimetype) ? $mimetype[0]['value'] : 'text/html';
          $show_render_native = $form_state->getValue('render_native');
          //@TODO there is code duplication here, we do this already at \Drupal\ami\AmiUtilityService::processMetadataDisplay
          // We should generilize the LoD aspect of this (at least!)
          $context_lod = [];
          $context_lod_contextual = [];
          $lod_mappings = \Drupal::service('ami.lod')
            ->getKeyValueMappingsPerAmiSet($id);
          if ($lod_mappings) {
            foreach ($lod_mappings as $source_column => $destination) {
              if (is_array($destination)) {
                foreach ($destination as $pos_approach) {
                  $context_lod[$source_column][$pos_approach] = $context_lod[$source_column][$pos_approach] ?? [];
                }
              }
              if (isset($jsondata[$source_column])) {
                // sad here. Ok, this is a work around for our normally
                // Strange CSV data structure
                $data_to_clean['data'][0] = [$jsondata[$source_column]];
                $labels = \Drupal::service('ami.utility')
                  ->getDifferentValuesfromColumnSplit($data_to_clean,
                    0);

                foreach ($labels as $label) {
                  $lod_for_label = \Drupal::service('ami.lod')
                    ->getKeyValuePerAmiSet($label, $id);
                  if (is_array($lod_for_label) && count($lod_for_label) > 0) {
                    foreach ($lod_for_label as $approach => $lod) {
                      if (isset($lod['lod'])) {
                        $context_lod[$source_column][$approach] = array_merge($context_lod[$source_column][$approach] ?? [],
                          $lod['lod']);
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

          // Set initial context.
          $context = [
            'node' => NULL,
            'iiif_server' => \Drupal::service('config.factory')
              ->get('format_strawberryfield.iiif_settings')
              ->get('pub_server_url'),
          ];

          $context['data'] = $jsondata;
          $context['data_lod'] = $context_lod;
          $context['data_lod_contextual'] = $context_lod_contextual;
          $context['setURL'] =  $preview_ami_set->toUrl('canonical', ['absolute' => TRUE])
            ->toString();
          $context['setId'] = $id;
          $context['rowId'] = $row;
          $context['setOp'] = NULL;
          $original_context = $context;
          // Allow other modules to provide extra Context!
          // Call modules that implement the hook, and let them add items.
          \Drupal::moduleHandler()
            ->alter('format_strawberryfield_twigcontext', $context);
          $context = $context + $original_context;
          $output = [];
          $output['json'] = [
            '#type' => 'details',
            '#title' => t('JSON Data'),
            '#description_display' => 'before',
            '#open' => FALSE,
          ];
          $output['json']['data'] = [
            '#title' => t('Your row data. e.g <b>{{ data.keyname }}</b> :'),
            '#type' => 'codemirror',
            '#rows' => 60,
            '#value' => json_encode($context['data'], JSON_PRETTY_PRINT),
            '#codemirror' => [
              'lineNumbers' => FALSE,
              'toolbar' => FALSE,
              'readOnly' => TRUE,
              'mode' => 'application/json',
            ],
          ];
          $output['json']['data_lod'] = [
            '#type' => 'codemirror',
            '#title' => t('Reconciliated LoD for this row <b>{{ data_lod.keyname.lod_endpoint_type }}</b> :'),
            '#rows' => 60,
            '#value' => json_encode($context['data_lod'], JSON_PRETTY_PRINT),
            '#codemirror' => [
              'lineNumbers' => FALSE,
              'toolbar' => FALSE,
              'readOnly' => TRUE,
              'mode' => 'application/json',
            ],
          ];
          $output['json']['data_lod_contextual'] = [
            '#type' => 'codemirror',
            '#title' => t('Reconciliated LoD for this row grouped by source reconciliated label <b>{{ data_lod_contextual.keyname.original_source_label.lod_endpoint_type }}</b> :'),
            '#rows' => 60,
            '#value' => json_encode($context['data_lod_contextual'], JSON_PRETTY_PRINT),
            '#codemirror' => [
              'lineNumbers' => FALSE,
              'toolbar' => FALSE,
              'readOnly' => TRUE,
              'mode' => 'application/json',
            ],
          ];
          $output['json']['dataOriginal'] = [
            '#type' => 'codemirror',
            '#title' => t('Original JSON of an ADO <b>{{ dataOriginal.keyname }}</b> :'),
            '#description' => t('This data structure will contain the original values (before modification) of an ADO only when updating. Sadly we can not at this time preview it during an AMI set preview.'),
            '#description_display' => 'before',
            '#rows' => 60,
            '#value' => json_encode('{}', JSON_PRETTY_PRINT),
            '#codemirror' => [
              'lineNumbers' => FALSE,
              'toolbar' => FALSE,
              'readOnly' => TRUE,
              'mode' => 'application/json',
            ],
          ];

          // Try to Ensure we're using the twig from user's input instead of the entity's
          // default.
          try {
            $input = $form_state->getUserInput();
            $entity->set('twig', $input['twig'][0], FALSE);
            $render = $entity->renderNative($context);
            if ($show_render_native) {
              $message = '';
              switch ($mimetype) {
                case 'application/ld+json':
                case 'application/json':
                  $json_decoded = json_decode((string) $render);
                  if (JSON_ERROR_NONE !== json_last_error()) {
                    throw new \Exception(
                      'Error parsing JSON: ' . json_last_error_msg(),
                      0,
                      NULL
                    );
                  }
                  else {
                    // If the test passed show it pretty printed so Allison
                    // has a better experience.
                    $render = json_encode($json_decoded, JSON_PRETTY_PRINT);
                  }
                  break;
                case 'text/html':
                  libxml_use_internal_errors(TRUE);
                  $dom = new \DOMDocument('1.0', 'UTF-8');
                  if ($dom->loadHTML((string) $render)) {
                    if ($error = libxml_get_last_error()) {
                      libxml_clear_errors();
                      $message = $error->message;
                    }
                    break;
                  }
                  else {
                    throw new \Exception(
                      'Error parsing HTML',
                      0,
                      NULL
                    );
                  }
                case 'application/xml':
                  libxml_use_internal_errors(TRUE);
                  try {
                    libxml_clear_errors();
                    $dom = new \SimpleXMLElement((string) $render);
                    if ($error = libxml_get_last_error()) {
                      $message = $error->message;
                    }
                  } catch (\Exception $e) {
                    throw new \Exception(
                      "Error parsing XML: {$e->getMessage()}",
                      0,
                      NULL
                    );
                  }
                  break;
              }
            }
            if (!$show_render_native || ($show_render_native && $mimetype != 'text/html')) {
              $output['preview'] = [
                '#type' => 'codemirror',
                '#title' => t('Processed Output:'),
                '#rows' => 60,
                '#value' => $render,
                '#codemirror' => [
                  'lineNumbers' => FALSE,
                  'toolbar' => FALSE,
                  'readOnly' => TRUE,
                  'mode' => $mimetype,
                ],
              ];
            }
            else {
              $output['preview'] = [
                '#type' => 'details',
                '#open' => TRUE,
                '#title' => 'HTML Output',
                '#description_display' => 'before',
                'render' => [
                  '#markup' => $render,
                ],
              ];
            }
            if(!empty($message)) {
              $preview_error = MetadataDisplayForm::buildAjaxPreviewError($message);
              $output['preview_error'] = $preview_error;
            }
          } catch (\Exception $exception) {
            // Make the Message easier to read for the end user
            if ($exception instanceof TwigError) {
              $message = $exception->getRawMessage() . ' at line ' . $exception->getTemplateLine();
            }
            else {
              $message = $exception->getMessage();
            }
            if(!empty($message)) {
              $preview_error = MetadataDisplayForm::buildAjaxPreviewError($message);
              $output['preview_error'] = $preview_error;
            }
          }
          $response->addCommand(new OpenOffCanvasDialogCommand(t('Preview'),
            $output, ['width' => '50%']));
        }
        else {
          $message = !$file ? 'The AMI set has no CSV File. The AMI set is empty.': 'The AMI set has no data for chosen row. The AMI set is empty.';
          if(!empty($message)) {
            $preview_error = MetadataDisplayForm::buildAjaxPreviewError($message);
            $output['preview_error'] = $preview_error;
          }
          $response->addCommand(new OpenOffCanvasDialogCommand(t('Preview'),
            $output, ['width' => '50%']));
        }
      }
    }
    // Always refresh the Preview Element too.
    $form['preview']['#open'] = TRUE;
    $response->addCommand(new ReplaceCommand('#metadata-preview-container', $form['preview']));
    \Drupal::messenger()->deleteByType(MessengerInterface::TYPE_STATUS);
    if ($form_state->getErrors()) {
      // Clear errors so the user does not get confused when reloading.
      \Drupal::messenger()->deleteByType(MessengerInterface::TYPE_ERROR);
      $form_state->clearErrors();
    }
    $form_state->setRebuild(TRUE);
    return $response;
  }

  /**
   * AJAX callback.
   */
  public static function rowAjaxCallback($form, FormStateInterface $form_state) {
    $response = new AjaxResponse();
    $id = $form_state->getValues()['ado_amiset_preview'] ?? NULL;
    $id = $id ?? EntityAutocomplete::extractEntityIdFromAutocompleteInput($form_state->getUserInput()['ado_amiset_preview']);
    $form['preview']['#open'] = TRUE;
    if ($id) {
      $form['preview']['ado_amiset_row_context_preview']['#autocomplete_route_parameters'] = ['ami_set_entity' => $id];
    }
    \Drupal::messenger()->deleteByType(MessengerInterface::TYPE_STATUS);
    $response->addCommand(new ReplaceCommand('#metadata-preview-container', $form['preview']));
    return $response;
  }

}

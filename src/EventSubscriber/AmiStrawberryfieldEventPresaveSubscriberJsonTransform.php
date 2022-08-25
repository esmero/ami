<?php

namespace Drupal\ami\EventSubscriber;

use Drupal\ami\AmiUtilityService;
use Drupal\Component\Uuid\Uuid;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Logger\LoggerChannelFactoryInterface;
use Drupal\Core\Render\RenderContext;
use Drupal\Core\Render\RendererInterface;
use Drupal\strawberryfield\Event\StrawberryfieldCrudEvent;
use Drupal\Core\Messenger\MessengerInterface;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\Core\StringTranslation\TranslationInterface;
use Drupal\Core\Session\AccountInterface;
use Drupal\Core\Config\ConfigFactoryInterface;
use Symfony\Component\HttpFoundation\RequestStack;
use Drupal\strawberryfield\EventSubscriber\StrawberryfieldEventPresaveSubscriber;

/**
 * Event subscriber for SBF bearing entity presave event.
 */
class AmiStrawberryfieldEventPresaveSubscriberJsonTransform extends StrawberryfieldEventPresaveSubscriber {

  use StringTranslationTrait;

  /**
   * @var int
   *
   *  This needs to run before any other Subscriber since it might affect the
   *  The complete output.
   */
  protected static $priority = -1200;

  /**
   * The messenger.
   *
   * @var \Drupal\Core\Messenger\MessengerInterface
   */
  protected $messenger;

  /**
   * The current user.
   *
   * @var \Drupal\Core\Session\AccountInterface
   */
  protected $account;

  /**
   * The logger factory.
   * @var \Drupal\Core\Logger\LoggerChannelFactoryInterface
   */
  protected $loggerFactory;

  /**
   * @var \Drupal\ami\AmiUtilityService
   */
  protected $AmiUtilityService;

  /**
   * The entity manager service.
   *
   * @var \Drupal\Core\Entity\EntityTypeManagerInterface
   */
  protected EntityTypeManagerInterface $entityTypeManager;

  /**
   * The request stack.
   *
   * @var \Symfony\Component\HttpFoundation\RequestStack
   */
  protected $requestStack;

  /**
   * The Drupal Renderer.
   *
   * @var \Drupal\Core\Render\RendererInterface
   */
  protected $renderer;

  /**
   * The configuration factory.
   *
   * @var \Drupal\Core\Config\ConfigFactoryInterface
   */
  protected $configFactory;


  /**
   * AmiStrawberryfieldEventPresaveSubscriberJsonTransform constructor.
   *
   * @param \Drupal\Core\StringTranslation\TranslationInterface $string_translation
   * @param \Drupal\Core\Messenger\MessengerInterface           $messenger
   * @param \Drupal\Core\Session\AccountInterface               $account
   * @param \Drupal\Core\Logger\LoggerChannelFactoryInterface   $logger_factory
   * @param \Drupal\ami\AmiUtilityService                       $ami_utility
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface      $entity_type_manager
   * @param \Symfony\Component\HttpFoundation\RequestStack      $request_stack
   * @param \Drupal\Core\Render\RendererInterface               $renderer
   * @param \Drupal\Core\Config\ConfigFactoryInterface          $config_factory
   */
  public function __construct(
    TranslationInterface $string_translation,
    MessengerInterface $messenger,
    AccountInterface $account,
    LoggerChannelFactoryInterface $logger_factory,
    AmiUtilityService $ami_utility,
    EntityTypeManagerInterface $entity_type_manager,
    RequestStack $request_stack,
    RendererInterface $renderer,
    ConfigFactoryInterface $config_factory
  ) {
    $this->stringTranslation = $string_translation;
    $this->messenger = $messenger;
    $this->loggerFactory = $logger_factory;
    $this->account = $account;
    $this->AmiUtilityService = $ami_utility;
    $this->entityTypeManager = $entity_type_manager;
    $this->requestStack = $request_stack;
    $this->renderer = $renderer;
    $this->configFactory = $config_factory;
  }


  /**
   * @param \Drupal\strawberryfield\Event\StrawberryfieldCrudEvent $event
   *
   * @throws \Drupal\Component\Plugin\Exception\InvalidPluginDefinitionException
   * @throws \Drupal\Component\Plugin\Exception\PluginNotFoundException
   */
  public function onEntityPresave(StrawberryfieldCrudEvent $event) {

    /*
     * $request = $event->getRequest();
    if ($request->getRequestFormat() !== 'api_json') {
      return;
    }


    if (empty($processed_metadata)) {
      $message = $this->t('Sorry, ADO with @uuid is empty or has wrong data/metadata. Check your data ROW in your CSV for set @setid or your Set Configuration for manually entered JSON that may break your setup.',[
        '@uuid' => $data->info['row']['uuid'],
        '@setid' => $data->info['set_id']
      ]);
      $this->messenger->addWarning($message);
      $this->loggerFactory->get('ami')->error($message);
      return;
    }


     */
    /** @var \Drupal\Core\Entity\ContentEntityInterface $entity*/
    $entity = $event->getEntity();
    $sbf_fields = $event->getFields();

    foreach ($sbf_fields as $field_name) {
      /** @var \Drupal\Core\Field\FieldItemInterface $field*/
      $field = $entity->get($field_name);
      /** @var \Drupal\strawberryfield\Field\StrawberryFieldItemList $field */
      if (!$field->isEmpty()) {
        $entity = $field->getEntity();
        /** @var $field \Drupal\Core\Field\FieldItemList */
        foreach ($field->getIterator() as $delta => $itemfield) {
          /** @var \Drupal\strawberryfield\Plugin\Field\FieldType\StrawberryFieldItem $itemfield */
          $fullvalues = $itemfield->provideDecoded(TRUE);

          if (!is_array($fullvalues)) {
            break;
          }
          /*
          We will search for ap:tasks ap:ami entry and process from there.
          If not we simply break and let the event Subscriber flow continue()
          "ap:ami": { metadata_display: 7, "entity:files": { "images": ["http://some.com/remote_file.jpeg"] }

          */
          /** @var \Drupal\format_strawberryfield\MetadataDisplayInterface|null $metadatadisplay_entity */
          $metadatadisplay_entity = NULL;
          if (isset($fullvalues['ap:tasks']['ap:ami']['metadata_display'])) {
            $ap_ami = $fullvalues['ap:tasks']['ap:ami'];
            if (is_numeric($ap_ami['metadata_display'])) {
              $metadatadisplay_entity = $this->entityTypeManager->getStorage('metadatadisplay_entity')
                ->load($ap_ami['metadata_display']);
            }
            elseif (is_string($ap_ami['metadata_display']) && Uuid::isValid($ap_ami['metadata_display'])) {
              $metadatadisplay_entity = $this->entityTypeManager->getStorage('metadatadisplay_entity')
                ->loadByProperties(['uuid' => $ap_ami['metadata_display']]);
              $metadatadisplay_entity = reset($metadatadisplay_entity);
            }
            if (!$metadatadisplay_entity) {
              // If JSON API we must trigger an exception event here.
              break;
            }
            if ($metadatadisplay_entity->get('mimetype') !== 'application/json') {
              // If JSON API we must trigger an exception event here.
              break;
            }
            // Remove since this can not persist/be part of the transformation
            unset($fullvalues['ap:tasks']['ap:ami']);
            // For safety we will also strip any further ap:ami key after the process
            // in case the user ends adding a new one and we end with possible
            // infinite loop of transforms? TBH is not possible. But
            // Could at least have the consequences that after any EDIT
            // a new transforms triggers? Which is bad? Well might be even intended!
            // @TODO what are the benefits? an everlasting automatic cleanup template?
            $context['node'] = $entity;
            $context['data'] = $fullvalues;
            $context['iiif_server'] = $this->configFactory->get('format_strawberryfield.iiif_settings')->get('pub_server_url');
            $original_context = $context;
            // Allow other modules to provide extra Context!
            // Call modules that implement the hook, and let them add items.
            \Drupal::moduleHandler()->alter('format_strawberryfield_twigcontext', $context);
            // In case someone decided to wipe the original context?
            // We bring it back!
            $context = $context + $original_context;

            // @see https://www.drupal.org/node/2638686 to understand
            // What cacheable, Bubbleable metadata and early rendering means.
            try {
              $cacheabledata = $this->renderer->executeInRenderContext(
                new RenderContext(),
                function () use ($context, $metadatadisplay_entity) {
                  return $metadatadisplay_entity->renderNative($context);
                }
              );
              if (count($cacheabledata)) {
                $jsonstring = $cacheabledata->__toString();
                $jsondata = json_decode($jsonstring, TRUE);
                $json_error = json_last_error();
                // To avoid passing the original data into the template transformed
                // data back. See @TODO about if this is desireable.
                if ($json_error != JSON_ERROR_NONE) {
                  $message = $this->t(
                    'We could not generate JSON via Metadata Display "@metadatadisplay" into ADO with UUID @uuid. This is the Template %output',
                    [
                      '@metadatadisplay' => $metadatadisplay_entity->label(),
                      '@uuid'              => $entity->uuid(),
                      '%output'            => $jsonstring,
                    ]
                  );
                  $this->loggerFactory->get('ami')->error($message);
                  break;
                }
                unset($jsondata['ap:tasks']['ap:ami']);
                if (!$itemfield->setMainValueFromArray((array) $jsondata)) {
                  $message = $this->t(
                    'We could not persist correct JSON via Metadata Display "@metadatadisplay" into future ADO with UUID @uuid.',
                    [
                      '@metadatadisplayid' => $metadatadisplay_entity->label(),
                      '@uuid'              => $entity->uuid(),
                    ]
                  );
                  $this->loggerFactory->get('ami')->error($message);
                }
              }
            }
            catch (\Exception $e) {
              $message = $this->t(
                'We could not generate JSON via Metadata Display "@metadatadisplay" into ADO with UUID @uuid. This is the exception %output',
                [
                  '@metadatadisplayid' => $metadatadisplay_entity->label(),
                  '@uuid'              => $entity->uuid(),
                  '%output'            => $e->getMessage(),
                ]
              );
              $this->loggerFactory->get('ami')->error($message);
              break;
            }
          }
        }
      }
    }
    $current_class = get_called_class();
    $event->setProcessedBy($current_class, TRUE);
  }
}

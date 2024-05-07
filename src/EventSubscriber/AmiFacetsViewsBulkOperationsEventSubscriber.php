<?php

namespace Drupal\ami\EventSubscriber;

use Drupal\Core\Session\AccountInterface;
use Drupal\Core\TempStore\PrivateTempStoreFactory;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Drupal\views_bulk_operations\Service\ViewsBulkOperationsViewDataInterface;
use Drupal\views_bulk_operations\ViewsBulkOperationsEvent;

/**
 * Defines VBO Facets event subscriber class.
 *
 * Stores Private TempStore key for data present in the Views exposed_input.
 * Used in a Facet processor that kicks in only during a Batch.
 * @see \Drupal\ami\Plugin\facets\processor\VboBatchProcessorHandler
 */
class AmiFacetsViewsBulkOperationsEventSubscriber implements EventSubscriberInterface {

  // Subscribe to the VBO event with high priority
  // to prepopulate the event data.
  const PRIORITY = 800;

  /**
   * Object that gets the current view data.
   *
   * @var \Drupal\views_bulk_operations\Service\ViewsbulkOperationsViewDataInterface
   */
  protected $viewData;

  /**
   * The tempstore service.
   *
   * @var \Drupal\Core\TempStore\PrivateTempStoreFactory
   */
  protected $tempStoreFactory;

  /**
   * The current user object.
   *
   * @var \Drupal\Core\Session\AccountInterface
   */
  protected $currentUser;

  /**
   * Object constructor.
   *
   * @param \Drupal\views_bulk_operations\Service\ViewsBulkOperationsViewDataInterface $viewData
   *   The VBO View Data provider service.
   * @param \Drupal\Core\Session\AccountInterface                                      $currentUser
   * @param \Drupal\Core\TempStore\PrivateTempStoreFactory                             $tempStoreFactory
   */
  public function __construct(ViewsBulkOperationsViewDataInterface $viewData,  AccountInterface $currentUser, PrivateTempStoreFactory $tempStoreFactory) {
    $this->viewData = $viewData;
    $this->currentUser = $currentUser;
    $this->tempStoreFactory = $tempStoreFactory;
  }

  /**
   * {@inheritdoc}
   */
  public static function getSubscribedEvents() {
    $events[ViewsBulkOperationsEvent::NAME][] = [
      'updateFacetCache',
      self::PRIORITY,
    ];
    return $events;
  }

  /**
   * Respond to view data request event.
   *
   * @var \Drupal\views_bulk_operations\ViewsBulkOperationsEvent $event
   *   The event to respond to.
   */
  public function updateFacetCache(ViewsBulkOperationsEvent $event) {
    $view_data = $event->getViewData();
    $exposed_input = $event->getView()->getExposedInput();
    $tempStoreName = 'ami_vbo_batch_facets_' . md5($event->getView()->id() . '_' . $event->getView()->current_display);
    // Do not override once the batch starts.
    // We do not know here if facets will or not be in a f[] so we pass
    // all values (also good, respects filters).
    // The VBO URL facet processor will know/read from the URL Processor settings.
    // Do not overridewrite once the batch starts or we will end with 0 filters.
    // Arguments will be 'op' = 'do', _format = 'json', id = a number the batch id
    // Normally just checking if this is happening under the unbrella of the actual Views Route is enough
    // BUT ... we need to also take in account layout builder .. so in that case we check for 'op' !== do && id (the batch)
    // @TODO. No idea how to deal with the blocks and other options
    // I could to the opposite> Save it anytime it is not a batch but bc Facets are basically processed all the time
    // anywhere that would be a lot of extra processing time.
    if ((\Drupal::routeMatch()->getRouteName() == 'view'. '.' . $event->getView()->id(). '.' .$event->getView()->current_display) ||
      (
        (\Drupal::routeMatch()->getRouteObject()->getOption('_layout_builder') ||
          $event->getView()->display_handler->getBaseId() == 'block') && (($exposed_input['op'] ?? NULL) !== "do") && !isset($exposed_input['id']))
    ) {
      $this->tempStoreFactory->get($tempStoreName)->set(
        $this->currentUser->id(), $exposed_input ?? []
      );
    }
  }
}

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
    // Maybe delete when on view.solr_search_content_with_find_and_replace.page_1?
    // We do not know here if facets will or not be in a f[] so we pass
    // all. The processor will know/read from the URL Processor settings.
    // Do not overridewrite once the batch starts. Arguments will be 'op', format, id
    if (!isset($exposed_input['op'])) {
      $this->tempStoreFactory->get($tempStoreName)->set(
        $this->currentUser->id(), $exposed_input ?? []
      );
    }
  }
}

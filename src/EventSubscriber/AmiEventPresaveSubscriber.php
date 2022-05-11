<?php

namespace Drupal\ami\EventSubscriber;

use Drupal\ami\AmiEventType;
use Drupal\ami\Event\AmiCrudEvent;
use Drupal\strawberryfield\StrawberryfieldEventType;
use Drupal\strawberryfield\Event\StrawberryfieldCrudEvent;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;

/**
 * Event subscriber for AMI entity presave event.
 */
abstract class AmiEventPresaveSubscriber implements EventSubscriberInterface {

  /**
   * @var int
   */
  protected static $priority = -800;

  /**
   * {@inheritdoc}
   */
  public static function getSubscribedEvents() {
    // Make sure we have access before everything else here since we want
    // Enrich our JSON before anything runs.
    // @TODO check event priority and adapt to future D9 needs.
    // Use late binding. Never use self:: or we will all get -800 in every class.
    $events[AmiEventType::PRESAVE][] = ['onEntityPresave', static::$priority];
    return $events;
  }


    /**
   * Method called when Event occurs.
   *
   * @param \Drupal\ami\Event\AmiCrudEvent $event
   *   The event.
   */
  abstract public function onEntityPresave(AmiCrudEvent $event);

}

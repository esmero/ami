<?php


namespace Drupal\ami;

/**
 * Class AmiEventType.
 *
 * @package Drupal\ami
 */
final class AmiEventType {

  /**
   * Name of the event fired when pre saving an AMI Entity.
   *
   * This event allows modules to perform an action whenever an AMI
   * is saved. The event listener method receives a
   * \Drupal\ami\Event\AmiCrudEvent instance.
   *
   * @Event
   *
   * @see ami_ami_set_entity_presave()
   * @see \Drupal\ami\Event\AmiCrudEvent
   * @see \Drupal\ami\EventSubscriber\AmiEventPresaveSubscriberProcessedLoDUpdater
   *
   * @var string
   */
  const PRESAVE = 'ami.ami_set_entity.presave';


}

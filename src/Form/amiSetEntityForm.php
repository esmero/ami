<?php
namespace Drupal\ami\Form;
use Drupal\Core\Entity\ContentEntityForm;
use Drupal\Core\Language\Language;
use Drupal\Core\Form\FormStateInterface;

/**
 * Form controller for the ami_set_entity entity edit forms.
 *
 * @ingroup ami
 */
class amiSetEntityForm extends ContentEntityForm {

  /**
   * {@inheritdoc}
   */
  public function buildForm(array $form, FormStateInterface $form_state) {
    /* @var $entity \Drupal\ami\Entity\amiSetEntity */
    $entity = $this->entity;
    $form = parent::buildForm($form, $form_state);
    return $form;
  }


  /**
   * {@inheritdoc}
   */
  public function save(array $form, FormStateInterface $form_state) {
    $status = parent::save($form, $form_state);
    $entity = $this->entity;
    $form_state->setRedirectUrl($entity->toUrl('canonical'));
    return $status;
  }
}

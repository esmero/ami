<?php

namespace Drupal\ami\Entity;

use Drupal\Core\Entity\EntityStorageInterface;
use Drupal\Core\Field\BaseFieldDefinition;
use Drupal\Core\Entity\ContentEntityBase;
use Drupal\Core\Entity\EntityTypeInterface;
use Drupal\Core\Entity\EntityChangedTrait;
use Drupal\ami\amiSetEntityInterface;
use Drupal\user\UserInterface;
use Drupal\Component\Utility\Environment;

/**
 * Defines the Ami Set Content entity.
 *
 * @ingroup ami
 *
 * This is the main definition of the entity type. From it, an entityType is
 * derived. The most important properties in this example are listed below.
 *
 * id: The unique identifier of this entityType. It follows the pattern
 * 'moduleName_xyz' to avoid naming conflicts.
 *
 * label: Human readable name of the entity type.
 *
 * handlers: Handler classes are used for different tasks. You can use
 * standard handlers provided by D8 or build your own, most probably derived
 * from the standard class. In detail:
 *
 * - view_builder: we use the standard controller to view an instance. It is
 *   called when a route lists an '_entity_view' default for the entityType
 *   (see routing.yml for details. The view can be manipulated by using the
 *   standard drupal tools in the settings.
 *
 * - list_builder: We derive our own list builder class from the
 *   entityListBuilder to control the presentation.
 *   If there is a view available for this entity from the views module, it
 *   overrides the list builder. @todo: any view? naming convention?
 *
 * - form: We derive our own forms to add functionality like additional fields,
 *   redirects etc. These forms are called when the routing list an
 *   '_entity_form' default for the entityType. Depending on the suffix
 *   (.add/.edit/.delete) in the route, the correct form is called.
 *
 * - access: Our own accessController where we determine access rights based on
 *   permissions.
 *
 * More properties:
 *
 *  - base_table: Define the name of the table used to store the data. Make sure
 *    it is unique. The schema is automatically determined from the
 *    BaseFieldDefinitions below. The table is automatically created during
 *    installation.
 *
 *  - fieldable: Can additional fields be added to the entity via the GUI?
 *    Analog to content types.
 *
 *  - entity_keys: How to access the fields. Analog to 'nid' or 'uid'.
 *
 *  - links: Provide links to do standard tasks. The 'edit-form' and
 *    'delete-form' links are added to the list built by the
 *    entityListController. They will show up as action buttons in an additional
 *    column.
 *
 * There are many more properties to be used in an entity type definition. For
 * a complete overview, please refer to the '\Drupal\Core\Entity\EntityType'
 * class definition.
 *
 * The following construct is the actual definition of the entity type which
 * is read and cached. Don't forget to clear cache after changes.
 *
 * @ContentEntityType(
 *   id = "ami_set_entity",
 *   label = @Translation("AMI Ingest Set Entity"),
 *   handlers = {
 *     "view_builder" = "Drupal\Core\Entity\EntityViewBuilder",
 *     "list_builder" = "Drupal\ami\Entity\Controller\amiSetEntityListBuilder",
 *     "views_data" = "Drupal\views\EntityViewsData",
 *     "form" = {
 *       "add" = "Drupal\ami\Form\amiSetEntityForm",
 *       "edit" = "Drupal\ami\Form\amiSetEntityForm",
 *       "delete" = "Drupal\ami\Form\amiSetEntityDeleteForm",
 *       "process" = "Drupal\ami\Form\amiSetEntityProcessForm",
 *       "deleteprocessed" = "Drupal\ami\Form\amiSetEntityDeleteProcessedForm",
 *       "reconcile" = "Drupal\ami\Form\amiSetEntityReconcileForm",
 *       "editreconcile" = "Drupal\ami\Form\amiSetEntityReconcileCleanUpForm"
 *     },
 *     "access" = "Drupal\ami\Entity\Controller\amiSetEntityAccessControlHandler",
 *   },
 *   base_table = "ami_setentity",
 *   admin_permission = "administer amiset entity",
 *   fieldable = TRUE,
 *   entity_keys = {
 *     "id" = "id",
 *     "label" = "name",
 *     "uuid" = "uuid",
 *     "status" = "status"
 *   },
 *   links = {
 *     "canonical" = "/amiset/{ami_set_entity}",
 *     "edit-form" = "/amiset/{ami_set_entity}/edit",
 *     "process-form" = "/amiset/{ami_set_entity}/process",
 *     "delete-process-form" = "/amiset/{ami_set_entity}/deleteprocessed",
 *     "reconcile-form" = "/amiset/{ami_set_entity}/reconcile",
 *     "edit-reconcile-form" = "/amiset/{ami_set_entity}/editreconcile",
 *     "delete-form" = "/amiset/{ami_set_entity}/delete",
 *     "collection" = "/amiset/list"
 *   },
 *   field_ui_base_route = "ami.amisetentity_settings",
 * )
 *
 * The 'links' above are defined by their path. For core to find the
 * route, the route name must follow the correct pattern:
 *
 * entity.<entity-name>.<link-name> (replace dashes with underscores)
 * Example: 'entity.content_entity_example_contact.canonical'
 *
 * See routing file above for the corresponding implementation
 *
 * This class defines methods and fields for the  Ami Set Entity
 *
 * Being derived from the ContentEntityBase class, we can override the methods
 * we want. In our case we want to provide access to the standard fields about
 * creation and changed time stamps.
 *
 * AmiSetEntityInterface also exposes the EntityOwnerInterface.
 * This allows us to provide methods for setting and providing ownership
 * information.
 *
 * The most important part is the definitions of the field properties for this
 * entity type. These are of the same type as fields added through the GUI, but
 * they can by changed in code. In the definition we can define if the user with
 * the rights privileges can influence the presentation (view, edit) of each
 * field.
 */
class amiSetEntity extends ContentEntityBase implements amiSetEntityInterface {

  // Implements methods defined by EntityChangedInterface.
  use EntityChangedTrait;

  /**
   * {@inheritdoc}
   *
   * When a new entity instance is added, set the user_id entity reference to
   * the current user as the creator of the instance.
   */
  public static function preCreate(EntityStorageInterface $storage_controller, array &$values) {
    parent::preCreate($storage_controller, $values);
    $values += [
      'user_id' => \Drupal::currentUser()->id(),
    ];
  }

  /**
   * {@inheritdoc}
   */
  public function getCreatedTime() {
    return $this->get('created')->value;
  }

  /**
   * {@inheritdoc}
   */
  public function getOwner() {
    return $this->get('user_id')->entity;
  }

  /**
   * {@inheritdoc}
   */
  public function getOwnerId() {
    return $this->get('user_id')->target_id;
  }

  /**
   * {@inheritdoc}
   */
  public function getStatus() {
    return $this->get('status');
  }

  /**
   * @param $status
   *
   * @return \Drupal\ami\amiSetEntityInterface
   */
  public function setStatus($status) {
    return $this->set('status', $status);
  }

  /**
   * {@inheritdoc}
   */
  public function setOwnerId($uid) {
    $this->set('user_id', $uid);
    return $this;
  }

  /**
   * {@inheritdoc}
   */
  public function setOwner(UserInterface $account) {
    $this->set('user_id', $account->id());
    return $this;
  }


  /**
   * {@inheritdoc}
   *
   * Define the field properties here.
   *
   * Field name, type and size determine the table structure.
   *
   * In addition, we can define how the field and its content can be manipulated
   * in the GUI. The behaviour of the widgets used can be determined here.
   */
  public static function baseFieldDefinitions(EntityTypeInterface $entity_type) {

    // Standard field, used as unique if primary index.
    $fields['id'] = BaseFieldDefinition::create('integer')
      ->setLabel(t('ID'))
      ->setDescription(t('The ID of the Ami Set.'))
      ->setReadOnly(TRUE);

    // Standard field, unique
    $fields['uuid'] = BaseFieldDefinition::create('uuid')
      ->setLabel(t('UUID'))
      ->setDescription(t('The UUID of the Ami Set.'))
      ->setReadOnly(TRUE);

    // Name field for the contact.
    // We set display options for the view as well as the form.
    // Users with correct privileges can change the view and edit configuration.
    $fields['name'] = BaseFieldDefinition::create('string')
      ->setLabel(t('Name'))
      ->setDescription(t('A name or label for this AMI Set'))
      ->setRevisionable(FALSE)
      ->setSettings([
        'default_value' => '',
        'max_length' => 255,
        'text_processing' => 0,
      ])
      ->setDisplayOptions('view', [
        'label' => 'above',
        'type' => 'string',
        'weight' => -6,
      ])
      ->setDisplayOptions('form', [
        'type' => 'string_textfield',
        'weight' => -6,
      ])
      ->setDisplayConfigurable('form', TRUE)
      ->setDisplayConfigurable('view', TRUE)
      ->setRequired(TRUE);

    // Holds the actual Set config and data
    $fields['set'] = BaseFieldDefinition::create('strawberryfield_field')
      ->setLabel(t('Set Config and associated data'))
      ->setTranslatable(TRUE)
      ->setDisplayOptions('view', [
        'label' => 'hidden',
        'type' => 'strawberry_default_formatter',
        'weight' => 0,
      ])
      ->setDisplayOptions('form', [
        'type' => 'string_textarea',
        'settings' => [
          'text_processing' => FALSE,
          'rows' => 10,
        ],
        'weight' => 0,
      ])
      ->setDisplayConfigurable('form', TRUE)
      ->setDisplayConfigurable('view', TRUE)
      ->setRequired(TRUE)
      ->addConstraint('NotBlank');

    // Owner field of the Ami Set Entity.
    // Entity reference field, holds the reference to the user object.
    // The view shows the user name field of the user.
    // The form presents a auto complete field for the user name.
    $fields['user_id'] = BaseFieldDefinition::create('entity_reference')
      ->setLabel(t('User Name'))
      ->setDescription(t('User owning this Set.'))
      ->setSetting('target_type', 'user')
      ->setSetting('handler', 'default')
      ->setDisplayOptions('view', [
        'label' => 'above',
        'type' => 'entity_reference_label',
        'weight' => -3,
      ])
      ->setDisplayOptions('form', [
        'type' => 'entity_reference_autocomplete',
        'settings' => [
          'match_operator' => 'CONTAINS',
          'size' => 60,
          'autocomplete_type' => 'tags',
          'placeholder' => '',
        ],
        'weight' => -3,
      ])
      ->setDisplayConfigurable('form', TRUE)
      ->setDisplayConfigurable('view', TRUE);

    $fields['langcode'] = BaseFieldDefinition::create('language')
      ->setLabel(t('Language code'))
      ->setDescription(t('The language code of Ami Set entity.'));
    $fields['created'] = BaseFieldDefinition::create('created')
      ->setLabel(t('Created'))
      ->setDescription(t('The time that the Ami Set was created.'));

    $fields['changed'] = BaseFieldDefinition::create('changed')
      ->setLabel(t('Changed'))
      ->setDescription(t('The time that the Ami Set was last edited.'));

    $fields['status'] = BaseFieldDefinition::create('list_string')
      ->setLabel(t('This Set last known status'))
      ->setDescription(t('Current Status of this Set'))
      ->setSettings([
        'default_value' => 'ready',
        'max_length' => 64,
        'cardinality' => 1,
        'allowed_values' => [
          'ready' => 'ready',
          'not ready' => 'notready',
          'processed' => 'processed',
          'enqueued' => 'enqueued',
        ],
      ])
      ->setRequired(TRUE)
      ->setDisplayOptions('view', [
        'region' => 'hidden',
      ])
      ->setDisplayConfigurable('view', TRUE)
      ->setDisplayConfigurable('form', TRUE)
      ->addConstraint('NotBlank');

    $validators = [
      'file_validate_extensions' => ['csv'],
      'file_validate_size' => [Environment::getUploadMaxSize()],
    ];
    // We may want to add an extra validator so uploaded CSV files have correct columns.
    // TO discuss if we even should allow this to be replaced.
    $fields['source_data'] = BaseFieldDefinition::create('file')
      ->setLabel(t('Source Data File '))
      ->setDescription(t('A CSV containing the Source Data to be processed'))
      ->setSetting('file_extensions', 'csv')
      ->setSetting('upload_validators', $validators)
      ->setRequired(FALSE)
      ->setDisplayOptions('view', [
        'label' => 'above',
        'type' => 'file',
        'weight' => -3,
      ])
      ->setDisplayOptions('form', [
        'type' => 'file',
        'description' => [
          'theme' => 'file_upload_help',
          'description' => t('Your Source Data for this Set')
        ]	,
        'settings' => [
          'upload_validators' => $validators,
        ],
        'weight' => -3,
      ])
      ->setDisplayConfigurable('view', TRUE)
      ->setDisplayConfigurable('form', TRUE)
      ->addConstraint('NotBlank');

    $fields['processed_data'] = BaseFieldDefinition::create('file')
      ->setLabel(t('Processed Data'))
      ->setDescription(t('A CSV containing the already  processed data'))
      ->setSetting('file_extensions', 'csv')
      ->setSetting('upload_validators', $validators)
      ->setRequired(FALSE)
      ->setDisplayOptions('view', [
        'label' => 'above',
        'type' => 'file',
        'weight' => -3,
      ])
      ->setDisplayConfigurable('view', TRUE)
      ->setDisplayConfigurable('form', TRUE);
    $validatorszip = [
      'file_validate_extensions' => ['zip'],
      'file_validate_size' => [Environment::getUploadMaxSize()],
    ];
    $fields['zip_file'] = BaseFieldDefinition::create('file')
      ->setLabel(t('Attached ZIP file'))
      ->setDescription(t('A Zip file containing accompanying Files for the Source Data'))
      ->setSetting('file_extensions', 'zip')
      ->setSetting('uri_scheme', 'private')
      ->setSetting('file_directory', '/ami/zip')
      ->setSetting('upload_validators', $validatorszip)
      ->setRequired(FALSE)
      ->setDisplayOptions('view', [
        'label' => 'above',
        'type' => 'file',
        'weight' => -3,
      ])
      ->setDisplayOptions('form', [
        'type' => 'file',
        'description' => [
          'theme' => 'file_upload_help',
          'description' => t('Source Files for this Set')
        ],
        'settings' => [
          'upload_validators' => $validatorszip,
        ],
        'weight' => -3,
      ])
      ->setDisplayConfigurable('view', TRUE)
      ->setDisplayConfigurable('form', TRUE);

    $fields['report_file'] = BaseFieldDefinition::create('file')
      ->setLabel(t('AMI Process Reports'))
      ->setDescription(t('Processed set reports in CSV format'))
      ->setSetting('file_extensions', 'csv')
      ->setSetting('upload_validators', $validators)
      ->setSetting('uri_scheme', 'private')
      ->setSetting('file_directory', '/ami/reports')
      ->setRequired(FALSE)
      ->setDisplayOptions('view', [
        'label' => 'above',
        'type' => 'file',
        'weight' => -2,
      ])
      ->setDisplayConfigurable('view', TRUE)
      ->setDisplayConfigurable('form', TRUE);
    return $fields;
  }
}

<?php

namespace Drupal\ami\Plugin\facets\processor;

use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Plugin\ContainerFactoryPluginInterface;
use Drupal\Core\TempStore\PrivateTempStoreFactory;
use Drupal\Core\Session\AccountInterface;
use Drupal\facets\Exception\InvalidProcessorException;
use Drupal\facets\FacetInterface;
use Drupal\facets\Processor\BuildProcessorInterface;
use Drupal\facets\Processor\PreQueryProcessorInterface;
use Drupal\facets\Processor\ProcessorPluginBase;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\HttpFoundation\Request;
use Drupal\Core\Http\RequestStack as DrupalRequestStack;
use Symfony\Component\HttpFoundation\RequestStack as SymfonyRequestStack;

/**
 * The URL processor handler triggers the actual url processor.
 *
 * The URL processor handler allows managing the weight of the actual URL
 * processor per Facet.  This handler will trigger the actual.
 *
 * @FacetsUrlProcessor, which can be configured on the Facet source.
 *
 * @FacetsProcessor(
 *   id = "ami_vbo_processor_handler",
 *   label = @Translation("VBO batch handler"),
 *   description = @Translation("VBO Batch Facet processor"),
 *   stages = {
 *     "pre_query" = 50,
 *   },
 *   locked = true
 * )
 */
class VboBatchProcessorHandler extends ProcessorPluginBase implements BuildProcessorInterface, PreQueryProcessorInterface, ContainerFactoryPluginInterface {


  /**
   * The clone of the current request object.
   *
   * @var \Symfony\Component\HttpFoundation\Request
   */
  protected $request;

  /**
   * An array of active filters.
   *
   * @var array
   *   An array containing the active filters with key being the facet id and
   *   value being an array of raw values.
   */
  protected array $activeFilters = [];

  /**
   * The actual url processor used for handing urls.
   *
   * @var \Drupal\facets\UrlProcessor\UrlProcessorInterface
   */
  protected $processor;

  /**
   * Gets the Processor.
   *
   * @return \Drupal\facets\UrlProcessor\UrlProcessorInterface
   *   The Processor.
   */
  public function getProcessor() {
    return $this->processor;
  }

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
   * The entity type manager.
   *
   * @var \Drupal\Core\Entity\EntityTypeManagerInterface
   */
  protected $entityTypeManager;

  /**
   * VboBatchProcessorHandler constructor.
   *
   * This Facet Query processor uses VBO temp storage during a Batch.
   *
   * @param array                                          $configuration
   * @param                                                $plugin_id
   * @param array                                          $plugin_definition
   * @param \Symfony\Component\HttpFoundation\Request      $request
   *
   * @param \Drupal\Core\TempStore\PrivateTempStoreFactory $tempStoreFactory
   * @param \Drupal\Core\Session\AccountInterface          $currentUser
   *
   * @param \Drupal\Core\Entity\EntityTypeManagerInterface $entity_type_manager
   *
   * @throws \Drupal\facets\Exception\InvalidProcessorException
   */
  public function __construct(array $configuration, $plugin_id, array $plugin_definition, Request $request, PrivateTempStoreFactory $tempStoreFactory, AccountInterface $currentUser, EntityTypeManagerInterface $entity_type_manager) {
    parent::__construct($configuration, $plugin_id, $plugin_definition);

    if (!isset($configuration['facet']) || !$configuration['facet'] instanceof FacetInterface) {
      throw new InvalidProcessorException("The VBO Batch Processor doesn't have the required 'facet' in the configuration array.");
    }
    $this->request = clone $request;
    $this->tempStoreFactory = $tempStoreFactory;
    $this->currentUser = $currentUser;
    $this->entityTypeManager = $entity_type_manager;

    // Only act when we are on Core System Batch
    // Because the action processor submit will clear the internal VBO
    // Private Storage, we need to act before, fetch it, keep it around
    // Also. Anonymous users do not have Private Store
    // So any query made in a batch as anonymous (e.g in Twig template) or
    // A subquery aggregating content will throw a Runtime exception
    // Here we avoid that.
    if (\Drupal::routeMatch()->getRouteName() == "system.batch_page.json" && $this->currentUser->isAuthenticated()) {
      /** @var \Drupal\facets\FacetInterface $facet */
      $facet = $this->configuration['facet'];
      // ONLY ACT ON NOT RENDERED?
      //$facet->getFacetSource()->isRenderedInCurrentRequest()
      /** @var \Drupal\facets\FacetSourceInterface $fs */
      $fs = $facet->getFacetSourceConfig();
      $url_processor_name = $fs->getUrlProcessorName();
      $manager = \Drupal::getContainer()->get('plugin.manager.facets.url_processor');
      $this->processor = $manager->createInstance($url_processor_name, ['facet' => $facet]);
      $this->initializeActiveFiltersFromStorage();
    }
  }

  /**
   * {@inheritdoc}
   */
  public function defaultConfiguration() {
    return [
        'enabled' => FALSE,
      ] + parent::defaultConfiguration();
  }

  /**
   * {@inheritdoc}
   */
  public function buildConfigurationForm(array $form, FormStateInterface $form_state, FacetInterface $facet) {
    $configuration = $this->getConfiguration();


    $build['enabled'] = [
      '#type' => 'checkbox',
      '#title' => $this->t('Use URL based facets in VBO Batches'),
      '#default_value' => $configuration['enabled'],
    ];

    return $build;
  }

  /**
   * {@inheritdoc}
   */
  public static function create(ContainerInterface $container, array $configuration, $plugin_id, $plugin_definition) {
    $request =  $container->get('request_stack')->getMainRequest();
    return new static(
      $configuration,
      $plugin_id,
      $plugin_definition,
      $request,
      $container->get('tempstore.private'),
      $container->get('current_user'),
      $container->get('entity_type.manager')
    );
  }

  /**
   * {@inheritdoc}
   */
  public function build(FacetInterface $facet, array $results) {
    return [];
  }

  /**
   * {@inheritdoc}
   */
  public function preQuery(FacetInterface $facet) {
    $this->setActiveItems($facet);
  }

  public function setActiveItems(FacetInterface $facet) {
    // Get the filter key of the facet.
    if (isset($this->activeFilters[$facet->id()])) {
      foreach ($this->activeFilters[$facet->id()] as $value) {
        $facet->setActiveItem(trim($value, '"'));
      }
    }
  }

  /**
   * Initializes the active filters from the VBO Storage.
   *
   * Get all the filters that are active by checking the VBO Temp Store
   * and put them in activeFilters which is an array where key is the facet id
   * and value is an array of raw values.
   */
  protected function initializeActiveFiltersFromStorage() {

    if (!$this->processor) {
      // Means we have not intialized the processor.
      // We only do it on the constructor only when needed to avoid
      // cluttering memory and stuff
      return;
    }

    /** @var \Drupal\facets\FacetInterface $facet */
    $facet = $this->configuration['facet'];

    /** @var \Drupal\facets\FacetSourceInterface $fs */
    $fs = $facet->getFacetSourceConfig();
    $fs_id = explode("__", $fs->id());
    // This will contain 4 parts: 'search_api', 'views type', 'Views ID', 'Display ID'
    // we want to check if there is an active VBO Temp Storage for the current user
    // that has Views ID and Display ID in its ID. If so we get the "arguments"
    // And check if there are any starting with the $filter_key
    if (!is_array($fs_id) || count($fs_id) != 4) {
      return;
    }
    $view_id = $fs_id[2];
    $display_id = $fs_id[3];
    $url_parameters = $this->request->query;
    // If this is too long we will get a Data too long for column 'collection' at
    // DatabaseExceptionWrapper. So we md5 all.
    $tempStoreName = 'ami_vbo_batch_facets_' . md5($view_id . '_' . $display_id);
    // Get the active facet parameters.
    $active_params = NULL;
    $views_params = $this->tempStoreFactory->get($tempStoreName)->get($this->currentUser->id());
    if (is_array($views_params)) {
      $active_params = $views_params[$this->processor->getFilterKey()] ?? [];
    }
    $facet_source_id = $this->configuration['facet']->getFacetSourceId();
    // When an invalid parameter is passed in the url, we can't do anything.
    if (!is_array($active_params) || !$this->processor) {
      return;
    }

    // Explode the active params on the separator.
    foreach ($active_params as $param) {
      $explosion = explode($this->processor->getSeparator(), $param);
      $url_alias = array_shift($explosion);
      $facet_id = $this->getFacetIdByUrlAlias($url_alias, $facet_source_id);
      $value = '';
      while (count($explosion) > 0) {
        $value .= array_shift($explosion);
        if (count($explosion) > 0) {
          $value .= $this->processor->getSeparator();
        }
      }
      if (!isset($this->activeFilters[$facet_id])) {
        $this->activeFilters[$facet_id] = [$value];
      }
      else {
        $this->activeFilters[$facet_id][] = $value;
      }
    }
  }

  /**
   * Gets the facet id from the url alias & facet source id.
   *
   * @param string $url_alias
   *   The url alias.
   * @param string $facet_source_id
   *   The facet source id.
   *
   * @return bool|string
   *   Either the facet id, or FALSE if that can't be loaded.
   */
  protected function getFacetIdByUrlAlias($url_alias, $facet_source_id) {
    $mapping = &drupal_static(__FUNCTION__);
    if (!isset($mapping[$facet_source_id][$url_alias])) {
      $storage = $this->entityTypeManager->getStorage('facets_facet');
      $facet = current($storage->loadByProperties(['url_alias' => $url_alias, 'facet_source_id' => $facet_source_id]));
      if (!$facet) {
        return NULL;
      }
      $mapping[$facet_source_id][$url_alias] = $facet->id();
    }
    return $mapping[$facet_source_id][$url_alias];
  }
}
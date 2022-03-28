<?php

namespace Drupal\ami;

use Twig\Markup;
use Twig\TwigTest;
use Twig\TwigFilter;
use Twig\TwigFunction;
use Twig\Extension\AbstractExtension;

/**
 * Class TwigExtension.
 *
 * @package Drupal\ami
 */
class TwigExtension extends AbstractExtension {

  /**
   * @inheritDoc
   */
  public function getFunctions() {
    return [
      new TwigFunction('ami_lod_reconcile',
        [$this, 'amiLodReconcile']),
    ];
  }


  public function amiLodReconcile(
    string $label,
    string $vocab,
    string $len = 'en'
  ): ?array {

    $column_options = [
      'loc;subjects;thing' => 'LoC subjects(LCSH)',
      'loc;names;athing' => 'LoC Name Authority File (LCNAF)',
      'loc;genreForms;thing' => 'LoC Genre/Form Terms (LCGFT)',
      'loc;graphicMaterials;thing' => 'LoC Thesaurus of Graphic Materials (TGN)',
      'loc;geographicAreas;thing' => 'LoC MARC List for Geographic Areas',
      'loc;relators;thing' => 'LoC Relators Vocabulary (Roles)',
      'loc;rdftype;CorporateName' => 'LoC MADS RDF by type: Corporate Name',
      'loc;rdftype;PersonalName' => 'LoC MADS RDF by type: Personal Name',
      'loc;rdftype;FmilyName' => 'LoC MADS RDF by type: Family Name',
      'loc;rdftype;Topic' => 'LoC MADS RDF by type: Topic',
      'loc;rdftype;GenreForm' =>  'LoC MADS RDF by type: Genre Form',
      'loc;rdftype;Geographic' => 'LoC MADS RDF by type: Geographic',
      'loc;rdftype;Temporal' =>  'LoC MADS RDF by type: Temporal',
      'loc;rdftype;ExtraterrestrialArea' => 'LoC MADS RDF by type: Extraterrestrial Area',
      'viaf;subjects;thing' => 'Viaf',
      'getty;aat;fuzzy' => 'Getty aat Fuzzy',
      'getty;aat;terms' => 'Getty aat Terms',
      'getty;aat;exact' => 'Getty aat Exact Label Match',
      'wikidata;subjects;thing' => 'Wikidata Q Items'
    ];
    $label = trim($label);
    try {
      $domain = \Drupal::service('request_stack')->getCurrentRequest()->getSchemeAndHttpHost();
      $lod_route_argument_list = explode(";", $vocab);
      $lod = \Drupal::service('ami.lod')->invokeLoDRoute($domain,
        $label, $lod_route_argument_list[0],
        $lod_route_argument_list[1], $lod_route_argument_list[2], $len ?? 'en', 1);
    }
    catch (\Exception $exception) {
      $message = t('@exception_type thrown in @file:@line while querying for @entity_type entity ids matching "@label". Message: @response',
        [
          '@exception_type' => get_class($exception),
          '@file' => $exception->getFile(),
          '@line' => $exception->getLine(),
          '@label' => $label,
          '@response' => $exception->getMessage(),
        ]);
      \Drupal::logger('ami')->warning($message);
      return NULL;
    }

    if (!empty($lod)) {
      return $lod;
    }
    return NULL;
  }
}

(function (Drupal, once) {

  'use strict';
  Drupal.behaviors.ami_preview_helper = {
    attach: function(context) {
      const elementsToAttach = once('ami_preview_helper', '#ami-preview-container-ado', context);
      elementsToAttach.forEach(element => {
        element.querySelectorAll('a').forEach(link => {
          link.removeAttribute('href');
        });
      });
    }};
})(Drupal, once);

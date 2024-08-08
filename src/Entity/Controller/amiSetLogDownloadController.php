<?php

namespace Drupal\ami\Entity\Controller;

use Drupal\ami\Entity\amiSetEntity;
use Drupal\Core\Controller\ControllerBase;
use Drupal\Core\StreamWrapper\StreamWrapperManagerInterface;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\HttpFoundation\BinaryFileResponse;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Exception\AccessDeniedHttpException;
use Symfony\Component\HttpKernel\Exception\NotFoundHttpException;
use Symfony\Component\HttpFoundation\ResponseHeaderBag;

/**
 * AMI Set Log Download Controller.
 */
class amiSetLogDownloadController extends ControllerBase {


  /**
   * The stream wrapper manager.
   *
   * @var \Drupal\Core\StreamWrapper\StreamWrapperManagerInterface
   */
  protected $streamWrapperManager;

  /**
   * FileDownloadController constructor.
   *
   * @param \Drupal\Core\StreamWrapper\StreamWrapperManagerInterface $streamWrapperManager
   *   The stream wrapper manager.
   */
  public function __construct(StreamWrapperManagerInterface $streamWrapperManager)
  {
    $this->streamWrapperManager = $streamWrapperManager;
  }

  /**
   * {@inheritdoc}
   */
  public static function create(ContainerInterface $container)
  {
    return new static(
      $container->get('stream_wrapper_manager')
    );
  }

  /**
   * Handles private AMI set Log Download Controller.
   *
   * @param \Symfony\Component\HttpFoundation\Request $request
   *   The request object.
   * @param AMI set Entity ID
   *   The file scheme, defaults to 'private'.
   *
   * @return \Symfony\Component\HttpFoundation\BinaryFileResponse
   *   The transferred file as response.
   *
   * @throws \Symfony\Component\HttpKernel\Exception\NotFoundHttpException
   *   Thrown when the requested file does not exist.
   * @throws \Symfony\Component\HttpKernel\Exception\AccessDeniedHttpException
   *   Thrown when the user does not have access to the file.
   *
   */
  public function download(Request $request, amiSetEntity $ami_set_entity)
  {
    // Logs are generated by who can process an AMI set. Thus the permission needs to match
    // those credentials.
    if ($ami_set_entity->access('process')) {
      $target = 'ami/logs/set' .$ami_set_entity->id() . '.log';
      // Merge remaining path arguments into relative file path.
      $uri = $this->streamWrapperManager->normalizeUri('private' . '://' . $target);
      if (is_file($uri)) {
        $headers= [
          'Content-type' => 'text/plain',
          'Content-Length' => filesize($uri),
        ];
        // Let other modules provide headers and controls access to the file.
        $response = new BinaryFileResponse($uri, 200, $headers, TRUE);
        $response->setContentDisposition(ResponseHeaderBag::DISPOSITION_ATTACHMENT);
        return $response;
      }
      throw new NotFoundHttpException();
    }
    else {
      throw new AccessDeniedHttpException();
    }
  }
}

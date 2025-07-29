import base64
import json
from io import BytesIO

from dependency_injector.wiring import Provide, inject
from flask import Response, render_template, request, send_file

from signs_dashboard.containers.application import Application
from signs_dashboard.query_params.signs import SignsQueryParameters
from signs_dashboard.repository.bbox_detections import BBOXDetectionsRepository
from signs_dashboard.services.frames import FramesService
from signs_dashboard.services.image_archiver import ImageArchiverService
from signs_dashboard.services.image_visualization import ImageVisualizationService

FRAMES_IDS_KEY = 'frames_ids'
SIGNS_IDS_KEY = 'signs_ids'


@inject
def crop(
    bbox_detections_repository: BBOXDetectionsRepository = Provide[Application.pg_repositories.bbox_detections],
    image_visualizer: ImageVisualizationService = Provide[Application.services.image_visualizer],
):
    sign_id = int(request.args.get('sign_id'))
    img_only = 'img_only' in request.args
    detection = bbox_detections_repository.get_detection(detection_id=sign_id)
    image_base64 = image_visualizer.get_crop_base64(detection)
    if img_only:
        return send_file(
            BytesIO(base64.b64decode(image_base64)),
            mimetype='image/jpg',
            download_name='crop_of_detection_%s.jpg' % sign_id,
        )

    return render_template('crop.html', image_base64=image_base64, detection=detection)


@inject
def download_crops(
    frames_service: FramesService = Provide[Application.services.frames],
    image_archiver: ImageArchiverService = Provide[Application.services.image_archiver],
):
    signs_ids = request.args.getlist(SIGNS_IDS_KEY, type=int)
    signs_ids = signs_ids if signs_ids else None
    frames_ids = request.args.getlist(FRAMES_IDS_KEY, type=int)
    frames = frames_service.get_frames(frames_ids)
    string_io = image_archiver.get_zip_with_crops(frames=frames, needed_signs_ids=signs_ids)
    return Response(
        string_io.read(),
        mimetype='application/zip',
        headers={
            'Content-Disposition': 'attachment; filename=crops.zip',
        },
    )


@inject
def download_frames(
    frames_service: FramesService = Provide[Application.services.frames],
    image_archiver: ImageArchiverService = Provide[Application.services.image_archiver],
):
    frames_ids = request.args.getlist(FRAMES_IDS_KEY, type=int)
    frames = frames_service.get_frames(frames_ids)
    string_io = image_archiver.get_zip_with_frames(frames=frames)
    return Response(
        string_io.read(),
        mimetype='application/zip',
        headers={
            'Content-Disposition': 'attachment; filename=frames.zip',
        },
    )


@inject
def download_predictions(frames_service: FramesService = Provide[Application.services.frames]):
    frames_ids = request.args.getlist(FRAMES_IDS_KEY, type=int)
    frames = frames_service.get_frames(frames_ids)
    frames_predictions = [frame.as_prediction_dict() for frame in frames]
    frames_predictions = json.dumps(frames_predictions, indent=2)
    return Response(
        frames_predictions,
        mimetype='application/json',
        headers={
            'Content-Disposition': 'attachment; filename=predictions.json',
        },
    )


@inject
def search_signs(
    bbox_detections_repository: BBOXDetectionsRepository = Provide[Application.pg_repositories.bbox_detections],
):
    query_params = SignsQueryParameters.from_request(request)
    found_signs = bbox_detections_repository.find(query_params)
    return render_template(
        'signs.html',
        signs=found_signs,
        query_params=query_params,
        download_params={
            SIGNS_IDS_KEY: [sign.id for sign in found_signs],
            FRAMES_IDS_KEY: list({sign.frame_id for sign in found_signs}),
        },
    )

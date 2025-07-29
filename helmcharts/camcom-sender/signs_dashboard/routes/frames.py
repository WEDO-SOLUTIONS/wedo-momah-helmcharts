import base64
import logging
from collections import Counter
from io import BytesIO
from typing import Optional

from dependency_injector.wiring import Provide, inject
from flask import abort, jsonify, render_template, request, send_file

from signs_dashboard.containers.application import Application
from signs_dashboard.models.frame import ModerationStatus
from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.query_params.frames import FramesQueryParameters
from signs_dashboard.routes.signs import FRAMES_IDS_KEY
from signs_dashboard.services.cvat.uploader import CVATUploader
from signs_dashboard.services.frames import FramesService
from signs_dashboard.services.frames_depth import FramesDepthService
from signs_dashboard.services.image import ImageService
from signs_dashboard.services.image_visualization import ImageVisualizationService
from signs_dashboard.services.interest_zones import InterestZonesService
from signs_dashboard.services.prediction import IZ_PREDICTOR_NAME, PredictionService
from signs_dashboard.services.predictors import PredictorsService
from signs_dashboard.services.translations import TranslationsService
from signs_dashboard.small_utils import placeholder_url_for

logger = logging.getLogger(__name__)

provide_frames_service = Provide[Application.services.frames]
provide_image_service = Provide[Application.services.image]


@inject
def frame_page(
    frame_id: int,
    theta: Optional[int] = None,
    frames_service: FramesService = provide_frames_service,
    image_service: ImageService = provide_image_service,
    image_visualizer: ImageVisualizationService = Provide[Application.services.image_visualizer],
):
    frame = frames_service.get_frame_w_detections(frame_id)
    if frame is None:
        abort(404)

    prev_frame = frames_service.get_prev(frame)
    next_frame = frames_service.get_next(frame)

    display_panorama = frame.panoramic and theta is None
    render_polygons = 'render_polygons' in request.args

    try:
        image = image_visualizer.get_visualized_image(
            frame,
            locale=None,
            show_info=not display_panorama,
            render_translations=False,
            theta=theta,
            render_polygons=render_polygons,
        )
    except Exception:
        logger.exception(f'Unable to render frame {frame.id}!')
        image = None

    image_base64 = None
    if image is not None:
        image_base64 = base64.b64encode(image).decode()

    return render_template(
        'frame.html',
        frame=frame,
        theta=theta,
        image_s3_path=image_service.get_s3_path(frame),
        next_frame=next_frame,
        prev_frame=prev_frame,
        image_base64=image_base64,
        display_panorama=display_panorama,
    )


@inject
def get_frame_predictions(
    frame_id: int,
    theta: Optional[int] = None,
    frames_service: FramesService = provide_frames_service,
    image_visualizer: ImageVisualizationService = Provide[Application.services.image_visualizer],
    translations_service: TranslationsService = Provide[Application.services.translations],
):
    show_info = 'info' in request.args
    render_labels = 'render_labels' in request.args
    locale_identifier = request.args.get('locale')
    detection_ids = request.args.getlist('detection_id', type=int)
    render_polygons = 'render_polygons' in request.args

    frame = frames_service.get_frame_w_detections(frame_id)
    if frame is None:
        abort(404)

    locale = translations_service.get_closest_or_default_locale(locale_identifier)

    image = image_visualizer.get_visualized_image(
        frame,
        theta=theta,
        locale=locale,
        show_info=show_info,
        detection_ids=detection_ids,
        render_translations=not render_labels,
        render_polygons=render_polygons,
    )
    if image is None:
        abort(404)

    return send_file(
        BytesIO(image),
        mimetype='image/jpg',
        download_name='frame_%s.jpg' % frame_id,
    )


@inject
def frame_proxy(
    frame_id: int,
    theta: Optional[int] = None,
    frames_service: FramesService = provide_frames_service,
    image_service: ImageService = provide_image_service,
):
    frame = frames_service.get_frame(frame_id)
    image = image_service.download_image(frame, theta=theta)

    return send_file(BytesIO(image), mimetype='image/jpeg')


@inject
def frame_depth_proxy(
    frame_id: int,
    theta: Optional[int] = None,
    frames_service: FramesService = provide_frames_service,
    depth_service: FramesDepthService = Provide[Application.services.frames_depth],
):
    frame = frames_service.get_frame(frame_id)
    depth_image = depth_service.download_depth_map(frame, theta=theta)

    return send_file(
        BytesIO(depth_image),
        mimetype='image/png',
        as_attachment=False,
        attachment_filename='depth_image.png',
    )


@inject
def frame_info(
    frame_id: int,
    frames_service: FramesService = provide_frames_service,
    image_service: ImageService = provide_image_service,
):
    frame = frames_service.get_frame_w_detections(frame_id)
    if not frame:
        return abort(404)

    return jsonify({
        'id': frame.id,
        'track_uuid': frame.track_uuid,
        'track_email': frame.track_email,
        'lat': frame.lat,
        'lon': frame.lon,
        'azimuth': frame.azimuth,
        'speed': frame.speed,
        'date': frame.date.isoformat(),
        'timezone_offset': frame.timezone_offset_str,
        'panoramic': frame.panoramic,
        'uploaded_photo': frame.uploaded_photo,
        's3': image_service.get_s3_location_info(frame),
    })


@inject
def search_frames(
    interest_zones_service: InterestZonesService = Provide[Application.services.interest_zones],
    frames_service: FramesService = provide_frames_service,
    prediction_service: PredictionService = Provide[Application.services.prediction],
    predictors_service: PredictorsService = Provide[Application.services.predictors],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
):
    query_params = FramesQueryParameters.from_request(request)
    found_frames = frames_service.find(query_params)
    frames_ids = [frame.id for frame in found_frames]

    regions = interest_zones_service.get_regions_for_search()

    frames_attributes = prediction_service.get_frames_attributes(
        found_frames,
        predictors=predictors_service.get_all_predictors() + [IZ_PREDICTOR_NAME],
    )

    signs_stat = {}
    for frame in found_frames:
        signs_stat[frame.id] = dict(Counter([sign.label for sign in frame.detections]))

    return render_template(
        'frames.html',
        frames=found_frames,
        signs_stat=signs_stat,
        frames_attributes=frames_attributes,
        query_params=query_params,
        search_regions=regions,
        download_params={FRAMES_IDS_KEY: frames_ids},
        search_predictors_names=predictors_service.get_all_predictors(),
        predictors_names=predictors_service.get_all_predictors(),
        predictor_has_prompt=predictors_service.has_prompt,
        moderation_statuses=list(ModerationStatus),
        cvat_uploading_enabled=modules_config.is_cvat_uploading_enabled(),
        frame_page_url_template=placeholder_url_for(
            'frame',
            placeholder='frame_id_placeholder',
            placeholder_field='frame_id',
        ),
        frame_with_predictions_url_template=placeholder_url_for(
            'get_frame_predictions',
            render_labels='true',
            placeholder='frame_id_placeholder',
            placeholder_field='frame_id',
        ),
    )


@inject
def predict_frames(
    frames_service: FramesService = provide_frames_service,
    predictors_service: PredictorsService = Provide[Application.services.predictors],
):
    frame_ids = list(map(int, request.json['frame_ids']))
    frames = frames_service.get_frames(frame_ids)
    predictor = request.json['predictor']
    predictors_service.send_frames_to_predictor(
        frames=frames,
        predictor=predictor,
        prompt=request.json.get('prompt'),
        recalculate_interest_zones=request.json.get('recalculate_interest_zones', False),
    )
    return {
        'predictor': predictor,
    }


@inject
def cvat_frames_upload(
    frames_service: FramesService = provide_frames_service,
    cvat_uploader: CVATUploader = Provide[Application.services.cvat_uploader],
):
    project_name = request.json['project_name']
    frames = frames_service.get_frames(request.json['frame_ids'])
    upload_uuid = cvat_uploader.create_upload_tasks(frames, project_name)

    return {'upload_uuid': upload_uuid}

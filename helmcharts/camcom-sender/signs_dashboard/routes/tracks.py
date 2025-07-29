import logging
from datetime import datetime
from io import BytesIO
from typing import Optional
from uuid import uuid4

from dependency_injector.wiring import Provide, inject
from flask import Response, abort, jsonify, make_response, render_template, request, send_file
from pydantic import ValidationError

from signs_dashboard.containers.application import Application
from signs_dashboard.models.frame import Frame
from signs_dashboard.models.track import Track
from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.query_params.tracks import TrackQueryParameters
from signs_dashboard.schemas.tracks import TracksLocalizationInSchema
from signs_dashboard.services.camcom.statistics import CamcomStatisticsService
from signs_dashboard.services.cvat.uploader import CVATUploader
from signs_dashboard.services.detected_objects import DetectedObjectsService
from signs_dashboard.services.events.tracks_lifecycle import TracksLifecycleService
from signs_dashboard.services.frames import FramesService
from signs_dashboard.services.interest_zones import InterestZonesService
from signs_dashboard.services.kml_generator import KMLGeneratorService
from signs_dashboard.services.prediction import PredictionService
from signs_dashboard.services.predictors import PredictorsService
from signs_dashboard.services.track_logs import TrackLogsService, find_logs_date
from signs_dashboard.services.tracks import TracksService
from signs_dashboard.services.users import UsersService
from signs_dashboard.small_utils import batch_iterator, placeholder_url_for


@inject
def tracks(
    tracks_service: TracksService = Provide[Application.services.tracks],
    interest_zones_service: InterestZonesService = Provide[Application.services.interest_zones],
    predictors_service: PredictorsService = Provide[Application.services.predictors],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
):
    query_params = TrackQueryParameters.from_request(request)
    tracks_stat = tracks_service.get_tracks_stats(query_params)

    regions = interest_zones_service.get_regions_for_search()

    if query_params.format == 'text':
        text = '\n'.join([track.track.uuid for track in tracks_stat])
        response = make_response(text, 200)
        response.mimetype = 'text/plain'
        return response

    if query_params.format == 'csv':
        response = make_response(
            render_template(
                'tracks.csv',
                tracks_stat=tracks_stat,
            ),
            200,
        )
        response.mimetype = 'text/csv'
        response.headers.set('Content-Disposition', 'attachment; filename=tracks.csv')
        return response

    return render_template(
        'tracks.html',
        tracks_stat=tracks_stat,
        search_regions=regions,
        query_params=query_params,
        track_localization_enabled=modules_config.is_track_localization_enabled(),
        track_map_enabled=modules_config.is_additional_maps_enabled(),
        predictors_names=predictors_service.get_all_predictors(),
        predictor_has_prompt=predictors_service.has_prompt,
        cvat_uploading_enabled=modules_config.is_cvat_uploading_enabled(),
        map_matching_enabled=modules_config.is_map_matching_enabled(),
    )


@inject
def download_kml(
    uuid: Optional[str] = None,
    service: KMLGeneratorService = Provide[Application.services.kml_generator],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
):
    if not uuid:
        uuid = request.args.get('track_id') or request.args.get('track_uuid')

    if not uuid:
        abort(400)

    kml = service.get_kml_from_track_id(uuid, modules_config.is_reporter_enabled('fiji'))

    return Response(
        kml,
        mimetype='application/vnd.google-earth.kml+xml',
        headers={
            'Content-Disposition': 'attachment; filename={track_uuid}.kml'.format(track_uuid=uuid),
        },
    )


@inject
def track_view(  # noqa: WPS211, WPS231
    uuid: str,
    tracks_service: TracksService = Provide[Application.services.tracks],
    frames_service: FramesService = Provide[Application.services.frames],
    camcom_statistics_service: CamcomStatisticsService = Provide[Application.services.camcom_statistics],
    prediction_service: PredictionService = Provide[Application.services.prediction],
    track_logs_service: TrackLogsService = Provide[Application.services.track_logs],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
    predictors_service: PredictorsService = Provide[Application.services.predictors],
    users_service: UsersService = Provide[Application.services.users],
):
    track, frames = _fetch_track_and_frames(uuid, tracks_service=tracks_service, frames_service=frames_service)

    camcom_stats_obj = camcom_statistics_service.get_statistics_by_frame_ids(frame_ids=[frame.id for frame in frames])

    predictions = prediction_service.get_frames_predictions_status(
        frames,
        predictors=predictors_service.get_active_predictors(),
        all_predictors=True,
    )

    track_upload = None
    if track and track.upload:
        track_upload = track.upload

    if frames:
        first_frame_tstamp = min(frame.date for frame in frames)
    else:
        first_frame_tstamp = '-'

    track_logs = track_logs_service.find_track_logs_around_date(
        track_uuid=uuid,
        track_date=find_logs_date(track_upload, first_frame_tstamp),
    )

    return render_template(
        'track.html',
        track_uuid=uuid,
        track_logs=track_logs,
        track_init_time=track_upload.init_time if track_upload else None,
        track_gps_time=track_upload.gps_time if track_upload else None,
        track_complete_time=track_upload.complete_time if track_upload else None,
        track_init_metadata=track_upload.init_metadata if track_upload else None,
        track_gps_points=track_upload.gps_points if track_upload else None,
        total_frames_count=track_upload.expected_frames_count if track_upload else None,
        api_user_info=users_service.render_api_user_info(track.api_user) if track and track.api_user else [],
        frames=frames,
        predictions=predictions,
        track_map_enabled=modules_config.is_additional_maps_enabled(),
        track_exists=track is not None,
        first_frame_tstamp=first_frame_tstamp,
        track_recorded=track.recorded if track else None,
        track_recorded_not_utc=track.recorded_not_utc if track else None,
        track_timezone_offset=track.timezone_offset if track else None,
        track_timezone_offset_str=track.timezone_offset_str if track else None,
        track_user_email=track.user_email if track else None,
        track_type=track.type if track else None,
        track_app_version=track.app_version if track else None,
        track_distance=track.distance if track else None,
        track_duration=track.duration if track else None,
        track_localization_text_status=track.localization_text_status if track else None,
        track_localization_can_be_forced=track.localization_can_be_forced() if track else None,
        track_pro_text_status=track.pro_text_status if track else None,
        track_filtering_description=track.filtering_description if track else None,
        track_fiji_text_status=track.fiji_text_status if track else None,
        track_sending_to_fiji_can_be_forced=track.sending_to_fiji_can_be_forced() if track else None,
        track_map_matching_text_status=track.map_matching_text_status if track else None,
        track_comment=track.comment if track else None,
        track_localization_enabled=modules_config.is_track_localization_enabled(),
        map_matching_enabled=modules_config.is_map_matching_enabled(),
        visual_localization_enabled=modules_config.is_visual_localization_enabled(),
        camcom_enabled=predictors_service.is_camcom_predictor_enabled(),
        camcom_stats_obj=camcom_stats_obj,
    )


@inject
def track_lidar_data(
    uuid: str,
    tracks_service: TracksService = Provide[Application.services.tracks],
):
    track = tracks_service.get(uuid)
    return render_template('lidar.html', lidar_uuid=track.lidar_uuid)


@inject
def track_log(
    uuid: str,
    log_id: str,
    tracks_service: TracksService = Provide[Application.services.tracks],
    track_logs_service: TrackLogsService = Provide[Application.services.track_logs],
    frames_service: FramesService = Provide[Application.services.frames],
):
    track, frames = _fetch_track_and_frames(uuid, tracks_service=tracks_service, frames_service=frames_service)

    track_upload = None
    if track and track.upload:
        track_upload = track.upload

    first_frame_tstamp = min(frame.date for frame in frames) if frames else datetime.now()

    log_file = track_logs_service.download_track_log(
        track_uuid=uuid,
        track_date=find_logs_date(track_upload, first_frame_tstamp),
        log_filename=log_id,
    )
    if not log_file:
        abort(404)

    return send_file(BytesIO(log_file), mimetype='text/plain')


@inject
def track_map(
    uuid: str,
    tracks_service: TracksService = Provide[Application.services.tracks],
    frames_service: FramesService = Provide[Application.services.frames],
    detected_objects_service: DetectedObjectsService = Provide[Application.services.detected_objects],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
):
    if not modules_config.is_additional_maps_enabled():
        abort(404)

    track = tracks_service.get(uuid)
    if not track:
        abort(404)

    track_points = [(point['latitude'], point['longitude']) for point in track.upload.gps_points]
    matched_track_points = []
    if track.upload.matched_gps_points:
        matched_track_points = [(point['latitude'], point['longitude']) for point in track.upload.matched_gps_points]

    frames = [
        {
            'id': frame.id,
            'date': frame.local_datetime.isoformat(),
            'azimuth': frame.azimuth,
            'point': (frame.lat, frame.lon),
            'matched': frame.matched_lat is not None and frame.matched_lon is not None,
            'matched_point': (frame.matched_lat, frame.matched_lon),
            'detections_count': len(frame.detections),
            'detections': {
                'localized': [
                    detection.as_json()
                    for detection in frame.detections
                    if detection.lon
                ],
                'not_localized': [
                    detection.as_json()
                    for detection in frame.detections
                    if not detection.lon
                ],
            },
        }
        for frame in frames_service.get_by_track(track)
    ]
    detected_objects_ids = {
        detection['detected_object_id']
        for frame in frames
        for detection in frame['detections']['localized']
        if detection['detected_object_id']
    }
    detected_objects = {
        str(detected_obj.id): detected_obj.as_json()
        for detected_obj in detected_objects_service.get_by_id_list(detected_objects_ids)
    }

    return render_template(
        'track_map.html',
        track_uuid=uuid,
        track_points=track_points,
        matched_track_points=matched_track_points,
        frames=frames,
        detected_objects=detected_objects,
        base_layer_tileserver_url_template='http://tile2.maps.2gis.com/tiles?x={x}&y={y}&z={z}',
        frame_url_template=placeholder_url_for(
            'frame',
            placeholder_field='frame_id',
            placeholder='frame_id_placeholder',
        ),
        frame_with_predictions_url_template=placeholder_url_for(
            'get_frame_predictions',
            info='true',
            placeholder_field='frame_id',
            placeholder='frame_id_placeholder',
        ),
        crop_url_template=placeholder_url_for(
            'crop',
            img_only='true',
            placeholder_field='sign_id',
            placeholder='detection_id_placeholder',
        ),
        track_localization_enabled=modules_config.is_track_localization_enabled(),
        track_map_matched=track.is_map_matching_or_visual_localization_done(),
    )


@inject
def predict_tracks(
    tracks_service: TracksService = Provide[Application.services.tracks],
    tracks_lifecycle_service: TracksLifecycleService = Provide[Application.services.tracks_lifecycle],
):
    predictor = request.json['predictor']
    tracks_uuids = request.json['tracks_uuids']
    tracks_for_prediction = tracks_service.get_by_uuids(tracks_uuids)
    for track in tracks_for_prediction:
        tracks_lifecycle_service.produce_dashboard_predict_event(
            track.uuid,
            track.user_email,
            predictor=predictor,
            prompt=request.json['prompt'],
        )
    return {'predictor': predictor, 'tracks_uuids': tracks_uuids}


@inject
def upload_tracks_to_cvat(
    cvat_uploader: CVATUploader = Provide[Application.services.cvat_uploader],
    tracks_service: TracksService = Provide[Application.services.tracks],
    tracks_lifecycle_service: TracksLifecycleService = Provide[Application.services.tracks_lifecycle],
):
    upload_uuid = str(uuid4())
    project_id = cvat_uploader.get_or_create_project_by_name(request.json['project_name']).id
    tracks_for_upload = tracks_service.get_by_uuids(request.json['tracks_uuids'])

    for track in tracks_for_upload:
        tracks_lifecycle_service.produce_dashboard_cvat_upload_event(
            track.uuid,
            track.user_email,
            project_id,
            upload_uuid,
        )
    return {'upload_uuid': upload_uuid}


@inject
def localization(
    tracks_service: TracksService = Provide[Application.services.tracks],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
):
    if not modules_config.is_track_localization_enabled():
        abort(404)

    try:
        data = TracksLocalizationInSchema.parse_obj(request.json)
    except (ValidationError, ValueError) as exc:
        logging.exception('vsl err')
        abort(400, str(exc))

    for tracks_uuids_batch in batch_iterator(data.track_uuids, 1000):
        tracks_service.bulk_change_localization_status(tracks_uuids_batch, data.action.value)

    return jsonify({'track_uuids': data.track_uuids})


def _fetch_track_and_frames(
    uuid: str,
    tracks_service: TracksService,
    frames_service: FramesService,
) -> tuple[Optional[Track], list[Frame]]:
    track = tracks_service.get_with_api_user(uuid)
    if track:
        frames = frames_service.get_by_track(track)
    else:
        frames = frames_service.get_by_track_uuid(uuid)

    if not track and not frames:
        abort(404)

    return track, frames

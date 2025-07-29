import datetime
import json
import logging

from dependency_injector.wiring import Provide, inject
from flask import Response, abort, jsonify, render_template, request
from pydantic.error_wrappers import ValidationError

from signs_dashboard.containers.application import Application
from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.query_params.detected_objects import DetectedObjectsQueryParams
from signs_dashboard.query_params.drivers import DriversQueryParams
from signs_dashboard.query_params.tracks import TrackQueryParameters
from signs_dashboard.schemas.tracks import ResendFramesProInSchema, TracksByDriversInSchema, TracksProInSchema
from signs_dashboard.services.detected_objects import DetectedObjectsService
from signs_dashboard.services.events.frames_lifecycle import FramesLifecycleService
from signs_dashboard.services.tracks import TracksService
from signs_dashboard.services.translations import TranslationsService
from signs_dashboard.services.twogis_pro.filters import TwoGisProFiltersService
from signs_dashboard.services.twogis_pro.filters_update import TwoGisProFiltersUpdateService
from signs_dashboard.small_utils import batch_iterator

logger = logging.getLogger(__name__)


@inject
def filters_page(
    filters_update_service: TwoGisProFiltersUpdateService = Provide[Application.services.twogis_pro_filters_update],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
):
    if not modules_config.is_reporter_enabled('pro'):
        abort(404)

    return render_template(
        'twogis_pro.html',
        asset_to_filters={
            asset: json.dumps(asset_filters, indent=4, ensure_ascii=False)
            for asset, asset_filters in filters_update_service.get_assets_mapping().items()
        },
    )


@inject
def api_update_filters_in_pro(
    filters_update_service: TwoGisProFiltersUpdateService = Provide[Application.services.twogis_pro_filters_update],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
):
    if not modules_config.is_reporter_enabled('pro'):
        abort(404)

    success = filters_update_service.sync()

    return jsonify({'success': success})


@inject
def api_detection_classes(
    filters_service: TwoGisProFiltersService = Provide[Application.services.twogis_pro_filters],
    translations_service: TranslationsService = Provide[Application.services.translations],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
):
    if not modules_config.is_reporter_enabled('pro'):
        abort(404)

    locale_identifier = request.args.get('locale')
    locale = translations_service.get_closest_or_default_locale(locale_identifier)

    return jsonify({'detection_classes': [
        {
            'code': det_class.code,
            'detector': det_class.predictor,
            'name': translations_service.get_translation_for_type(det_class.code, locale),
        }
        for det_class in filters_service.detection_classes
    ]})


@inject
def resend_frames(
    frames_lifecycle_service: FramesLifecycleService = Provide[Application.services.frames_lifecycle],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
):
    if not modules_config.is_reporter_enabled('pro'):
        abort(404)

    try:
        frames = ResendFramesProInSchema.parse_obj(request.json).__root__
    except ValidationError as exc:
        abort(400, str(exc))

    logger.warning(f'Resending to PRO events for {len(frames)} frames')
    for frame in frames:
        frames_lifecycle_service.produce_pro_resend_event(frame_id=frame.frame_id, track_uuid=frame.track_uuid)

    return Response(status=200)


@inject
def resend_detected_objects(
    detected_objects_service: DetectedObjectsService = Provide[Application.services.detected_objects],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
):
    if not modules_config.is_reporter_enabled('pro'):
        abort(404)

    try:
        query = DetectedObjectsQueryParams.parse_obj(request.json)
    except Exception as exc:
        logging.error(f'Unable to parse request: {exc}')
        return abort(400, str(exc))

    matching_objects = detected_objects_service.find(query)
    for matching_object in matching_objects:
        detected_objects_service.send_resend_event(matching_object.id)

    return jsonify({'resended': len(matching_objects)})


@inject
def resend_drivers(
    tracks_service: TracksService = Provide[Application.services.tracks],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
):
    if not modules_config.is_reporter_enabled('pro'):
        abort(404)

    try:
        query = DriversQueryParams.parse_obj(request.json)
    except Exception as exc:
        logging.error(f'Unable to parse request: {exc}')
        return abort(400, str(exc))

    drivers_activity = tracks_service.get_active_drivers(query)
    for driver_daily_activity in drivers_activity:
        logger.debug(
            f'Triggering gps reload for driver {driver_daily_activity.email} @ {driver_daily_activity.date_for}',
        )
        tracks_service.send_resend_gps_to_pro_event(
            user_email=driver_daily_activity.email,
            track_uuid=driver_daily_activity.example_track_uuid,
        )

    return jsonify({'resended_drivers_at_dates': len(drivers_activity)})


@inject
def resend_tracks(
    tracks_service: TracksService = Provide[Application.services.tracks],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
):
    if not modules_config.is_reporter_enabled('pro'):
        abort(404)

    try:
        data = TracksProInSchema.parse_obj(request.json)
    except (ValidationError, ValueError) as exc:
        abort(400, str(exc))

    for tracks_uuids_batch in batch_iterator(data.track_uuids, 1000):
        tracks_service.bulk_change_pro_status(tracks_uuids_batch, data.action.value)

    return jsonify({'track_uuids': data.track_uuids})


@inject
def resend_tracks_by_drivers(
    tracks_service: TracksService = Provide[Application.services.tracks],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
):
    if not modules_config.is_reporter_enabled('pro'):
        abort(404)

    query_params = TrackQueryParameters.from_request(request)

    try:
        data = TracksByDriversInSchema.parse_obj(request.json)
    except (ValidationError, ValueError) as exc:
        abort(400, str(exc))

    drivers = set()
    updated_tracks_count = 0

    tracks_batch = []
    for driver in data.drivers:
        query_params.email = driver.email
        query_params.from_dt = datetime.datetime.combine(driver.date, datetime.datetime.min.time())
        query_params.to_dt = query_params.from_dt + datetime.timedelta(days=1)

        tracks_batch.extend(tracks_service.find_tracks_by_query_params(query_params))
        if len(tracks_batch) >= 1000:
            tracks_uuids_batch = [track.uuid for track in tracks_batch]
            drivers.update({track.user_email for track in tracks_batch})

            tracks_service.bulk_change_pro_status(tracks_uuids_batch, data.action.value)
            updated_tracks_count += len(tracks_uuids_batch)
            tracks_batch = []

    if tracks_batch:
        tracks_uuids_batch = [track.uuid for track in tracks_batch]
        drivers.update({track.user_email for track in tracks_batch})

        tracks_service.bulk_change_pro_status(tracks_uuids_batch, data.action.value)
        updated_tracks_count += len(tracks_uuids_batch)

    return jsonify(
        {
            'updated_tracks_count': updated_tracks_count,
            'drivers': list(drivers),
        },
    )

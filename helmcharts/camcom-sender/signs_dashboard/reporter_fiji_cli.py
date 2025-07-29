import logging
import os
import time
import typing as tp
from collections import Counter
from datetime import datetime, timedelta, timezone

from dependency_injector.wiring import Provide, inject
from requests import exceptions

from signs_dashboard.containers.application import Application
from signs_dashboard.context import context_aware_track_iterator
from signs_dashboard.models.frame import Frame
from signs_dashboard.models.track import Track, TrackStatuses, TrackType
from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.schemas.fiji.request import FijiRequest
from signs_dashboard.schemas.fiji.response import FijiResponse
from signs_dashboard.schemas.track_processing_status import TrackProcessingState
from signs_dashboard.services.fiji_client import FijiClient
from signs_dashboard.services.fiji_quality import IMAGE_QUALITY_LABELS, FijiQualityChecker, FijiQualityCheckResult
from signs_dashboard.services.frames import FramesService
from signs_dashboard.services.image import ImageService
from signs_dashboard.services.prediction import FramesBatchAttributes, PredictionService, PredictionStatusProxy
from signs_dashboard.services.tracks import TracksService

logger = logging.getLogger(__name__)

DAYS_TO_UPLOAD_EXPIRED = 1
SUSPEND_TIME_SEC = int(os.environ.get('REPORTER_FIJI_SUSPEND_TIME') or 10)


# flake8: noqa: C901
@inject
def run(
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
    tracks_service: TracksService = Provide[Application.services.tracks],
    prediction_service: PredictionService = Provide[Application.services.prediction],
    frames_service: FramesService = Provide[Application.services.frames],
    image_service: ImageService = Provide[Application.services.image],
    fiji_client: FijiClient = Provide[Application.services.fiji_client],
    fiji_quality: FijiQualityChecker = Provide[Application.services.fiji_quality],
):
    timeout_days = modules_config.get_fiji_reporter_timeout() or DAYS_TO_UPLOAD_EXPIRED
    needed_predictors = modules_config.get_predictors_for('fiji')

    while True:
        tracks = tracks_service.get_fiji_uploading_tracks(fiji_client.max_retries, fiji_client.retries_timeout)
        logger.info(f'Processing - {len(tracks)} tracks.')

        counter = 0
        for track in context_aware_track_iterator(tracks):
            id_log = f'[id]: {track.uuid}'
            logger.info(f'{id_log}, Processing track.')

            frames = frames_service.get_by_track(track)
            total_signs = 0
            for frame in frames:
                frame.track_email = track.user_email
                total_signs += len(frame.detections)

            predictions_status = prediction_service.get_frames_predictions_status(frames, needed_predictors)

            status = _proceed_track(track, frames, predictions_status, timeout_days=timeout_days)

            if status in {TrackStatuses.NOT_COMPLETE, TrackStatuses.FIJI_UNSUPPORTED_TRACK_TYPE}:
                tracks_service.change_fiji_status(track.uuid, status)
                logger.warning(f'{id_log}, Track is not complete or unsupported type.')
                counter += 1

            if status == TrackStatuses.SENT_FIJI:
                frames_attributes = prediction_service.get_frames_attributes(frames, needed_predictors)

                labels_stats = _build_labels_stats(frames_attributes)
                forced_send = track.is_forced_fiji_send()

                track_request = create_fiji_request(
                    prediction_service=prediction_service,
                    fiji_quality=fiji_quality,
                    predictions_status=predictions_status,
                    image_service=image_service,
                    modules_config=modules_config,
                    track=track,
                    frames=frames,
                    forced_send=forced_send,
                )

                if track_request is None:
                    logger.warning(f'{id_log}, Track frames quality is too low.')
                    tracks_service.change_fiji_status(track.uuid, TrackStatuses.LOW_QUALITY)
                    continue

                forced_fiji_host = track.upload.init_metadata.get('fiji_host', None)

                tracks_service.change_fiji_status(track.uuid, status)

                fiji_response, is_last_try = _get_fiji_response(
                    track,
                    track_request=track_request,
                    forced_fiji_host=forced_fiji_host,
                    fiji_client=fiji_client,
                    tracks_service=tracks_service,
                )

                tracks_service.save_fiji_results(track, fiji_response, labels_stats, total_signs)

                if is_last_try:
                    logger.info(f'{id_log}, Track complete.')
                else:
                    logger.warning(f'{id_log}, Fiji request failed. Retry scheduled.')

                counter += 1

        if counter == 0:
            time.sleep(SUSPEND_TIME_SEC)


def create_fiji_request(
    prediction_service: PredictionService,
    fiji_quality: FijiQualityChecker,
    predictions_status: PredictionStatusProxy,
    image_service: ImageService,
    modules_config: ModulesConfig,
    track: Track,
    frames: list[Frame],
    forced_send: bool,
) -> tp.Optional[FijiRequest]:
    needed_predictors = modules_config.get_predictors_for('fiji')
    frames_attributes = prediction_service.get_frames_attributes(frames, needed_predictors)

    quality_checks = fiji_quality.check_frames_quality(frames_attributes)
    if not quality_checks.passed and track.is_mobile() and not forced_send:
        return None

    return FijiRequest(
        id=track.uuid,
        type=track.type,
        user_email=track.user_email,
        metadata=track.upload.to_fiji_metadata(),
        gps_track=track.upload.current_gps_points,
        frames=[
            _create_fiji_request(frame, predictions_status, frames_attributes, image_service=image_service)
            for frame in frames
        ],
        quality_check=_build_quality_check_result(quality_checks, forced_send),
    )


def _get_fiji_response(
    track: Track,
    track_request: FijiRequest,
    forced_fiji_host: str,
    fiji_client: FijiClient,
    tracks_service: TracksService,
) -> tuple[tp.Optional[FijiResponse], bool]:
    need_retry = False

    retry_counter = 0
    if track.fiji_request:
        retry_counter = track.fiji_request.retries + 1
        is_last_try = retry_counter >= fiji_client.max_retries
    else:
        is_last_try = False

    if retry_counter > 0:
        logger.info(
            f'Retrying request to Fiji for track_uuid {track.uuid} {retry_counter} of {fiji_client.max_retries}',
        )

    try:
        response, fiji_response = fiji_client(track_request, forced_fiji_host=forced_fiji_host)
        response_status = response.status_code
        response_text = response.text
    except (
        exceptions.ConnectionError,
        exceptions.ReadTimeout,
        exceptions.ConnectTimeout,
    ) as exc:
        response, fiji_response = None, None

        exc_log_level = logging.ERROR if is_last_try else logging.INFO
        logger.log(exc_log_level, f'Fiji request failed: {exc}', exc_info=True)

        response_status = None
        response_text = str(exc)

    last_request_time = datetime.now(timezone.utc)

    if fiji_response is None:
        fiji_status = TrackStatuses.FIJI_INVALID_TRACK

        if not response_status or response_status >= 500 or response_status == 200:
            need_retry = True

            if response_status != 200:
                fiji_status = TrackStatuses.FIJI_NOT_AVAILABLE
    else:
        need_retry = fiji_response.processing_status in TrackStatuses.FIJI_RETRYABLE
        fiji_status = fiji_response.processing_status

    tracks_service.save_fiji_request(
        track_uuid=track.uuid,
        last_response=response_text,
        last_request_time=last_request_time,
        retries=retry_counter,
        last_response_status=response_status,
        last_fiji_status=fiji_status,
    )

    retry_scheduled = need_retry and not is_last_try
    if retry_scheduled:
        if track.fiji_status != TrackStatuses.FIJI_SENDING_IN_PROCESS:
            tracks_service.change_fiji_status(track.uuid, TrackStatuses.FIJI_SENDING_IN_PROCESS)

    if is_last_try and fiji_response is None:
        tracks_service.change_fiji_status(track.uuid, fiji_status)

    return fiji_response if not retry_scheduled else None, is_last_try


def _build_quality_check_result(quality_checks: FijiQualityCheckResult, forced: bool):
    return {
        'passed': True if forced else quality_checks.passed,
        'forced': forced,
        'checks': quality_checks.checks,
    }


def _build_labels_stats(frames_attributes: FramesBatchAttributes) -> dict:
    if 'labels' not in frames_attributes.predictors:
        return {'total': 0}
    labels = sum(
        [
            frames_attributes.get_frame_attribute(frame_id, IMAGE_QUALITY_LABELS, [])
            for frame_id in frames_attributes.frame_ids
            if frames_attributes.get_frame_attribute(frame_id, IMAGE_QUALITY_LABELS, [])
        ],
        [],
    )
    counter = dict(Counter(labels))
    counter['total'] = len(frames_attributes.frame_ids)
    return counter


def _create_fiji_request(
    frame: Frame,
    predictions_result: PredictionStatusProxy,
    frames_attributes: FramesBatchAttributes,
    image_service: ImageService,
) -> dict:
    link = image_service.get_s3_path(frame)

    errors = predictions_result.get_errors(frame.id)
    if errors:
        predictions = {
            'errors': errors,
        }
    else:
        signs_prediction = predictions_result.get_prediction('signs', frame.id)

        signs = []
        if signs_prediction.raw_data:
            for index, sign_data in enumerate(signs_prediction.raw_data):
                sign_data.update(
                    {
                        'backref_id': f'{frame.id}_{index}',
                    }
                )
                signs.append(sign_data)

        recognized_road_marking = None
        road_marking_distance = frames_attributes.get_frame_attribute(frame.id, 'road_marking_distance')
        if road_marking_distance is not None:
            recognized_road_marking = {'distance': road_marking_distance}

        predictions = {
            'signs': signs,
            'labels': frames_attributes.get_frame_attribute(frame.id, IMAGE_QUALITY_LABELS, []),
            'road_surface': {
                'surface': frames_attributes.get_frame_attribute(frame.id, 'surface') or 'unknown',
                'asphalt_quality': frames_attributes.get_frame_attribute(frame.id, 'asphalt_quality') or 'unknown',
            },
            'recognized_road_marking': recognized_road_marking,
        }

    return {
        'id': frame.id,
        'track_point': {
            'coordinate': {
                'lat': frame.lat,
                'lon': frame.lon,
            },
            'datetime_utc': frame.timestamp,
            'azimuth': frame.azimuth,
            'speed': frame.speed,
        },
        'link': link,
        **predictions,
    }


def _proceed_track(
    track: Track,
    frames: tp.List[Frame],
    prediction_status: PredictionStatusProxy,
    timeout_days: int,
) -> tp.Optional[int]:
    processing_status = TrackProcessingState(
        track=track,
        frames=frames,
        predictions_status=prediction_status,
    )
    if track.type == TrackType.video360 or any(frame.panoramic for frame in frames):
        return TrackStatuses.FIJI_UNSUPPORTED_TRACK_TYPE

    if processing_status.all_frames_uploaded_and_predicted:
        return TrackStatuses.SENT_FIJI

    if processing_status.timedelta_since_last_upload_action > timedelta(days=timeout_days):
        return TrackStatuses.NOT_COMPLETE

    return None

import logging
from datetime import datetime

from dependency_injector.wiring import Provide, inject

from signs_dashboard.containers.application import Application
from signs_dashboard.context import ContextService
from signs_dashboard.errors.workers import ParseMessageError
from signs_dashboard.events_tools import parse_message_key, parse_track_from_lifecycle_event
from signs_dashboard.models.frame import Frame
from signs_dashboard.models.track import Track, TrackType
from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.schemas.events.tracks_lifecycle import TrackEventType
from signs_dashboard.services.detection_clusterization import DetectionClusterizationService
from signs_dashboard.services.detection_localization import DetectionLocalizationService
from signs_dashboard.services.frames import FramesService
from signs_dashboard.services.kafka_service import KafkaService, commit_single_message
from signs_dashboard.services.prediction import PredictionService
from signs_dashboard.services.tracks import TracksService

logger = logging.getLogger(__name__)

CONSUME_TIMEOUT_MS = 30 * 60 * 1000
DAYS_TO_UPLOAD_EXPIRED = 1
EXPECTED_TRACK_EVENT_TYPES = (
    TrackEventType.localization_required,
    TrackEventType.localization_forced,
)
EXPECTED_TRACK_TYPES = (
    TrackType.mobile,
)


@inject
def run(
    kafka_service: KafkaService = Provide[Application.services.kafka],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
    tracks_service: TracksService = Provide[Application.services.tracks],
    clusterization_service: DetectionClusterizationService = Provide[Application.services.detections_clusterization],
    prediction_service: PredictionService = Provide[Application.services.prediction],
    frames_service: FramesService = Provide[Application.services.frames],
    localization_service: DetectionLocalizationService = Provide[Application.services.detections_localization],
):
    if not modules_config.is_track_localization_enabled():
        logger.error('Detections localization disabled!')
        return

    logger.warning('Starting detections-localizer...')

    consumer = kafka_service.get_detections_localizer_consumer(consume_message_timeout_ms=CONSUME_TIMEOUT_MS)

    for message in consumer:
        user_email = parse_message_key(message)

        try:
            track, event = parse_track_from_lifecycle_event(
                message,
                expected_event_types=EXPECTED_TRACK_EVENT_TYPES,
                expected_track_types=EXPECTED_TRACK_TYPES,
                track_getter=tracks_service.get_with_localization_statuses,
            )
        except ParseMessageError as exc:
            logger.exception(f'Unable to parse message {message.topic} with user_email {user_email}: {exc}')
            track, event = None, None

        if not track:
            commit_single_message(consumer, message)
            continue

        with ContextService(track_uuid=track.uuid):
            logger.warning(f'Processing track {track.uuid}')

            timestamp = datetime.now()

            frames = frames_service.get_by_track_for_localization(
                track,
                ignore_predictions_status=event.event_type == TrackEventType.localization_forced,
            )
            affected_detectors = list({
                detection.detector_name
                for frame in frames
                for detection in frame.detections
            })
            tracks_service.mark_localization_started(track.uuid, detectors=affected_detectors)

            try:
                _localize_track(
                    track=track,
                    frames=frames,
                    localization_service=localization_service,
                    clusterization_service=clusterization_service,
                    prediction_service=prediction_service,
                )
            except Exception as exc:
                logger.exception(f'Got exception while localizing track {track.uuid}: {exc}')
                tracks_service.mark_localization_failed(track.uuid, detectors=affected_detectors)
            else:
                tracks_service.mark_localization_done(
                    track_uuid=track.uuid,
                    detectors=affected_detectors,
                    last_done=timestamp,
                )

        logger.warning(f'Done processing track {track.uuid}')
        commit_single_message(consumer, message)


def _localize_track(  # noqa: WPS211, WPS213
    track: Track,
    frames: list[Frame],
    clusterization_service: DetectionClusterizationService,
    prediction_service: PredictionService,
    localization_service: DetectionLocalizationService,
):
    logger.warning(f'Processing {len(frames)} frames')
    if frames:
        updated_track_detections = localization_service.localize_frames(frames)

        prediction_service.save_detections_locations(updated_track_detections)

        logger.info(f'Saved {len(updated_track_detections)} signs locations from {len(frames)} frames')

        _clusterize_track_detections(
            track,
            updated_track_detections,
            clusterization_service=clusterization_service,
        )


def _clusterize_track_detections(
    track: Track,
    updated_track_detections,
    clusterization_service: DetectionClusterizationService,
):
    localized_detections = [
        detection
        for detection in updated_track_detections
        if detection.lat and detection.lon
    ]
    if localized_detections:
        logger.info(f'Track {track.uuid}, running clusterization...')
        clusterization_service.clusterize(localized_detections, for_track=track)
        logger.info(f'Track {track.uuid}, done clusterization')

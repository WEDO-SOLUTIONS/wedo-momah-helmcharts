import logging
from itertools import product

from dependency_injector.wiring import Provide, inject

from signs_dashboard.containers.application import Application
from signs_dashboard.context import ContextService
from signs_dashboard.errors.workers import ParseMessageError
from signs_dashboard.events_tools import parse_message_key, parse_track_from_lifecycle_event
from signs_dashboard.schemas.events.tracks_lifecycle import TrackEventType
from signs_dashboard.services.events.frames_lifecycle import FramesLifecycleService
from signs_dashboard.services.kafka_service import KafkaService, commit_single_message
from signs_dashboard.services.pano_conversions.common import CropsParams
from signs_dashboard.services.predictors import PredictorsService
from signs_dashboard.services.tracks import TracksService
from signs_dashboard.services.video_frames_saver import VideoFramesSaverService

logger = logging.getLogger(__name__)

REQUIRED_TRACK_EVENT_TYPES = (
    TrackEventType.remote_upload_completed,
)
REQUIRED_TRACK_TYPES = (
    'video_360',
)
VIDEO_PROCESSING_TIMEOUT_MS = 30 * 60 * 1000


@inject
def video_frames_save(
    kafka_service: KafkaService = Provide[Application.services.kafka],
    tracks_service: TracksService = Provide[Application.services.tracks],
    video_frames_saver_service: VideoFramesSaverService = Provide[Application.services.video_frames_saver],
    frames_lifecycle_service: FramesLifecycleService = Provide[Application.services.frames_lifecycle],
    predictors_service: PredictorsService = Provide[Application.services.predictors],
):
    consumer = kafka_service.get_video_frames_saver_consumer(
        consume_message_timeout_ms=VIDEO_PROCESSING_TIMEOUT_MS,
    )

    for message in consumer:
        user_email = parse_message_key(message)

        try:
            track, _ = parse_track_from_lifecycle_event(
                message,
                expected_event_types=REQUIRED_TRACK_EVENT_TYPES,
                expected_track_types=REQUIRED_TRACK_TYPES,
                track_getter=tracks_service.get,
            )
        except ParseMessageError as exp:
            logger.exception(
                f'Unable to parse message {message.topic} with user_email {user_email}, skipping event: {exp.message}',
            )
            track = None

        if track:
            with ContextService(track_uuid=track.uuid):
                frames = video_frames_saver_service.save_video_frames(track)

                for frame, theta in product(frames, CropsParams.CROPS_Z_POSITIONS):
                    frames_lifecycle_service.produce_uploaded_event(
                        frame,
                        required_predictors=predictors_service.get_active_predictors(),
                        prompt=predictors_service.get_prompt(),
                        theta=theta,
                    )

        commit_single_message(consumer, message)

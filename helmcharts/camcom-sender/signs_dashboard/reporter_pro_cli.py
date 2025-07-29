import logging

from dependency_injector.wiring import Provide, inject
from kafka.consumer.fetcher import ConsumerRecord

from signs_dashboard.containers.application import Application
from signs_dashboard.context import ContextService
from signs_dashboard.errors.workers import ParseMessageError
from signs_dashboard.events_tools import (
    parse_frame_from_lifecycle_event,
    parse_message_key,
    parse_object_lifecycle_event,
    parse_track_lifecycle_event,
)
from signs_dashboard.schemas.events.detected_objects_lifecycle import DetectedObjectEventType
from signs_dashboard.schemas.events.frame_lifecycle import FrameEventType
from signs_dashboard.schemas.events.tracks_lifecycle import TrackEventType
from signs_dashboard.services.frames import FramesService
from signs_dashboard.services.kafka_service import KafkaService, commit_single_message
from signs_dashboard.services.twogis_pro.synchronization import TwoGisProSyncService

logger = logging.getLogger(__name__)
REQUIRED_FRAME_EVENT_TYPES = (
    FrameEventType.uploaded,
    FrameEventType.prediction_saved,
    FrameEventType.moderation_saved,
    FrameEventType.pro_resend,
    FrameEventType.map_matching_done,
    FrameEventType.visual_localization_done,
)
REQUIRED_OBJECT_EVENT_TYPES = (
    DetectedObjectEventType.created,
    DetectedObjectEventType.updated,
    DetectedObjectEventType.deleted,
    DetectedObjectEventType.pro_resend,
)
REQUIRED_TRACK_EVENT_TYPES = (
    TrackEventType.gps_track_added,
    TrackEventType.uploaded,
    TrackEventType.predicted_for_pro,
    TrackEventType.resend_gps_track_to_pro,
    TrackEventType.map_matching_done,
    TrackEventType.visual_localization_done,
)


@inject
def run(
    kafka_service: KafkaService = Provide[Application.services.kafka],
    twogis_pro_sync_service: TwoGisProSyncService = Provide[Application.services.twogis_pro_sync],
    frames_service: FramesService = Provide[Application.services.frames],
):
    consumer = kafka_service.get_pro_reporter_consumer()

    for message in consumer:
        if message.topic == kafka_service.topics.frames_lifecycle:
            _handle_frame_event(message, twogis_pro_sync_service, frames_service=frames_service)
        elif message.topic == kafka_service.topics.objects_lifecycle:
            _handle_object_event(message, twogis_pro_sync_service)
        elif message.topic == kafka_service.topics.tracks_lifecycle:
            _handle_track_event(message, twogis_pro_sync_service)
        commit_single_message(consumer, message)


def _handle_frame_event(
    message: ConsumerRecord,
    twogis_pro_sync_service: TwoGisProSyncService,
    frames_service: FramesService,
):
    track_uuid = parse_message_key(message)

    if not track_uuid:
        logger.error(f'No track uuid in message key from {message.topic}, skipping event')
        return

    try:
        frame, event = parse_frame_from_lifecycle_event(
            message,
            expected_event_types=REQUIRED_FRAME_EVENT_TYPES,
            frames_service=frames_service,
        )
    except ParseMessageError as exp:
        logger.exception(
            f'Unable to parse message {message.topic} with track_uuid {track_uuid}, skipping event: {exp.message}',
        )
        frame, event = None, None

    if frame and frame.uploaded_photo and not frame.panoramic:
        twogis_pro_sync_service.sync_frames_by_event_type(event.event_type, [frame])


def _handle_object_event(
    message: ConsumerRecord,
    twogis_pro_sync_service: TwoGisProSyncService,
):
    try:
        event = parse_object_lifecycle_event(message, expected_event_types=REQUIRED_OBJECT_EVENT_TYPES)
    except ParseMessageError as exp:
        logger.exception(
            f'Unable to parse message {message.topic}, skipping event: {exp.message}',
        )
        event = None

    if event:
        twogis_pro_sync_service.sync_object(event)


def _handle_track_event(
    message: ConsumerRecord,
    twogis_pro_sync_service: TwoGisProSyncService,
):
    user_email = parse_message_key(message)

    if not user_email:
        logger.error(f'No user email in message key from {message.topic}, skipping event')
        return

    try:
        event = parse_track_lifecycle_event(message, expected_event_types=REQUIRED_TRACK_EVENT_TYPES)
    except ParseMessageError as exp:
        logger.exception(
            f'Unable to parse message {message.topic}, skipping event: {exp.message}',
        )
        event = None

    if not event:
        return

    with ContextService(track_uuid=event.track_uuid):
        twogis_pro_sync_service.sync_driver(event.track_uuid, user_email)

import logging
from typing import Optional

from dependency_injector.wiring import Provide, inject
from kafka.consumer.fetcher import ConsumerRecord

from signs_dashboard.containers.application import Application
from signs_dashboard.errors.workers import ParseMessageError
from signs_dashboard.events_tools import parse_frame_lifecycle_event, parse_message_key
from signs_dashboard.models.frame import Frame
from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.schemas.events.frame_lifecycle import AnyFrameEvent, FrameEventType
from signs_dashboard.services.camcom.camcom_sender import CamcomSenderService
from signs_dashboard.services.events.frames_lifecycle import FramesLifecycleService
from signs_dashboard.services.frames import FramesService
from signs_dashboard.services.interest_zones import InterestZonesService
from signs_dashboard.services.kafka_service import KafkaService, commit_single_message
from signs_dashboard.services.prediction import IZ_PREDICTOR_NAME, PredictionService

logger = logging.getLogger(__name__)
REQUIRED_FRAME_EVENT_TYPES_W_MAPMATCHING = (
    FrameEventType.prediction_required,
    FrameEventType.map_matching_done,
)
REQUIRED_FRAME_EVENT_TYPES_WO_MAPMATCHING = (
    FrameEventType.prediction_required,
    FrameEventType.uploaded,
)


@inject
def camcom_sender(
    kafka_service: KafkaService = Provide[Application.services.kafka],
    camcom_sender_service: CamcomSenderService = Provide[Application.services.camcom_sender],
    prediction_service: PredictionService = Provide[Application.services.prediction],
    frames_service: FramesService = Provide[Application.services.frames],
    frames_lifecycle_services: FramesLifecycleService = Provide[Application.services.frames_lifecycle],
    interest_zones_service: InterestZonesService = Provide[Application.services.interest_zones],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
):
    consumer = kafka_service.get_camcom_frames_events_consumer()
    frame_event_types = REQUIRED_FRAME_EVENT_TYPES_WO_MAPMATCHING
    if modules_config.is_map_matching_enabled():
        frame_event_types = REQUIRED_FRAME_EVENT_TYPES_W_MAPMATCHING

    for message in consumer:
        track_uuid = parse_message_key(message)

        try:
            event, frame = parse_frame_from_lifecycle_event_to_camcom(
                message,
                expected_event_types=frame_event_types,
                frames_service=frames_service,
            )
        except ParseMessageError as exp:
            logger.exception(
                f'Unable to parse message {message.topic} with track_uuid {track_uuid}, skiping event: {exp.message}',
            )
            event, frame = None, None

        if event and frame:
            if event.event_type == FrameEventType.prediction_required and event.recalculate_interest_zones:
                interest_zones_service.update_frame_interest_zones(frame)
                frames_lifecycle_services.produce_pro_resend_event(frame_id=frame.id, track_uuid=frame.track_uuid)
            attributes = prediction_service.get_frame_attributes(frame, IZ_PREDICTOR_NAME)
            camcom_sender_service.send(frame, attributes)

        commit_single_message(consumer, message)


def parse_frame_from_lifecycle_event_to_camcom(  # noqa: C901
    message: ConsumerRecord,
    expected_event_types: tuple[FrameEventType, ...],
    frames_service: FramesService,
) -> tuple[Optional[AnyFrameEvent], Optional[Frame]]:
    event = parse_frame_lifecycle_event(message, expected_event_types)
    if not event:
        return None, None

    camcom_required = 'camcom' in getattr(event, 'required_predictors', [])

    if event.event_type != FrameEventType.map_matching_done and not camcom_required:
        return None, None

    if getattr(event, 'frame_type', None) == 'video_360':
        return None, None

    return event, frames_service.get_frame(event.frame_id)

import json
import logging
from datetime import datetime, timedelta
from enum import Enum
from functools import partial
from typing import Callable, Generic, Optional, TypeVar

from kafka.consumer.fetcher import ConsumerRecord
from pydantic import BaseModel, parse_obj_as

from signs_dashboard.errors.workers import ParseMessageError
from signs_dashboard.models.frame import Frame
from signs_dashboard.models.track import Track
from signs_dashboard.schemas.events.detected_objects_lifecycle import DetectedObjectEvent
from signs_dashboard.schemas.events.frame_lifecycle import AnyFrameEvent, FrameEventType
from signs_dashboard.schemas.events.tracks_lifecycle import AnyTrackEvent, TrackEventType
from signs_dashboard.services.frames import FramesService

logger = logging.getLogger(__name__)

EventType = TypeVar('EventType', bound=BaseModel)


def parse_lifecycle_event(
    message: ConsumerRecord,
    expected_event_types: tuple[Enum, ...],
    event_type: Generic[EventType],
    id_field: str,
) -> Optional[EventType]:
    try:
        message_body = json.loads(message.value)
    except Exception:
        raise ParseMessageError(f'Got non-json lifecycle event with key {message.key}: {message.value}')

    try:
        event = parse_obj_as(event_type, message_body)
    except Exception:
        raise ParseMessageError(f'Can not parse message body to event: {message_body}')

    if event.event_type not in expected_event_types:
        logger.debug(f'Got lifecycle event with unexpected type {event.event_type}: {message_body}')
        return None

    produced = _get_event_produced_diff(message)
    logger.warning(
        f'Got lifecycle event with type={event.event_type} for {getattr(event, id_field)} {message.key}, {produced}',
    )
    return event


parse_object_lifecycle_event = partial(parse_lifecycle_event, event_type=DetectedObjectEvent, id_field='object_id')
parse_frame_lifecycle_event = partial(parse_lifecycle_event, event_type=AnyFrameEvent, id_field='frame_id')
parse_track_lifecycle_event = partial(parse_lifecycle_event, event_type=AnyTrackEvent, id_field='track_uuid')


def parse_frame_from_lifecycle_event(  # noqa: C901
    message: ConsumerRecord,
    expected_event_types: tuple[FrameEventType, ...],
    frames_service: FramesService,
) -> tuple[Optional[Frame], Optional[AnyFrameEvent]]:
    event = parse_frame_lifecycle_event(message, expected_event_types)
    if not event:
        return None, None

    if event.event_type == FrameEventType.uploaded and event.frame_type == 'video_360':
        return None, None

    return frames_service.get_frame_for_pro(event.frame_id), event


def parse_track_from_lifecycle_event(  # noqa: C901
    message: ConsumerRecord,
    expected_event_types: tuple[TrackEventType, ...],
    expected_track_types: tuple[str],
    track_getter: Callable[[str], Optional[Track]],
) -> tuple[Optional[Track], Optional[AnyTrackEvent]]:
    event = parse_track_lifecycle_event(message, expected_event_types)
    if not event:
        return None, None

    track = track_getter(event.track_uuid)
    if track and track.type in expected_track_types:
        return track, event
    return None, None


def parse_message_key(message: ConsumerRecord) -> Optional[str]:
    try:
        return message.key.decode('utf-8')
    except Exception as exc:
        logger.exception(f'Unable to decode message key {message.key}: {exc}')
        return None


def _get_event_produced_diff(message: ConsumerRecord) -> str:
    delta = datetime.now().timestamp() - message.timestamp / 1000
    return f'produced {timedelta(seconds=delta)} ago'

import logging
from typing import Optional

from dependency_injector.wiring import Provide, inject

from signs_dashboard.containers.application import Application
from signs_dashboard.context import ContextService
from signs_dashboard.errors.workers import ParseMessageError
from signs_dashboard.events_tools import parse_track_lifecycle_event
from signs_dashboard.models.frame import Frame
from signs_dashboard.models.track import Track, TrackStatuses
from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.schemas.events.tracks_lifecycle import AnyTrackEvent, TrackEventType
from signs_dashboard.services.events.frames_lifecycle import FramesLifecycleService
from signs_dashboard.services.events.tracks_lifecycle import TracksLifecycleService
from signs_dashboard.services.kafka_service import KafkaService, commit_single_message
from signs_dashboard.services.map_matching.client import UnableToMatchTrackError
from signs_dashboard.services.map_matching.service import LegacyMapMatchingService, MapMatchingService
from signs_dashboard.services.tracks import TracksService

logger = logging.getLogger(__name__)
REQUIRED_TRACK_EVENT_TYPES = (
    TrackEventType.uploaded,
    TrackEventType.map_matching_required,
)


@inject
def map_matcher(
    kafka_service: KafkaService = Provide[Application.services.kafka],
    tracks_service: TracksService = Provide[Application.services.tracks],
    tracks_lifecycle_service: TracksLifecycleService = Provide[Application.services.tracks_lifecycle],
    frames_lifecycle_service: FramesLifecycleService = Provide[Application.services.frames_lifecycle],
    legacy_map_matching_service: LegacyMapMatchingService = Provide[Application.services.legacy_map_matching],
    map_matching_service: MapMatchingService = Provide[Application.services.map_matching],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
):
    if not modules_config.is_map_matching_enabled():
        logger.error('Map matching disabled, exiting')
        return

    consumer = kafka_service.get_map_matcher_consumer()

    if modules_config.is_interpolating_map_matching_enabled():
        logger.info('Using map matching implementation with interpolation')
        map_matching_service = legacy_map_matching_service

    for message in consumer:
        try:
            event = parse_track_lifecycle_event(message, expected_event_types=REQUIRED_TRACK_EVENT_TYPES)
        except ParseMessageError as exp:
            logger.exception(
                f'Unable to parse message {message.topic}, skipping event: {exp.message}',
            )
            event = None

        if event:
            _process_event(
                event,
                tracks_service=tracks_service,
                map_matching_service=map_matching_service,
                tracks_lifecycle_service=tracks_lifecycle_service,
                frames_lifecycle_service=frames_lifecycle_service,
            )

        commit_single_message(consumer, message)


def _process_event(
    event: AnyTrackEvent,
    tracks_service: TracksService,
    map_matching_service: MapMatchingService,
    tracks_lifecycle_service: TracksLifecycleService,
    frames_lifecycle_service: FramesLifecycleService,
):
    with ContextService(track_uuid=event.track_uuid):
        track, frames = _try_match_track(
            event=event,
            tracks_service=tracks_service,
            map_matching_service=map_matching_service,
        )
        if track:
            tracks_lifecycle_service.produce_map_matching_done_event(
                track_uuid=track.uuid,
                user_email=track.user_email,
            )

        for frame in frames:
            frames_lifecycle_service.produce_map_matching_done_event(frame)


def _try_match_track(
    event: AnyTrackEvent,
    tracks_service: TracksService,
    map_matching_service: MapMatchingService,
) -> tuple[Optional[Track], list[Frame]]:
    track = tracks_service.get(event.track_uuid)
    if not track:
        logger.info(f'Track {event.track_uuid} not found')
        return None, []

    if track.map_matching_status == TrackStatuses.MAP_MATCHING_DISABLED:
        logger.warning(f'Track {track.uuid} skipped cause map matching is disabled for it')
        return None, []

    tracks_service.change_map_matching_status(track.uuid, TrackStatuses.MAP_MATCHING_IN_PROGRESS)

    matched = False
    frames = []
    try:
        frames = map_matching_service.match_track(track)
    except UnableToMatchTrackError as exc:
        logger.exception(f'Track {track.uuid} was not matched: {exc}')
        tracks_service.change_map_matching_status(track.uuid, TrackStatuses.MAP_MATCHING_ERROR)
    else:
        logger.info(f'Track {track.uuid} was successfully matched')
        tracks_service.change_map_matching_status(track.uuid, TrackStatuses.MAP_MATCHING_DONE)
        matched = True

    if matched:
        return track, frames
    return None, []

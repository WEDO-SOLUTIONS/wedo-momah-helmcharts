import logging
import os
import time
from datetime import timedelta
from typing import Iterable

from dependency_injector.wiring import Provide, inject

from signs_dashboard.containers.application import Application
from signs_dashboard.context import ContextService
from signs_dashboard.models.track import Track, TrackStatuses, TrackType
from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.schemas.config.reporters import DetectionsLocalizerConfig
from signs_dashboard.services.events.tracks_lifecycle import TracksLifecycleService
from signs_dashboard.services.tracks import TracksService
from signs_dashboard.small_utils import parse_timedelta_from_minutes_env_var

logger = logging.getLogger(__name__)

SUSPEND_TIME_SEC = int(os.environ.get('LIFECYCLE_CONTROLLER_SUSPEND_TIME') or 10)
LOCALIZATION_NOT_POSSIBLE_STATUSES = (
    TrackStatuses.LOCALIZATION_UNSUPPORTED_TRACK_TYPE,
    TrackStatuses.LOCALIZATION_DISABLED,
)
LOCALIZATION_RESCHEDULABLE_STATUSES = (
    TrackStatuses.LOCALIZATION_FORCED,
    TrackStatuses.LOCALIZATION_SCHEDULED,
    TrackStatuses.LOCALIZATION_IN_PROGRESS,
)
LOCALIZATION_EXPECTED_TRACK_TYPES = (
    TrackType.mobile,
)
UPLOADING_TRACK_MIN_LOCALIZATION_INTERVAL = parse_timedelta_from_minutes_env_var(
    'UPLOADING_TRACK_MIN_LOCALIZATION_INTERVAL_MINUTES',
    default=timedelta(minutes=10),
)
UPLOADED_TRACK_MIN_LOCALIZATION_INTERVAL = parse_timedelta_from_minutes_env_var(
    'UPLOADED_TRACK_MIN_LOCALIZATION_INTERVAL_MINUTES',
    default=timedelta(minutes=5),
)
SCHEDULED_LOCALIZATION_TIMEOUT = parse_timedelta_from_minutes_env_var(
    'SCHEDULED_LOCALIZATION_TIMEOUT_MINUTES',
    default=timedelta(hours=23),
)


@inject
def run(
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
    tracks_service: TracksService = Provide[Application.services.tracks],
    tracks_lifecycle_service: TracksLifecycleService = Provide[Application.services.tracks_lifecycle],
):
    if not modules_config.is_track_localization_enabled():
        logger.error('Detections localization disabled!')
        return

    detections_localizer_config = modules_config.detections_localizer

    logger.warning(f'Starting lifecycle controller with {detections_localizer_config.predictors=}...')
    if not detections_localizer_config.naive_localization:
        logger.warning(
            f'Localization requires detections from `{detections_localizer_config.depth_detector_name}`',
        )

    while True:
        tracks = tracks_service.get_localization_pending_tracks(
            expected_track_types=LOCALIZATION_EXPECTED_TRACK_TYPES,
            skip_localization_statuses=LOCALIZATION_NOT_POSSIBLE_STATUSES,
            scheduled_processing_timeout=SCHEDULED_LOCALIZATION_TIMEOUT,
            localization_requires_detections_from=detections_localizer_config.requires_detection_from_detector,
            track_upload_timeout=timedelta(days=detections_localizer_config.track_upload_timeout_days),
            uploading_track_localization_interval=UPLOADING_TRACK_MIN_LOCALIZATION_INTERVAL,
            uploaded_track_localization_interval=UPLOADED_TRACK_MIN_LOCALIZATION_INTERVAL,
        )
        logger.info(f'Processing - {len(tracks)} tracks.')

        counter = 0
        for track, new_detections_exists_for in _context_iterator(tracks):
            logger.info(f'Begin processing track, {new_detections_exists_for=}.')
            scheduled, forced = _process_track(
                track,
                detections_localizer_config=detections_localizer_config,
                updated_detectors=new_detections_exists_for,
                tracks_service=tracks_service,
                tracks_lifecycle_service=tracks_lifecycle_service,
            )
            if scheduled:
                logger.warning(f'Track {track.uuid} scheduled for localization, {forced=}.')
                counter += 1

        if counter == 0:
            time.sleep(SUSPEND_TIME_SEC)


def _process_track(  # noqa: WPS231
    track: Track,
    detections_localizer_config: DetectionsLocalizerConfig,
    updated_detectors: list[str],
    tracks_service: TracksService,
    tracks_lifecycle_service: TracksLifecycleService,
) -> tuple[bool, bool]:
    if updated_detectors:
        if not detections_localizer_config.naive_localization:
            if updated_detectors == {detections_localizer_config.depth_detector_name}:
                return False, False

        tracks_service.mark_localization_scheduled(track.uuid, updated_detectors)
        tracks_lifecycle_service.produce_localization_required_event(track.uuid, track.user_email)
        return True, False

    forced = track.localization_status == TrackStatuses.LOCALIZATION_FORCED
    if track.localization_status in LOCALIZATION_RESCHEDULABLE_STATUSES:
        all_detectors, updated_detectors = tracks_service.fetch_track_detectors(track.uuid)
        if forced and all_detectors:
            tracks_service.mark_localization_scheduled(track.uuid, all_detectors)
            tracks_lifecycle_service.produce_localization_forced_event(track.uuid, track.user_email)
        elif updated_detectors:
            tracks_service.mark_localization_scheduled(track.uuid, updated_detectors)
            tracks_lifecycle_service.produce_localization_required_event(track.uuid, track.user_email)
        else:
            logger.warning(
                f'Inconsistent status {track.localization_status} and {all_detectors=}, {updated_detectors=}',
            )
            return False, False
        return True, forced

    logger.error('Unexpected state for track!')
    return False, False


def _context_iterator(tracks: Iterable[tuple[Track, list]]) -> Iterable[tuple[Track, list]]:
    for track, detectors in tracks:
        with ContextService(track_uuid=track.uuid):
            yield track, detectors

import logging
import os
import time
import typing as tp
from datetime import datetime, timedelta

from dependency_injector.wiring import Provide, inject

from signs_dashboard.containers.application import Application
from signs_dashboard.context import context_aware_track_iterator
from signs_dashboard.models.frame import Frame
from signs_dashboard.models.track import Track, TrackStatuses, TrackType
from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.schemas.events.frame_lifecycle import FrameEventType
from signs_dashboard.schemas.track_processing_status import TrackProcessingState
from signs_dashboard.services.frames import FramesService
from signs_dashboard.services.prediction import PredictionService, PredictionStatusProxy
from signs_dashboard.services.tracks import TracksService
from signs_dashboard.services.twogis_pro.synchronization import TwoGisProSyncService

logger = logging.getLogger(__name__)
DAYS_TO_UPLOAD_EXPIRED = 1
SUSPEND_TIME_SEC = int(os.environ.get('REPORTER_PRO_SUSPEND_TIME') or 10)


# flake8: noqa: C901
@inject
def run(
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
    tracks_service: TracksService = Provide[Application.services.tracks],
    frames_service: FramesService = Provide[Application.services.frames],
    prediction_service: PredictionService = Provide[Application.services.prediction],
    twogis_pro_sync_service: TwoGisProSyncService = Provide[Application.services.twogis_pro_sync],
):
    timeout_days = modules_config.get_pro_reporter_timeout() or DAYS_TO_UPLOAD_EXPIRED
    needed_predictors = modules_config.get_predictors_for('pro')
    logger.info(f'[PRO] Starting worker: {timeout_days=}, {needed_predictors=}')

    while True:
        tracks = tracks_service.get_pro_uploading_tracks()
        logger.info(f'[PRO] Processing - {len(tracks)} tracks.')

        counter = 0
        for track in context_aware_track_iterator(tracks):
            id_log = f'[PRO][id]: {track.uuid}'
            logger.info(f'{id_log}, Processing track.')

            frames = frames_service.get_by_track_for_pro(track)
            for frame in frames:
                frame.track_email = track.user_email

            frames_prediction_status = prediction_service.get_frames_predictions_status(frames, needed_predictors)

            status = _proceed_track(track, frames, frames_prediction_status, timeout_days=timeout_days)

            if status in {track.pro_status, TrackStatuses.PRO_UNSUPPORTED_TRACK_TYPE}:
                counter += 1
                if status != track.pro_status:
                    tracks_service.change_pro_status(track.uuid, status)
                logger.warning(f'{id_log}, Track skipped: status={status}')
                continue

            if track.pro_status == TrackStatuses.WILL_BE_HIDDEN_PRO:
                twogis_pro_sync_service.sync_frames_by_event_type(
                    event_type=FrameEventType.pro_hide,
                    frames=frames,
                )
                tracks_service.change_pro_status(track.uuid, status=TrackStatuses.HIDDEN_PRO)
                twogis_pro_sync_service.sync_driver(track.uuid, track.user_email)
                logger.warning(f'{id_log}, Track was hidden in Pro')
                continue

            if track.pro_status == TrackStatuses.FORCED_SEND:
                uploaded_frames = [
                    frame
                    for frame in frames
                    if frame.uploaded_photo
                ]
                twogis_pro_sync_service.sync_frames_by_event_type(
                    event_type=FrameEventType.pro_resend,
                    frames=uploaded_frames,
                )
                twogis_pro_sync_service.sync_driver(track.uuid, track.user_email)
                logger.warning(f'{id_log}, Track was send to Pro')
                if status is None:
                    tracks_service.change_pro_status(track.uuid, TrackStatuses.UPLOADING)
                    logger.warning(f'{id_log}, Track status set to uploading')

            if status == TrackStatuses.NOT_COMPLETE:
                tracks_service.change_pro_status(track.uuid, status)
                logger.warning(f'{id_log}, Track is not complete. {datetime.now()}, {track.upload.init_time}, {datetime.now() - track.upload.init_time}')
                counter += 1

            if status in (TrackStatuses.SENT_PRO, TrackStatuses.SENT_PRO_WITHOUT_PREDICTIONS):

                tracks_service.change_pro_status(track.uuid, status)

                if status == TrackStatuses.SENT_PRO:
                    tracks_service.produce_predicted_for_pro_event(track)
                    logger.info(f'{id_log}, Track sent.')
                else:
                    logger.info(f'{id_log}, Track sent without predictions.')
                counter += 1

        if counter == 0:
            time.sleep(SUSPEND_TIME_SEC)


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
        return TrackStatuses.PRO_UNSUPPORTED_TRACK_TYPE

    if processing_status.all_frames_uploaded and track.pro_status == TrackStatuses.UPLOADING:
        return TrackStatuses.SENT_PRO_WITHOUT_PREDICTIONS

    if processing_status.all_frames_uploaded_and_predicted:
        return TrackStatuses.SENT_PRO

    if processing_status.timedelta_since_last_upload_action > timedelta(days=timeout_days):
        return TrackStatuses.NOT_COMPLETE

    return None

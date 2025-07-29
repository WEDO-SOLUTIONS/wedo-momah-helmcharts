import logging
import typing as tp
from datetime import datetime

from signs_dashboard.models.frame import Frame
from signs_dashboard.models.track import Track
from signs_dashboard.query_params.frames import FramesQueryParameters
from signs_dashboard.repository.frames import FramesRepository
from signs_dashboard.small_utils import correct_round

logger = logging.getLogger(__name__)

P = tp.TypeVar('P')  # noqa: WPS111


class FramesService:
    def __init__(self, frames_repository: FramesRepository):
        self._frames_repository = frames_repository

    def get_or_create(self, uuid, meta) -> Frame:
        utc_dt = datetime.utcfromtimestamp(meta['ts'] / 1000)
        timezone_offset = meta.get('timezone_offset') or '+00:00'
        frame = self._frames_repository.get(utc_dt, uuid)

        if not frame:
            frame = Frame(
                track_uuid=uuid,
                lat=correct_round(meta['lat']),
                lon=correct_round(meta['lon']),
                azimuth=meta['azimuth'],
                speed=meta.get('speed', 0.0),  # noqa: WPS358
                date=utc_dt,
                timezone_offset=timezone_offset,
                track_email=meta.get('track_email'),
            )
            self._frames_repository.upsert(frame)
        return frame

    def save(self, frame: Frame):
        self._frames_repository.upsert(frame)

    def find(self, query_params: FramesQueryParameters) -> tp.List[Frame]:
        return self._frames_repository.find(query_params)

    def find_by_bbox(self, point1, point2, limit: int, scope: tp.Optional[str]) -> tp.List[Frame]:
        return self._frames_repository.find_by_bbox(point1, point2, limit, scope)

    def find_similar_frames(self, frame: Frame, distance: float, direction: float, limit: int) -> tp.List[Frame]:
        return self._frames_repository.find_similar_frames(frame, distance, direction, limit)

    def get_frame(self, frame_id: int) -> tp.Optional[Frame]:
        return self._frames_repository.get_frame(
            frame_id,
            include_detections=False,
            include_app_version=False,
            include_api_user=False,
        )

    def get_frame_w_detections(self, frame_id: int, from_detector: tp.Optional[str] = None):
        return self._frames_repository.get_frame(
            frame_id,
            include_detections=True,
            include_app_version=False,
            from_detector=from_detector,
            include_api_user=False,
        )

    def get_frame_for_pro(self, frame_id: int):
        return self._frames_repository.get_frame(
            frame_id,
            include_detections=True,
            include_app_version=True,
            include_api_user=True,
        )

    def get_next(self, frame: Frame) -> tp.Optional[Frame]:
        return self._frames_repository.get_next(frame)

    def get_prev(self, frame: Frame) -> tp.Optional[Frame]:
        return self._frames_repository.get_prev(frame)

    def get_frames(self, frames_ids: tp.List[int]) -> tp.List[Frame]:
        return self._frames_repository.get_frames(frames_ids)

    def get_by_track(self, track: Track) -> tp.List[Frame]:
        return self._frames_repository.get_by_track(track, include_app_version=False, include_api_user=False)

    def get_by_track_for_localization(self, track: Track, ignore_predictions_status: bool) -> tp.List[Frame]:
        return self._frames_repository.get_by_track_for_localization(
            track,
            ignore_predictions_status=ignore_predictions_status,
        )

    def get_by_track_for_pro(self, track: Track) -> tp.List[Frame]:
        return self._frames_repository.get_by_track(track, include_app_version=True, include_api_user=True)

    def get_by_track_uuid(self, track_uuid: str) -> tp.List[Frame]:
        return self._frames_repository.get_by_track_uuids([track_uuid])

    def count_by_track(self, track_uuid: str) -> int:
        return self._frames_repository.count_by_track(track_uuid)

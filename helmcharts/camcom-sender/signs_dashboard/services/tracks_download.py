import base64
import logging
from datetime import datetime

from signs_dashboard.errors.service import ImageReadError
from signs_dashboard.models.frame import Frame
from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.services.frames import FramesService
from signs_dashboard.services.image import ImageService
from signs_dashboard.services.track_length import TrackLengthService
from signs_dashboard.services.tracks import Track, TracksService

logger = logging.getLogger(__name__)


class TracksDownloaderService:
    def __init__(
        self,
        image_service: ImageService,
        tracks_service: TracksService,
        track_length_service: TrackLengthService,
        frames_service: FramesService,
        modules_config: ModulesConfig,
    ):
        self._track_length_service = track_length_service
        self._tracks_service = tracks_service
        self._frames_service = frames_service
        self._image_service = image_service
        self._modules_config = modules_config

    def download_track(self, request_data: dict, track_uuid: str, event_dt: datetime):
        upload_status = self._tracks_service.get_upload_status(track_uuid)

        if request_data['type'] == 'init':
            track_type = request_data.get('track_type') or 'dashcam'
            recorded = request_data.get('recorded')

            self._tracks_service.create_track_from_init_request(
                request_data=request_data,
                track_uuid=track_uuid,
                event_dt=event_dt,
                track_type=track_type,
                recorded=recorded,
            )
            upload_status.init_metadata = request_data
            upload_status.init_time = event_dt

        if request_data['type'] == 'add_gps_track':
            points = request_data['gps_track']
            upload_status.gps_time = event_dt
            if points:
                recorded_time = _get_recorded_time(points)
                distance = self._track_length_service.calculate_length_km(points)
                upload_status.recorded_time = recorded_time
                upload_status.gps_points = points
                self._tracks_service.set_track_recorded_time_and_distance(
                    track_uuid,
                    distance_km=distance,
                    recorded_time=recorded_time,
                )

        if request_data['type'] == 'complete':
            upload_status.complete_time = event_dt

            if track := self.get_track(track_uuid):
                self._tracks_service.produce_remote_upload_completed_event(
                    track_uuid=track_uuid,
                    user_email=track.user_email,
                    track_type=track.type,
                )

        #  надо сохранить перед ntv потому что денормализация
        self._tracks_service.save_upload_status(upload_status)

        if self._is_track_uploaded(track_uuid):
            self._tracks_service.mark_track_as_uploaded(track_uuid)

    def download_frame(self, message_body: dict, track_uuid: str) -> Frame:
        try:
            image_bytes = base64.b64decode(message_body['frame'])
        except Exception:
            logger.exception(f'Unable to decode frame for track {track_uuid}')
            raise ImageReadError(key=track_uuid)

        frame = self._frames_service.get_or_create(track_uuid, message_body['meta'])

        self._image_service.upload_frame(frame, image_bytes)

        frame.uploaded_photo = True

        self._frames_service.save(frame)

        if self._is_track_uploaded(track_uuid):
            self._tracks_service.mark_track_as_uploaded(track_uuid)

        return frame

    def get_track(self, uuid: str) -> Track:
        return self._tracks_service.get(uuid)

    def _is_track_uploaded(self, track_uuid: str) -> bool:
        status = self._tracks_service.get_upload_status(track_uuid)
        frames_count = self._frames_service.count_by_track(track_uuid)

        return status.is_ready_to_send() and frames_count == status.expected_frames_count


def _get_recorded_time(points):
    if not points:
        return None
    timestamp = points[0]['timestamp'] / 1e3
    return datetime.utcfromtimestamp(timestamp)

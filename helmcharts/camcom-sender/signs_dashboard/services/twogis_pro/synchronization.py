import json
import logging
from collections import defaultdict

from signs_dashboard.models.frame import Frame
from signs_dashboard.models.track import TrackStatuses
from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.schemas.events.detected_objects_lifecycle import DetectedObjectEvent, DetectedObjectEventType
from signs_dashboard.schemas.events.frame_lifecycle import FrameEventType
from signs_dashboard.services.detected_objects import DetectedObjectsService
from signs_dashboard.services.frames import FramesService
from signs_dashboard.services.kafka_service import KafkaService
from signs_dashboard.services.prediction import IZ_PREDICTOR_NAME, PredictionService
from signs_dashboard.services.tracks import TracksService
from signs_dashboard.services.twogis_pro.kafka.drivers import TwoGisProDriversService
from signs_dashboard.services.twogis_pro.kafka.frames import TwoGisProFramesService
from signs_dashboard.services.twogis_pro.kafka.objects import TwoGisProObjectsService

logger = logging.getLogger(__name__)
PRO_DELETE_OPERATION_HEADER = ('op', b'\x02')


class TwoGisProSyncService:

    def __init__(
        self,
        frames_service: FramesService,
        tracks_service: TracksService,
        prediction_service: PredictionService,
        pro_frames_service: TwoGisProFramesService,
        pro_objects_service: TwoGisProObjectsService,
        pro_drivers_service: TwoGisProDriversService,
        detected_objects_service: DetectedObjectsService,
        modules_config: ModulesConfig,
        kafka_service: KafkaService,
    ):
        self._frames_service = frames_service
        self._tracks_service = tracks_service
        self._prediction_service = prediction_service
        self._pro_frames_service = pro_frames_service
        self._pro_objects_service = pro_objects_service
        self._pro_drivers_service = pro_drivers_service
        self._detected_objects_service = detected_objects_service
        self._modules_config = modules_config
        self._kafka_service = kafka_service

        self._producer = self._kafka_service.get_producer(
            key_serializer=lambda key: key.encode('utf-8'),
            value_serializer=lambda val: val.encode('utf-8'),
            max_request_size=5 * 1024 * 1024,  # 5 MB
        )

        self._needed_predictors = []
        if self._modules_config.is_reporter_enabled('pro'):
            self._needed_predictors = self._modules_config.get_predictors_for('pro')

    def sync_frames_by_event_type(self, event_type: FrameEventType, frames: list[Frame]):  # noqa: WPS231
        track_frames_map = defaultdict(list)
        for frame in frames:
            track_frames_map.setdefault(frame.track_uuid, []).append(frame)

        for track_uuid, frames_list in track_frames_map.items():
            if event_type == FrameEventType.pro_hide:
                self._send_frames_deletion(track_uuid, frames_list)
            elif event_type.requires_sync_with_pro:
                pro_status = self._tracks_service.get_pro_status(track_uuid)
                if pro_status == TrackStatuses.HIDDEN_PRO:
                    logger.warning(
                        'Skip sending frames in PRO by event %s because track %s is hidden',
                        event_type,
                        track_uuid,
                    )
                    continue

                self._sync_frames(frames_list)

    def sync_object(self, event: DetectedObjectEvent):
        if event.event_type == DetectedObjectEventType.deleted:
            self._send_object_deletion(event.object_id)
        else:
            detected_object = self._detected_objects_service.get(event.object_id)
            if not detected_object:
                logger.info(f'Got lifecycle event {event.event_type} for deleted object {event.object_id}.')
                return
            object_payload = self._pro_objects_service.get_payload(detected_object)
            if object_payload:
                self._send_object(detected_object.id, object_payload)

    def sync_driver(self, track_uuid: str, user_email: str):
        recorded_datetime = self._tracks_service.get_recorded(track_uuid)
        if not recorded_datetime:
            logger.error(f'Encountered track without recorded field: {track_uuid}.')
            return

        target_date = recorded_datetime.date()
        tracks_statistics = self._tracks_service.get_daily_tracks_stats(user_email, target_date=target_date)

        if not tracks_statistics.tracks_uuids:
            self._send_drivers_deletion(
                user_email=user_email,
                pro_id=tracks_statistics.pro_id,
            )
            return

        payload = self._pro_drivers_service.get_payload(tracks_statistics)
        if not payload:
            logger.debug(f'Prepared empty payload for driver {tracks_statistics.pro_id}')
            return

        future = self._producer.send(
            self._kafka_service.topics.pro_drivers,
            value=json.dumps(payload),
            key=user_email,
        )
        future.get(self._kafka_service.producer_timeout_seconds)
        logger.info(f'Driver {user_email} synced with Pro.')

    def _sync_frames(self, frames: list[Frame]):
        predictions_status = self._prediction_service.get_frames_predictions_status(frames, self._needed_predictors)
        frames_attributes = self._prediction_service.get_frames_attributes(
            frames,
            predictors=self._needed_predictors + [IZ_PREDICTOR_NAME],
        )

        for frame in frames:
            frame_attributes = frames_attributes.get_frame_attributes(frame.id)
            frame_payload = self._pro_frames_service.get_payload(
                frame,
                frame_attributes=frame_attributes,
                predicted=predictions_status.frame_has_all_predictions(frame.id),
            )

            self._send_frame(frame, frame_payload)
            logger.info(f'Track {frame.track_uuid}: Frame {frame.id} sent.')

    def _send_frames_deletion(self, track_uuid: str, frames: list[Frame]):
        for frame in frames:
            self._send_frame_deletion(track_uuid, frame.id)

    def _send_frame(self, frame: Frame, payload: dict):
        future = self._producer.send(
            self._kafka_service.topics.pro_frames,
            value=json.dumps(payload),
            key=frame.track_uuid,
        )
        future.get(self._kafka_service.producer_timeout_seconds)
        logger.info(f'Frame {frame.id} sent to Pro.')

    def _send_object(self, object_id: int, payload: dict):
        future = self._producer.send(
            self._kafka_service.topics.pro_objects,
            key=str(object_id),
            value=json.dumps(payload),
        )
        future.get(self._kafka_service.producer_timeout_seconds)
        logger.info(f'Object {object_id} sent to Pro.')

    def _send_frame_deletion(self, track_uuid: str, frame_id: int):
        future = self._producer.send(
            self._kafka_service.topics.pro_frames,
            key=track_uuid,
            value=str(frame_id),
            headers=[PRO_DELETE_OPERATION_HEADER],
        )
        future.get(self._kafka_service.producer_timeout_seconds)
        logger.info(f'Frame {frame_id} deletion sent to Pro.')

    def _send_object_deletion(self, object_id: int):
        future = self._producer.send(
            self._kafka_service.topics.pro_objects,
            key=str(object_id),
            value=str(object_id),
            headers=[PRO_DELETE_OPERATION_HEADER],
        )
        future.get(self._kafka_service.producer_timeout_seconds)
        logger.info(f'Object {object_id} deletion sent to Pro.')

    def _send_drivers_deletion(self, user_email: str, pro_id: str):
        future = self._producer.send(
            self._kafka_service.topics.pro_drivers,
            key=user_email,
            value=pro_id,
            headers=[PRO_DELETE_OPERATION_HEADER],
        )
        future.get(self._kafka_service.producer_timeout_seconds)
        logger.info(f'Driver {user_email}, id {pro_id} deletion sent to Pro.')

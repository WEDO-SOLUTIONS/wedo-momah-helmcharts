import logging
from typing import Optional, Type

from signs_dashboard.models.bbox_detection import BBOXDetection
from signs_dashboard.models.frame import Frame
from signs_dashboard.schemas.events.frame_lifecycle import (
    AnyFrameEvent,
    BBox,
    FrameEvent,
    FrameEventType,
    FrameType,
    PredictionRequiredFrameEvent,
    UploadedFrameEvent,
)
from signs_dashboard.services.events.base import BaseLifecycleService
from signs_dashboard.services.image import ImageService

logger = logging.getLogger(__name__)


class FramesLifecycleService(BaseLifecycleService[AnyFrameEvent]):
    def __init__(self, image_service: ImageService, **kwargs):
        super().__init__(**kwargs)
        self._image_service = image_service

    def produce_uploaded_event(
        self,
        frame: Frame,
        required_predictors: list[str],
        prompt: Optional[str],
        theta: Optional[int] = None,
    ):
        return self._produce_prediction_required_event(
            frame=frame,
            required_predictors=required_predictors,
            prompt=prompt,
            theta=theta,
            event_class=UploadedFrameEvent,
            event_type=FrameEventType.uploaded,
        )

    def produce_prediction_required_event(
        self,
        frame: Frame,
        required_predictors: list[str],
        prompt: Optional[str],
        theta: Optional[int] = None,
        recalculate_interest_zones: bool = False,
    ):
        return self._produce_prediction_required_event(
            frame=frame,
            required_predictors=required_predictors,
            prompt=prompt,
            theta=theta,
            event_class=PredictionRequiredFrameEvent,
            event_type=FrameEventType.prediction_required,
            recalculate_interest_zones=recalculate_interest_zones,
        )

    def produce_prediction_on_bboxes_required_event(
        self,
        frame: Frame,
        required_predictors: list[str],
        bboxes: list[BBOXDetection],
    ):
        return self._produce_prediction_required_event(
            frame=frame,
            required_predictors=required_predictors,
            prompt=None,
            theta=None,
            event_class=PredictionRequiredFrameEvent,
            event_type=FrameEventType.prediction_required,
            recalculate_interest_zones=False,
            bboxes=[
                BBox(
                    bbox_id=bbox.id,
                    xmin=bbox.x_from,
                    ymin=bbox.y_from,
                    xmax=bbox.x_to,
                    ymax=bbox.y_to,
                ) for bbox in bboxes
            ],
        )

    def produce_predicted_event(self, frame: Frame):
        return self._produce_frame_event(frame.id, frame.track_uuid, FrameEventType.prediction_saved)

    def produce_pro_resend_event(self, frame_id: int, track_uuid: str):
        return self._produce_frame_event(frame_id, track_uuid, FrameEventType.pro_resend)

    def produce_pro_hide_event(self, frame_id: int, track_uuid: str):
        return self._produce_frame_event(frame_id, track_uuid, FrameEventType.pro_hide)

    def produce_moderation_saved_event(self, frame: Frame):
        return self._produce_frame_event(frame.id, frame.track_uuid, FrameEventType.moderation_saved)

    def produce_map_matching_done_event(self, frame: Frame):
        return self._produce_frame_event(frame.id, frame.track_uuid, FrameEventType.map_matching_done)

    def _produce_prediction_required_event(
        self,
        frame: Frame,
        required_predictors: list[str],
        prompt: Optional[str],
        theta: Optional[int],
        event_class: Type[AnyFrameEvent],
        event_type: FrameEventType,
        **kwargs,
    ):
        if frame.panoramic:
            image_url = self._image_service.get_s3_track360_crop_path(frame, theta)
            frame_type = FrameType.video360
        else:
            image_url = self._image_service.get_s3_path(frame)
            frame_type = None

        self._produce_event(
            track_uuid=frame.track_uuid,
            event=event_class(
                frame_id=frame.id,
                event_type=event_type,
                image_url=image_url,
                required_predictors=required_predictors,
                prompt=prompt,
                theta=theta,
                frame_type=frame_type,
                **kwargs,
            ),
        )

    def _produce_frame_event(self, frame_id: int, track_uuid: str, event_type: FrameEventType):
        self._produce_event(
            track_uuid=track_uuid,
            event=FrameEvent(
                frame_id=frame_id,
                event_type=event_type,
            ),
        )

    def _produce_event(self, track_uuid: str, event: AnyFrameEvent):
        self._send(
            event_key=track_uuid,
            event=event,
            topic=self._kafka_service.topics.frames_lifecycle,
        )

import logging
import typing as tp

from signs_dashboard.schemas.events.tracks_lifecycle import (
    AnyTrackEvent,
    DashboardCVATUploadEvent,
    DashboardPredictEvent,
    RemoteUploadCompletedEvent,
    TrackEvent,
    TrackEventType,
)
from signs_dashboard.services.events.base import BaseLifecycleService

logger = logging.getLogger(__name__)


class TracksLifecycleService(BaseLifecycleService[TrackEvent]):

    def produce_created_event(self, track_uuid: str, user_email: str):
        self._produce_track_event(track_uuid, user_email, event_type=TrackEventType.created)

    def produce_gps_track_added_event(self, track_uuid: str, user_email: str):
        self._produce_track_event(track_uuid, user_email, event_type=TrackEventType.gps_track_added)

    def produce_map_matching_done_event(self, track_uuid: str, user_email: str):
        self._produce_track_event(track_uuid, user_email, event_type=TrackEventType.map_matching_done)

    def produce_uploaded_event(self, track_uuid: str, user_email: str):
        self._produce_track_event(track_uuid, user_email, event_type=TrackEventType.uploaded)

    def produce_localization_required_event(self, track_uuid: str, user_email: str):
        self._produce_track_event(track_uuid, user_email, event_type=TrackEventType.localization_required)

    def produce_localization_forced_event(self, track_uuid: str, user_email: str):
        self._produce_track_event(track_uuid, user_email, event_type=TrackEventType.localization_forced)

    def produce_predicted_for_pro_event(self, track_uuid: str, user_email: str):
        self._produce_track_event(track_uuid, user_email, event_type=TrackEventType.predicted_for_pro)

    def produce_resend_gps_track_to_pro_event(self, track_uuid: str, user_email: str):
        self._produce_track_event(track_uuid, user_email, event_type=TrackEventType.resend_gps_track_to_pro)

    def produce_dashboard_predict_event(
        self, track_uuid: str, user_email: str, predictor: str, prompt: tp.Optional[str],
    ):
        event = DashboardPredictEvent(
            track_uuid=track_uuid,
            event_type=TrackEventType.dashboard_predict_event,
            predictor=predictor,
            prompt=prompt,
        )
        self._produce_event(user_email, event)

    def produce_dashboard_cvat_upload_event(self, track_uuid: str, user_email: str, project_id: int, upload_uuid: str):
        event = DashboardCVATUploadEvent(
            track_uuid=track_uuid,
            event_type=TrackEventType.dashboard_cvat_upload_event,
            project_id=project_id,
            upload_uuid=upload_uuid,
        )
        self._produce_event(user_email, event)

    def produce_remote_upload_completed_event(self, track_uuid: str, user_email: str, track_type: str):
        event = RemoteUploadCompletedEvent(
            track_uuid=track_uuid,
            event_type=TrackEventType.remote_upload_completed,
            track_type=track_type,
        )
        self._produce_event(user_email, event)

    def _produce_track_event(self, track_uuid: str, user_email: str, event_type: TrackEventType):
        self._produce_event(
            user_email=user_email,
            event=TrackEvent(
                track_uuid=track_uuid,
                event_type=event_type,
            ),
        )

    def _produce_event(self, user_email: str, event: AnyTrackEvent):
        self._send(
            event_key=user_email,
            event=event,
            topic=self._kafka_service.topics.tracks_lifecycle,
        )

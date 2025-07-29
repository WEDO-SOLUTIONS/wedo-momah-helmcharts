import logging

from signs_dashboard.repository.detected_objects import ClusterizationResult
from signs_dashboard.schemas.events.detected_objects_lifecycle import DetectedObjectEvent, DetectedObjectEventType
from signs_dashboard.services.events.base import BaseLifecycleService

logger = logging.getLogger(__name__)


class DetectedObjectsLifecycleService(BaseLifecycleService[DetectedObjectEvent]):

    def handle_clusterization_result(
        self,
        stats: ClusterizationResult,
    ):
        for object_id in stats.deleted_ids:
            self._produce_event(
                object_id=object_id,
                event_type=DetectedObjectEventType.deleted,
            )
        for object_id in stats.updated_ids:
            self._produce_event(
                object_id=object_id,
                event_type=DetectedObjectEventType.updated,
            )
        for object_id in stats.created_ids:
            self._produce_event(
                object_id=object_id,
                event_type=DetectedObjectEventType.created,
            )

    def produce_updated_event(self, object_id: int):
        self._produce_event(
            object_id=object_id,
            event_type=DetectedObjectEventType.updated,
        )

    def produce_pro_resend_event(self, object_id: int):
        self._produce_event(
            object_id=object_id,
            event_type=DetectedObjectEventType.pro_resend,
        )

    def _produce_event(self, object_id: int, event_type: DetectedObjectEventType):
        return self._send(
            event=DetectedObjectEvent(
                object_id=object_id,
                event_type=event_type,
            ),
            event_key=str(object_id),
            topic=self._kafka_service.topics.objects_lifecycle,
        )

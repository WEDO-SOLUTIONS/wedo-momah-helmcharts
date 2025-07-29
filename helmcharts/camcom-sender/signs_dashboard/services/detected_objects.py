from typing import Iterable, Optional

from signs_dashboard.models.bbox_detection import BBOXDetection
from signs_dashboard.models.detected_object import DetectedObject
from signs_dashboard.query_params.detected_objects import DetectedObjectsQueryParams
from signs_dashboard.repository.detected_objects import DetectedObjectsRepository
from signs_dashboard.services.events.detected_objects_lifecycle import DetectedObjectsLifecycleService

STATUS_NEW = 'new'
STATUS_IN_PROGRESS = 'in_progress'
STATUS_COMPLETED = 'completed'
STATUS_REJECTED = 'rejected'


class DetectedObjectsService:

    _possible_status_transitions = {
        STATUS_NEW: (STATUS_IN_PROGRESS, STATUS_REJECTED),
        STATUS_IN_PROGRESS: (STATUS_COMPLETED, ),
    }

    def __init__(
        self,
        detected_objects_repository: DetectedObjectsRepository,
        detected_objects_lifecycle_service: DetectedObjectsLifecycleService,
    ):
        self._detected_objects_repository = detected_objects_repository
        self._detected_objects_lifecycle_service = detected_objects_lifecycle_service

    def get(self, detected_object_id: int) -> Optional[DetectedObject]:
        return self._detected_objects_repository.get(detected_object_id)

    def get_by_id_list(self, detected_object_ids: list[int]) -> list[DetectedObject]:
        return self._detected_objects_repository.get_by_id_list(detected_object_ids)

    def find(self, query_params: DetectedObjectsQueryParams) -> list[DetectedObject]:
        return self._detected_objects_repository.find(query_params)

    def find_near_detections(
        self,
        detections: list[BBOXDetection],
        radius_meters: float,
    ) -> list[DetectedObject]:
        return self._detected_objects_repository.find_near_detections(detections, radius_meters=radius_meters)

    def find_by_bbox(self, point1, point2, limit: int) -> list[DetectedObject]:
        return self._detected_objects_repository.find_by_bbox(point1, point2, limit=limit)

    def update_status(self, detected_object: DetectedObject, status_code: str):
        if not self.transition_possible(detected_object.status, status_code):
            raise ValueError(
                f'Object {detected_object.id}: unable to change {detected_object.status} to {status_code}',
            )

        self._detected_objects_repository.update_object_status(detected_object.id, status_code)
        self._detected_objects_lifecycle_service.produce_updated_event(detected_object.id)

    def save_state(
        self,
        expected_to_affect_detections: list[BBOXDetection],
        detections_to_unlink: list[BBOXDetection],
        objects_to_add: list[DetectedObject],
        objects_to_update: list[DetectedObject],
        objects_to_remove: list[DetectedObject],
    ):
        statistics = self._detected_objects_repository.save_state(
            expected_to_affect_detections=expected_to_affect_detections,
            detections_to_unlink=detections_to_unlink,
            objects_to_add=objects_to_add,
            objects_to_update=objects_to_update,
            objects_to_remove=objects_to_remove,
            default_status=self.get_default_status(),
        )
        self._detected_objects_lifecycle_service.handle_clusterization_result(statistics)

    def send_resend_event(self, detected_object_id: int):
        self._detected_objects_lifecycle_service.produce_pro_resend_event(detected_object_id)

    def transition_possible(self, current_status: str, next_status: str) -> bool:
        return next_status in self.get_next_statuses(current_status)

    def get_next_statuses(self, status: str) -> Iterable[str]:
        return self._possible_status_transitions.get(status, ())

    def get_default_status(self) -> str:
        return STATUS_NEW

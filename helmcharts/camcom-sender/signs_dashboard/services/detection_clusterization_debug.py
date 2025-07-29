import json
import logging
import os
import traceback
from datetime import datetime
from io import BytesIO, TextIOWrapper

from signs_dashboard.models.bbox_detection import BBOXDetection
from signs_dashboard.models.detected_object import DetectedObject
from signs_dashboard.models.track import Track
from signs_dashboard.services.s3_service import S3Service

logger = logging.getLogger(__name__)

LOG_CLUSTERIZATION_INPUT_OUTPUT = os.environ.get('LOG_CLUSTERIZATION_INPUT_OUTPUT', False)


class DetectionClusterizationDebugService:

    def __init__(self, s3_service: S3Service, track: Track):
        self._s3_service = s3_service
        self.run_id = datetime.now().isoformat()
        self.bucket = self._s3_service.buckets.get_log_bucket(track.uploaded or track.recorded)
        self.track_uuid = track.uuid
        self.enabled = LOG_CLUSTERIZATION_INPUT_OUTPUT

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug('Exiting DetectionClusterizationDebugService')
        if exc_type:
            filename = f'clusterization_{self.run_id}_error.json'
            data = {
                'traceback': traceback.format_exception(exc_type, exc_val, exc_tb),
                'exc_type': str(exc_type),
                'exc_val': str(exc_val),
            }
            self._upload_json(data, filename)
        return False

    def save_input(
        self,
        detections: list[BBOXDetection],
        nearest_objects: list[DetectedObject],
        clustering_params: dict,
    ):
        if not self.enabled:
            return

        filename = f'clusterization_{self.run_id}_input.json'
        logger.info(f'Saving input to {filename}')
        data = {
            'detections': [self._detection(detection) for detection in detections],
            'nearest_objects': [self._detected_object(obj) for obj in nearest_objects],
            'clustering_params': clustering_params,
        }
        self._upload_json(data, filename)

    def save_output(
        self,
        objects_to_add: list[DetectedObject],
        objects_to_update: list[DetectedObject],
        objects_to_remove: list[DetectedObject],
        detections_to_unlink: list[BBOXDetection],
    ):
        if not self.enabled:
            return

        filename = f'clusterization_{self.run_id}_output.json'
        logger.info(f'Saving output to {filename}')
        data = {
            'objects_to_add': [self._detected_object(obj) for obj in objects_to_add],
            'objects_to_update': [self._detected_object(obj) for obj in objects_to_update],
            'objects_to_remove': [self._detected_object(obj) for obj in objects_to_remove],
            'detections_to_unlink': [self._detection(detection) for detection in detections_to_unlink],
        }
        self._upload_json(data, filename)

    def _upload_json(self, data: dict, filename: str):
        buffer = BytesIO()
        text_buffer = TextIOWrapper(buffer, encoding='utf-8')
        json.dump(data, text_buffer)
        text_buffer.seek(0)
        self._s3_service.upload_fileobj(
            bucket=self.bucket,
            key=f'{self.track_uuid}/{filename}',
            content_type='application/json',
            fileobj=buffer,
        )

    def _detection(self, detection: BBOXDetection) -> dict:
        return {
            'id': detection.id,
            'frame_id': detection.frame_id,
            'base_bbox_detection_id': detection.base_bbox_detection_id,
            'detected_object_id': detection.detected_object_id,
            'date': detection.date.isoformat(),
            'label': detection.label,
            'x_from': detection.x_from,
            'y_from': detection.y_from,
            'width': detection.width,
            'height': detection.height,
            'prob': detection.prob,
            'lat': detection.lat,
            'lon': detection.lon,
            'is_side': detection.is_side,
            'is_side_prob': detection.is_side_prob,
            'directions': detection.directions,
            'directions_prob': detection.directions_prob,
            'is_tmp': detection.is_tmp,
            'sign_value': detection.sign_value,
            'status': detection.status,
            'detector_name': detection.detector_name,
            'attributes': detection.attributes,
            'polygon': detection.polygon,
        }

    def _detected_object(self, detected_object: DetectedObject) -> dict:
        return {
            'id': detected_object.id,
            'lat': detected_object.lat,
            'lon': detected_object.lon,
            'detector_name': detected_object.detector_name,
            'updated': detected_object.updated.isoformat() if detected_object.updated else None,
            'status': detected_object.status,
            'label': detected_object.label,
            'sign_value': detected_object.sign_value,
            'is_tmp': detected_object.is_tmp,
            'directions': detected_object.directions,
            'detections': [
                self._detection(detection)
                for detection in detected_object.detections
            ],
        }

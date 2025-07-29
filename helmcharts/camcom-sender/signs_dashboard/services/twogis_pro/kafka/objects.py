import logging
from datetime import timezone
from typing import Optional
from urllib.parse import urljoin

from signs_dashboard.models.bbox_detection import BBOXDetection
from signs_dashboard.models.detected_object import DetectedObject
from signs_dashboard.services.detected_objects import DetectedObjectsService
from signs_dashboard.services.twogis_pro.filters import TwoGisProFiltersService
from signs_dashboard.services.twogis_pro.kafka.localization import TwoGisProKafkaLocalizerService

FRAME_SIZE = 1920 * 1080
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S (%Z)'


class TwoGisProObjectsService:
    def __init__(
        self,
        dashboard_domain: str,
        filters_service: TwoGisProFiltersService,
        localization_service: TwoGisProKafkaLocalizerService,
        detected_objects_service: DetectedObjectsService,
    ):
        self._filters_service = filters_service
        self._dashboard_domain = dashboard_domain
        self._localization_service = localization_service
        self._detected_objects_service = detected_objects_service

    def get_payload(self, detected_object: DetectedObject) -> Optional[dict]:
        detection_class = self._filters_service.detection_classes_map.get(detected_object.label)
        if not detection_class:
            logging.warning(f'Unable to generate Pro event for detected object {detected_object.id}')
            logging.error(f'Attempting to sync with Pro object with unknown label: {detected_object.label}')
            return None

        if not detected_object.detections:
            logging.error(f'Attempting to sync with Pro object with no detections: {detected_object.id}')
            return None

        selected_detection = _get_closest_to_ten_percent_of_frame(detected_object.detections)
        min_date_detection = selected_detection
        max_date_detection = selected_detection
        frames_ids = set()
        for detection in detected_object.detections:
            frames_ids.add(detection.frame_id)
            if detection.date < min_date_detection.date:
                min_date_detection = detection
            if detection.date > max_date_detection.date:
                max_date_detection = detection

        filters_ui_fields, filters_index_data = self._filters_service.get_ui_and_index_fields([detected_object.label])

        fields = [
            *filters_ui_fields,
            *self._get_object_type_fields(detected_object),
            {
                'type': 'string',
                **self._localization_service.get_caption_translations(field='object_first_seen'),
                **self._localization_service.get_value_translations_as_strftime(
                    min_date_detection.frame.local_datetime,
                    field='object_first_seen',
                    key='template',
                    default=DATETIME_FORMAT,
                ),
            },
            {
                'type': 'string',
                **self._localization_service.get_caption_translations(field='object_last_seen'),
                **self._localization_service.get_value_translations_as_strftime(
                    max_date_detection.frame.local_datetime,
                    field='object_last_seen',
                    key='template',
                    default=DATETIME_FORMAT,
                ),
            },
            {
                'type': 'string',
                'value': selected_detection.frame.track_email,
                **self._localization_service.get_caption_translations(field='object_last_seen_by'),
            },
            {
                'type': 'string',
                'value': ', '.join(map(str, sorted(frames_ids))),
                **self._localization_service.get_caption_translations(field='object_frames_ids'),
            },
            {
                'type': 'photo_verification',
                'value': {
                    'buttons': self._get_buttons(detected_object),
                    'moderation_status': self._get_object_status_desc(detected_object),
                    'photo': {
                        'url': self._frame_url(selected_detection),
                    },
                    'datetime_utc': detected_object.updated_timestamp,
                },
            },
        ]
        obj_tz = max_date_detection.frame.local_datetime.tzinfo
        index_data = {
            'status': detected_object.status,
            'labels': [detected_object.label],
            'sign_value': detected_object.sign_value,
            'sign_is_tmp': detected_object.is_tmp,
            'updated_datetime_utc': detected_object.updated_timestamp,
            'first_seen_datetime_utc': min_date_detection.timestamp,
            'last_seen_datetime_utc': max_date_detection.timestamp,
            'updated_datetime': detected_object.updated.replace(tzinfo=timezone.utc).astimezone(obj_tz).isoformat(),  # noqa: E501
            'first_seen_datetime': min_date_detection.frame.local_datetime.astimezone(obj_tz).isoformat(),
            'last_seen_datetime': max_date_detection.frame.local_datetime.isoformat(),
            **filters_index_data,
        }
        return {
            'id': detected_object.id,
            'name': str(detected_object.id),
            '{index}': index_data,
            'point': {
                'lat': detected_object.lat,
                'lon': detected_object.lon,
            },
            'field_groups': [{
                'fields': fields,
            }],
        }

    def _frame_url(self, detection: BBOXDetection) -> str:
        return urljoin(
            self._dashboard_domain,
            f'/api/frames/{detection.frame_id}/predictions?detection_id={detection.id}&locale={{locale}}',
        )

    def _api_url(self, object_id: int, status: str) -> str:
        return urljoin(self._dashboard_domain, f'/api/objects/{object_id}/{status}')

    def _get_object_status_desc(self, detected_object: DetectedObject) -> dict:
        return {
            'type': 'string',
            **self._localization_service.get_caption_translations(field='object_status', key=detected_object.status),
            'value': detected_object.status,
        }

    def _get_buttons(self, detected_object: DetectedObject) -> list[dict]:
        buttons = []
        for status in self._detected_objects_service.get_next_statuses(detected_object.status):
            buttons.append({
                'type': 'action_button',
                'value': {
                    'url': self._api_url(object_id=detected_object.id, status=status),
                    'action': 'PUT',
                },
                **self._localization_service.get_caption_translations(
                    field='object_status_transition',
                    key=status,
                ),
            })
        return buttons

    def _get_object_type_fields(self, detected_object: DetectedObject):
        fields = [
            {
                'type': 'string',
                **self._localization_service.get_caption_translations(field='object_type'),
                **self._localization_service.get_value_translations(field='type', key=detected_object.label),
            },
        ]
        if detected_object.sign_value:
            fields.append({
                'type': 'string',
                'value': str(detected_object.sign_value),
                **self._localization_service.get_caption_translations(field='object_sign_value'),
            })
        fields.append({
            'type': 'string',
            **self._localization_service.get_value_translations(
                field='object_is_tmp',
                key=str(detected_object.is_tmp),
            ),
            **self._localization_service.get_caption_translations(field='object_is_tmp'),
        })
        return fields


def _get_closest_to_ten_percent_of_frame(detections: list[BBOXDetection]):
    return min(
        detections,
        key=lambda detection: abs(0.1 - detection.width * detection.height / FRAME_SIZE),
    )

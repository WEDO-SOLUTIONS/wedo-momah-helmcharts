import json
import logging
from collections import defaultdict
from typing import Any, Union
from urllib.parse import quote_plus, urljoin

from signs_dashboard.models.bbox_detection import BBOXDetection
from signs_dashboard.models.frame import Frame, ModerationStatus
from signs_dashboard.models.twogis_pro_filters import DetectionClass
from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.services.predictors import PredictorsService
from signs_dashboard.services.twogis_pro.filters import TwoGisProFiltersService
from signs_dashboard.services.twogis_pro.kafka.driver_userinfo_fields import DriverInformationRenderingMixin
from signs_dashboard.services.twogis_pro.kafka.localization import TwoGisProKafkaLocalizerService
from signs_dashboard.small_utils import uniques_preserving_order

logger = logging.getLogger(__name__)

MUNICIPALITY_ID = 'municipality_id'
SUBMUNICIPALITY_ID = 'submunicipality_id'


def _get_moderation_status(frame: Frame) -> str:
    if frame.moderation_status == ModerationStatus.moderated_good.value:
        return 'good'
    if frame.moderation_status == ModerationStatus.moderated_wrong.value:
        return 'wrong'
    return 'not_processed'


def _get_frame_status(frame: Frame, predicted: bool, has_detections: bool) -> str:
    if frame.moderation_status == ModerationStatus.moderated_wrong.value:
        return 'wrong_detections'
    if not predicted:
        return 'waiting_detections'
    if has_detections:
        return 'with_detections'
    return 'wo_detections'


class TwoGisProFramesService(DriverInformationRenderingMixin):
    def __init__(
        self,
        dashboard_domain: str,
        config: dict,
        predictors_service: PredictorsService,
        filters_service: TwoGisProFiltersService,
        localization_service: TwoGisProKafkaLocalizerService,
        modules_config: ModulesConfig,
    ):
        self._dashboard_domain = dashboard_domain
        self._link_proxy_base = config.get('pro_link_proxy_base', None)
        self._filters_service = filters_service
        self._predictors_service = predictors_service
        self._localization_service = localization_service
        self._map_matching_enabled = modules_config.is_map_matching_enabled()

    def filter_known_detections(
        self,
        detections: list[BBOXDetection],
    ) -> tuple[list[BBOXDetection], list[DetectionClass]]:
        known = self._filters_service.detection_classes_map
        filtered_detections, matching_classes = [], []
        for detection in detections:
            matching_class = known.get(detection.label)
            if not matching_class:
                logger.debug(f'Unknown detection: {detection.label} {detection.id}')
                continue

            matching_classes.append(matching_class)
            filtered_detections.append(detection)

        return filtered_detections, matching_classes

    def get_payload(
        self,
        frame: Frame,
        frame_attributes: dict,
        predicted: bool,
    ) -> dict:
        detections, detections_classes = self.filter_known_detections(frame.detections)
        labels = [detection.label for detection in detections]

        detections_index_fields = self._detections_index_fields(detections)
        filters_ui_fields, filters_index_fields = self._filters_service.get_ui_and_index_fields(labels)

        frame_status = _get_frame_status(frame, predicted=predicted, has_detections=bool(labels))
        frame_moderation_status = _get_moderation_status(frame)

        buttons = [
            {
                'type': 'action_button',
                **self._localization_service.get_caption_translations(field='frame_moderation_status_ok_link'),
                'value': {
                    'url': self._url('moderation_feedback', frame_id=frame.id, moderation_status='ok'),
                    'action': 'PUT',
                },
                'tag': 'frame_ok_link',
            },
            {
                'type': 'action_button',
                **self._localization_service.get_caption_translations(
                    field='frame_moderation_status_errors_link',
                ),
                'value': {
                    'url': self._url('moderation_feedback', frame_id=frame.id, moderation_status='fail'),
                    'action': 'PUT',
                },
                'tag': 'frame_errors_link',
            },
        ]

        fields = [
            *self._frame_attributes_ui_fields(frame_attributes),
            *filters_ui_fields,
            {
                'type': 'string',
                **self._localization_service.get_caption_translations(field='frame_types'),
                **self._localization_service.get_value_translations(field='type', keys=labels),
            },
            *self._detections_ui_fields(detections_index_fields),
            {
                'type': 'string',
                **self._localization_service.get_caption_translations(field='frame_shooting_date'),
                **self._localization_service.get_value_translations_as_strftime(
                    frame.local_datetime,
                    field='frame_shooting_date',
                    key='template',
                    default='%d/%m/%Y %H:%M:%S (%Z)',
                ),
            },
            *self._driver_extra_ui_fields(frame.api_user),
            {
                'type': 'string',
                **self._localization_service.get_caption_translations(field='frame_driver'),
                'value': frame.track_email,
            },
            {
                'type': 'number',
                **self._localization_service.get_caption_translations(field='frame_azimuth'),
                'value': frame.azimuth,
            },
            {
                'type': 'number',
                **self._localization_service.get_caption_translations(field='frame_speed'),
                'value': frame.speed,
            },
            {
                'type': 'string',
                **self._localization_service.get_caption_translations(field='frame_status'),
                **self._localization_service.get_value_translations(field='frame_status', key=frame_status),
            },
            {
                'type': 'string',
                **self._localization_service.get_caption_translations(field='frame_app_version'),
                'value': frame.app_version or '',
            },
            {
                'type': 'photo_verification',
                'value': {
                    'buttons': buttons,
                    'moderation_status': {
                        'type': 'string',
                        'value': frame_moderation_status,
                        **self._localization_service.get_caption_translations(
                            field='frame_moderation_status',
                            key=frame_moderation_status,
                        ),
                    },
                    'photo': {
                        'id': frame.id,
                        'url': self._url('get_frame_predictions', frame_id=frame.id),
                    },
                    'driver': frame.track_email,
                    'datetime_utc': frame.timestamp,
                },
            },
        ]

        index_data = {
            'driver': frame.track_email,
            'datetime_utc': frame.timestamp,
            'datetime': frame.local_datetime.isoformat(),
            'azimuth': frame.azimuth,
            'speed': frame.speed,
            'labels': labels,
            'status': frame_status,
            'moderation_status': frame_moderation_status,
            'track_uuid': frame.track_uuid,
            'app_version': frame.app_version,
            **filters_index_fields,
            **_normalize_index_fields(frame_attributes),
            **_normalize_index_fields(detections_index_fields),
            **self._driver_extra_index_fields(frame.api_user),
        }

        if self._map_matching_enabled:
            fields += [{
                'type': 'string',
                **self._localization_service.get_caption_translations(field='frame_map_matched'),
                **self._localization_service.get_value_translations(
                    field='frame_map_matched',
                    key=str(frame.map_matched),
                ),
            }]
            index_data.update({'map_matching_done': frame.map_matched})

        return {
            'id': frame.id,
            'name': str(frame.id),
            'searchable_keywords': [frame.track_email, *self._driver_extra_searchable_keywords(frame.api_user)],
            '{index}': index_data,
            'point': {
                'lat': frame.current_lat,
                'lon': frame.current_lon,
            },
            'field_groups': [{
                'fields': fields,
            }],
        }

    def _url(self, route_name, **kwargs):
        if route_name == 'get_frame_predictions':
            url = urljoin(self._dashboard_domain, f"/api/frames/{kwargs['frame_id']}/predictions?locale={{locale}}")
            if self._link_proxy_base:
                return f'{self._link_proxy_base}{quote_plus(url)}'
            return url
        if route_name == 'moderation_feedback':
            return urljoin(
                self._dashboard_domain,
                f"/api/frames/{kwargs['frame_id']}/moderate/{kwargs['moderation_status']}",
            )
        raise ValueError(f'Failed url generation. Unknown route name: {route_name}')

    def _frame_attributes_ui_fields(self, frame_attributes: dict) -> list[dict]:
        fields = []
        if self._predictors_service.is_camcom_predictor_enabled():
            fields += [
                self._frame_attribute_ui_field(MUNICIPALITY_ID, frame_attributes.get(MUNICIPALITY_ID)),
                self._frame_attribute_ui_field(SUBMUNICIPALITY_ID, frame_attributes.get(SUBMUNICIPALITY_ID)),
            ]
        for attribute, attribute_value in frame_attributes.items():
            if attribute in {MUNICIPALITY_ID, SUBMUNICIPALITY_ID}:
                continue
            fields.append(self._frame_attribute_ui_field(attribute, attribute_value))
        return fields

    def _frame_attribute_ui_field(self, attribute: str, attribute_value: Union[str, list, bool, None]) -> dict:
        value_args = {'key': attribute_value}
        if attribute_value is None or isinstance(attribute_value, bool):
            value_args = {'key': str(attribute_value)}
        if isinstance(attribute_value, list):
            value_args = {'keys': attribute_value}
        return {
            'type': 'string',
            **self._localization_service.get_value_translations(
                field=attribute,
                **value_args,
                default=str(attribute_value or ''),
            ),
            **self._localization_service.get_caption_translations(
                field=attribute,
                default=attribute.replace('_', ' ').capitalize(),
            ),
        }

    def _detections_ui_fields(self, detection_attributes: dict[str, list[str]]) -> list[dict]:
        fields = []

        for attribute in sorted(detection_attributes.keys()):
            attribute_values = detection_attributes[attribute]
            fields.append({
                'type': 'string',
                'value': ', '.join(map(str, attribute_values)),
                'caption': attribute.replace('_', ' ').capitalize(),
            })
        return fields

    def _detections_index_fields(self, detections: list[BBOXDetection]) -> dict[str, list[str]]:
        index_fields = defaultdict(list)
        for detection in detections:
            if not isinstance(detection.attributes, dict):
                continue
            for attribute, value in detection.attributes.items():
                if value is not None:
                    index_fields[attribute].append(value)

        return {
            field_name: uniques_preserving_order(field_values)
            for field_name, field_values in index_fields.items()
        }


def _normalize_index_fields(fields: dict[str, Any]) -> dict:
    normalized = {}
    for field_name, field_value in fields.items():
        if isinstance(field_value, str):
            normalized[field_name] = _normalize_bools(field_value)
        elif isinstance(field_value, list):
            normalized[field_name] = [
                _normalize_bools(subvalue)
                for subvalue in field_value
            ]
        else:
            normalized[field_name] = field_value
    return normalized


def _normalize_bools(field_value: Union[str, bool]):
    normalized = str(field_value).lower()
    if normalized in {'true', 'false'}:
        return json.loads(normalized)
    return field_value

from collections import defaultdict
from enum import Enum
from typing import Optional

from signs_dashboard.schemas.prediction import BBox, PredictionResults, PredictorAnswer
from signs_dashboard.small_utils import get_value


class Predictors(Enum):
    labels = 'labels'
    signs = 'signs'
    surface = 'surface'


class PredictorAnswerParser:
    def __init__(self):
        self.old_format_predictors_map = {
            Predictors.labels.value: self._labels_format,
            Predictors.signs.value: self._signs_format,
            Predictors.surface.value: self._surface_format,
        }

    def parse_predictor_answer(
        self,
        message_body: dict,
        predictor_name_override: Optional[str] = None,
    ) -> PredictorAnswer:
        if predictor_name_override:
            message_body['predictor'] = predictor_name_override

        if predictor_name_override in self.old_format_predictors_map:
            return self.old_format_predictors_map[predictor_name_override](message_body, predictor_name_override)  # noqa: E501, WPS529

        return PredictorAnswer(**message_body)

    def _labels_format(self, message_body: dict, predictor_name: str = Predictors.labels.value) -> PredictorAnswer:
        if 'labels' not in message_body:
            return PredictorAnswer(**message_body)

        return PredictorAnswer(
            predictor=predictor_name,
            meta=message_body.get('meta'),
            error_info=message_body.get('error_info'),
            results=PredictionResults(
                attributes={'quality_labels': message_body.get('labels')},
            ),
        )

    def _signs_format(self, message_body: dict, predictor_name: str = Predictors.signs.value) -> PredictorAnswer:
        if 'recognized_signs' not in message_body:
            return PredictorAnswer(**message_body)

        if distance := message_body.get('recognized_road_marking'):
            distance = distance.get('distance')

        bboxes = None
        if signs_data := message_body.get('recognized_signs'):
            bboxes = defaultdict(list)
            for sign_data in signs_data:
                bbox_base = self._create_bbox_from_signs_format(sign_data)

                for plate_data in get_value(sign_data, 'plates', []):
                    plate_bbox = self._create_bbox_from_signs_format(plate_data)
                    bbox_base.related_bboxes.append(plate_bbox)

                bboxes.setdefault(bbox_base.label, []).append(bbox_base)

        return PredictorAnswer(
            predictor=predictor_name,
            meta=message_body.get('meta'),
            error_info=message_body.get('error_info'),
            results=PredictionResults(
                bboxes=bboxes,
                attributes={'road_marking_distance': distance},
            ),
            raw_data=signs_data,
        )

    def _surface_format(self, message_body: dict, predictor_name: str = Predictors.surface.value) -> PredictorAnswer:
        if 'road_surface' not in message_body:
            return PredictorAnswer(**message_body)

        return PredictorAnswer(
            predictor=predictor_name,
            meta=message_body.get('meta'),
            error_info=message_body.get('error_info'),
            results=PredictionResults(
                attributes=message_body.get('road_surface'),
            ),
        )

    def _create_bbox_from_signs_format(self, data: dict) -> BBox:
        x_min, y_min, x_max, y_max = data['mask']

        return BBox(
            label=data['sign'],
            xmin=x_min,
            xmax=x_max,
            ymin=y_min,
            ymax=y_max,
            probability=data['prob'],
            is_side=data['is_side'],
            is_side_prob=data['is_side_prob'],
            directions=data.get('directions'),
            directions_prob=data.get('directions_prob'),
            is_tmp=data.get('is_tmp', False),
            sign_value=get_value(data, 'value', None),
        )

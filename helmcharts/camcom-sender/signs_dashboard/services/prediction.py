import logging
from collections import defaultdict
from functools import cached_property
from typing import Optional

from signs_dashboard.errors.workers import ParseMessageError
from signs_dashboard.models.bbox_detection import BBOXDetection
from signs_dashboard.models.frame import Frame
from signs_dashboard.models.frames_attributes import FrameAttribute
from signs_dashboard.models.prediction import Prediction
from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.repository.bbox_detections import BBOXDetectionsRepository
from signs_dashboard.repository.predictions import PredictionsRepository
from signs_dashboard.schemas.frames import FramesOperations
from signs_dashboard.schemas.prediction import BBox, PredictorAnswer
from signs_dashboard.services.frames_depth import FramesDepthService
from signs_dashboard.services.pano_conversions.service import PanoramicConversionsService
from signs_dashboard.services.prediction_answer_parser import PredictorAnswerParser

logger = logging.getLogger(__name__)

IZ_PREDICTOR_NAME = 'interest_zones'


class PredictionStatusProxy:
    def __init__(self, predictions_result: list[Prediction], required_predictors: list[str]):
        self.required_predictors = required_predictors
        self.other_predictors = set()

        self._result: dict[int, dict[str, Prediction]] = defaultdict(dict)
        for prediction in predictions_result:
            self._result[prediction.frame_id][prediction.detector_name] = prediction

            if prediction.detector_name not in self.required_predictors:
                self.other_predictors.add(prediction.detector_name)

    @cached_property
    def predictors(self) -> list[str]:
        return [*self.required_predictors, *self.other_predictors]

    @property
    def frame_ids(self) -> list[int]:
        return list(self._result)

    def count_predictions(self, predictor: str) -> int:
        return sum(1 for predictions_map in self._result.values() if predictions_map.get(predictor))

    def count_ready_frames(self) -> int:
        return sum(
            1 for predictions_map in self._result.values() if len(predictions_map) == len(self.required_predictors)
        )

    def frame_has_all_predictions(self, frame_id: int) -> bool:
        return len(self._result.get(frame_id, {})) == len(self.required_predictors)

    def has_all_predictions(self) -> bool:
        return all((
            self.frame_has_all_predictions(frame_id)
            for frame_id in self._result
        ))

    def get_prediction(self, predictor: str, frame_id: int) -> Optional[Prediction]:
        return self._result.get(frame_id, {}).get(predictor)

    def get_errors(self, frame_id: int) -> list:
        return [prediction.error for prediction in self._result.get(frame_id, {}).values() if prediction.error]


class FramesBatchAttributes:
    def __init__(self, frames: list[Frame], predictors: list[str]):
        self._result: dict[int, dict[str, Optional[dict]]] = {  # noqa: WPS234
            frame.id: {predictor: None for predictor in predictors}  # noqa: C420 confuses list with dict
            for frame in frames
        }
        self.predictors = predictors

    @property
    def frame_ids(self) -> list[int]:
        return list(self._result)

    def get_frame_attributes(self, frame_id: int) -> dict:
        frame_attributes = {}
        for _, attributes in self._result[frame_id].items():
            if isinstance(attributes, dict):
                frame_attributes.update(attributes)
        return frame_attributes

    def get_frame_attribute(self, frame_id: int, attribute: str, default=None):
        return self.get_frame_attributes(frame_id).get(attribute, default)

    def add_attributes(self, predictor: str, frame_id: int, frame_attribute: FrameAttribute):
        self._result[frame_id][predictor] = frame_attribute.attributes


class PredictionService:
    def __init__(
        self,
        modules_config: ModulesConfig,
        predictions_repository: PredictionsRepository,
        bbox_detections_repository: BBOXDetectionsRepository,
        frames_depth_service: FramesDepthService,
        prediction_answer_parser: PredictorAnswerParser,
        panoramic_conversions_service: PanoramicConversionsService,
    ):
        self._modules_config = modules_config
        self._predictions_repository = predictions_repository
        self._bbox_detections_repository = bbox_detections_repository
        self._frames_depth_service = frames_depth_service
        self._prediction_answer_parser = prediction_answer_parser
        self._panoramic_conversions_service = panoramic_conversions_service

    def get_frames_predictions_status(
        self,
        frames: list[Frame],
        predictors: list[str],
        all_predictors: bool = False,
    ) -> PredictionStatusProxy:
        frames_operations = FramesOperations(frames_map={frame.id: frame for frame in frames})
        min_frames_date, max_frames_date = frames_operations.min_max_frames_date

        predictions = self._predictions_repository.find(
            frame_ids=frames_operations.list_ids,
            predictors=None if all_predictors else predictors,
            min_date=min_frames_date,
            max_date=max_frames_date,
        )

        return PredictionStatusProxy(
            predictions_result=predictions,
            required_predictors=predictors,
        )

    def get_frames_attributes(
        self,
        frames: list[Frame],
        predictors: list[str],
    ) -> FramesBatchAttributes:
        frame_ids = [frame.id for frame in frames]
        raw_attributes = self._predictions_repository.get_frames_attributes(
            frame_ids,
            detector_names=predictors,
            min_frame_date=min(frame.date for frame in frames) if frames else None,
            max_frame_date=max(frame.date for frame in frames) if frames else None,
        )
        frames_batch_attributes = FramesBatchAttributes(frames, predictors)
        for attribute in raw_attributes:
            frames_batch_attributes.add_attributes(attribute.detector_name, attribute.frame_id, attribute)
        return frames_batch_attributes

    def get_frame_attributes(self, frame: Frame, predictor: str) -> Optional[dict]:
        frame_attributes = self._predictions_repository.get_frames_attributes(
            [frame.id],
            detector_names=[predictor],
            min_frame_date=frame.date,
            max_frame_date=frame.date,
        )
        if frame_attributes:
            return frame_attributes[0].attributes
        return None

    def save_prediction(self, frame: Frame, prediction: PredictorAnswer):
        # см. https://confluence.2gis.ru/pages/viewpage.action?pageId=457872492
        if not frame.panoramic and prediction.theta is not None:
            raise ParseMessageError(f'Received prediction with theta {prediction.theta} for non-panoramic frame')

        if prediction.results:
            self._process_results(prediction, frame)

        self._predictions_repository.save_with_raw(
            Prediction(
                frame_id=frame.id,
                detector_name=prediction.predictor,
                error=prediction.error_info,
                raw_data=prediction.raw_data,
                date=frame.date,
            ),
        )

    def save_interest_zones_attributes(self, frame: Frame, attributes: dict):
        self._predictions_repository.save_frame_attributes(
            frame,
            detector_name=IZ_PREDICTOR_NAME,
            attributes=attributes,
        )

    def save_bbox_predictions(
        self,
        frame: Frame,
        results: dict[str, list[BBox]],
        predictor_name: str,
        theta: Optional[int],
    ):
        if theta is None:
            self._bbox_detections_repository.delete_by_frame_id_and_detector_name(frame.id, predictor_name)
        else:
            detection_to_delete = self._panoramic_conversions_service.find_detections_from_crop(
                frame,
                theta=theta,
                detector_name=predictor_name,
            )
            if detection_to_delete:
                self._bbox_detections_repository.delete_by_id([det.id for det in detection_to_delete])
            results = self._panoramic_conversions_service.convert_bboxes_to_equirectangle_projection(
                results,
                theta=theta,
            )

        for label, bboxes in results.items():
            for bbox in bboxes:
                base_bbox = self._bbox_detections_repository.create(
                    frame=frame,
                    detector_name=predictor_name,
                    label=label,
                    bbox=bbox,
                    polygon=bbox.polygon,
                )

                for related_bbox in bbox.related_bboxes:
                    self._bbox_detections_repository.create(
                        frame=frame,
                        detector_name=predictor_name,
                        bbox=related_bbox,
                        base_bbox=base_bbox,
                    )

    def save_detections_locations(self, detections: list[BBOXDetection]):
        for detection in detections:
            self._bbox_detections_repository.save_location_info(detection)

    def parse_prediction(self, message_body: dict, predictor_name_override: Optional[str]) -> PredictorAnswer:
        try:
            predictor_answer = self._prediction_answer_parser.parse_predictor_answer(
                message_body=message_body,
                predictor_name_override=predictor_name_override,
            )
        except Exception as exc:
            raise ParseMessageError(str(exc)) from exc

        return predictor_answer

    def get_bbox_predictions_by_frame_and_detector(self, frame: Frame, detector_name: str) -> list[BBOXDetection]:
        return self._bbox_detections_repository.get_by_frame_and_detector(frame, detector_name)

    def _process_results(self, prediction: PredictorAnswer, frame: Frame) -> None:
        results = prediction.results
        predictor = prediction.predictor

        if results.depth_map:
            self._frames_depth_service.save_frame_depth_map(frame, results.depth_map.data, theta=prediction.theta)
        if results.bboxes is not None:
            self.save_bbox_predictions(frame, results.bboxes, predictor, theta=prediction.theta)
        if results.attributes:
            self._predictions_repository.save_frame_attributes(frame, predictor, results.attributes)
        if results.bbox_attributes:
            for bbox in results.bbox_attributes:
                self._bbox_detections_repository.update_attributes(bbox.bbox_id, bbox.attributes)

import json
import logging
from datetime import datetime
from typing import Optional

from cachetools import LRUCache
from dependency_injector.wiring import Provide, inject

from signs_dashboard.containers.application import Application
from signs_dashboard.context import ContextService
from signs_dashboard.errors.service import ImageReadError
from signs_dashboard.errors.workers import ParseMessageError
from signs_dashboard.events_tools import parse_message_key
from signs_dashboard.models.frame import Frame
from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.schemas.prediction import PredictorAnswer
from signs_dashboard.services.camcom.camcom_sender import CamcomSenderService
from signs_dashboard.services.events.frames_lifecycle import FramesLifecycleService
from signs_dashboard.services.frames import FramesService
from signs_dashboard.services.interest_zones import InterestZonesService
from signs_dashboard.services.kafka_service import KafkaService, commit_single_message
from signs_dashboard.services.prediction import PredictionService
from signs_dashboard.services.predictors import PredictorsService
from signs_dashboard.services.tracks_download import TracksDownloaderService

logger = logging.getLogger(__name__)


@inject
def download_tracks(
    kafka_service: KafkaService = Provide[Application.services.kafka],
    download_service: TracksDownloaderService = Provide[Application.services.tracks_downloader],
):
    consumer = kafka_service.get_tracks_consumer()
    for message in consumer:
        partition, offset = message.partition, message.offset
        key = parse_message_key(message)
        if not key:
            logger.error(f'Kafka message with empty key: {message}')
            commit_single_message(consumer, message)
            continue

        with ContextService(track_uuid=key):
            meta_info = f'key: {key}, partition: {partition}, offset: {offset}'
            logger.warning(f'Kafka recieve message with {meta_info}')
            request_data = json.loads(message.value)
            request_type = request_data.get('type')
            meta_info = f'{meta_info}, type: {request_type}'
            logger.warning(f'Kafka start process message with {meta_info}')
            event_dt = datetime.utcfromtimestamp(message.timestamp / 1e3)
            download_service.download_track(request_data, key, event_dt)
            commit_single_message(consumer, message)
            logger.warning(f'Kafka Commit message with {meta_info}')


@inject
def download_frames(  # noqa: WPS231
    kafka_service: KafkaService = Provide[Application.services.kafka],
    download_service: TracksDownloaderService = Provide[Application.services.tracks_downloader],
    frames_lifecycle_service: FramesLifecycleService = Provide[Application.services.frames_lifecycle],
    interest_zones_service: InterestZonesService = Provide[Application.services.interest_zones],
    predictors: PredictorsService = Provide[Application.services.predictors],
):
    consumer = kafka_service.get_frames_consumer()
    track_uuid2track_type = LRUCache(maxsize=100000)
    for message in consumer:
        key = parse_message_key(message)
        if not key:
            logger.error(f'Kafka message with empty key: {message}')
            commit_single_message(consumer, message)
            continue

        try:
            message_body = json.loads(message.value)
            frame = download_service.download_frame(message_body, key)
        except ImageReadError:
            logger.exception(f'Unable to read image from message with key={key}, skipped')
            frame = None

        if frame:
            required_predictors = predictors.get_active_predictors()
            track_type = _get_track_type(frame.track_uuid, track_uuid2track_type, download_service)

            #  Для видеорегистраторов не предсказываем глубину, т.к. там в экзифах фреймов нет фокусных расстояний,
            # которые нужны глубине. Если мы начали обрабатывать фрейм до того, как в базе создался фрейм, то
            # фрейм всё равно отправится на глубину. Но это очень редкий случай
            if track_type == 'dashcam':
                required_predictors = [name for name in required_predictors if name != 'depth-detection']
            interest_zones_service.update_frame_interest_zones(frame)
            frames_lifecycle_service.produce_uploaded_event(
                frame,
                required_predictors=required_predictors,
                prompt=predictors.get_prompt(),
            )

        commit_single_message(consumer, message)


def _get_track_type(
    uuid: str, track_uuid2track_type: LRUCache, download_service: TracksDownloaderService,
) -> Optional[str]:
    track_type = track_uuid2track_type.get(uuid)
    if track_type:
        return track_type

    if track := download_service.get_track(uuid):
        track_type = track.type
        track_uuid2track_type[uuid] = track_type
        return track_type
    return None


@inject
def download_predictions(  # noqa: C901, WPS231
    camcom_sender_service: CamcomSenderService = Provide[Application.services.camcom_sender],
    frames_service: FramesService = Provide[Application.services.frames],
    frames_lifecycle_service: FramesLifecycleService = Provide[Application.services.frames_lifecycle],
    prediction_service: PredictionService = Provide[Application.services.prediction],
    kafka_service: KafkaService = Provide[Application.services.kafka],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
):
    consumer = kafka_service.get_predictions_consumer()

    for message in consumer:
        key = parse_message_key(message)
        if not key:
            logger.error(f'Kafka message with empty key: {message}')
            commit_single_message(consumer, message)
            continue
        message_body = json.loads(message.value)

        predictor_name_override = None
        if message.topic != kafka_service.get_unified_predictions_topic():
            predictor_name_override = kafka_service.get_predictor_by_topic(message.topic)

        if predictor_name_override in {'labels', 'signs', 'surface'} and 'ts' not in message_body.get('meta', {}):
            # особенность воркеров на фреймворке mlgis
            continue

        frame, predictor_name = _save_predictions(
            key,
            message_body,
            predictor_name_override=predictor_name_override,
            frames_service=frames_service,
            prediction_service=prediction_service,
        )
        if frame and predictor_name == 'camcom':
            _update_camcom_job(key, frame, camcom_sender_service=camcom_sender_service)

        if frame:
            frames_lifecycle_service.produce_predicted_event(frame)

            if _signboard_text_recognition_required(predictor_name, modules_config):
                frames_lifecycle_service.produce_prediction_on_bboxes_required_event(
                    frame=frame,
                    required_predictors=['signboard-text-recognition'],
                    bboxes=prediction_service.get_bbox_predictions_by_frame_and_detector(frame, 'signboard-detection'),
                )

        commit_single_message(consumer, message)


def _save_predictions(
    key: str,
    message_body: dict,
    predictor_name_override: Optional[str],
    frames_service: FramesService,
    prediction_service: PredictionService,
) -> tuple[Optional[Frame], Optional[str]]:
    try:
        prediction = prediction_service.parse_prediction(
            message_body=message_body,
            predictor_name_override=predictor_name_override,
        )
    except ParseMessageError as exc:
        logger.exception(f'Unable to parse prediction: {message_body=}, {exc=}')
        return None, None

    frame = _load_frame(key, prediction, frames_service=frames_service)

    if not frame:
        logger.error(f'Got predictions for unknown frame {key=} {message_body=}')
        return None, None

    try:
        prediction_service.save_prediction(prediction=prediction, frame=frame)
    except ParseMessageError as exc:
        logger.exception(f'Unable to save prediction for frame {frame.id=}, {message_body=}, {exc=}')
        return None, None

    return frame, prediction.predictor


def _load_frame(key: str, prediction: PredictorAnswer, frames_service: FramesService) -> Optional[Frame]:
    from_detector = None
    if prediction.frame_type == 'video_360' and prediction.theta is not None:
        from_detector = prediction.predictor

    if prediction.frame_id:
        if from_detector:
            frame = frames_service.get_frame_w_detections(prediction.frame_id, from_detector=prediction.predictor)
        else:
            frame = frames_service.get_frame(prediction.frame_id)
    elif prediction.meta:
        frame = frames_service.get_or_create(key, prediction.meta)
    else:
        frame = None

    return frame


def _update_camcom_job(
    key: str,
    frame: Frame,
    camcom_sender_service: CamcomSenderService,
):
    camcom_job = camcom_sender_service.complete(key)
    if not camcom_job:
        logger.error(f'Received CamCom prediction for unknown job {key}, {frame.id=}')


def _signboard_text_recognition_required(
    predictor_name: str,
    modules_config: ModulesConfig,
) -> bool:
    return (
        predictor_name == 'signboard-detection'
        and modules_config.is_signboard_text_recognition_enabled()  # noqa: W503
    )

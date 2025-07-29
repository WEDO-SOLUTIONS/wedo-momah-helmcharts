import json
import logging
import ssl
import typing as tp
from dataclasses import dataclass

from kafka import KafkaConsumer, KafkaProducer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.structs import OffsetAndMetadata, TopicPartition

from signs_dashboard.modules_config import ModulesConfig

logger = logging.getLogger(__name__)


@dataclass
class TopicNames:
    tracks: list[str]
    frames: list[str]
    prediction: list[str]
    pro_frames: str
    pro_objects: tp.Optional[str]
    pro_drivers: str
    tracks_reload: str
    frames_lifecycle: str
    objects_lifecycle: tp.Optional[str]
    tracks_lifecycle: str
    logs: list[str]
    cvat_upload: tp.Optional[str]


@dataclass
class ConsumerGroups:
    frames_saver: str
    track_metadata_saver: str
    predictions_saver: str
    camcom_sender: str
    tracks_map_matcher: str
    tracks_reload: str
    track_logs_saver: str
    reporter_pro: str
    cvat_uploader: str
    video_frames_saver: str
    detections_localizer: str


class KafkaService:

    def __init__(self, config: dict, modules_config: ModulesConfig):
        self._modules_config = modules_config
        self._topics = TopicNames(
            tracks=modules_config.get_track_metadata_saver_topics(),
            frames=modules_config.get_frames_saver_topics(),
            prediction=modules_config.get_all_predictions_topics(),
            pro_frames=modules_config.get_pro_frames_topic(),
            pro_objects=modules_config.get_pro_objects_topic(),
            pro_drivers=modules_config.get_pro_drivers_topic(),
            tracks_reload=modules_config.get_reload_topic(),
            logs=modules_config.get_logs_saver_topics(),
            frames_lifecycle=modules_config.get_lifecycle_frames_topic(),
            objects_lifecycle=modules_config.get_lifecycle_objects_topic(),
            tracks_lifecycle=modules_config.get_lifecycle_tracks_topic(),
            cvat_upload=modules_config.get_cvat_upload_topic(),
        )

        groups_config = config['consumer_groups']
        group_prefix = groups_config['prefix']
        self._groups = ConsumerGroups(
            frames_saver=group_prefix + groups_config.get('frames_saver', 'frames_group'),
            track_metadata_saver=group_prefix + groups_config.get('track_metadata_saver', 'tracks_group'),
            tracks_map_matcher=group_prefix + groups_config.get('tracks_map_matcher', 'tracks_map_matcher_group'),
            track_logs_saver=group_prefix + groups_config.get('logs_saver', 'logs_saver_group'),
            predictions_saver=group_prefix + groups_config.get('predictions_saver', 'prediction_group'),
            camcom_sender=group_prefix + groups_config.get('camcom_sender', 'camcom_sender_group'),
            tracks_reload=group_prefix + groups_config.get('tracks_reload', 'tracks_reload_group'),
            reporter_pro=group_prefix + groups_config.get('reporter_pro', 'reporter_pro_group'),
            cvat_uploader=group_prefix + groups_config.get('cvat_uploader', 'cvat_uploader_group'),
            video_frames_saver=group_prefix + groups_config.get('video_frames_saver', 'video_frames_saver_group'),
            detections_localizer=group_prefix + groups_config.get('detections_localizer', 'detections_localizer_group'),
        )

        security_config = self._prepare_security_config(config['security'])

        self._consumer_params = {
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': False,
            'max_partition_fetch_bytes': 1857600,
            'fetch_max_bytes': 1857600,
            'max_poll_records': 100,
            'bootstrap_servers': config['bootstrap_servers'],
        }
        self._consumer_params.update(security_config)

        self._producer_params = {
            'bootstrap_servers': config['bootstrap_servers'],
            'retries': 3,
            'retry_backoff_ms': 1000,
            'acks': 'all',
        }
        self._producer_params.update(security_config)
        self.producer_timeout_seconds = config.get('producer_timeout_seconds', 5)

    @property
    def topics(self) -> TopicNames:
        return self._topics

    def get_predictor_by_topic(self, topic_name) -> tp.Optional[str]:
        return self._modules_config.get_predictor_by_topic(topic_name)

    def get_unified_predictions_topic(self) -> tp.Optional[str]:
        return self._modules_config.get_unified_predictions_topic()

    def get_tracks_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            *self._topics.tracks,
            group_id=self._groups.track_metadata_saver,
            **self._consumer_params,
        )

    def get_video_frames_saver_consumer(self, consume_message_timeout_ms: float) -> KafkaConsumer:
        consumer_params = dict(self._consumer_params)
        consumer_params['max_poll_interval_ms'] = consume_message_timeout_ms
        consumer_params['max_poll_records'] = 1
        return KafkaConsumer(
            self._topics.tracks_lifecycle,
            group_id=self._groups.video_frames_saver,
            **consumer_params,
        )

    def get_detections_localizer_consumer(self, consume_message_timeout_ms: float) -> KafkaConsumer:
        consumer_params = dict(self._consumer_params)
        consumer_params['max_poll_interval_ms'] = consume_message_timeout_ms
        consumer_params['max_poll_records'] = 1
        return KafkaConsumer(
            self._topics.tracks_lifecycle,
            group_id=self._groups.detections_localizer,
            **consumer_params,
        )

    def get_logs_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            *self._topics.logs,
            group_id=self._groups.track_logs_saver,
            **self._consumer_params,
        )

    def get_frames_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            *self._topics.frames,
            group_id=self._groups.frames_saver,
            **self._consumer_params,
        )

    def get_predictions_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            *self._topics.prediction,
            group_id=self._groups.predictions_saver,
            **self._consumer_params,
        )

    def get_camcom_frames_events_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            self._topics.frames_lifecycle,
            group_id=self._groups.camcom_sender,
            **self._consumer_params,
        )

    def get_map_matcher_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            self._topics.tracks_lifecycle,
            group_id=self._groups.tracks_map_matcher,
            **self._consumer_params,
        )

    def get_pro_reporter_consumer(self) -> KafkaConsumer:
        topics = [
            self._topics.frames_lifecycle,
            self._topics.tracks_lifecycle,
        ]
        if self._topics.objects_lifecycle:
            topics.append(self._topics.objects_lifecycle)
        return KafkaConsumer(
            *topics,
            group_id=self._groups.reporter_pro,
            **self._consumer_params,
        )

    def get_reload_tracks_consumer(self, consume_message_timeout_ms: int) -> KafkaConsumer:
        reload_params = dict(self._consumer_params)
        reload_params['max_poll_interval_ms'] = consume_message_timeout_ms
        reload_params['max_poll_records'] = 1
        return KafkaConsumer(
            self._topics.tracks_reload,
            group_id=self._groups.tracks_reload,
            **reload_params,
        )

    def get_cvat_uploader_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            self._topics.cvat_upload,
            group_id=self._groups.cvat_uploader,
            **self._consumer_params,
        )

    def get_producer(self, **kwargs) -> KafkaProducer:
        logger.warning('Initializing kafka producer...')
        kwargs = {
            'value_serializer': lambda data: json.dumps(data).encode('utf-8'),
            **kwargs,
        }
        return KafkaProducer(
            **self._producer_params,
            **kwargs,
        )

    def _prepare_security_config(self, config: dict) -> dict:
        if config.get('security_protocol', 'PLAINTEXT').upper() == 'SSL':
            if not (config.get('ssl_certfile') and config.get('ssl_keyfile')):
                raise RuntimeError('invalid kafka.security config: missing ssl certificate PEM or ssl keyfile PEM')

            ctx = ssl.create_default_context()
            ctx.load_cert_chain(
                certfile=config['ssl_certfile'],
                keyfile=config['ssl_keyfile'],
            )
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

            return {
                'security_protocol': 'SSL',
                'ssl_context': ctx,
            }

        return config


def commit_single_message(consumer: KafkaConsumer, message: ConsumerRecord):
    partition, offset = message.partition, message.offset
    topic_partition = TopicPartition(message.topic, partition)
    options = {
        topic_partition: OffsetAndMetadata(offset=offset + 1, metadata=None),
    }
    consumer.commit(options)

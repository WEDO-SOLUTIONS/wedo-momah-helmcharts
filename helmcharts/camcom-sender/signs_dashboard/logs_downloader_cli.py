import logging

from dependency_injector.wiring import Provide, inject
from kafka import KafkaConsumer

from signs_dashboard.containers.application import Application
from signs_dashboard.events_tools import parse_message_key
from signs_dashboard.schemas.track_log import TrackLog
from signs_dashboard.services.kafka_service import KafkaService, commit_single_message
from signs_dashboard.services.track_logs import TrackLogsService

logger = logging.getLogger(__name__)


@inject
def download_logs(
    kafka_service: KafkaService = Provide[Application.services.kafka],
    track_logs_service: TrackLogsService = Provide[Application.services.track_logs],
):
    consumer: KafkaConsumer = kafka_service.get_logs_consumer()
    for message in consumer:
        partition, offset = message.partition, message.offset
        message_key = parse_message_key(message)
        if not message_key:
            logger.warning(f'Kafka receive message without key: {message.value}')
            commit_single_message(consumer, message)
            continue

        meta_info = f'key: {message_key}, partition: {partition}, offset: {offset}, timestamp_ms: {message.timestamp}'
        logger.warning(f'Kafka receive message with {meta_info}')
        track_log = TrackLog(
            log_data=message.value,
            timestamp_ms=message.timestamp,
            track_uuid=message_key,
        )
        try:
            track_logs_service.upload_track_log(track_log)
        except Exception as exc:
            logger.exception(f'Unable to save log for track {message_key}: {exc}')
        commit_single_message(consumer, message)

import json
import logging
import signal

from dependency_injector.wiring import Provide, inject

from signs_dashboard.containers.application import Application
from signs_dashboard.events_tools import parse_message_key
from signs_dashboard.services.kafka_service import KafkaService, commit_single_message
from signs_dashboard.services.tracks_reload import TrackReloadRequest, TracksReloadService

logger = logging.getLogger(__name__)

EXTRA_TIMEOUT_MS = 1000


@inject
def reload_tracks(  # noqa: WPS213
    kafka_service: KafkaService = Provide[Application.services.kafka],
    reload_service: TracksReloadService = Provide[Application.services.tracks_reloader],
):
    poll_timeout = reload_service.reload_track_timeout_seconds * 1000 + EXTRA_TIMEOUT_MS
    consumer = kafka_service.get_reload_tracks_consumer(consume_message_timeout_ms=poll_timeout)

    for message in consumer:
        partition, offset = message.partition, message.offset
        key = parse_message_key(message)
        meta_info = f'key: {key}, partition: {partition}, offset: {offset}'
        logger.warning(f'Kafka receive message with {meta_info}')

        reload_payload = json.loads(message.value)
        track = TrackReloadRequest(**reload_payload['track'])
        new_track_uid = reload_payload['track_new_id']
        task_hash = reload_payload['task_hash']
        logger.warning(f'Starts reload track with new id: {new_track_uid}, task_hash: {task_hash}')

        signal.signal(signal.SIGALRM, _handle_alarm_signal)
        signal.alarm(reload_service.reload_track_timeout_seconds)
        try:
            reload_service.reload_track(
                track=track,
                new_track_uid=new_track_uid,
                task_hash=task_hash,
            )
        except ProcessingTimeoutError:
            reload_service.mark_task_as_failed(task_hash)
            logger.error(f'Reload timeout, setting status = error: {new_track_uid}, task_hash: {task_hash}')
        else:
            signal.alarm(0)
            logger.warning(f'Finish reload track with new id: {new_track_uid}, task_hash: {task_hash}')
        commit_single_message(consumer, message)


@inject
def _handle_alarm_signal(signum, frame):
    logger.warning('Raising processing timeout error')
    raise ProcessingTimeoutError('Reload track timeout')


class ProcessingTimeoutError(Exception):
    """Raised when processing timeout is exceeded"""

import json
import logging

from dependency_injector.wiring import Provide, inject

from signs_dashboard.containers.application import Application
from signs_dashboard.events_tools import parse_message_key
from signs_dashboard.services.cvat.uploader import CVATUploader
from signs_dashboard.services.kafka_service import KafkaService, commit_single_message

logger = logging.getLogger(__name__)


@inject
def run(
    kafka_service: KafkaService = Provide[Application.services.kafka],
    cvat_upload_service: CVATUploader = Provide[Application.services.cvat_uploader],
):
    consumer = kafka_service.get_cvat_uploader_consumer()
    for message in consumer:
        message_key = parse_message_key(message)
        logger.info('Receives message with key %s', message_key)
        message_data = json.loads(message.value)
        logger.info('Starts process task %s', message_data)
        try:
            cvat_upload_service.upload_task(
                message_data['task_name'],
                message_data['project_id'],
                message_data['upload_uuid'],
                message_data['frame_ids'],
            )
        except Exception as err:
            logger.exception('Unable to save message %s with error %s', message_key, err)
        logger.info('Finish message with key %s', message_key)
        commit_single_message(consumer, message)

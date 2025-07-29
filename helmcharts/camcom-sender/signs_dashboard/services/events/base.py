import logging
from typing import Generic, TypeVar

from kafka import KafkaProducer
from pydantic import BaseModel

from signs_dashboard.services.kafka_service import KafkaService

logger = logging.getLogger(__name__)

EventType = TypeVar('EventType', bound=BaseModel)


class BaseLifecycleService(Generic[EventType]):
    def __init__(self, kafka_service: KafkaService):
        self._kafka_service = kafka_service
        self._producer = None

    @property
    def producer(self) -> KafkaProducer:
        if not self._producer:
            self._producer = self._kafka_service.get_producer(
                key_serializer=lambda key: key.encode('utf-8'),
                value_serializer=lambda val: val.encode('utf-8'),
            )
        return self._producer

    def _send(self, topic: str, event: EventType, event_key: str):
        logger.debug(f'Serializing {event.json()}')
        future = self.producer.send(
            topic,
            key=event_key,
            value=event.json(),
        )
        future.get(self._kafka_service.producer_timeout_seconds)

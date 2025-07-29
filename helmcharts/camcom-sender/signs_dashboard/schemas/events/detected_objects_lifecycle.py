from enum import Enum, auto

from pydantic import BaseModel, Extra, validator


class DetectedObjectEventType(Enum):
    created = auto()
    updated = auto()
    deleted = auto()
    pro_resend = auto()


class DetectedObjectEvent(BaseModel):
    object_id: int
    event_type: DetectedObjectEventType

    @validator('event_type', always=True, pre=True)
    def _validate_event_type(cls, raw_event_type) -> DetectedObjectEventType:  # noqa: N805 pylint: disable=E0213
        if isinstance(raw_event_type, DetectedObjectEventType):
            return raw_event_type
        try:
            return DetectedObjectEventType[raw_event_type]
        except KeyError as kexc:
            raise ValueError(f'Invalid event type: {kexc}')

    class Config:
        json_encoders = {DetectedObjectEventType: lambda evt: evt.name}
        extra = Extra.forbid

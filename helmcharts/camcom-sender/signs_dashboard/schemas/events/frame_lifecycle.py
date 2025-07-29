from enum import Enum, auto
from typing import Optional, Union

from pydantic import BaseModel, Extra, validator
from typing_extensions import Literal


class BBox(BaseModel):
    bbox_id: int
    xmin: int
    ymin: int
    xmax: int
    ymax: int


class FrameType(Enum):
    video360 = 'video_360'


class FrameEventType(Enum):
    uploaded = auto()
    prediction_required = auto()
    prediction_saved = auto()
    moderation_saved = auto()
    map_matching_done = auto()
    visual_localization_done = auto()
    pro_resend = auto()
    pro_hide = auto()

    @property
    def requires_sync_with_pro(self):
        return self in {
            FrameEventType.uploaded,
            FrameEventType.prediction_saved,
            FrameEventType.moderation_saved,
            FrameEventType.pro_resend,
        }


class FrameEvent(BaseModel):
    frame_id: int
    event_type: FrameEventType

    @validator('event_type', always=True, pre=True)
    def _validate_event_type(cls, raw_event_type) -> FrameEventType:  # noqa: N805 pylint: disable=E0213
        if isinstance(raw_event_type, FrameEventType):
            return raw_event_type
        try:
            return FrameEventType[raw_event_type]
        except KeyError as kexc:
            raise ValueError(f'Invalid event type: {kexc}')

    class Config:
        json_encoders = {FrameEventType: lambda evt: evt.name}
        extra = Extra.forbid


class _CommonFrameEvent(FrameEvent):
    image_url: str
    required_predictors: list[str]
    prompt: Optional[str]
    frame_type: Optional[FrameType] = None
    theta: Optional[int] = None


class UploadedFrameEvent(_CommonFrameEvent):
    event_type: Literal[FrameEventType.uploaded]


class PredictionRequiredFrameEvent(_CommonFrameEvent):
    event_type: Literal[FrameEventType.prediction_required]
    recalculate_interest_zones: bool = False
    bboxes: Optional[list[BBox]] = None


# NB! от наиболее специфичных типов к наименее специфичным
AnyFrameEvent = Union[
    UploadedFrameEvent,
    PredictionRequiredFrameEvent,
    FrameEvent,
]

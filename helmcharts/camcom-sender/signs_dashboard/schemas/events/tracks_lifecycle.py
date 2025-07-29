import typing as tp
from enum import Enum, auto

from pydantic import BaseModel, Extra, validator


class TrackEventType(Enum):
    created = auto()
    gps_track_added = auto()
    uploaded = auto()
    map_matching_required = auto()
    map_matching_done = auto()
    visual_localization_done = auto()
    predicted_for_pro = auto()
    resend_gps_track_to_pro = auto()
    dashboard_predict_event = auto()
    dashboard_cvat_upload_event = auto()
    remote_upload_completed = auto()
    localization_required = auto()
    localization_forced = auto()


class TrackEvent(BaseModel):
    track_uuid: str
    event_type: TrackEventType

    @validator('event_type', always=True, pre=True)
    def _validate_event_type(cls, raw_event_type) -> TrackEventType:  # noqa: N805 pylint: disable=E0213
        if isinstance(raw_event_type, TrackEventType):
            return raw_event_type
        try:
            return TrackEventType[raw_event_type]
        except KeyError as kexc:
            raise ValueError(f'Invalid event type: {kexc}')

    class Config:
        json_encoders = {TrackEventType: lambda evt: evt.name}
        extra = Extra.forbid


class DashboardPredictEvent(TrackEvent):
    predictor: str
    prompt: tp.Optional[str]


class DashboardCVATUploadEvent(TrackEvent):
    project_id: int
    upload_uuid: str


class RemoteUploadCompletedEvent(TrackEvent):
    track_type: str


# NB! от наиболее специфичных типов к наименее специфичным
AnyTrackEvent = tp.Union[
    RemoteUploadCompletedEvent,
    DashboardPredictEvent,
    DashboardCVATUploadEvent,
    TrackEvent,
]

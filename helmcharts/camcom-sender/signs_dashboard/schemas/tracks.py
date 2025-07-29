from datetime import date, datetime
from enum import IntEnum

from pydantic import BaseModel, validator

from signs_dashboard.small_utils import FORM_DATE_FORMAT


def _str_to_enum(enum, value):  # noqa: N805 pylint: disable=E0213
    if isinstance(value, str):
        try:
            return enum[value]
        except KeyError as exc:
            raise ValueError(f'Invalid action: {exc}')
    return value


class ResendProItemInSchema(BaseModel):
    frame_id: int
    track_uuid: str


class ResendFramesProInSchema(BaseModel):
    __root__: list[ResendProItemInSchema]


class TracksProActions(IntEnum):
    resend = 1005
    hide = 1006


class TracksLocalizationActions(IntEnum):
    force = 2005


class BaseTracksProInSchema(BaseModel):
    action: TracksProActions

    @validator('action', pre=True)
    def str_to_enum(cls, value):  # noqa: N805 pylint: disable=E0213
        if isinstance(value, str):
            try:
                return TracksProActions[value]
            except KeyError as exc:
                raise ValueError(f'Invalid action: {exc}')
        return value


class TracksProInSchema(BaseTracksProInSchema):
    track_uuids: list[str]


class DriverData(BaseModel):
    email: str
    date: date

    @validator('date', pre=True)
    def format_date(cls, value):  # noqa: N805 pylint: disable=E0213
        try:
            return datetime.strptime(value, FORM_DATE_FORMAT).date()
        except ValueError:
            raise ValueError('Wrong date format, should be DD-MM-YYYY')


class TracksByDriversInSchema(BaseTracksProInSchema):
    drivers: list[DriverData]


class TracksLocalizationInSchema(BaseModel):
    action: TracksLocalizationActions
    track_uuids: list[str]

    @validator('action', pre=True)
    def str_to_enum(cls, value):  # noqa: N805 pylint: disable=E0213
        if isinstance(value, str):
            try:
                return TracksLocalizationActions[value]
            except KeyError as exc:
                raise ValueError(f'Invalid action: {exc}')
        return value

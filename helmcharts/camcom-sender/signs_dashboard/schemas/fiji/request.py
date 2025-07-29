from enum import Enum
from typing import List, Optional, Union

from numpy import double
# pylint: disable=W0611
from pydantic import BaseModel, Extra


class TypeEnum(str, Enum):  # noqa: WPS600
    mobile = 'mobile'
    dashcam = 'dashcam'


class RequestMetadataModel(BaseModel, extra=Extra.forbid):
    distance_in_meters: int
    duration_in_millis: int
    frames_count: int
    app_version: str
    video_path: Optional[str]


class Coordinate(BaseModel, extra=Extra.forbid):
    lat: double
    lon: double


class RequestTrackPoint(BaseModel, extra=Extra.forbid):
    coordinate: Coordinate
    datetime_utc: float
    azimuth: float
    speed: float


class RequestRoadSurface(BaseModel, extra=Extra.forbid):
    surface: str
    asphalt_quality: str


class RequestFramesError(BaseModel, extra=Extra.forbid):
    id: int
    track_point: RequestTrackPoint
    link: str
    errors: List[str]


class RequestFrames(BaseModel, extra=Extra.forbid):
    id: int
    track_point: RequestTrackPoint
    link: str
    signs: List[dict]
    labels: List[str]
    road_surface: RequestRoadSurface
    recognized_road_marking: Optional[dict]


class QualityCheckResult(BaseModel, extra=Extra.forbid):
    label: str
    threshold: float
    value: float


class QualityCheck(BaseModel, extra=Extra.forbid):
    passed: bool
    forced: bool
    checks: list[QualityCheckResult]


class FijiRequest(BaseModel, extra=Extra.forbid):
    id: str
    type: TypeEnum
    user_email: str
    metadata: RequestMetadataModel
    frames: List[Union[RequestFrames, RequestFramesError]]
    gps_track: List[dict]
    fusion_track: Optional[dict]
    quality_check: QualityCheck

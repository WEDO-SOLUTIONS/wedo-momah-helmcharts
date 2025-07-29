from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class TypeEnum(str, Enum):  # noqa: WPS600
    mobile = 'mobile'
    dashcam = 'dashcam'


class FijiSign(BaseModel):
    id: int
    class_id: int


class FijiError(BaseModel):
    id: int
    types: List[int]
    created: datetime
    sign: FijiSign


class FijiClarification(BaseModel):
    id: int
    class_id: int
    created: datetime
    updated: datetime
    sign: Optional[FijiSign]


class FijiStatistics(BaseModel):
    filtered: List[FijiSign]
    road_ids: List[int]


class FijiResponse(BaseModel):
    processing_status: int
    projects: List[str]
    only_in_projects: bool
    statistics: FijiStatistics
    errors: List[FijiError]
    clarifications: List[FijiClarification]

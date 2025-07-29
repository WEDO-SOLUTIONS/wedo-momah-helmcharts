import json
from typing import Any, List, Optional, Union

from pydantic import BaseModel, Extra, Field, fields, validator

BBoxType = Union[str]


class BBox(BaseModel):
    xmin: int
    xmax: int
    ymin: int
    ymax: int

    related_bboxes: List['BBox'] = fields.Field(default_factory=list)

    probability: Optional[float] = None  # FIXME: check FIELD default
    attributes: Optional[dict[str, Any]] = None

    polygon: Optional[list[int]] = None
    # additional if signs format
    label: Optional[str] = None
    is_side: Optional[bool] = None
    is_side_prob: Optional[float] = None
    directions: Optional[list] = None
    directions_prob: Optional[float] = None
    is_tmp: Optional[bool] = False
    sign_value: Optional[float] = None


class BBoxAttributes(BaseModel):
    bbox_id: int
    attributes: dict[str, Any]


class DepthMap(BaseModel):
    data: str


class PredictionResults(BaseModel):
    bboxes: Optional[dict[BBoxType, list[BBox]]] = None  # noqa: WPS234
    attributes: Optional[dict[str, Any]] = None
    version: int = Field(1, const=True)
    depth_map: Optional[DepthMap] = None
    bbox_attributes: Optional[list[BBoxAttributes]] = None

    class Config:
        extra = Extra.forbid

    @validator('attributes', always=True, pre=True)
    def _validate_attributes(cls, attributes: Optional[dict]) -> Optional[dict]:  # noqa: N805 pylint: disable=E0213
        if isinstance(attributes, dict):
            return {
                attr: attr_value
                for attr, attr_value in attributes.items()
                if attr_value is not None
            }
        return attributes


class PredictorAnswer(BaseModel):
    results: Optional[PredictionResults] = None
    predictor: str
    # TODO: уточнить про новый формат - объект или строка
    error_info: Optional[str] = None

    meta: Optional[dict] = None
    frame_id: Optional[int] = None

    frame_type: Optional[str] = None
    theta: Optional[int] = None

    # field save to DB
    raw_data: Optional[Union[dict, list]] = None

    @validator('error_info')
    @classmethod
    def convert_error_info(cls, error_info: Optional[Union[dict, str]]):
        if isinstance(error_info, dict):
            error_info = json.dumps(error_info)
        return error_info

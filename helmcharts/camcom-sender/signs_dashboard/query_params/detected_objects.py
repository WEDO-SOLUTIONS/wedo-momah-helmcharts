from typing import Optional

from pydantic import BaseModel


class DetectedObjectsQueryParams(BaseModel):
    region_ids: Optional[list[int]]
    label: Optional[str]

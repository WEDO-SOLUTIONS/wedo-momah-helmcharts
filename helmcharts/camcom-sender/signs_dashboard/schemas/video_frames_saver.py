from datetime import datetime
from typing import Optional

from pydantic.main import BaseModel


class VideoFrame(BaseModel):
    image: bytes
    timestamp_ms: float
    sample_time_ms: float
    gps_data: dict
    meta: dict

    @property
    def date_from_meta(self) -> Optional[datetime]:
        if self.meta is not None:
            return datetime.utcfromtimestamp(self.meta['ts'] / 1000)
        return None

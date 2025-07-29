import os
from dataclasses import dataclass
from datetime import datetime

LOG_FILENAME_PREFIX = 'log_'


@dataclass
class TrackLog:
    """Хранит информацию о логе трека."""

    log_data: bytes
    timestamp_ms: int
    track_uuid: str

    @property
    def date(self):
        return datetime.utcfromtimestamp(self.timestamp_ms / 1000).date()


@dataclass
class LogFileOnS3:
    """Информация о сохраненном на s3 лог-файле."""

    key: str
    filename: str
    last_modified: datetime
    bucket: str
    size_kb: float

    @classmethod
    def from_s3_obj(cls, s3_obj: dict, bucket: str):
        key = s3_obj['Key']
        return cls(
            key=key,
            filename=os.path.split(key)[-1],
            last_modified=s3_obj['LastModified'],
            bucket=bucket,
            size_kb=round(s3_obj['Size'] / 1024, 2),
        )

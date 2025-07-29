import datetime
from dataclasses import dataclass
from typing import Optional

from signs_dashboard.models.camcom_job import (
    CAMCOM_JOB_BAD_STATUSES,
    CAMCOM_JOB_SENT_STATUSES,
    STATUS_CODE_TO_STATUS_TEXT,
)


@dataclass
class CamComStatShort:

    status: int
    frames_count: int

    @property
    def status_text(self) -> str:
        return STATUS_CODE_TO_STATUS_TEXT[self.status]

    @property
    def is_bad_status(self) -> bool:
        return self.status in CAMCOM_JOB_BAD_STATUSES

    @property
    def is_sent_status(self) -> bool:
        return self.status in CAMCOM_JOB_SENT_STATUSES


@dataclass
class CamComStat(CamComStatShort):

    http_code: int
    sample_response: Optional[str] = None


@dataclass
class DailyStatistics:
    for_date: datetime.date
    statuses: list[list[CamComStat]]

    @property
    def rows_count(self) -> int:
        return len(
            [
                stat
                for stat_http_statuses in self.statuses
                for stat in stat_http_statuses
            ],
        )


@dataclass
class CamComTrackStat:

    all_frames: int = 0
    sent: int = 0
    errors: int = 0

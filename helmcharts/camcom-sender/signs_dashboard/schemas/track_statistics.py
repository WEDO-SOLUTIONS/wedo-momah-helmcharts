import datetime
from dataclasses import dataclass
from typing import Optional

import shapely

from signs_dashboard.models.track import Track, TrackStatuses
from signs_dashboard.models.track_upload_status import STATUS_NOT_UPLOADED, STATUS_UPLOADED
from signs_dashboard.models.user import ApiUser


@dataclass
class TrackStatistics:
    track: Track
    date: datetime.datetime
    total_errors_count: int
    new_errors_count: int
    missed_errors_count: int
    resolved_errors_count: int
    good_errors_count: int
    completed_sla_error_count: int

    clarifications_count: int
    resolved_clarifications_count: int
    good_clarifications_count: int
    deleted_clarifications_count: int
    completed_sla_clarifications_count: int

    frames_count: int
    map_matched: bool

    @classmethod
    def create(cls, track: Track, date_type: str):
        errors = [error for error in track.errors if not error.is_deleted()]
        clarifications = [cl for cl in track.clarifications if not cl.is_deleted()]

        return cls(
            track=track,
            date=track.uploaded if date_type == 'uploaded' else track.recorded,
            total_errors_count=len(errors),
            new_errors_count=len([error for error in errors if error.is_new_sign()]),
            missed_errors_count=len([error for error in errors if error.is_missing_sign()]),
            resolved_errors_count=len([error for error in errors if error.is_resolved()]),
            good_errors_count=len([error for error in errors if error.is_good_error()]),
            completed_sla_error_count=len([error for error in errors if error.is_sla_completed()]),
            clarifications_count=len(clarifications),
            resolved_clarifications_count=len([cl for cl in clarifications if cl.is_resolved()]),
            good_clarifications_count=len([cl for cl in clarifications if cl.is_good()]),
            deleted_clarifications_count=len([cl for cl in clarifications if cl.is_deleted()]),
            completed_sla_clarifications_count=len([cl for cl in clarifications if cl.is_sla_completed()]),
            frames_count=track.frames_count,
            map_matched=track.is_map_matching_done() if track else False,
        )


@dataclass
class DailyTracksStatistics:
    user_email: str
    app_versions: list[str]
    for_date: datetime.date
    tracks_uuids: list[str]
    distance_km: float
    duration: datetime.timedelta
    centroid_wkt: str
    gps_track_wkt: str
    pro_statuses: list[int]
    upload_statuses: list[int]
    api_user: Optional[ApiUser]

    @property
    def centroid_point(self) -> shapely.geometry.Point:
        return shapely.from_wkt(self.centroid_wkt)

    @property
    def pro_id(self) -> str:
        return f'{self.user_email} @ {self.for_date}'

    @property
    def timestamp(self) -> int:
        return int(datetime.datetime.combine(self.for_date, datetime.datetime.min.time()).timestamp() * 1000)

    @property
    def processing_status(self) -> str:
        if self.upload_statuses == [STATUS_NOT_UPLOADED]:
            return 'uploading_from_phone'
        if self.upload_statuses == [STATUS_UPLOADED]:
            if self.pro_statuses == [TrackStatuses.SENT_PRO]:
                return 'processed'
        return 'being_processed'

    @classmethod
    def build_empty(
        cls,
        user_email: str,
        for_date: datetime.date,
        api_user: Optional[ApiUser],
    ) -> 'DailyTracksStatistics':
        return cls(
            user_email=user_email,
            for_date=for_date,
            centroid_wkt='MULTIPOINT EMPTY',
            gps_track_wkt='LINESTRING EMPTY',
            app_versions=[],
            tracks_uuids=[],
            distance_km=0,
            duration=datetime.timedelta(),
            pro_statuses=[],
            upload_statuses=[],
            api_user=api_user,
        )

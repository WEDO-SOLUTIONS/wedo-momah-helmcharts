from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Set

from signs_dashboard.schemas.track_log_data import AUDITED_SIGN_CLASSES, TRUCK_SIGN_CLASSES
from signs_dashboard.schemas.track_statistics import TrackStatistics

DATE_FORMAT = '%d-%m-%Y'  # noqa: WPS323


class SignsGroupCounter:
    def __init__(self):
        self.all = 0
        self.all_after = 0
        self.audited = 0
        self.truck = 0

    def __iter__(self):
        yield from (
            ('all', self.all),
            ('audited', self.audited),
            ('truck', self.truck),
        )


@dataclass
class DriverStatistics:
    date: str
    email: str
    type: str
    app_version: str
    projects: Set
    only_in_projects: bool
    distance: float
    duration: float

    total_errors_count: int
    new_errors_count: int
    missed_errors_count: int
    resolved_errors_count: int
    good_errors_count: int

    clarifications_count: int
    resolved_clarifications_count: int
    good_clarifications_count: int
    deleted_clarifications_count: int

    good_statuses_count: int
    rejected_statuses_count: int
    failed_statuses_count: int

    pro_good_statuses_count: int
    pro_failed_statuses_count: int

    completed_sla_error_count: int
    completed_sla_clarifications_count: int

    total_signs: SignsGroupCounter
    total_good: SignsGroupCounter

    total_tracks_received: int
    total_tracks_uploaded: int

    frames_count: int
    matched_frames_count: int

    @property
    def accuracy(self):
        if not self.total_signs.audited:
            return 0

        accuracy = self.total_good.audited / self.total_signs.audited * 100
        return round(100 - accuracy, 2)

    @property
    def kpd(self):
        good_total = self.good_errors_count + self.good_clarifications_count
        resolved_total = self.resolved_errors_count + self.resolved_clarifications_count

        if resolved_total == 0:
            return ''
        return round(good_total / resolved_total, 2)

    @property
    def freq_stat(self):
        total = self.total_errors_count + self.clarifications_count
        if total == 0 or self.distance == 0:
            return ''
        return round(total / self.distance, 2)

    @property
    def sla(self):
        total = self.total_errors_count + self.clarifications_count
        completed_total = self.completed_sla_error_count + self.completed_sla_clarifications_count
        if total == 0:
            return ''
        return round(100 * (completed_total / total), 1)

    @property
    def signs_density(self):
        if not self.distance:
            return 0
        return round(self.total_signs.all_after / self.distance, 2)

    @property
    def signs_density_before(self):
        if not self.distance:
            return 0
        return round(self.total_signs.all / self.distance, 2)

    @property
    def next_day_date(self) -> str:
        curr_dt = datetime.strptime(self.date, DATE_FORMAT)
        return (curr_dt + timedelta(days=1)).strftime(DATE_FORMAT)

    @classmethod
    def create(cls, tr_stats: List[TrackStatistics]):

        distance = sum(stat.track.distance for stat in tr_stats)
        duration = sum(stat.track.duration for stat in tr_stats)

        total_tracks_uploaded = sum(stat.track.upload.status for stat in tr_stats if stat.track.upload)
        total_tracks_received = len(tr_stats)

        return cls(
            date=tr_stats[0].date.strftime(DATE_FORMAT),
            email=tr_stats[0].track.user_email,
            type=tr_stats[0].track.type,
            app_version=tr_stats[0].track.app_version,
            projects=set(sum([stat.track.projects or [] for stat in tr_stats], [])),
            only_in_projects=any(stat.track.only_in_projects for stat in tr_stats),
            distance=round(distance, 2),
            duration=round(duration, 2),
            total_errors_count=sum(stat.total_errors_count for stat in tr_stats),
            new_errors_count=sum(stat.new_errors_count for stat in tr_stats),
            missed_errors_count=sum(stat.missed_errors_count for stat in tr_stats),
            resolved_errors_count=sum(stat.resolved_errors_count for stat in tr_stats),
            good_errors_count=sum(stat.good_errors_count for stat in tr_stats),
            clarifications_count=sum(stat.clarifications_count for stat in tr_stats),
            resolved_clarifications_count=sum(stat.resolved_clarifications_count for stat in tr_stats),
            good_clarifications_count=sum(stat.good_clarifications_count for stat in tr_stats),
            deleted_clarifications_count=sum(stat.deleted_clarifications_count for stat in tr_stats),
            good_statuses_count=sum(stat.track.is_fiji_good_status() for stat in tr_stats),
            rejected_statuses_count=sum(stat.track.is_fiji_rejected_status() for stat in tr_stats),
            failed_statuses_count=sum(stat.track.is_fiji_failed_status() for stat in tr_stats),
            pro_good_statuses_count=sum(stat.track.is_pro_good_status() for stat in tr_stats),
            pro_failed_statuses_count=sum(stat.track.is_pro_bad_status() for stat in tr_stats),
            completed_sla_error_count=sum(stat.completed_sla_error_count for stat in tr_stats),
            completed_sla_clarifications_count=sum(stat.completed_sla_clarifications_count for stat in tr_stats),
            total_signs=cls.encount_all_signs(tr_stats),
            total_good=cls.encount_good_signs(tr_stats),
            total_tracks_received=total_tracks_received,
            total_tracks_uploaded=total_tracks_uploaded,
            frames_count=sum(stat.frames_count for stat in tr_stats if stat.frames_count),
            matched_frames_count=sum(stat.frames_count for stat in tr_stats if stat.frames_count and stat.map_matched),
        )

    @classmethod
    def encount_all_signs(cls, tr_stats):
        total_signs = SignsGroupCounter()
        for tr_stat in tr_stats:
            if not tr_stat.track.num_important_signs:
                continue

            total_signs.all += tr_stat.track.num_important_signs
            total_signs.all_after += tr_stat.track.num_important_signs
            total_signs.all_after += tr_stat.new_errors_count - tr_stat.missed_errors_count
            total_signs.audited += tr_stat.track.num_audited_signs
            total_signs.truck += tr_stat.track.num_truck_signs
        return total_signs

    @classmethod
    def encount_good_signs(cls, tr_stats):
        total_good = SignsGroupCounter()

        for tr_stat in tr_stats:
            for error in tr_stat.track.errors:
                cls._encount_good_error(error, total_good)

            for clarification in tr_stat.track.clarifications:
                cls._encount_good_clarification(clarification, total_good)

        return total_good

    @classmethod
    def _encount_good_clarification(cls, clarification, total_good):
        if not clarification.is_good():
            return

        total_good.all += 1

        if clarification.sign_type in list(AUDITED_SIGN_CLASSES.values()):
            total_good.audited += 1
        if clarification.sign_type in list(TRUCK_SIGN_CLASSES.values()):
            total_good.truck += 1

    @classmethod
    def _encount_good_error(cls, error, total_good):
        if not error.is_good_error():
            return

        total_good.all += 1

        if error.sign_type in list(AUDITED_SIGN_CLASSES.values()):
            total_good.audited += 1
        if error.sign_type in list(TRUCK_SIGN_CLASSES.values()):
            total_good.truck += 1


@dataclass
class DriverDailyActivity:
    date_for: date
    email: str
    example_track_uuid: str

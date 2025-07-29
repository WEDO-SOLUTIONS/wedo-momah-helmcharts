from itertools import groupby

from signs_dashboard.models.camcom_job import CamcomJobStatus
from signs_dashboard.query_params.camcom import CamComStatsQueryParams
from signs_dashboard.repository.camcom_job import CamcomJobRepository
from signs_dashboard.schemas.camcom_statistics import CamComStat, CamComStatShort, CamComTrackStat, DailyStatistics

expected_status_order = (
    CamcomJobStatus.WILL_BE_SENT,
    CamcomJobStatus.CREATED,
    CamcomJobStatus.SENT,
    CamcomJobStatus.CAMCOM_ERROR,
    CamcomJobStatus.CAMCOM_COMPLETE,
)


class CamcomStatisticsService:
    def __init__(self, camcom_job_repository: CamcomJobRepository):
        self._camcom_job_repository = camcom_job_repository

    def statistics(self, query_params: CamComStatsQueryParams) -> list[DailyStatistics]:
        stats_rows = self._camcom_job_repository.statistics(query_params)
        if not stats_rows:
            return []

        stats = [DailyStatistics(for_date=stats_rows[0].date, statuses=[])]
        raw_statuses = []
        for stat in stats_rows:
            if stat.date != stats[-1].for_date:
                stats[-1].statuses = self._sort_statuses(raw_statuses)
                raw_statuses = []
                stats.append(DailyStatistics(for_date=stat.date, statuses=[]))
            raw_statuses.append(
                CamComStat(
                    frames_count=stat.frames_count,
                    http_code=stat.http_code,
                    status=stat.status,
                    sample_response=stat.sample_response,
                ),
            )

        # last iteration
        stats[-1].statuses = self._sort_statuses(raw_statuses)

        return stats

    def get_statistics_by_frame_ids(self, frame_ids: list[int]) -> CamComTrackStat:
        stats = self._get_simple_statistics_by_frame_ids(frame_ids)
        camcom_stats_obj = CamComTrackStat()
        for stat in stats:
            camcom_stats_obj.all_frames += stat.frames_count

            if stat.is_sent_status:
                camcom_stats_obj.sent += stat.frames_count
            if stat.is_bad_status:
                camcom_stats_obj.errors += stat.frames_count
        return camcom_stats_obj

    def _get_simple_statistics_by_frame_ids(self, frame_ids: list[int]) -> list[CamComStatShort]:
        return [CamComStatShort(*stat) for stat in self._camcom_job_repository.get_statistics_by_frame_ids(frame_ids)]

    def _sort_http_statuses(self, statuses: list[CamComStat]) -> list[CamComStat]:
        return sorted(statuses, key=lambda stat: stat.http_code or -1)

    def _sort_statuses(self, statuses: list[CamComStat]) -> list[list[CamComStat]]:
        return [
            self._sort_http_statuses(stats)
            for _, stats in groupby(sorted(statuses, key=_status_sort_key), key=_status_sort_key)
        ]


def _status_sort_key(status_info: CamComStat) -> list[bool, ...]:
    return [
        status_info.status == status
        for status in expected_status_order
    ]

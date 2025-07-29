import itertools
import logging
from datetime import date, datetime, timedelta
from typing import Optional, Sequence

from signs_dashboard.models.clarification import Clarification
from signs_dashboard.models.error import Error
from signs_dashboard.models.track import Track, TrackStatuses, TrackType
from signs_dashboard.models.track_upload_status import TrackUploadStatus
from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.query_params.drivers import DriversQueryParams
from signs_dashboard.query_params.tracks import TrackQueryParameters
from signs_dashboard.repository.fiji_request import FijiRequestRepository
from signs_dashboard.repository.tracks import TracksRepository
from signs_dashboard.repository.tracks_localization import TracksLocalizationRepository
from signs_dashboard.schemas.driver_statistics import DriverDailyActivity, DriverStatistics
from signs_dashboard.schemas.fiji.response import FijiResponse
from signs_dashboard.schemas.track_log_data import TrackStatisticsData
from signs_dashboard.schemas.track_statistics import DailyTracksStatistics, TrackStatistics
from signs_dashboard.services.events.tracks_lifecycle import TracksLifecycleService
from signs_dashboard.services.track_gps_points_handler import (
    Track as TrackPoints,
    TrackGPSPointsHandlerService,
    TrackPoint,
)
from signs_dashboard.services.users import UsersService
from signs_dashboard.small_utils import correct_round

SORT_DATE_FORMAT = '%Y-%m-%d'  # noqa: WPS323
logger = logging.getLogger(__name__)


def _sort_key(track_stat: TrackStatistics):
    return '{date}_{email}'.format(
        date=track_stat.date.strftime(SORT_DATE_FORMAT),
        email=track_stat.track.user_email,
    )


class TracksService:
    def __init__(
        self,
        tracks_repository: TracksRepository,
        tracks_localization_repository: TracksLocalizationRepository,
        fiji_request_repository: FijiRequestRepository,
        users_service: UsersService,
        tracks_lifecycle_service: TracksLifecycleService,
        track_gps_points_handler_service: TrackGPSPointsHandlerService,
        modules_config: ModulesConfig,
    ) -> None:
        self._tracks_repository = tracks_repository
        self._tracks_localization_repository = tracks_localization_repository
        self._fiji_request_repository = fiji_request_repository
        self._users_service = users_service
        self._tracks_lifecycle_service = tracks_lifecycle_service
        self._track_gps_points_handler_service = track_gps_points_handler_service
        self._modules_config = modules_config

    @property
    def default_localization_status(self):
        if self._modules_config.is_track_localization_enabled():
            return TrackStatuses.LOCALIZATION_PENDING
        return TrackStatuses.LOCALIZATION_DISABLED

    @property
    def default_map_matching_status(self):
        if self._modules_config.is_map_matching_enabled():
            return TrackStatuses.MAP_MATCHING_PENDING
        return TrackStatuses.MAP_MATCHING_DISABLED

    def find_tracks_by_query_params(self, query_params: TrackQueryParameters) -> list[Track]:
        return self._tracks_repository.find(query_params, tracks_only=True)

    def get_pro_status(self, uuid: str) -> Optional[int]:
        if fields := self._tracks_repository.get_fields_values(uuid, [Track.pro_status]):
            return fields.pro_status
        return None

    def get_recorded(self, uuid: str) -> Optional[datetime]:
        if fields := self._tracks_repository.get_fields_values(uuid, [Track.recorded]):
            return fields.recorded
        return None

    def get(self, uuid: str) -> Optional[Track]:
        return self._tracks_repository.get_by_uuid(uuid)

    def get_with_localization_statuses(self, uuid: str) -> Optional[Track]:
        return self._tracks_repository.get_by_uuid(uuid, with_localization_statuses=True)

    def get_with_api_user(self, uuid: str) -> Optional[Track]:
        return self._tracks_repository.get_by_uuid(uuid, with_api_user=True)

    def get_by_uuids(self, uuids: list[str]) -> list[Track]:
        return self._tracks_repository.get_by_uuids(uuids)

    def get_fiji_uploading_tracks(self, fiji_retries: int, fiji_retries_timeout: int) -> list[Track]:
        return self._tracks_repository.find_with_retries_requests_by_fiji_status(
            [
                TrackStatuses.UPLOADING,
                TrackStatuses.FORCED_SEND,
                TrackStatuses.SENT_FIJI,
                TrackStatuses.FIJI_SENDING_IN_PROCESS,
            ],
            max_retries=fiji_retries,
            retries_timeout=fiji_retries_timeout,
        )

    def get_pro_uploading_tracks(self) -> list[Track]:
        return self._tracks_repository.find_by_pro_status([
            TrackStatuses.UPLOADING,
            TrackStatuses.SENT_PRO_WITHOUT_PREDICTIONS,
            TrackStatuses.FORCED_SEND,
            TrackStatuses.WILL_BE_HIDDEN_PRO,
        ])

    def get_localization_pending_tracks(
        self,
        expected_track_types: Sequence[str],
        skip_localization_statuses: Sequence[int],
        localization_requires_detections_from: Optional[str],
        scheduled_processing_timeout: timedelta,
        track_upload_timeout: timedelta,
        uploading_track_localization_interval: timedelta,
        uploaded_track_localization_interval: timedelta,
    ) -> list[tuple[Track, list]]:
        return self._tracks_repository.find_localization_pending(
            expected_track_types=expected_track_types,
            skip_localization_statuses=skip_localization_statuses,
            scheduled_processing_timeout=scheduled_processing_timeout,
            localization_requires_detections_from=localization_requires_detections_from,
            track_upload_timeout=track_upload_timeout,
            uploading_track_localization_interval=uploading_track_localization_interval,
            uploaded_track_localization_interval=uploaded_track_localization_interval,
        )

    def fetch_track_detectors(self, track_uuid: str) -> tuple[list[str], list[str]]:
        return self._tracks_repository.fetch_track_detectors(track_uuid)

    def change_fiji_status(self, uuid: str, status: int):
        self._tracks_repository.update_track_fiji_status(uuid, status)

    def change_pro_status(self, uuid: str, status: int):
        self._tracks_repository.update_track_pro_status(uuid, status)

    def bulk_change_pro_status(self, uuids: list[str], status: int):
        self._tracks_repository.bulk_update_track_field(uuids, pro_status=status)

    def change_localization_status(self, uuid: str, status: int):
        self._tracks_repository.update_track_localization_status(uuid, status)

    def bulk_change_localization_status(self, uuids: list[str], status: int):
        self._tracks_repository.bulk_update_track_field(uuids, localization_status=status)

    def mark_localization_done(self, track_uuid: str, detectors: list[str], last_done: datetime):
        return self._set_localization_status(
            track_uuid,
            detectors=detectors,
            status=TrackStatuses.LOCALIZATION_DONE,
            last_done=last_done,
        )

    def mark_localization_failed(self, track_uuid: str, detectors: list[str]):
        return self._set_localization_status(track_uuid, detectors=detectors, status=TrackStatuses.LOCALIZATION_ERROR)

    def mark_localization_started(self, track_uuid: str, detectors: list[str]):
        return self._set_localization_status(
            track_uuid,
            detectors=detectors,
            status=TrackStatuses.LOCALIZATION_IN_PROGRESS,
        )

    def mark_localization_scheduled(self, track_uuid: str, detectors: list[str]):
        return self._set_localization_status(
            track_uuid,
            detectors=detectors,
            status=TrackStatuses.LOCALIZATION_SCHEDULED,
        )

    def change_map_matching_status(self, uuid: str, status: int):
        self._tracks_repository.update_track_map_matching_status(uuid, status)

    def mark_track_as_uploaded(self, uuid: str):
        updated_track_email = self._tracks_repository.set_uploaded(uuid)
        if updated_track_email:
            self._tracks_lifecycle_service.produce_uploaded_event(track_uuid=uuid, user_email=updated_track_email)

    def set_track_recorded_time_and_distance(self, track_uuid: str, recorded_time: datetime, distance_km: float):
        updated_track_email = self._tracks_repository.set_track_recorded_time_and_distance(
            track_uuid,
            distance_km=distance_km,
            recorded_time=recorded_time,
        )
        if updated_track_email:
            self._tracks_lifecycle_service.produce_gps_track_added_event(
                track_uuid=track_uuid,
                user_email=updated_track_email,
            )

    def save_fiji_results(
        self,
        track: Track,
        response: Optional[FijiResponse],
        labels_stats: dict,
        total_signs: int,
    ):
        track.filter_label_to_count = labels_stats
        track.num_detections = total_signs

        if response:
            statistics_data = TrackStatisticsData.from_fiji(response.statistics)
            track.num_important_signs = len(statistics_data.filtered_ids)
            track.num_audited_signs = len(statistics_data.filtered_audited_ids)
            track.num_truck_signs = len(statistics_data.filtered_truck_ids)
            track.errors = [Error.from_fiji_api(error, track.uuid) for error in response.errors]
            track.clarifications = [Clarification.from_fiji_api(cl, track.uuid) for cl in response.clarifications]
            track.fiji_status = response.processing_status
            track.projects = response.projects
            track.only_in_projects = response.only_in_projects

        self._tracks_repository.save_track(track)

    def save_fiji_request(
        self,
        track_uuid: str,
        last_response: str,
        last_request_time: datetime,
        retries: int,
        last_response_status: Optional[int],
        last_fiji_status: Optional[int],
    ):
        if last_response_status == 200:
            last_response = None

        self._fiji_request_repository.upsert(
            track_uuid=track_uuid,
            last_response=last_response,
            last_response_status=last_response_status,
            last_request_time=last_request_time,
            retries=retries,
            last_fiji_status=last_fiji_status,
        )

    def create_track_from_init_request(
        self,
        request_data: dict,
        track_uuid: str,
        event_dt: datetime,
        track_type: str,
        recorded: Optional[str],
    ):
        localization_status = self.default_localization_status
        if track_type != TrackType.mobile:
            localization_status = TrackStatuses.LOCALIZATION_UNSUPPORTED_TRACK_TYPE
        created_track_email = self._tracks_repository.create_track_from_init_request(
            request_data=request_data,
            track_uuid=track_uuid,
            event_dt=event_dt,
            track_type=track_type,
            localization_status=localization_status,
            map_matching_status=self.default_map_matching_status,
            recorded=recorded,
        )
        if created_track_email:
            self._tracks_lifecycle_service.produce_created_event(track_uuid, user_email=created_track_email)

    def produce_predicted_for_pro_event(self, track: Track):
        self._tracks_lifecycle_service.produce_predicted_for_pro_event(
            track_uuid=track.uuid,
            user_email=track.user_email,
        )

    def produce_remote_upload_completed_event(self, track_uuid: str, user_email: str, track_type: str):
        self._tracks_lifecycle_service.produce_remote_upload_completed_event(
            track_uuid=track_uuid,
            user_email=user_email,
            track_type=track_type,
        )

    def get_upload_status(self, track_uuid: str) -> TrackUploadStatus:
        return self._tracks_repository.get_upload_status(track_uuid)

    def save_upload_status(self, upload_status: TrackUploadStatus):
        self._tracks_repository.save_upload_status(upload_status)

    def get_daily_tracks_stats(self, user_email: str, target_date: date) -> DailyTracksStatistics:
        stats = self._tracks_repository.get_tracks_summary_at_date(
            user_email,
            target_date=target_date,
        )
        api_user = self._users_service.get_by_email(user_email)
        tracks_uuids = stats['tracks_uuids']
        if not tracks_uuids:
            return DailyTracksStatistics.build_empty(
                user_email=user_email,
                for_date=target_date,
                api_user=api_user,
            )

        optimized_daily_tracks = []
        for track_uuid in tracks_uuids:
            track_info = self._tracks_repository.get_daily_gps_track_info(track_uuid)
            if not track_info:
                logger.info(f'No track info extracted for {track_uuid}')
                continue
            logger.debug(f'Daily track info: {track_info}')
            optimized_daily_tracks.append(
                self._track_gps_points_handler_service.optimize(
                    TrackPoints(
                        points=[
                            TrackPoint(
                                coords=track_point[0],
                                speed=track_point[1],
                            )
                            for track_point in track_info
                        ],
                    ),
                ),
            )

        daily_geometry = self._track_gps_points_handler_service.get_daily_geometry(optimized_daily_tracks)

        distance_km = stats.pop('distance_km')

        return DailyTracksStatistics(
            user_email=user_email,
            for_date=target_date,
            centroid_wkt=daily_geometry.centroid_wkt,
            gps_track_wkt=daily_geometry.gps_track_wkt,
            distance_km=correct_round(distance_km, 2),
            api_user=api_user,
            **stats,
        )

    def get_tracks_stats(self, query_params: TrackQueryParameters) -> list[TrackStatistics]:
        tracks = self._tracks_repository.find(query_params)
        return [TrackStatistics.create(track, query_params.date_type) for track in tracks]

    def get_drivers_stats(self, query_params: TrackQueryParameters) -> list[DriverStatistics]:
        tracks_stats = self.get_tracks_stats(query_params)

        tracks_stats = sorted(tracks_stats, key=_sort_key, reverse=True)
        groups = [list(group) for _, group in itertools.groupby(tracks_stats, _sort_key)]

        return [DriverStatistics.create(group) for group in groups]

    def get_active_drivers(self, query_params: DriversQueryParams):
        activity = self._tracks_repository.get_active_drivers(query_params)
        return [DriverDailyActivity(**row) for row in activity]

    def send_resend_gps_to_pro_event(self, track_uuid: str, user_email: str):
        self._tracks_lifecycle_service.produce_resend_gps_track_to_pro_event(track_uuid, user_email=user_email)

    def _set_localization_status(
        self,
        track_uuid: str,
        detectors: list[str],
        status: int,
        last_done: Optional[datetime] = None,
    ):
        for detector_name in detectors:
            self._tracks_localization_repository.create_or_update(
                track_uuid=track_uuid,
                detector_name=detector_name,
                status=status,
                last_done=last_done,
            )
        self.change_localization_status(track_uuid, status)

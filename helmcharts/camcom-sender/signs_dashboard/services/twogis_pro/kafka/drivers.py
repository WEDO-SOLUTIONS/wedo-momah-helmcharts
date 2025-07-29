from datetime import datetime
from typing import Optional

from signs_dashboard.schemas.track_statistics import DailyTracksStatistics
from signs_dashboard.services.twogis_pro.kafka.driver_userinfo_fields import DriverInformationRenderingMixin
from signs_dashboard.services.twogis_pro.kafka.localization import TwoGisProKafkaLocalizerService

SECONDS_IN_HOUR = 60 * 60


class TwoGisProDriversService(DriverInformationRenderingMixin):
    def __init__(
        self,
        localization_service: TwoGisProKafkaLocalizerService,
    ):
        self._localization_service = localization_service

    def get_payload(
        self,
        tracks_statistics: DailyTracksStatistics,
    ) -> Optional[dict]:
        index = {
            'email': tracks_statistics.user_email,
            'app_versions': tracks_statistics.app_versions,
            'distance_km': tracks_statistics.distance_km,
            'datetime_utc': tracks_statistics.timestamp,
            'duration_hours': round(tracks_statistics.duration.total_seconds() / SECONDS_IN_HOUR, 2),
            'status': tracks_statistics.processing_status,
            **self._driver_extra_index_fields(tracks_statistics.api_user),
        }
        fields = [
            *self._driver_extra_ui_fields(tracks_statistics.api_user),
            {
                'type': 'string',
                'value': tracks_statistics.user_email,
                **self._localization_service.get_caption_translations(field='driver_email'),
            },
            {
                'type': 'string',
                'value': str(tracks_statistics.distance_km),
                **self._localization_service.get_caption_translations(field='driver_daily_distance_km'),
            },
            {
                'type': 'string',
                **self._localization_service.get_value_translations_as_strftime(
                    datetime.min + tracks_statistics.duration,  # converts timedelta to datetime for strftime
                    field='driver_daily_tracks_duration',
                    key='template',
                ),
                **self._localization_service.get_caption_translations(field='driver_daily_tracks_duration'),
            },
            {
                'type': 'string',
                **self._localization_service.get_value_translations_as_strftime(
                    tracks_statistics.for_date,
                    field='driver_stat_date',
                    key='template',
                ),
                **self._localization_service.get_caption_translations(field='driver_stat_date'),
            },
            {
                'type': 'string',
                **self._localization_service.get_value_translations(
                    field='driver_daily_tracks_status',
                    key=tracks_statistics.processing_status,
                ),
                **self._localization_service.get_caption_translations(field='driver_daily_tracks_status'),
            },
            {
                'type': 'string',
                'value': ', '.join(tracks_statistics.app_versions),
                **self._localization_service.get_caption_translations(field='driver_daily_app_versions'),
            },
        ]
        point = tracks_statistics.centroid_point
        if point.is_empty:
            return None
        return {
            'id': tracks_statistics.pro_id,
            'name': tracks_statistics.user_email,
            'searchable_keywords': [
                tracks_statistics.user_email,
                *self._driver_extra_searchable_keywords(tracks_statistics.api_user),
            ],
            '{index}': index,
            'field_groups': [{
                'fields': fields,
            }],
            'wkt': tracks_statistics.gps_track_wkt,
            'point': {
                'lat': point.y,
                'lon': point.x,
            },
        }

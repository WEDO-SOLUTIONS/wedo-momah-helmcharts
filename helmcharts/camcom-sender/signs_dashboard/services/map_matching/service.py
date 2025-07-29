import logging
from dataclasses import dataclass
from itertools import groupby
from operator import attrgetter, itemgetter
from typing import Optional

from signs_dashboard.models.frame import Frame
from signs_dashboard.models.track import Track
from signs_dashboard.services.frames import FramesService
from signs_dashboard.services.gps_interpolation import GPSInterpolationService
from signs_dashboard.services.interest_zones import InterestZonesService
from signs_dashboard.services.map_matching.client import MapMatchingAPIClient, MapMatchingResult
from signs_dashboard.services.map_matching.exceptions import UnableToMatchTrackError
from signs_dashboard.services.s3_service import S3Service
from signs_dashboard.services.track_length import TrackLengthService
from signs_dashboard.services.tracks import TracksService
from signs_dashboard.small_utils import batch_iterator, correct_round

logger = logging.getLogger(__name__)

FRAMES_TIMESTAMP_MAX_DIFF_SEC = 10
FRAMES_TIMESTAMP_MAX_DIFF_MS = FRAMES_TIMESTAMP_MAX_DIFF_SEC * 1000
INTERPOLATED_GPS_POINTS_INTERVAL_SEC = 7


@dataclass
class TrackMapMatchingResult:
    matched_points: list[dict]
    distance_meters: float

    @property
    def distance_km(self) -> float:
        return correct_round(self.distance_meters / 1000, rank=3)

    @classmethod
    def empty(cls):
        return cls(matched_points=[], distance_meters=0)


class MapMatchingService:
    def __init__(
        self,
        map_matching_config: dict,
        tracks_service: TracksService,
        track_length_service: TrackLengthService,
        frames_service: FramesService,
        interest_zones_service: InterestZonesService,
        s3_service: S3Service,
        gps_interpolation_service: GPSInterpolationService,
    ):
        self._tracks_service = tracks_service
        self._track_length_service = track_length_service
        self._frames_service = frames_service
        self._interest_zones_service = interest_zones_service
        self._client = MapMatchingAPIClient(s3_service, map_matching_config)
        self._gps_interpolation_service = gps_interpolation_service

    def match_track(self, track: Track):
        logger.info(f'Map matching track {track.uuid}, track has {len(track.upload.gps_points)} raw gps points')

        frames = sorted(self._frames_service.get_by_track(track), key=attrgetter('timestamp'))
        if not frames:
            raise UnableToMatchTrackError(f'Track {track.uuid} has no frames')

        points_for_matching = self._get_points_for_matching(track, frames)

        points_for_matching = self._interpolate_missing_points(points_for_matching)

        match_result = self._match(points_for_matching, track=track)

        track.upload.matched_gps_points = match_result.matched_points
        self._tracks_service.save_upload_status(track.upload)

        points_mapping = {
            mpoint['timestamp']: mpoint
            for mpoint in match_result.matched_points
        }

        for frame in frames:
            point = points_mapping.get(frame.timestamp)
            if not point:
                raise UnableToMatchTrackError(
                    f'Frame {frame.id} was not matched - frame point missing from map matching results',
                )
            frame.matched_lat = correct_round(point['latitude'])
            frame.matched_lon = correct_round(point['longitude'])
            self._frames_service.save(frame)
            self._interest_zones_service.update_frame_interest_zones(frame)

        self._tracks_service.set_track_recorded_time_and_distance(
            track.uuid,
            recorded_time=track.recorded,
            distance_km=self._track_length_service.calculate_length_km(match_result.matched_points),
        )
        return frames

    def _match(self, gps_points: list[dict], track: Track) -> TrackMapMatchingResult:
        batch_size = 1000
        if len(gps_points) > 1000:
            batch_size = 900

        matched = TrackMapMatchingResult.empty()
        for batch in batch_iterator(gps_points, batch_size):
            match_result = self._match_via_api(batch, track=track)
            matched.matched_points += [
                self._gps_point_from_matched_point(point)
                for point in match_result.query
            ]
            matched.distance_meters += match_result.distance
        return matched

    def _get_points_for_matching(self, track: Track, frames: list[Frame]) -> list[dict]:
        points = self._select_gps_points_between_frames(None, frames[0], track.upload.gps_points)

        for idx, current_frame in enumerate(frames):
            points.append(self._match_point_from_frame(current_frame))
            if idx == len(frames) - 1:
                continue

            next_frame = frames[idx + 1]
            if current_frame.timestamp - next_frame.timestamp > FRAMES_TIMESTAMP_MAX_DIFF_MS:
                points += self._select_gps_points_between_frames(current_frame, next_frame, track.upload.gps_points)

        points += self._select_gps_points_between_frames(frames[-1], None, track.upload.gps_points)
        return points

    def _interpolate_missing_points(self, points: list[dict]) -> list[dict]:
        resulting_points = []
        for idx, current_point in enumerate(points):
            resulting_points.append(current_point)
            if idx == len(points) - 1:
                continue

            next_point = points[idx + 1]
            ts_diff = next_point['utc'] - current_point['utc']
            if ts_diff > FRAMES_TIMESTAMP_MAX_DIFF_SEC:
                logger.debug(f'Too big time difference between points: {current_point} ({idx=}), {next_point}')
                resulting_points += self._gps_interpolation_service.interpolate_mm_points(
                    point1=current_point,
                    point2=next_point,
                    intermediate_points=int(ts_diff // INTERPOLATED_GPS_POINTS_INTERVAL_SEC) + 1,
                )
        return resulting_points

    def _select_gps_points_between_frames(  # noqa: WPS231
        self,
        frame1: Optional[Frame],
        frame2: Optional[Frame],
        gps_points: list[dict],
    ) -> list[dict]:
        points = []
        for gps_point in gps_points:
            if frame1 and gps_point['timestamp'] < frame1.timestamp:
                continue
            if frame2 and gps_point['timestamp'] > frame2.timestamp:
                continue
            points.append(self._match_point_from_gps_point(gps_point))
        return points

    def _match_point_from_gps_point(self, point: dict) -> dict:
        return {
            'lat': point['latitude'],
            'lon': point['longitude'],
            'utc': point['timestamp'] / 1000,
            'speed': point['speed'],
            'azimuth': int(point['bearing']),
        }

    def _match_point_from_frame(self, frame: Frame) -> dict:
        return {
            'lat': frame.lat,
            'lon': frame.lon,
            'utc': frame.timestamp / 1000,
            'speed': frame.speed,
            'azimuth': int(frame.azimuth),
        }

    def _gps_point_from_matched_point(self, point: dict) -> dict:
        return {
            'latitude': point['lat_matched'],
            'longitude': point['lon_matched'],
            'speed': point['speed'],
            'bearing': point['azimuth'],
            'timestamp': point['utc'] * 1000,
        }

    def _match_via_api(self, points: list[dict], track: Track) -> MapMatchingResult:
        return self._client.match(points, track=track)


class LegacyMapMatchingService(MapMatchingService):
    def match_track(self, track: Track):
        logger.info(f'Map matching track {track.uuid}, track has {len(track.upload.gps_points)} raw gps points')

        points_for_matching = self._prepare_for_matching(track.upload.gps_points)

        match_result = self._match(points_for_matching, track=track)

        track.upload.matched_gps_points = match_result.matched_points
        self._tracks_service.save_upload_status(track.upload)

        self._tracks_service.set_track_recorded_time_and_distance(
            track.uuid,
            recorded_time=track.recorded,
            distance_km=self._track_length_service.calculate_length_km(match_result.matched_points),
        )

        frames = self._frames_service.get_by_track(track)
        for frame in frames:
            frame_point = self._gps_interpolation_service.interpolate_frame_point(
                frame.timestamp,
                gps_points=match_result.matched_points,
            )
            lat, lon = frame_point['latitude'], frame_point['longitude']
            frame.matched_lat = correct_round(lat)
            frame.matched_lon = correct_round(lon)
            self._frames_service.save(frame)
            self._interest_zones_service.update_frame_interest_zones(frame)

        return frames

    def _match_point_from_gps_point(self, point: dict) -> dict:
        return {
            'lat': point['latitude'],
            'lon': point['longitude'],
            'utc': int(point['timestamp'] / 1000),
            'speed': point['speed'],
            'azimuth': int(point['bearing']),
        }

    def _prepare_for_matching(self, gps_points: list[dict]) -> list[dict]:
        interpolated_points = []
        last_gps_point = None
        for gps_point in gps_points:
            if last_gps_point:
                interpolated_points += self._gps_interpolation_service.interpolate_gps_points(last_gps_point, gps_point)
            else:
                interpolated_points.append(gps_point)
            last_gps_point = gps_point

        timestamp_key = itemgetter('timestamp')
        group_by_seconds = groupby(sorted(interpolated_points, key=timestamp_seconds_key), key=timestamp_seconds_key)
        return sorted(
            [
                min(group, key=timestamp_key)
                for _, group in group_by_seconds
            ],
            key=timestamp_key,
        )

    def _match_via_api(self, gps_points: list[dict], track: Track) -> MapMatchingResult:
        points = [
            self._match_point_from_gps_point(point)
            for point in gps_points
        ]
        return self._client.match(points, track=track)


def timestamp_seconds_key(point: dict) -> int:
    return point['timestamp'] // 1000

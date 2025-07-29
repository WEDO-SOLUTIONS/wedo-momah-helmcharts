import logging
import math
from functools import partial

logger = logging.getLogger(__name__)


class GPSInterpolationService:
    def interpolate_gps_points(  # noqa: WPS210
        self,
        point1: dict,
        point2: dict,
        intermediate_points: int = 100,
    ) -> list[dict]:
        interpolate = partial(interpolate_value, intermediate_points=intermediate_points)

        latitudes = interpolate(point1['latitude'], point2['latitude'])
        longitudes = interpolate(point1['longitude'], point2['longitude'])
        timestamps = interpolate(point1['timestamp'], point2['timestamp'])
        speeds = interpolate(point1['speed'], point2['speed'])
        bearings = interpolate(point1['bearing'], point2['bearing'])
        altitudes = (
            interpolate(point1['altitude'], point2['altitude'])
            if _interpolatable(point1.get('altitude')) and _interpolatable(point2.get('altitude'))
            else [None] * intermediate_points  # noqa: WPS435
        )
        include_altitude = 'altitude' in point1

        return [
            {
                'latitude': lat,
                'longitude': lon,
                'timestamp': int(ts),
                'speed': speed,
                'bearing': bearing,
                **({'altitude': altitude} if include_altitude else {}),
            }
            for lat, lon, ts, speed, bearing, altitude in zip(
                latitudes, longitudes, timestamps, speeds, bearings, altitudes,
            )
        ]

    def interpolate_mm_points(  # noqa: WPS210
        self,
        point1: dict,
        point2: dict,
        intermediate_points: int,
    ) -> list[dict]:
        interpolate = partial(interpolate_value, intermediate_points=intermediate_points)

        latitudes = interpolate(point1['lat'], point2['lat'])
        longitudes = interpolate(point1['lon'], point2['lon'])
        timestamps = interpolate(point1['utc'], point2['utc'])
        speeds = interpolate(point1['speed'], point2['speed'])
        azimuths = interpolate(point1['azimuth'], point2['azimuth'])

        return [
            {
                'lat': lat,
                'lon': lon,
                'utc': ts,
                'speed': speed,
                'azimuth': int(azimuth),
            }
            for lat, lon, ts, speed, azimuth in zip(
                latitudes, longitudes, timestamps, speeds, azimuths,
            )
        ]

    def interpolate_frame_point(self, frame_timestamp: float, gps_points: list[dict]) -> dict:  # noqa: WPS210
        prev_point = None
        next_point = None
        points = gps_points

        point_ts_to_point = {point['timestamp']: point for point in gps_points}
        if gps_point := point_ts_to_point.get(frame_timestamp):
            logger.debug(f'Found point for frame {frame_timestamp} with exact timestamp')
            return gps_point

        for idx in range(len(gps_points) - 1):
            if gps_points[idx]['timestamp'] <= frame_timestamp <= gps_points[idx + 1]['timestamp']:
                prev_point = gps_points[idx]
                next_point = gps_points[idx + 1]
                break

        if prev_point and next_point:
            points_count = int(next_point['timestamp'] - prev_point['timestamp'])
            points = self.interpolate_gps_points(prev_point, next_point, intermediate_points=points_count)

        if not points:
            raise ValueError(f'No points for interpolation, {frame_timestamp=}, {gps_points=}')

        point = min(points, key=lambda point: abs(point['timestamp'] - frame_timestamp))
        point_distance = point['timestamp'] - frame_timestamp
        logger.debug(f'Closest point for frame {frame_timestamp} is in {point_distance} ms')
        return point


def interpolate_value(value1, value2, intermediate_points: int):
    """Вспомогательная функция для интерполяции значений."""
    return [
        value1 + (idx / (intermediate_points + 1)) * (value2 - value1)
        for idx in range(1, intermediate_points + 1)
    ]


def _interpolatable(value) -> bool:
    return isinstance(value, float) and not math.isnan(value)

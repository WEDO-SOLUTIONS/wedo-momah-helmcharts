from geopy.distance import GeodesicDistance


class TrackLengthService:
    def __init__(self):
        self._geodesic = GeodesicDistance(
            ellipsoid='WGS-84',
        )

    def calculate_length_km(self, points: list[dict]):
        length = sum([
            self._geodesic.measure(
                _track_point_as_latlon(point1),
                _track_point_as_latlon(point2),
            )
            for point1, point2 in pairwise(points)
        ])
        return round(length, 3)


def _track_point_as_latlon(point: dict):
    return point['latitude'], point['longitude']


def pairwise(iterable):
    # pairwise('ABCDEFG') --> AB BC CD DE EF FG
    # todo: use itertools.pairwise in python3.10+
    return zip(iterable[:-1], iterable[1:])

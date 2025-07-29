from dataclasses import dataclass
from typing import Optional

import shapely
from shapely.geometry import LineString, MultiLineString


@dataclass
class TrackPoint:
    coords: list[float]
    speed: Optional[float] = None


@dataclass
class Track:
    points: list[TrackPoint]


@dataclass
class TrackGeometry:
    gps_track_wkt: str
    centroid_wkt: str


class TrackGPSPointsHandlerService:
    def __init__(self):
        self._simplify_tolerance = 0.00001  # ~ meters per degree

    def optimize(self, track: Track) -> Track:
        self._remove_duplicate_coords_points(track)
        self._remove_zero_speed_points(track)

        if len(track.points) == 1:
            # if there is a track with 1 point, there are conditions on shapely objects
            track.points.append(track.points[0])
        else:
            self._simplify(track)
        return track

    @staticmethod
    def get_daily_geometry(tracks: list[Track], wkt_precision: int = 6) -> TrackGeometry:
        multilinestring = MultiLineString(
            [[point.coords for point in track.points] for track in tracks],
        )

        return TrackGeometry(
            gps_track_wkt=_remove_extra_spaces(shapely.to_wkt(multilinestring, rounding_precision=wkt_precision)),
            centroid_wkt=multilinestring.centroid.wkt,
        )

    @staticmethod
    def _remove_duplicate_coords_points(track: Track):
        optimized_points = []
        last_coords = None
        for point in track.points:
            if last_coords and last_coords == point.coords:
                continue
            last_coords = point.coords
            optimized_points.append(point)
        track.points = optimized_points

    @staticmethod
    def _remove_zero_speed_points(track: Track):
        optimized_points = []
        for point in track.points:
            if point.speed:
                optimized_points.append(point)
            elif not optimized_points or optimized_points[-1].speed != 0:
                optimized_points.append(point)

        try:
            if optimized_points[-1] != track.points[-1]:
                optimized_points.append(track.points[-1])
        except IndexError:
            pass  # noqa: WPS420

        track.points = optimized_points

    def _simplify(self, track: Track):
        linestring = LineString([point.coords for point in track.points])

        # TODO: explore different coordinate systems if higher accuracy is needed
        simplified_linestring = linestring.simplify(self._simplify_tolerance)

        track.points = [TrackPoint(coords=coords) for coords in list(simplified_linestring.coords)]


def _remove_extra_spaces(wkt_string: str) -> str:
    return wkt_string.replace(', ', ',').replace('MULTILINESTRING (', 'MULTILINESTRING(')

from typing import Optional

import simplekml

from signs_dashboard.models.track import Track
from signs_dashboard.query_params.tracks import TrackQueryParameters
from signs_dashboard.services.tracks import TracksService


class KMLGeneratorService:
    def __init__(self, tracks_service: TracksService):
        self._tracks_service = tracks_service

    def get_kml_from_track_id(self, track_uuid: str, fiji_enabled: bool) -> str:
        track = self._tracks_service.get(track_uuid)
        points = {track.uuid: track.upload.gps_points}
        return _add_gps_tracks_to_kml([track], points, fiji_enabled=fiji_enabled)

    def get_kml_from_email_and_date(self, query_params: TrackQueryParameters, fiji_enabled: bool) -> str:
        tracks = self._tracks_service.find_tracks_by_query_params(query_params)
        points = {track.uuid: self._get_gps_points(track.uuid) for track in tracks}
        return _add_gps_tracks_to_kml(tracks, points, fiji_enabled=fiji_enabled)

    def _get_gps_points(self, track_uuid: str) -> Optional[list[dict]]:
        upload_status = self._tracks_service.get_upload_status(track_uuid)
        if upload_status:
            return upload_status.gps_points
        return None


def _add_gps_tracks_to_kml(tracks: list[Track], points: dict[str, list], fiji_enabled: bool):
    kml = simplekml.Kml()

    for track in tracks:
        track_coords = [
            (point.get('Longitude') or point.get('longitude'), point.get('Latitude') or point.get('latitude'))
            for point in points[track.uuid] or []
        ]
        ls = kml.newlinestring(coords=track_coords)

        if track.is_fiji_good_status() or not fiji_enabled:
            ls.style.linestyle.color = simplekml.Color.green
            ls.name = track.uuid
        elif track.is_fiji_rejected_status():
            ls.style.linestyle.color = simplekml.Color.yellow
            ls.name = '{uuid} ({status})'.format(uuid=track.uuid, status='rejected')
        else:
            ls.style.linestyle.color = simplekml.Color.red
            ls.name = '{uuid} ({status})'.format(uuid=track.uuid, status='failed')
        ls.style.linestyle.width = 10

    return kml.kml()

import logging
from typing import Optional, Union

from signs_dashboard.models.frame import Frame
from signs_dashboard.models.interest_zones import (
    INTEREST_ZONE_TYPE_PRODUCE_ATTRIBUTE,
    InterestRegionInfo,
    InterestZone,
    InterestZoneType,
)
from signs_dashboard.repository.interest_zones import InterestZonesRepository, PolygonAndName
from signs_dashboard.services.prediction import PredictionService

logger = logging.getLogger(__name__)


class InterestZonesService:
    def __init__(self, interest_zones_repository: InterestZonesRepository, prediction_service: PredictionService):
        self._interest_zones_repository = interest_zones_repository
        self._prediction_service = prediction_service

    def add_interest_zone(
        self,
        zone_name: str,
        zone_type: InterestZoneType,
        polygons: list[PolygonAndName],
        from_srid: str,
    ):
        self._interest_zones_repository.add_interest_zone(zone_name, zone_type, polygons, from_srid)

    def get_interest_zone(self, zone_name: str) -> Optional[InterestZone]:
        return self._interest_zones_repository.get_interest_zone(zone_name)

    def delete_interest_zone(self, zone: InterestZone):
        self._interest_zones_repository.delete_interest_zone(zone)

    def get_interest_zones(self) -> list[InterestZone]:
        return self._interest_zones_repository.get_interest_zones()

    def get_interest_zone_regions_as_geojson(self, zone: InterestZone) -> str:
        return self._interest_zones_repository.get_interest_zone_regions_as_geojson(zone)

    def get_regions_for_search(self) -> list[InterestRegionInfo]:
        return self._interest_zones_repository.get_regions_for_search()

    def recreate_zone_polygons(self, zone: InterestZone, polygons: list[PolygonAndName], from_srid: str):
        self._interest_zones_repository.recreate_zone_polygons(zone, polygons, from_srid=from_srid)

    def update_frame_interest_zones(self, frame: Frame):
        zones_attributes = self._get_interest_zones_attributes(frame)
        if zones_attributes:
            self._prediction_service.save_interest_zones_attributes(frame, zones_attributes)

    def _get_interest_zones_attributes(self, frame: Frame) -> dict[str, Union[str, bool]]:  # noqa: WPS231
        zones = self._interest_zones_repository.get_interest_zones(INTEREST_ZONE_TYPE_PRODUCE_ATTRIBUTE)
        if not zones:
            return {}

        matches = self._interest_zones_repository.select_frame_match_zones(frame, INTEREST_ZONE_TYPE_PRODUCE_ATTRIBUTE)
        zone_name_to_zone: dict[str, InterestZone] = {zone.name: zone for zone in zones}

        attributes = self._get_default_attributes(zones)
        for zone_match in matches:
            zone = zone_name_to_zone.get(zone_match.zone_name)
            if not zone:
                raise ValueError(f'Unknown zone type: {zone_match.zone_name}')

            if zone.simple_match:
                attributes.update({zone.name: zone_match.match})
            elif zone.match_yields_name:
                if len(zone_match.names) > 1:
                    logger.error(f'Frame {frame.id}: multiple regions found for "{zone.name}": {zone_match.names}')
                    continue
                attributes.update({zone.name: zone_match.names[0] if zone_match.names else None})
        return attributes

    def _get_default_attributes(self, zones: list[InterestZone]) -> dict[str, Union[str, bool]]:
        return {zone.name: zone.default for zone in zones}

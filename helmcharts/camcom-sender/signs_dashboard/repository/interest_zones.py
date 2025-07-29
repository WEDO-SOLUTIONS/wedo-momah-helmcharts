from typing import Optional

from geoalchemy2.functions import ST_AsGeoJSON
from shapely.geometry.polygon import Polygon
from sqlalchemy import String, cast, distinct, func, insert, select
from sqlalchemy.dialects.postgresql import JSONB

from signs_dashboard.models.frame import Frame
from signs_dashboard.models.interest_zones import (
    INTEREST_ZONE_TYPE_REQUIRES_NAME,
    SRID4326_ID,
    InterestRegionInfo,
    InterestZone,
    InterestZoneRegion,
    InterestZoneType,
)

PolygonAndName = tuple[Polygon, Optional[str]]


class InterestZonesRepository:
    def __init__(self, session_factory):
        self.session_factory = session_factory

    def add_interest_zone(
        self,
        zone_name: str,
        zone_type: InterestZoneType,
        polygons: list[PolygonAndName],
        from_srid: str,
    ):
        with self.session_factory() as session:
            query = insert(InterestZone).values(
                name=zone_name,
                zone_type=zone_type,
                created_at=func.now(),
            ).returning(InterestZone.id)
            zone_id = session.execute(query).first()[0]
            self._recreate_zone_polygons(session, zone_id, polygons, from_srid)
            session.commit()

    def delete_interest_zone(self, zone: InterestZone):
        with self.session_factory() as session:
            session.query(InterestZoneRegion).filter(InterestZoneRegion.zone_id == zone.id).delete()
            session.query(InterestZone).filter(InterestZone.id == zone.id).delete()
            session.commit()

    def get_interest_zone_regions_as_geojson(self, zone: InterestZone) -> str:
        with self.session_factory() as session:
            fields = [
                InterestZoneRegion.region,
            ]
            if zone.zone_type in INTEREST_ZONE_TYPE_REQUIRES_NAME:
                fields.append(InterestZoneRegion.name)

            subquery = select(*fields)
            subquery = subquery.select_from(InterestZoneRegion).join(InterestZone)
            subquery = subquery.filter(InterestZone.name == zone.name)
            subquery = subquery.subquery()

            query = select(
                cast(
                    func.jsonb_build_object(
                        'type',
                        'FeatureCollection',
                        'features',
                        func.jsonb_agg(cast(ST_AsGeoJSON(subquery, 'region'), JSONB)),
                    ),
                    String,
                ).label('polygons'),
            )
            query = query.select_from(subquery)
            return session.execute(query).first().polygons

    def get_interest_zone(self, zone_name: str) -> Optional[InterestZone]:
        with self.session_factory() as session:
            return session.query(InterestZone).filter(InterestZone.name == zone_name).first()

    def get_interest_zones(self, zone_types: Optional[tuple[InterestZoneType, ...]] = None) -> list[InterestZone]:
        with self.session_factory() as session:
            query = session.query(InterestZone)
            if zone_types:
                query = query.filter(InterestZone.zone_type.in_(zone_types))
            query = query.order_by(InterestZone.name.asc())
            return query.all()

    def get_regions_for_search(self) -> list[InterestRegionInfo]:
        with self.session_factory() as session:
            query = select(
                InterestZoneRegion.id,
                InterestZone.name.label('zone_name'),
                InterestZoneRegion.name.label('region_name'),
            )
            query = query.join(InterestZone)
            query = query.filter(InterestZone.zone_type == InterestZoneType.search_by_region)
            regions = session.execute(query).all()
            return [InterestRegionInfo(**region) for region in regions]

    def recreate_zone_polygons(self, zone: InterestZone, polygons: list[PolygonAndName], from_srid: str):
        with self.session_factory() as session:
            self._recreate_zone_polygons(session, zone.id, polygons, from_srid)
            session.commit()

    def select_frame_match_zones(self, frame: Frame, zone_types: tuple[str, ...]):
        with self.session_factory() as session:
            subquery = select(
                InterestZone.name.label('zone_name'),
                func.ST_Intersects(
                    InterestZoneRegion.region,
                    func.ST_SetSRID(func.ST_MakePoint(frame.current_lon, frame.current_lat), SRID4326_ID),
                ).label('match'),
                InterestZoneRegion.name.label('region_name'),
            )
            subquery = subquery.select_from(InterestZoneRegion).join(InterestZone)
            subquery = subquery.where(InterestZone.zone_type.in_(zone_types))

            query = session.query(
                subquery.c.zone_name,
                func.bool_or(subquery.c.match).label('match'),
                func.array_agg(distinct(subquery.c.region_name)).label('names'),
            )
            query = query.select_from(subquery)
            query = query.filter(subquery.c.match.is_(True))
            query = query.group_by(subquery.c.zone_name)
            query = query.order_by(subquery.c.zone_name.asc())
            return session.execute(query).all()

    def _recreate_zone_polygons(self, session, zone_id: int, polygons: list[PolygonAndName], from_srid: str):
        from_srid_id = int(from_srid.replace('epsg:', ''))
        session.query(InterestZoneRegion).filter(InterestZoneRegion.zone_id == zone_id).delete()

        for polygon, polygon_name in polygons:
            region = polygon.wkt
            if from_srid_id != SRID4326_ID:
                region = func.ST_Transform(func.ST_GeomFromText(polygon.wkt, from_srid_id), SRID4326_ID)
            query = insert(InterestZoneRegion).values(
                region=region,
                created_at=func.now(),
                name=polygon_name,
                zone_id=zone_id,
            )
            session.execute(query)

from dataclasses import dataclass
from enum import Enum, auto
from typing import Union

from geoalchemy2 import Geometry
from sqlalchemy import Column, DateTime, Enum as SQLAlchemyEnum, ForeignKey, Integer, String, func
from sqlalchemy.orm import relationship

from signs_dashboard.pg_database import Base

SRID4326_ID = 4326


@dataclass
class InterestRegionInfo:
    id: int
    region_name: str
    zone_name: str


class InterestZoneType(Enum):
    any_region_intersection_as_attribute = auto()
    region_name_as_attribute = auto()
    search_by_region = auto()


INTEREST_ZONE_TYPE_REQUIRES_NAME = (
    InterestZoneType.search_by_region,
    InterestZoneType.region_name_as_attribute,
)
INTEREST_ZONE_TYPE_PRODUCE_ATTRIBUTE = (
    InterestZoneType.any_region_intersection_as_attribute,
    InterestZoneType.region_name_as_attribute,
)


class InterestZone(Base):
    __tablename__ = 'interest_zones'

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=func.now)
    name = Column(String, nullable=False)
    zone_type: InterestZoneType = Column(
        SQLAlchemyEnum(
            InterestZoneType,
            native_enum=False,
            create_constraint=False,
        ),
        nullable=False,
    )

    regions = relationship(
        'InterestZoneRegion',
        back_populates='zone',
        lazy='noload',
        primaryjoin='and_(InterestZone.id==InterestZoneRegion.zone_id)',
    )

    @property
    def default(self) -> Union[bool, None]:
        if self.zone_type == InterestZoneType.any_region_intersection_as_attribute:
            return False
        return None

    @property
    def match_yields_name(self) -> bool:
        return self.zone_type == InterestZoneType.region_name_as_attribute

    @property
    def simple_match(self) -> bool:
        return self.zone_type == InterestZoneType.any_region_intersection_as_attribute


class InterestZoneRegion(Base):
    __tablename__ = 'interest_zone_regions'

    id = Column(Integer, primary_key=True)
    zone_id = Column(Integer, ForeignKey(InterestZone.id))
    created_at = Column(DateTime, default=func.now)
    name = Column(String)
    region = Column(Geometry(geometry_type='POLYGON', srid=SRID4326_ID))

    zone = relationship(
        InterestZone,
        back_populates='regions',
        lazy='joined',
        primaryjoin='and_(InterestZone.id==InterestZoneRegion.zone_id)',
    )

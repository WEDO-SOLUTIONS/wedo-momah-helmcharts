import os
from datetime import datetime, timedelta, timezone
from enum import Enum
from itertools import groupby
from operator import attrgetter
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Boolean, Column, DateTime, Float, Integer, SmallInteger, String, func
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.orm import relationship

from signs_dashboard.models.user import ApiUser
from signs_dashboard.pg_database import Base
from signs_dashboard.small_utils import correct_round, timezone_offset_str

if TYPE_CHECKING:
    from signs_dashboard.models.bbox_detection import BBOXDetection


class ModerationStatus(Enum):
    not_moderated = 0
    moderated_wrong = 1
    moderated_good = 2


class Frame(Base):
    __tablename__ = 'frames'

    id = Column(Integer, primary_key=True)
    track_uuid = Column(String)
    track_email = Column(String)
    lat = Column(Float)
    lon = Column(Float)
    matched_lat = Column(Float, default=None)
    matched_lon = Column(Float, default=None)
    azimuth = Column(Float)
    speed = Column(Float)
    date = Column(DateTime)
    timezone_offset: timedelta = Column(INTERVAL(fields='HOUR TO MINUTE'), nullable=False, default='00:00')
    panoramic = Column(Boolean, default=False)
    uploaded_photo = Column(Boolean, default=False)
    detections: list['BBOXDetection'] = relationship(
        'BBOXDetection',
        back_populates='frame',
        uselist=True,
        lazy='joined',
        primaryjoin='and_(Frame.id==BBOXDetection.frame_id, Frame.date==BBOXDetection.date)',
    )
    moderation_status = Column(SmallInteger, nullable=False, default=ModerationStatus.not_moderated.value)

    attributes = relationship(
        'FrameAttribute',
        back_populates='frame',
        lazy='noload',
        uselist=True,
        primaryjoin='and_(Frame.id==FrameAttribute.frame_id, Frame.date==FrameAttribute.date)',
    )

    track = relationship(
        'Track',
        lazy='noload',
        uselist=False,
        primaryjoin='Frame.track_uuid==Track.uuid',
        foreign_keys=[track_uuid],
    )

    api_user: ApiUser = relationship(
        'ApiUser',
        foreign_keys=[track_email],
        primaryjoin=func.lower(track_email) == func.lower(ApiUser.email),
        lazy='noload',
        viewonly=True,
    )

    def get_coords_str(self, transformer):
        coord1, coord2 = transformer.transform(self.lat, self.lon)
        return '{coord1} {coord2}'.format(coord1=coord1, coord2=coord2)

    @property
    def map_matched(self):
        return self.matched_lat is not None and self.matched_lon is not None

    @property
    def current_lat(self) -> float:
        return self.matched_lat if self.map_matched else self.lat

    @property
    def current_lon(self) -> float:
        return self.matched_lon if self.map_matched else self.lon

    @property
    def timestamp(self) -> int:
        return int(self.date.replace(tzinfo=timezone.utc).timestamp() * 1000)

    @property
    def image_name(self) -> str:
        return '{ts}_{lat:.12f}_{lon:.12f}.jpg'.format(
            ts=self.timestamp,
            lat=correct_round(self.lat),
            lon=correct_round(self.lon),
        )

    @property
    def local_datetime(self) -> datetime:
        return self.date.replace(tzinfo=timezone.utc).astimezone(timezone(self.timezone_offset))

    @property
    def app_version(self) -> Optional[str]:
        if self.track:
            return self.track.app_version
        return None

    def as_prediction_dict(self) -> dict:
        return {
            'fname': os.path.join(self.track_uuid, self.image_name),
            'recognized_signs': [detection.as_prediction_dict() for detection in self.detections],
        }

    @property
    def detections_mapping(self) -> dict[str, list['BBOXDetection']]:
        grouper = attrgetter('label')
        return groupby(sorted(self.detections, key=grouper), key=grouper)

    @property
    def detections_as_manual_prediction(self) -> dict:
        return {
            'meta': self.meta,
            'predictor': 'manual',
            'results': {
                'version': 1,
                'bboxes': {
                    label: [
                        det.as_bbox_with_attributes()
                        for det in label_detections
                    ]
                    for label, label_detections in self.detections_mapping
                },
            },
        }

    @property
    def timezone_offset_str(self) -> str:
        return timezone_offset_str(self.timezone_offset)

    @property
    def meta(self) -> dict:
        return {
            'lat': self.lat,
            'lon': self.lon,
            'azimuth': self.azimuth,
            'speed': self.speed,
            'ts': self.timestamp,
            'track_email': self.track_email,
            'timezone_offset': self.timezone_offset_str,
        }

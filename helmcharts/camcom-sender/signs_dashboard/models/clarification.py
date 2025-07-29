from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Integer, String

from signs_dashboard.pg_database import Base
from signs_dashboard.schemas.fiji.response import FijiClarification

STATUS_OPEN = 0
STATUS_FIXED = 1
STATUS_REJECTED = 2

TYPE_LANE_DIRECTIONS = 3541
TYPE_PLATES = 3542
TYPE_TRUCK_SIGNS = 5023
TYPE_TRAFFIC_LIGHT = 5056

SECONDS_IN_DAY = 86400
NEW_SIGN_PROCESSING_SLA = SECONDS_IN_DAY * 4


class Clarification(Base):
    __tablename__ = 'clarifications'

    id = Column(Integer, primary_key=True)
    created = Column(DateTime)
    updated = Column(DateTime)
    type = Column(Integer)
    sign_id = Column(Integer, nullable=True)
    sign_type = Column(String)
    track_uuid = Column(String)
    status = Column(Integer)
    deleted = Column(Boolean)
    is_new_sign = Column(Boolean, default=False)

    def is_resolved(self):
        return self.status != STATUS_OPEN

    def is_deleted(self):
        return self.deleted or self.type != TYPE_LANE_DIRECTIONS

    def is_good(self):
        return self.status == STATUS_FIXED and self.is_new_sign

    def is_sla_completed(self):
        return self._get_processing_seconds() < NEW_SIGN_PROCESSING_SLA

    def update(self, **kwargs):
        for attr_name, attr_value in kwargs.items():
            setattr(self, attr_name, attr_value)
        return self

    @classmethod
    def from_fiji_api(cls, clarification: FijiClarification, uuid: str):
        return cls(
            id=clarification.id,
            created=clarification.created,
            updated=clarification.updated,
            type=clarification.class_id,
            sign_id=clarification.sign.id if clarification.sign else None,
            sign_type=clarification.sign.class_id if clarification.sign else None,
            track_uuid=uuid,
            status=STATUS_OPEN,
            deleted=False,
            is_new_sign=False,
        )

    def _get_processing_seconds(self):
        if self.type != TYPE_LANE_DIRECTIONS:
            return 0
        if self.is_deleted():
            return (self.updated - self.created).total_seconds()
        if self.is_resolved():
            return (self.updated - self.created).total_seconds()
        return (datetime.now() - self.created).total_seconds()

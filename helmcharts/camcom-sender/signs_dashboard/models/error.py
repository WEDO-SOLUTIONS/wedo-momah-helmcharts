from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Integer, String

from signs_dashboard.pg_database import Base
from signs_dashboard.schemas.fiji.response import FijiError

STATUS_OPENED = 0
STATUS_RESOLVED = 2

RESOLUTION_FIXED = 1
RESOLUTION_REJECTED = 2

ERROR_TYPE_MISSING_SIGN = 0
ERROR_TYPE_LOW_PROBABILITY = 1
ERROR_TYPE_NOT_MATCHED = 2
ERROR_TYPE_NEW_SIGN = 4
ERROR_TYPE_MANUAL_ACTUALIZATION = 5
ERROR_TYPE_SIDE_SIGN = 6

SECONDS_IN_DAY = 86400
MISSING_SIGN_PROCESSING_SLA = SECONDS_IN_DAY * 6
NEW_SIGN_PROCESSING_SLA = SECONDS_IN_DAY * 4


class Error(Base):
    __tablename__ = 'errors'

    id = Column(Integer, primary_key=True)
    created = Column(DateTime)
    updated = Column(DateTime)
    assignee = Column(String)
    sign_id = Column(Integer)
    sign_type = Column(String)
    track_uuid = Column(String)
    status = Column(Integer)
    type = Column(Integer, nullable=True)
    resolution = Column(Integer, nullable=True)
    deleted = Column(Boolean)

    def is_new_sign(self):
        return ERROR_TYPE_NEW_SIGN == self.type

    def is_missing_sign(self):
        return ERROR_TYPE_MISSING_SIGN == self.type

    def is_deleted(self):
        return self.deleted and not self.is_resolved()

    def is_resolved(self):
        return self.status == STATUS_RESOLVED

    def is_good_error(self):
        if self.type == ERROR_TYPE_NOT_MATCHED:
            return False
        return self.resolution == RESOLUTION_FIXED

    def is_sla_completed(self):
        if self.is_new_sign():
            return self._get_processing_seconds() < NEW_SIGN_PROCESSING_SLA
        return self._get_processing_seconds() < MISSING_SIGN_PROCESSING_SLA

    def update(self, **kwargs):
        for attr_name, attr_value in kwargs.items():
            setattr(self, attr_name, attr_value)
        return self

    def __repr__(self):
        return f'<Error {self.id}>'

    @classmethod
    def from_fiji_api(cls, error: FijiError, uuid: str):
        return cls(
            id=error.id,
            created=error.created,
            updated=error.created,
            assignee='',
            sign_id=error.sign.id,
            sign_type=error.sign.class_id,
            track_uuid=uuid,
            status=STATUS_OPENED,
            type=error.types[0],
            deleted=False,
        )

    def _get_processing_seconds(self):
        if self.is_deleted():
            return (self.updated - self.created).total_seconds()
        if self.is_resolved():
            return (self.updated - self.created).total_seconds()
        return (datetime.now() - self.created).total_seconds()

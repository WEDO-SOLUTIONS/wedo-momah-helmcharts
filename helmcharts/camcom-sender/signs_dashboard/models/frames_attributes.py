from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from signs_dashboard.models.frame import Frame
from signs_dashboard.pg_database import Base


class FrameAttribute(Base):
    __tablename__ = 'frames_attributes'

    frame_id = Column(Integer, ForeignKey(Frame.id), primary_key=True)
    date = Column(DateTime, ForeignKey(Frame.date))
    detector_name = Column(String, primary_key=True)

    attributes = Column(JSONB)

    frame = relationship('Frame', foreign_keys='FrameAttribute.frame_id', lazy='noload')

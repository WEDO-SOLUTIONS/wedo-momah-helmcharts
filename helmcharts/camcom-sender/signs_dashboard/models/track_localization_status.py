from sqlalchemy import Column, DateTime, Integer, String

from signs_dashboard.pg_database import Base


class TrackLocalizationStatus(Base):
    __tablename__ = 'localization_status'

    uuid = Column(String, nullable=False, primary_key=True)
    detector_name = Column(String, nullable=False, primary_key=True)
    status = Column(Integer, nullable=False)
    last_done = Column(DateTime)
    updated = Column(DateTime)

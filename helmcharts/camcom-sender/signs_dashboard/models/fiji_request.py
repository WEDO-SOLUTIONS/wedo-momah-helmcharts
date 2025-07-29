from sqlalchemy import Column, DateTime, ForeignKey, Integer, String

from signs_dashboard.pg_database import Base


class FijiRequest(Base):
    __tablename__ = 'fiji_requests'

    id = Column(Integer, primary_key=True)
    track_uuid = Column(String, ForeignKey('tracks.uuid'), nullable=False)
    last_response = Column(String, nullable=True)
    last_response_status = Column(Integer, nullable=True)
    last_fiji_status = Column(Integer, nullable=True)
    last_request_time = Column(DateTime, nullable=False)
    retries = Column(Integer, nullable=False, default=1)

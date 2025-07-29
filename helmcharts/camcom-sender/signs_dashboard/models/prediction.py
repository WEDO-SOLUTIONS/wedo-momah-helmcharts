from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import JSONB

from signs_dashboard.pg_database import Base


class Prediction(Base):
    __tablename__ = 'prediction'

    frame_id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=False, primary_key=True)
    detector_name = Column(String, primary_key=True)
    error = Column(JSONB, nullable=True)
    raw_data = Column(JSONB, nullable=True)
    created = Column(DateTime, nullable=True)
    updated = Column(DateTime, nullable=True)

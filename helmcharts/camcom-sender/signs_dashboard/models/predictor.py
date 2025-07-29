from sqlalchemy import Column, DateTime, String, func
from sqlalchemy.dialects.postgresql import JSONB

from signs_dashboard.pg_database import Base


class Predictor(Base):
    __tablename__ = 'predictor'

    name = Column(String, primary_key=True)
    last_register_time = Column(DateTime, server_default=func.now())
    labels = Column(JSONB)

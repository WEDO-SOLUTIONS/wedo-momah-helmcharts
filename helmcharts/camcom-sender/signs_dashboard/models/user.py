from sqlalchemy import Boolean, Column, DateTime, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB

from signs_dashboard.pg_database import Base


class ApiUser(Base):
    __tablename__ = 'api_users'

    id = Column(Integer, autoincrement=True, primary_key=True)
    email = Column(String, nullable=False)
    created = Column(DateTime, nullable=False, server_default=func.now())
    enabled = Column(Boolean, nullable=False, default=True)
    oidc_meta = Column(JSONB, nullable=True)

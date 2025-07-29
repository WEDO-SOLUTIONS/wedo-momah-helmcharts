from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from signs_dashboard.pg_database import Base


class Translation(Base):
    __tablename__ = 'translations'

    id = Column(Integer, primary_key=True)
    locale_id = Column(Integer, ForeignKey('pro_locales.id'), nullable=False)
    field = Column(String, nullable=False)
    key = Column(String, nullable=True)
    value = Column(String, nullable=False)

    locale = relationship(
        'Locale',
        lazy='joined',
        backref='translations',
        primaryjoin='Translation.locale_id == Locale.id',
    )

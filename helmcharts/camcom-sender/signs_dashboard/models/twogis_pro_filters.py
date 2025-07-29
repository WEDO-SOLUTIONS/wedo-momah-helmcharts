from enum import Enum, auto

from sqlalchemy import Boolean, Column, Enum as SQLAlchemyEnum, ForeignKey, Integer, String, Table
from sqlalchemy.orm import relationship

from signs_dashboard.pg_database import Base


class Locale(Base):
    __tablename__ = 'pro_locales'

    id = Column(Integer, primary_key=True)
    locale = Column(String)
    required = Column(Boolean)
    default = Column(Boolean)


class DetectionClass(Base):
    __tablename__ = 'pro_detection_classes'

    id = Column(Integer, primary_key=True)
    code = Column(String)
    predictor = Column(String)


class ProFiltersType(Enum):
    show_in_card = auto()
    hide_in_card = auto()


class Filter(Base):
    __tablename__ = 'pro_filters'

    id = Column(Integer, primary_key=True)
    index_field = Column(String)
    filter_type: ProFiltersType = Column(
        SQLAlchemyEnum(
            ProFiltersType,
            native_enum=False,
            create_constraint=False,
        ),
        nullable=False,
    )

    options: list['FilterOption'] = relationship(
        'FilterOption',
        foreign_keys='FilterOption.filter_id',
        lazy='joined',
    )


filter_option_table = Table(
    'pro_filters_options_classes',
    Base.metadata,
    Column('filter_option_id', ForeignKey('pro_filters_options.id'), primary_key=True),
    Column('class_id', ForeignKey('pro_detection_classes.id'), primary_key=True),
)


class FilterOption(Base):
    __tablename__ = 'pro_filters_options'

    id = Column(Integer, primary_key=True)
    code = Column(String)
    filter_id = Column(Integer, ForeignKey('pro_filters.id'))

    detection_classes = relationship(
        'DetectionClass',
        secondary=filter_option_table,
        lazy='joined',
    )

    @property
    def detection_classes_labels(self) -> list[str]:
        return [
            detection_class.code
            for detection_class in self.detection_classes
        ]

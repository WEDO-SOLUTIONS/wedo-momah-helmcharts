import typing as tp
from datetime import timezone
from functools import cached_property

from sqlalchemy import ARRAY, Boolean, Column, DateTime, Float, ForeignKey, Integer, SmallInteger, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from signs_dashboard.models.frame import Frame
from signs_dashboard.pg_database import Base

if tp.TYPE_CHECKING:
    from signs_dashboard.small_utils import PointsList

DetectedObjectTypeFields = tuple[str, str, tp.Optional[float], bool, tp.Optional[tuple]]


class BBOXDetection(Base):
    __tablename__ = 'bbox_detections'

    id = Column(Integer, primary_key=True)
    frame_id = Column(Integer, ForeignKey(Frame.id))
    base_bbox_detection_id = Column(Integer, ForeignKey('bbox_detections.id'), nullable=True)
    detected_object_id = Column(Integer, ForeignKey('detected_objects.id'), nullable=True)
    date = Column(DateTime, ForeignKey(Frame.date))
    label = Column(String)
    x_from = Column(Integer)
    y_from = Column(Integer)
    width = Column(Integer)
    height = Column(Integer)
    prob = Column(Float)
    lat = Column(Float, nullable=True)
    lon = Column(Float, nullable=True)
    is_side = Column(Boolean)
    is_side_prob = Column(Float)
    directions = Column(JSONB, nullable=True)
    directions_prob = Column(Float, nullable=True)
    is_tmp = Column(Boolean)
    sign_value = Column(Float, nullable=True)
    status = Column(SmallInteger, nullable=False, default=0)
    detector_name = Column(String, nullable=False)
    attributes = Column(JSONB, nullable=True)
    polygon = Column(ARRAY(SmallInteger), nullable=True)

    frame = relationship(
        'Frame',
        foreign_keys='BBOXDetection.frame_id',
        lazy='joined',
        primaryjoin='and_(Frame.id==BBOXDetection.frame_id, Frame.date==BBOXDetection.date)',
    )
    base_bbox = relationship('BBOXDetection', remote_side=id, backref='plates')
    detected_object = relationship(
        'DetectedObject',
        foreign_keys='BBOXDetection.detected_object_id',
        back_populates='detections',
    )

    _polygon_cv2 = None

    @cached_property
    def fast_id(self):
        """
        обычный .id у алхимии работает долго, поэтому там, где надо много раз у одного и того же объекта обращаться
        к id - кэшируем
        """
        return self.id

    @property
    def polygon_cv2(self) -> tp.Optional[list['PointsList']]:
        return self._polygon_cv2

    @polygon_cv2.setter
    def polygon_cv2(self, polygon: list['PointsList']):
        self._polygon_cv2 = polygon  # noqa: WPS601

    @property
    def x_to(self) -> int:
        return self.x_from + self.width

    @property
    def y_to(self) -> int:
        return self.y_from + self.height

    @property
    def is_ai(self) -> tp.Optional[bool]:
        if self.attributes:
            value = self.attributes.get('is_ai')
            if value is not None:
                return bool(value)
        return None

    @property
    def directions_as_tuple(self):
        if not self.directions:
            return self.directions
        return tuple(tuple(elem) for elem in self.directions)

    @property
    def detected_object_fields(self):
        return self.detector_name, self.label, self.sign_value, self.is_tmp, self.directions_as_tuple

    @property
    def label_with_value(self) -> str:
        if self.sign_value:
            return f'{self.label}_n{self.sign_value:g}'
        return self.label

    @property
    def timestamp(self):
        return int(self.date.replace(tzinfo=timezone.utc).timestamp() * 1000)

    def get_info_as_str(self, sep: str = '\n') -> str:
        sign_info = [
            f'{self.label}: {self.prob:.2f}',
            f'is_tmp: {self.is_tmp}',
            f'is_side: {self.is_side}, is_side_prob: {self.is_side_prob}',
        ]
        sign_info.extend(self._get_auxiliary_info())
        return f'{sep}'.join(sign_info)

    def as_prediction_dict(self) -> dict:
        return {
            'label': self.label,
            'prob': self.prob,
            'mask': [],  # todo: store mask in Signs table (not used now)
            'is_side': self.is_side,
            'is_side_prob': self.is_side_prob,
            'directions': self.directions,
            'directions_prob': self.directions_prob,
            'is_tmp': self.is_tmp,
            'value': self.sign_value,
        }

    def as_json(self) -> dict:
        return {
            'id': self.id,
            'frame_id': self.frame_id,
            'point': (self.lat, self.lon),
            'date': self.date.isoformat(),
            'params': self.as_prediction_dict(),
            'detector': self.detector_name,
            'detected_object_id': self.detected_object_id,
        }

    def as_bbox_with_attributes(self) -> dict[str, int]:
        return {
            'xmin': self.x_from,
            'ymin': self.y_from,
            'xmax': self.x_from + self.width,
            'ymax': self.y_from + self.height,
            'attributes': self.attributes,
        }

    def location_not_none(self) -> bool:
        return self.lon is not None and self.lat is not None

    def _get_auxiliary_info(self) -> tp.List[str]:
        auxiliary_info = []
        if self.sign_value:
            auxiliary_info.append(f'value: {self.sign_value}')
        if self.directions:
            auxiliary_info.append(f'directions: {self.directions}')
            auxiliary_info.append(f'directions_prob: {self.directions_prob}')
        return auxiliary_info

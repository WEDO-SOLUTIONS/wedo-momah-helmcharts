import logging
from datetime import timezone
from functools import cached_property
from typing import TYPE_CHECKING, Optional

import h3
import numpy as np
import utm
from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from signs_dashboard.pg_database import Base

if TYPE_CHECKING:
    from signs_dashboard.models.bbox_detection import BBOXDetection

H3_RESOLUTION = 11
logger = logging.getLogger(__name__)

COMMON_DETECTION_FIELDS = (
    'detector_name',
    'label',
    'sign_value',
    'is_tmp',
    'directions',
)
DETECTION_KEY_TYPE = tuple[str, Optional[str], Optional[float], bool, Optional[dict]]


class DetectedObject(Base):
    __tablename__ = 'detected_objects'

    id = Column(Integer, primary_key=True)
    lat = Column(Float)
    lon = Column(Float)
    detector_name = Column(String, nullable=False)
    updated = Column(DateTime)
    status: str = Column(String)

    label = Column(String)
    is_tmp = Column(Boolean)
    sign_value = Column(Float, nullable=True)
    directions = Column(JSONB, nullable=True)

    detections: list['BBOXDetection'] = relationship(
        'BBOXDetection',
        uselist=True,
        foreign_keys='BBOXDetection.detected_object_id',
    )

    @cached_property
    def fast_id(self):
        """
        обычный .id у алхимии работает долго, поэтому там, где надо много раз у одного и того же объекта обращаться
        к id - кэшируем
        """
        return self.id

    @property
    def h3(self):
        return h3.latlng_to_cell(self.lat, self.lon, res=H3_RESOLUTION)

    @classmethod
    def create_from_detections(
        cls,
        detections: list['BBOXDetection'],
        cluster_ids: list[int],
        from_latest_detection: bool,
    ) -> list['DetectedObject']:
        if len(detections) != len(cluster_ids):
            logger.info(f'N detections: {len(detections)}, N cluster_ids {len(cluster_ids)}')
            raise ValueError('Cluster labels count not consistent with detections count!')

        if not detections:
            return []

        new_objects = []
        cluster2dets = {cluster_id: [] for cluster_id in set(cluster_ids)}
        for detection, cluster_id in zip(detections, cluster_ids):
            cluster2dets[cluster_id].append(detection)

        first_detection = detections[0]
        for cluster in cluster2dets.values():
            created = cls(
                detections=cluster,
                **{
                    field_name: getattr(first_detection, field_name)
                    for field_name in COMMON_DETECTION_FIELDS
                },
            )
            created.calculate_latlon_from_detections(from_latest_detection)
            new_objects.append(created)
        return new_objects

    @property
    def mean_latlon(self) -> tuple[float, float]:
        detections_latlon = [[detection.lat, detection.lon] for detection in self.detections]
        if not all(det.location_not_none() for det in self.detections):
            logging.warning(
                f'Not all detections have location for object {self.id} Please, clean database manually.',
            )
            detections_latlon = [det for det in detections_latlon if det[0] is not None and det[1] is not None]

        if not detections_latlon:
            return self.lat, self.lon

        detections_latlon = np.array(
            [
                [detection.lat, detection.lon]
                for detection in self.detections
            ],
            dtype=np.float64,
            ndmin=2,
        )
        return detections_latlon.mean(axis=0).tolist()

    @property
    def latest_latlon(self) -> tuple[float, float]:
        sorted_detections = sorted(
            [det for det in self.detections if det.location_not_none()],
            key=lambda det: det.timestamp,
        )
        try:
            return sorted_detections[-1].lat, sorted_detections[-1].lon
        except IndexError:
            return self.lat, self.lon

    @property
    def mean_utm_latlon(self) -> tuple[float, float]:
        mean_lat, mean_lon = self.mean_latlon
        return utm.from_latlon(mean_lat, mean_lon)[:2]

    @property
    def latest_utm_latlon(self) -> tuple[float, float]:
        lat, lon = self.latest_latlon
        return utm.from_latlon(lat, lon)[:2]

    @property
    def updated_timestamp(self) -> int:
        return int(self.updated.replace(tzinfo=timezone.utc).timestamp() * 1000)

    def calculate_latlon_from_detections(self, from_latest_detection: bool) -> None:
        if from_latest_detection:
            self.lat, self.lon = self.latest_latlon  # noqa: WPS414, WPS601
        else:
            self.lat, self.lon = self.mean_latlon  # noqa: WPS414, WPS601

    def is_near(
        self,
        other: 'DetectedObject',
        threshold_meters: float,
        from_latest_detection: bool,
    ) -> bool:
        # TODO: in original the is self coordinates compared to itself
        if from_latest_detection:
            obj1_utm = self.latest_utm_latlon
            obj2_utm = other.latest_utm_latlon
        else:
            obj1_utm = self.mean_utm_latlon
            obj2_utm = other.mean_utm_latlon
        # TODO: check if objects not in 1 zone
        dist = ((obj1_utm[0] - obj2_utm[0]) ** 2 + (obj1_utm[1] - obj2_utm[1]) ** 2) ** 0.5
        return dist < threshold_meters

    def merge(self, another_obj: 'DetectedObject', from_latest_detection: bool) -> None:
        existing_det_ids = [detection.id for detection in self.detections]
        for detection in another_obj.detections:
            if detection.id not in existing_det_ids:
                self.detections.append(detection)
        self.calculate_latlon_from_detections(from_latest_detection)

    def as_json(self) -> dict:
        return {
            'id': self.id,
            'point': (self.lat, self.lon),
            'detector_name': self.detector_name,
            'label': self.label,
            'is_tmp': self.is_tmp,
            'sign_value': self.sign_value,
            'directions': self.directions,
            'updated': self.updated,
            'status': self.status,
            'detections': [
                detection.as_json()
                for detection in self.detections
            ],
        }

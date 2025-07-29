from typing import Optional

from sqlalchemy import Column, DateTime, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import column_property, deferred, relationship

from signs_dashboard.pg_database import Base

STATUS_NOT_UPLOADED = 0
STATUS_UPLOADED = 1
STATUS_PROCESSING_FAILED = 2

META_BODIES = 'meta_bodies'


class TrackUploadStatus(Base):
    __tablename__ = 'upload_status'

    id = Column(Integer, primary_key=True)
    uuid = Column(String)

    init_time = Column(DateTime, nullable=True)
    recorded_time = Column(DateTime, nullable=True)
    gps_time = Column(DateTime, nullable=True)
    complete_time = Column(DateTime, nullable=True)

    status = Column(Integer, nullable=True, default=STATUS_NOT_UPLOADED)

    gps_points = deferred(Column(JSONB, default=[]), group=META_BODIES)
    matched_gps_points = deferred(Column(JSONB, default=None), group=META_BODIES)
    init_metadata = deferred(Column(JSONB, default=[]), group=META_BODIES)

    track = relationship(
        'Track',
        back_populates='upload',
        foreign_keys=[uuid],
        primaryjoin='Track.uuid == TrackUploadStatus.uuid',
        lazy='noload',
    )

    current_gps_points = column_property(
        func.coalesce(matched_gps_points, gps_points),
        deferred=True,
        group=META_BODIES,
    )

    def update(self, **kwargs):
        for attr_name, attr_value in kwargs.items():
            setattr(self, attr_name, attr_value)
        return self

    def is_reloaded(self):
        return self.init_metadata and 'reload' in self.init_metadata['app_version']

    @property
    def text_status(self):
        if self.status == STATUS_UPLOADED:
            text = 'Загружен'
        elif self.status == STATUS_PROCESSING_FAILED:
            text = 'Ошибка обработки'
        else:
            text = 'Не полностью'
        return text

    @property
    def expected_frames_count(self) -> Optional[int]:
        if self.init_metadata:
            return self.init_metadata.get('frames_count')
        return None

    @property
    def video_recording_start_timestamp_ms(self) -> Optional[float]:
        if self.init_metadata:
            if video_recording_start_timestamp_ms := self.init_metadata.get('video_recording_start_timestamp_ms'):
                return video_recording_start_timestamp_ms
        return None

    def is_ready_to_send(self):
        return self.init_time and self.gps_time and self.complete_time

    def to_fiji_metadata(self) -> dict:
        return {
            'distance_in_meters': self.init_metadata['distance_in_meters'],
            'duration_in_millis': self.init_metadata['duration_in_millis'],
            'frames_count': self.init_metadata['frames_count'],
            'app_version': self.init_metadata['app_version'],
            'video_path': self.init_metadata.get('video_path'),
        }

    def __repr__(self):
        return f'<TrackUploadStatus {self.uuid}>'

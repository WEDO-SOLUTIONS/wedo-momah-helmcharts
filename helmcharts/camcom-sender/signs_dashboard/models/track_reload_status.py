from sqlalchemy import Column, DateTime, Integer, String

from signs_dashboard.pg_database import Base


class TrackReloadStatus(Base):
    __tablename__ = 'reload_status'
    id = Column(Integer, primary_key=True)
    task_hash = Column(String)
    uuid = Column(String)
    uuid_after_upload = Column(String)
    status = Column(String)
    created = Column(DateTime)
    n_frames = Column(Integer, nullable=True)
    n_complete_frames = Column(Integer, nullable=True)
    message = Column(String, nullable=True)

    def as_status_response(self):
        return {
            'track_id': self.uuid,
            'total_frames': self.n_frames if self.n_frames else 0,
            'uploaded_frames': self.n_complete_frames if self.n_complete_frames else 0,
            'message': self.message if self.message else '',
        }

    @property
    def text_status(self):
        return {
            'error': 'ошибка',
            'in_progress': 'перезаливается',
            'complete': 'перезалит',
            'stop': 'остановлен',
            'pending': 'ждёт перезаливки',
        }[self.status]

    @property
    def text_n_frames(self):
        return self.n_frames if self.n_frames is not None else 0

    @property
    def text_n_complete_frames(self):
        return self.n_complete_frames if self.n_complete_frames is not None else 0

    def __repr__(self):
        return f'{self.uuid}  {self.created}   {self.status}'

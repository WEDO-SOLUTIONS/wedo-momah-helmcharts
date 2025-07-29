from sqlalchemy import Column, DateTime, Integer, String, func

from signs_dashboard.pg_database import Base


class CVATUploadStatus:
    PRENDING = 'pending'
    PROCESSING = 'processing'
    ERROR = 'error'
    COMPLETED = 'completed'


class CVATUploadTask(Base):
    __tablename__ = 'cvat_upload_task'
    id = Column(Integer, primary_key=True)
    created = Column(DateTime, server_default=func.now())

    upload_uuid = Column(String)
    project_id = Column(Integer)
    name = Column(String)
    cvat_task_id = Column(Integer)
    status = Column(String)

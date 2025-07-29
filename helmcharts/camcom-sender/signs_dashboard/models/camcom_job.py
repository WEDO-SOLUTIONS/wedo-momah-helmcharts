from enum import IntEnum
from types import MappingProxyType

from sqlalchemy import Column, DateTime, Integer, String

from signs_dashboard.pg_database import Base


class CamcomJobStatus(IntEnum):
    CREATED = 0
    SENT = 2
    EXPIRED = 4
    NOT_PROCESSED = 8
    CAMCOM_ERROR = 256
    CAMCOM_COMPLETE = 512
    WILL_BE_SENT = 1024


STATUS_CODE_TO_STATUS_TEXT = MappingProxyType({
    0: 'Подготовлено к отправке в CamCom',
    2: 'Ожидаем ответ CamCom',
    4: 'Ошибка',
    8: 'Ошибка',
    256: 'Ошибка',
    512: 'Успешно получен ответ CamCom',
    1024: 'Будет отправлено в CamCom',
})
CAMCOM_JOB_BAD_STATUSES = (4, 8, 256)
CAMCOM_JOB_SENT_STATUSES = (2, 512)


class CamcomJob(Base):
    __tablename__ = 'camcom_job'

    job_id = Column(String)
    frame_id = Column(Integer, primary_key=True)
    sent_date = Column(DateTime)
    status = Column(Integer)
    response_status = Column(Integer)


class CamcomJobLog(Base):
    __tablename__ = 'camcom_job_log'

    job_id = Column(String, primary_key=True)
    sent_date = Column(DateTime)
    response_status = Column(Integer)
    response_text = Column(String)

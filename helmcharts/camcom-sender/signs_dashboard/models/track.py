from datetime import timedelta
from enum import Enum
from types import MappingProxyType
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, func
from sqlalchemy.dialects.postgresql import ARRAY, INTERVAL, JSONB
from sqlalchemy.orm import relationship

from signs_dashboard.models.fiji_request import FijiRequest
from signs_dashboard.models.track_upload_status import TrackUploadStatus
from signs_dashboard.models.user import ApiUser
from signs_dashboard.pg_database import Base
from signs_dashboard.small_utils import timezone_offset_str

if TYPE_CHECKING:
    from signs_dashboard.models.clarification import Clarification
    from signs_dashboard.models.error import Error
    from signs_dashboard.models.track_localization_status import TrackLocalizationStatus

# Статусы Фиджи
# см. newmap/fiji -> SignDetectionModel/CasinoRecognition/ProcessingStatus.cs
# 1 Успешно Uploaded
# 2 Шаг Init Init,
# 18 Шаг корректировки азимутов
# 3 Шаг MatchTrack MatchTrack,
# 4 Шаг ProcessSigns ProcessSigns,
# 5 Шаг Simplify Simplify,
# 6 Шаг Master Master
# 7 Шаг SaveResult SaveResult,
# 8 Успешно завершено Completed,
# 9 Точек GPS должно быть больше либо равно двух NotEnoughGpsPoints,
# 10 Метаданные не заполнены MetadataMissed
# 11 Нет распознанных знаков RecognizedSingsMissed
# 12 Ошибка при парсинге ParsingFailed
# 13 Ошибка валидации ValidationError
# 14 Ошибка при обработке видео Error
# 15 В запросе нет фреймов NoFrames
# 16 В запросе нет распознанных знаков NoRecognizedSigns
# 17 Ошибка матчинга
# 19 Плохое качество кадров.
# 20 Видео не найдено
# 21 Сервис не доступен
# 1001 Финальный статус, трек не обработан
# 1002 Не финальный статус, трек отправлен в Fiji

# Статусы dashcam
# 1 - Ждет обработки,
# 2 - Обрабатывается,
# 3 - Нет трека,
# 4 - Нет распознанных знаков,
# 5 - Есть распознанные знаки,
# 6 - Ошибка,
# 7 - Отменено,
# 8 - Ошибка матчинга

STATUS_CODE_TO_STATUS_TEXT_FIJI = MappingProxyType({
    0: 'Загружется с телефона',
    1: 'Ожидает обработки',
    2: 'Обрабатывается',
    3: 'Обрабатывается',
    4: 'Обрабатывается',
    5: 'Обрабатывается',
    6: 'Обрабатывается',
    7: 'Обрабатывается',
    8: 'Обработан',
    9: 'Некорректный трек',
    10: 'Некорректный трек',
    11: 'Нет распознанных знаков',
    12: 'Ошибка',
    13: 'Ошибка',
    14: 'Ошибка',
    15: 'В запросе нет фреймов',
    16: 'Нет распознанных знаков',
    17: 'Ошибка притяжки трека',
    18: 'Обрабатывается',
    19: 'Плохое качество',
    20: 'Отменено',
    21: 'Fiji не доступен',
    1001: 'Трек не загружен полностью',
    1002: 'Трек отправлен в Fiji',
    1004: 'Плохое качество',
    1005: 'Трек будет отправлен в Fiji',
    1009: 'Некорректный трек',
    1020: 'Трек отправляется в Fiji',
    1021: 'Fiji не доступен',
    1022: 'Неподдерживаемый тип трека',
})

STATUS_CODE_TO_STATUS_TEXT_PRO = MappingProxyType({
    0: 'Загружется с телефона',
    1001: 'Трек не загружен полностью',
    1003: 'Трек отправлен в PRO',
    1005: 'Трек будет отправлен в PRO',
    1006: 'Трек будет скрыт из PRO',
    1007: 'Трек скрыт из PRO',
    1012: 'Неподдерживаемый тип трека',
    1013: 'Трек без предиктов отправлен в PRO',
})


class TrackStatuses:
    FIJI_VALIDATION_ERROR = 13
    FIJI_VIDEO_PROCESSING_ERROR = 14
    FIJI_LOW_QUALITY = 19
    FIJI_SERVICE_NOT_AVAILABLE = 21
    FIJI_INVALID_TRACK = 1009
    FIJI_SENDING_IN_PROCESS = 1020
    FIJI_NOT_AVAILABLE = 1021
    FIJI_UNSUPPORTED_TRACK_TYPE = 1022
    FIJI_RETRYABLE = (
        FIJI_VALIDATION_ERROR, FIJI_VIDEO_PROCESSING_ERROR, FIJI_SERVICE_NOT_AVAILABLE, FIJI_NOT_AVAILABLE,
    )
    GOOD = (1, 2, 3, 4, 5, 6, 7, 8, 18)
    REJECTED = (FIJI_INVALID_TRACK, 9, 10, 11, 15, 16, FIJI_LOW_QUALITY, 1001, FIJI_UNSUPPORTED_TRACK_TYPE)
    FAILED = (
        12, 17, 20, FIJI_VALIDATION_ERROR, FIJI_VIDEO_PROCESSING_ERROR, FIJI_SERVICE_NOT_AVAILABLE, FIJI_NOT_AVAILABLE,
    )
    UPLOADING = 0
    NOT_COMPLETE = 1001
    SENT_FIJI = 1002
    SENT_PRO = 1003
    LOW_QUALITY = 1004
    FORCED_SEND = 1005
    WILL_BE_HIDDEN_PRO = 1006
    HIDDEN_PRO = 1007
    PRO_UNSUPPORTED_TRACK_TYPE = 1012
    SENT_PRO_WITHOUT_PREDICTIONS = 1013
    LOCALIZATION_PENDING = 2001
    LOCALIZATION_IN_PROGRESS = 2002
    LOCALIZATION_DONE = 2003
    LOCALIZATION_ERROR = 2004
    LOCALIZATION_FORCED = 2005
    LOCALIZATION_DISABLED = 2006
    LOCALIZATION_SCHEDULED = 2007
    LOCALIZATION_UNSUPPORTED_TRACK_TYPE = 2022
    MAP_MATCHING_PENDING = 3001
    MAP_MATCHING_IN_PROGRESS = 3002
    MAP_MATCHING_DONE = 3003
    MAP_MATCHING_ERROR = 3004
    MAP_MATCHING_DISABLED = 3006

    VISUAL_LOCALIZATION_IN_PROGRESS = 4002
    VISUAL_LOCALIZATION_DONE = 4003

    @classmethod
    def from_change_pro_status_request(cls, status: int):
        if status not in {cls.FORCED_SEND, cls.WILL_BE_HIDDEN_PRO}:
            raise ValueError(f'Invalid status: {status}')
        return status


STATUS_CODE_TO_STATUS_TEXT_LOCALIZATION = MappingProxyType({
    TrackStatuses.LOCALIZATION_PENDING: 'Ожидается',
    TrackStatuses.LOCALIZATION_SCHEDULED: 'Запланирована',
    TrackStatuses.LOCALIZATION_IN_PROGRESS: 'Выполняется',
    TrackStatuses.LOCALIZATION_DONE: 'Выполнена',
    TrackStatuses.LOCALIZATION_ERROR: 'Ошибка',
    TrackStatuses.LOCALIZATION_FORCED: 'Ожидается',
    TrackStatuses.LOCALIZATION_DISABLED: 'Не выполнялась',
    TrackStatuses.LOCALIZATION_UNSUPPORTED_TRACK_TYPE: 'Неподдерживаемый тип трека',
    TrackStatuses.FORCED_SEND: 'Результаты будут пересинхронизированы с PRO',
})


STATUS_CODE_TO_STATUS_TEXT_MAP_MATCHING = MappingProxyType({
    TrackStatuses.MAP_MATCHING_PENDING: 'Ожидается',
    TrackStatuses.MAP_MATCHING_IN_PROGRESS: 'Выполняется',
    TrackStatuses.MAP_MATCHING_DONE: 'Выполнена',
    TrackStatuses.MAP_MATCHING_ERROR: 'Ошибка',
    TrackStatuses.MAP_MATCHING_DISABLED: 'Не выполнялась',
    TrackStatuses.VISUAL_LOCALIZATION_IN_PROGRESS: 'Выполняется визуальная притяжка',
    TrackStatuses.VISUAL_LOCALIZATION_DONE: 'Выполнена визуальная притяжка',
})


class TrackType(str, Enum):  # noqa: WPS600
    mobile = 'mobile'
    dashcam = 'dashcam'
    video360 = 'video_360'


class Track(Base):
    __tablename__ = 'tracks'

    id = Column(Integer, primary_key=True)
    uuid = Column(String)

    recorded = Column(DateTime)
    uploaded = Column(DateTime)
    timezone_offset: timedelta = Column(INTERVAL(fields='HOUR TO MINUTE'), nullable=False, default='00:00')

    distance = Column(Float)
    duration = Column(Float)
    user_email = Column(String)
    app_version = Column(String)
    fiji_status = Column(Integer)
    pro_status = Column(Integer)
    localization_status = Column(Integer, default=TrackStatuses.LOCALIZATION_PENDING)
    map_matching_status = Column(Integer, default=TrackStatuses.MAP_MATCHING_PENDING)
    type = Column(String)
    reloaded = Column(Boolean, default=False)
    projects = Column(ARRAY(Integer), default=[])
    only_in_projects = Column(Boolean, default=False)

    num_important_signs = Column(Integer, nullable=True)
    num_audited_signs = Column(Integer, nullable=True)
    num_truck_signs = Column(Integer, nullable=True)

    upload_status = Column(Integer, nullable=True, default=0)

    filter_label_to_count = Column(JSONB, nullable=True, default={})
    num_detections = Column(Integer, nullable=True, default=0)

    errors: list['Error'] = relationship(
        'Error',
        foreign_keys=[uuid],
        primaryjoin='Track.uuid == Error.track_uuid',
        viewonly=True,
        uselist=True,
        lazy='joined',
    )
    clarifications: list['Clarification'] = relationship(
        'Clarification',
        foreign_keys=[uuid],
        primaryjoin='Track.uuid == Clarification.track_uuid',
        viewonly=True,
        uselist=True,
        lazy='joined',
    )

    upload: TrackUploadStatus = relationship(
        'TrackUploadStatus',
        foreign_keys=[uuid],
        primaryjoin='Track.uuid == TrackUploadStatus.uuid',
        lazy='joined',
    )

    localizations: list['TrackLocalizationStatus'] = relationship(
        'TrackLocalizationStatus',
        foreign_keys=[uuid],
        primaryjoin='Track.uuid == TrackLocalizationStatus.uuid',
        lazy='noload',
        uselist=True,
        viewonly=True,
    )

    fiji_request: FijiRequest = relationship(
        'FijiRequest',
        foreign_keys=[uuid],
        primaryjoin='Track.uuid == FijiRequest.track_uuid',
        lazy='noload',
        viewonly=True,
    )

    api_user: ApiUser = relationship(
        'ApiUser',
        foreign_keys=[user_email],
        primaryjoin=func.lower(user_email) == func.lower(ApiUser.email),
        lazy='noload',
        viewonly=True,
    )

    @property
    def comment(self) -> Optional[str]:
        if not self.upload or not self.upload.init_metadata:
            return None
        return self.upload.init_metadata.get('comment')

    @property
    def frames_count(self) -> Optional[int]:
        if not self.upload or not self.upload.init_metadata:
            return None
        return self.upload.init_metadata.get('frames_count')

    @property
    def timezone_offset_str(self) -> str:
        return timezone_offset_str(self.timezone_offset)

    @property
    def recorded_not_utc(self):
        return bool(self.timezone_offset.total_seconds())

    @property
    def fiji_text_status(self):
        return STATUS_CODE_TO_STATUS_TEXT_FIJI[self.fiji_status]

    @property
    def pro_text_status(self):
        return STATUS_CODE_TO_STATUS_TEXT_PRO[self.pro_status]

    @property
    def localization_text_status(self):
        return STATUS_CODE_TO_STATUS_TEXT_LOCALIZATION[self.localization_status]

    @property
    def map_matching_text_status(self):
        return STATUS_CODE_TO_STATUS_TEXT_MAP_MATCHING[self.map_matching_status]

    def update(self, **kwargs):
        for attr_name, attr_value in kwargs.items():
            setattr(self, attr_name, attr_value)
        return self

    def is_fiji_good_status(self):
        return self.fiji_status in TrackStatuses.GOOD

    def is_fiji_rejected_status(self):
        return self.fiji_status in TrackStatuses.REJECTED

    def is_fiji_failed_status(self):
        return self.fiji_status in TrackStatuses.FAILED

    def sending_to_fiji_can_be_forced(self):
        return any([
            self.fiji_status == TrackStatuses.LOW_QUALITY,
            self.fiji_status == TrackStatuses.FIJI_LOW_QUALITY,
            self.is_fiji_failed_status(),
        ])

    def is_pro_good_status(self):
        return self.pro_status == TrackStatuses.SENT_PRO

    def is_pro_bad_status(self):
        return self.pro_status == TrackStatuses.NOT_COMPLETE

    def is_map_matching_done(self) -> bool:
        return self.map_matching_status == TrackStatuses.MAP_MATCHING_DONE

    def is_map_matching_or_visual_localization_done(self) -> bool:
        return self.map_matching_status in {TrackStatuses.MAP_MATCHING_DONE, TrackStatuses.VISUAL_LOCALIZATION_DONE}

    def localization_can_be_forced(self):
        return self.localization_status == TrackStatuses.LOCALIZATION_ERROR

    def is_mobile(self):
        return self.type == 'mobile'

    def is_forced_fiji_send(self):
        return self.fiji_status == TrackStatuses.FORCED_SEND

    @property
    def with_lidar(self):
        return self.user_email == 'momra@urbi.ae'

    @property
    def lidar_uuid(self):
        return '_'.join(self.uuid.split('_')[:3])

    @property
    def path(self) -> str:
        return '/'.join((
            self.recorded.strftime('%Y-%m-%d'),
            self.user_email,
            self.uuid,
        ))

    @property
    def filtering_description(self):
        lines = (
            f'{label}: {label_values}'
            for label, label_values in self.filter_label_to_count.items()
        )
        return '\n'.join(lines)

    def __repr__(self):
        return f'<Track {self.uuid}>'

    @classmethod
    def init_from_data(cls, track_data, gps_points=None):
        track = Track(**track_data)
        track.upload = TrackUploadStatus(
            uuid=track.uuid,
            status=track_data.get('upload_status'),
            init_time=track_data.get('uploaded'),
            recorded_time=track_data.get('recorded'),
            gps_points=gps_points,
            init_metadata=None,
        )
        return track

from dependency_injector import containers, providers

from signs_dashboard.pg_database import Database
from signs_dashboard.repository.bbox_detections import BBOXDetectionsRepository
from signs_dashboard.repository.camcom_job import CamcomJobRepository
from signs_dashboard.repository.cvat_upload import CVATUploadTaskRepository
from signs_dashboard.repository.detected_objects import DetectedObjectsRepository
from signs_dashboard.repository.fiji_request import FijiRequestRepository
from signs_dashboard.repository.frames import FramesRepository
from signs_dashboard.repository.interest_zones import InterestZonesRepository
from signs_dashboard.repository.predictions import PredictionsRepository
from signs_dashboard.repository.predictors import PredictorsRepository
from signs_dashboard.repository.tracks import TracksRepository
from signs_dashboard.repository.tracks_localization import TracksLocalizationRepository
from signs_dashboard.repository.tracks_reload import ReloadedTracksRepository
from signs_dashboard.repository.translations import TranslationsRepository
from signs_dashboard.repository.twogis_pro_filters import TwogisProFiltersRepository
from signs_dashboard.repository.users import UsersRepository


class PgRepositories(containers.DeclarativeContainer):
    config = providers.Configuration()

    _db = providers.Singleton(
        Database,
        connection_url=config.db_connections.postgres,
        pool_size=config.db_connections.postgres_pool_size,
        pool_max_overflow=config.db_connections.postgres_pool_max_overflow,
    )

    tracks = providers.Factory(TracksRepository, session_factory=_db.provided.session)
    tracks_localization = providers.Factory(TracksLocalizationRepository, session_factory=_db.provided.session)
    frames = providers.Factory(FramesRepository, session_factory=_db.provided.session)
    predictions = providers.Factory(PredictionsRepository, session_factory=_db.provided.session)
    camcom_job = providers.Factory(CamcomJobRepository, session_factory=_db.provided.session)
    fiji_request = providers.Factory(FijiRequestRepository, session_factory=_db.provided.session)
    bbox_detections = providers.Factory(BBOXDetectionsRepository, session_factory=_db.provided.session)
    detected_objects = providers.Factory(DetectedObjectsRepository, session_factory=_db.provided.session)
    reloaded_tracks = providers.Factory(ReloadedTracksRepository, session_factory=_db.provided.session)
    users = providers.Factory(UsersRepository, session_factory=_db.provided.session)
    twogis_pro_filters = providers.Factory(TwogisProFiltersRepository, session_factory=_db.provided.session)
    interest_zones = providers.Factory(InterestZonesRepository, session_factory=_db.provided.session)
    translations = providers.Factory(TranslationsRepository, session_factory=_db.provided.session)
    cvat_upload_task_repo = providers.Factory(CVATUploadTaskRepository, session_factory=_db.provided.session)
    predictors = providers.Factory(PredictorsRepository, session_factory=_db.provided.session)

from dependency_injector import containers, providers

from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.services.camcom.camcom_sender import CamcomSenderService
from signs_dashboard.services.camcom.SA_administrative_division import SAAdministrativeDivisionNamesService
from signs_dashboard.services.camcom.statistics import CamcomStatisticsService
from signs_dashboard.services.cvat.session import CVATConfig, CVATSession
from signs_dashboard.services.cvat.uploader import CVATUploader
from signs_dashboard.services.detected_objects import DetectedObjectsService
from signs_dashboard.services.detection_clusterization import ClusterizationParams, DetectionClusterizationService
from signs_dashboard.services.detection_localization import DetectionLocalizationService
from signs_dashboard.services.dump import CropsDumpService
from signs_dashboard.services.events.detected_objects_lifecycle import DetectedObjectsLifecycleService
from signs_dashboard.services.events.frames_lifecycle import FramesLifecycleService
from signs_dashboard.services.events.tracks_lifecycle import TracksLifecycleService
from signs_dashboard.services.fiji_client import FijiClient
from signs_dashboard.services.fiji_quality import FijiQualityChecker
from signs_dashboard.services.frames import FramesService
from signs_dashboard.services.frames_depth import FramesDepthService
from signs_dashboard.services.gps_interpolation import GPSInterpolationService
from signs_dashboard.services.image import ImageService
from signs_dashboard.services.image_archiver import ImageArchiverService
from signs_dashboard.services.image_visualization import ImageVisualizationService
from signs_dashboard.services.interest_zones import InterestZonesService
from signs_dashboard.services.kafka_service import KafkaService
from signs_dashboard.services.kml_generator import KMLGeneratorService
from signs_dashboard.services.map_matching.service import LegacyMapMatchingService, MapMatchingService
from signs_dashboard.services.pano_conversions.service import PanoramicConversionsService
from signs_dashboard.services.prediction import PredictionService
from signs_dashboard.services.prediction_answer_parser import PredictorAnswerParser
from signs_dashboard.services.predictors import PredictorsService
from signs_dashboard.services.s3_client import S3ClientService
from signs_dashboard.services.s3_keys import S3KeysService
from signs_dashboard.services.s3_service import S3Service
from signs_dashboard.services.track_gps_points_handler import TrackGPSPointsHandlerService
from signs_dashboard.services.track_length import TrackLengthService
from signs_dashboard.services.track_logs import TrackLogsService
from signs_dashboard.services.tracks import TracksService
from signs_dashboard.services.tracks_download import TracksDownloaderService
from signs_dashboard.services.tracks_reload import TracksReloadService
from signs_dashboard.services.translations import TranslationsService
from signs_dashboard.services.twogis_pro.client import TwoGisProAPIClient
from signs_dashboard.services.twogis_pro.filters import TwoGisProFiltersService
from signs_dashboard.services.twogis_pro.filters_update import TwoGisProFiltersUpdateService
from signs_dashboard.services.twogis_pro.kafka.drivers import TwoGisProDriversService
from signs_dashboard.services.twogis_pro.kafka.frames import TwoGisProFramesService
from signs_dashboard.services.twogis_pro.kafka.localization import TwoGisProKafkaLocalizerService
from signs_dashboard.services.twogis_pro.kafka.objects import TwoGisProObjectsService
from signs_dashboard.services.twogis_pro.synchronization import TwoGisProSyncService
from signs_dashboard.services.users import UsersService
from signs_dashboard.services.video_frames_cutter import VideoFramesCutterService
from signs_dashboard.services.video_frames_saver import VideoFramesSaverService


class ServicesContainer(containers.DeclarativeContainer):
    config = providers.Configuration()
    modules_config = providers.Singleton(ModulesConfig, config=config)
    pg_repositories = providers.DependenciesContainer()

    s3_client = providers.Resource(
        S3ClientService,
        s3_config=config.s3,
    )
    s3_keys = providers.Resource(
        S3KeysService,
        s3_config=config.s3,
    )
    s3_service = providers.Resource(
        S3Service,
        s3_config=config.s3,
        s3_client=s3_client,
        s3_keys=s3_keys,
    )
    kafka = providers.Factory(KafkaService, config=config.kafka, modules_config=modules_config)

    translations = providers.Factory(
        TranslationsService,
        translations_repository=pg_repositories.translations,
    )

    image = providers.Factory(
        ImageService,
        s3_service=s3_service,
    )

    track_logs = providers.Factory(
        TrackLogsService,
        s3_service=s3_service,
    )
    frames = providers.Factory(
        FramesService,
        frames_repository=pg_repositories.frames,
    )
    frames_depth = providers.Factory(
        FramesDepthService,
        s3_service=s3_service,
    )
    frames_lifecycle = providers.Factory(
        FramesLifecycleService,
        kafka_service=kafka,
        image_service=image,
    )
    predictors = providers.Factory(
        PredictorsService,
        cfg=config,
        frames_lifecycle_service=frames_lifecycle,
        predictors_repository=pg_repositories.predictors,
    )

    users = providers.Factory(
        UsersService,
        users_repository=pg_repositories.users,
        translations_service=translations,
        config=config,
    )

    panoramic_conversions = providers.Factory(PanoramicConversionsService)
    prediction_answer_parser = providers.Resource(
        PredictorAnswerParser,
    )
    prediction = providers.Factory(
        PredictionService,
        modules_config=modules_config,
        predictions_repository=pg_repositories.predictions,
        bbox_detections_repository=pg_repositories.bbox_detections,
        frames_depth_service=frames_depth,
        prediction_answer_parser=prediction_answer_parser,
        panoramic_conversions_service=panoramic_conversions,
    )
    image_archiver = providers.Factory(
        ImageArchiverService,
        image_service=image,
    )
    image_visualizer = providers.Factory(
        ImageVisualizationService,
        image_service=image,
        translations_service=translations,
        panoramic_conversions_service=panoramic_conversions,
    )
    crops_dump = providers.Factory(
        CropsDumpService,
        image_service=image,
    )

    track_gps_points_handler = providers.Factory(
        TrackGPSPointsHandlerService,
    )
    tracks_lifecycle = providers.Factory(
        TracksLifecycleService,
        kafka_service=kafka,
    )
    tracks = providers.Factory(
        TracksService,
        tracks_repository=pg_repositories.tracks,
        tracks_localization_repository=pg_repositories.tracks_localization,
        fiji_request_repository=pg_repositories.fiji_request,
        users_service=users,
        tracks_lifecycle_service=tracks_lifecycle,
        track_gps_points_handler_service=track_gps_points_handler,
        modules_config=modules_config,
    )
    track_length = providers.Factory(TrackLengthService)
    kml_generator = providers.Factory(KMLGeneratorService, tracks_service=tracks)

    interest_zones = providers.Factory(
        InterestZonesService,
        interest_zones_repository=pg_repositories.interest_zones,
        prediction_service=prediction,
    )

    gps_interpolation = providers.Factory(GPSInterpolationService)
    video_frames_cutter = providers.Factory(
        VideoFramesCutterService,
        gps_interpolation_service=gps_interpolation,
    )
    video_frames_saver = providers.Factory(
        VideoFramesSaverService,
        frames_repository=pg_repositories.frames,
        tracks_repository=pg_repositories.tracks,
        video_frames_cutter_service=video_frames_cutter,
        interest_zones_service=interest_zones,
        image_service=image,
        s3_service=s3_service,
        s3_config=config.s3,
        panorama_rotation_config=config.enabled_modules.panorama_rotation,
    )

    fiji_client = providers.Factory(
        FijiClient,
        config=config.fiji_client,
    )
    fiji_quality = providers.Factory(
        FijiQualityChecker,
        config=config,
    )

    tracks_downloader = providers.Factory(
        TracksDownloaderService,
        image_service=image,
        tracks_service=tracks,
        track_length_service=track_length,
        frames_service=frames,
        modules_config=modules_config,
    )
    tracks_reloader = providers.Factory(
        TracksReloadService,
        config=config.tracks_uploader,
        tracks_service=tracks,
        frames_service=frames,
        reloaded_tracks_repository=pg_repositories.reloaded_tracks,
        kafka_service=kafka,
        modules_config=modules_config,
        fiji_client=fiji_client,
    )

    detected_objects_lifecycle = providers.Factory(
        DetectedObjectsLifecycleService,
        kafka_service=kafka,
    )
    detected_objects = providers.Factory(
        DetectedObjectsService,
        detected_objects_repository=pg_repositories.detected_objects,
        detected_objects_lifecycle_service=detected_objects_lifecycle,
    )
    is_naive_localization = providers.Callable(modules_config.provided.is_detections_localizer_naive.call())

    detections_localization = providers.Factory(
        DetectionLocalizationService,
        image_service=image,
        depth_map_service=frames_depth,
        naive_localization=is_naive_localization,
        s3_config=config.s3,
    )
    clusterization_params = config.clusterization_params
    detections_clusterization = providers.Factory(
        DetectionClusterizationService,
        detected_objects_service=detected_objects,
        s3_service=s3_service,
        clusterization_params=clusterization_params.as_(lambda cfg: ClusterizationParams(**cfg)),
        naive_localization=is_naive_localization,
    )

    saudi_arabia_administrative_divisions_names = providers.Factory(
        SAAdministrativeDivisionNamesService,
    )
    twogis_pro_kafka_localization = providers.Factory(
        TwoGisProKafkaLocalizerService,
        translations_service=translations,
    )
    twogis_pro_api_client = providers.Factory(
        TwoGisProAPIClient,
        config=config.pro.client,
    )
    twogis_pro_filters = providers.Factory(
        TwoGisProFiltersService,
        pro_filters_repository=pg_repositories.twogis_pro_filters,
        predictors_service=predictors,
        saudi_arabia_administrative_divisions_names=saudi_arabia_administrative_divisions_names,
        localization_service=twogis_pro_kafka_localization,
    )
    twogis_pro_filters_update = providers.Factory(
        TwoGisProFiltersUpdateService,
        api_client=twogis_pro_api_client,
        filters_service=twogis_pro_filters,
        assets_config=config.pro.assets,
    )
    twogis_pro_frames = providers.Factory(
        TwoGisProFramesService,
        dashboard_domain=config.dashboard_domain,
        config=config,
        predictors_service=predictors,
        filters_service=twogis_pro_filters,
        localization_service=twogis_pro_kafka_localization,
        modules_config=modules_config,
    )
    twogis_pro_objects = providers.Factory(
        TwoGisProObjectsService,
        dashboard_domain=config.dashboard_domain,
        filters_service=twogis_pro_filters,
        localization_service=twogis_pro_kafka_localization,
        detected_objects_service=detected_objects,
    )
    twogis_pro_drivers = providers.Factory(
        TwoGisProDriversService,
        localization_service=twogis_pro_kafka_localization,
    )
    twogis_pro_sync = providers.Factory(
        TwoGisProSyncService,
        frames_service=frames,
        tracks_service=tracks,
        prediction_service=prediction,
        pro_frames_service=twogis_pro_frames,
        pro_objects_service=twogis_pro_objects,
        pro_drivers_service=twogis_pro_drivers,
        detected_objects_service=detected_objects,
        modules_config=modules_config,
        kafka_service=kafka,
    )

    legacy_map_matching = providers.Factory(
        LegacyMapMatchingService,
        map_matching_config=config.enabled_modules.matching,
        tracks_service=tracks,
        track_length_service=track_length,
        gps_interpolation_service=gps_interpolation,
        frames_service=frames,
        interest_zones_service=interest_zones,
        s3_service=s3_service,
    )
    map_matching = providers.Factory(
        MapMatchingService,
        map_matching_config=config.enabled_modules.matching,
        tracks_service=tracks,
        track_length_service=track_length,
        gps_interpolation_service=gps_interpolation,
        frames_service=frames,
        interest_zones_service=interest_zones,
        s3_service=s3_service,
    )

    camcom_sender = providers.Factory(
        CamcomSenderService,
        camcom_job_repository=pg_repositories.camcom_job,
        config=config.camcom,
        frames_image_service=image,
        interest_zones_service=interest_zones,
        predictors_service=predictors,
        users_service=users,
    )
    camcom_statistics = providers.Factory(
        CamcomStatisticsService,
        camcom_job_repository=pg_repositories.camcom_job,
    )

    cvat_session = providers.Singleton(
        CVATSession,
        cfg=config.cvat.as_(lambda cfg: CVATConfig(**cfg) if cfg else CVATConfig.create_empty()),
    )

    cvat_uploader = providers.Factory(
        CVATUploader,
        cvat_session=cvat_session,
        kafka_service=kafka,
        cvat_upload_task_repo=pg_repositories.cvat_upload_task_repo,
        frames_service=frames,
        modules_config=modules_config,
        dashboard_domain=config.dashboard_domain,
    )

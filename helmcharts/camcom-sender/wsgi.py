import logging
import logging.config
import os
from datetime import datetime

import sentry_sdk
from flask import Flask
from flask.cli import AppGroup
from flask_babel import Babel
from flask_bootstrap import Bootstrap
from flask_cors import CORS
from pythonjsonlogger import jsonlogger
from sentry_sdk.integrations.flask import FlaskIntegration

from signs_dashboard import (
    camcom_sender_cli,
    context,
    cvat_uploader_cli,
    events_tools,
    lifecycle_controller_cli,
    logs_downloader_cli,
    reporter_fiji_cli,
    reporter_pro_cli,
    reporter_pro_tracks_cli,
    s3_bucket_lifecycle_cli,
    template_decorators,
    tracks_detections_localization_cli,
    tracks_downloader_cli,
    tracks_map_matching_cli,
    tracks_reload_cli,
    video_frames_saver_cli,
)
from signs_dashboard.containers.application import Application
from signs_dashboard.keycloak import authentication_middleware
from signs_dashboard.metrics import setup_metrics
from signs_dashboard.routes import (
    camcom as camcom_routes,
    cvat as cvat_routes,
    detected_objects as detected_objects_routes,
    driver as driver_routes,
    feedback as feedback_routes,
    fiji_request as fiji_request_routes,
    frames as frames_routes,
    interest_zones as interest_zones_routes,
    predictors_api as predictors_api_routes,
    reload as reload_routes,
    signs as signs_routes,
    tracks as tracks_routes,
    tracks_api as tracks_api_routes,
    twogis_pro as twogis_pro_routes,
    users as user_routes,
    wfs as wfs_routes,
)
from signs_dashboard.routes.set_routes import set_routes

sentry_sdk.init(
    dsn=os.environ.get('SENTRY_DSN'),
    environment=os.environ.get('SENTRY_ENVIRONMENT', 'unknown'),
    integrations=[FlaskIntegration()],
    traces_sample_rate=0,
)
KEYCLOAK_SETTINGS_ENV = os.environ.get('KEYCLOAK_SETTINGS')
ENABLE_METRICS_ENV = os.environ.get('ENABLE_METRICS')


class ElkJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        log_record['@timestamp'] = datetime.now().isoformat()
        log_record['level'] = record.levelname
        log_record['logger'] = record.name
        log_record.update(context.get_context_log_fields())


def _configure_logging(log_level):
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'elk': {
                'class': 'wsgi.ElkJsonFormatter',
            },
        },
        'handlers': {
            'default': {
                'level': log_level,
                'formatter': 'elk',
                'class': 'logging.StreamHandler',
            },
        },
        'loggers': {
            '': {
                'handlers': ['default'],
                'level': log_level,
                'propagate': True,
            },
        },
    })


def create_app():
    wired_modules = [
        camcom_routes,
        driver_routes,
        tracks_routes,
        tracks_api_routes,
        reload_routes,
        frames_routes,
        signs_routes,
        wfs_routes,
        feedback_routes,
        user_routes,
        interest_zones_routes,
        detected_objects_routes,
        twogis_pro_routes,
        tracks_downloader_cli,
        logs_downloader_cli,
        lifecycle_controller_cli,
        reporter_fiji_cli,
        reporter_pro_cli,
        reporter_pro_tracks_cli,
        camcom_sender_cli,
        tracks_reload_cli,
        tracks_map_matching_cli,
        tracks_detections_localization_cli,
        s3_bucket_lifecycle_cli,
        video_frames_saver_cli,
        template_decorators,
        events_tools,
        cvat_routes,
        cvat_uploader_cli,
        predictors_api_routes,
        fiji_request_routes,
    ]
    di_app_container = _init_containers(modules=wired_modules)

    _configure_logging(di_app_container.config.get('log_level'))

    app = Flask(__name__, template_folder='signs_dashboard/templates')
    # ключ для шифрования кук
    app.config['SECRET_KEY'] = '3d6f45a5fc12445dbac2f59c3b6c7cb1'  # noqa: S105
    app.config['BOOTSTRAP_SERVE_LOCAL'] = True
    Bootstrap().init_app(app)

    if KEYCLOAK_SETTINGS_ENV:
        app.wsgi_app = authentication_middleware.AuthenticationMiddleware(
            app.wsgi_app,
            app.config,
            app.session_interface,
            domain=di_app_container.config.get('dashboard_domain'),
        )

    build_cli(app)
    set_routes(app)
    if ENABLE_METRICS_ENV:
        setup_metrics(
            app,
            app_name=di_app_container.config.get('metrics_app_name'),
        )
    CORS(app)

    Babel(
        app,
        default_locale=di_app_container.config.get('default_locale', 'ru'),
        default_translation_directories=os.path.join(
            os.path.dirname(__file__),
            'signs_dashboard/translations',
        ),
    )

    app.template_filter('prettify_hours')(template_decorators.prettify_hours)
    app.template_filter('prettify_date')(template_decorators.prettify_date)
    app.template_filter('prettify_time_ms')(template_decorators.prettify_time_ms)
    app.template_filter('prettify_date_ms')(template_decorators.prettify_date_ms)

    app.template_test('feature_enabled')(template_decorators.is_feature_enabled)

    app.context_processor(template_decorators.template_context_processor)

    return app


def _init_containers(modules: list) -> Application:
    application = Application()
    yaml_file_config = os.environ['CONFIG_PATH']
    application.config.from_yaml(yaml_file_config)
    application.wire(modules)

    return application


def build_cli(app: Flask):
    downloader_group = AppGroup('downloader')
    downloader_group.command('download_frames_ftp')(tracks_downloader_cli.download_frames)
    downloader_group.command('download_tracks_ftp')(tracks_downloader_cli.download_tracks)
    downloader_group.command('download_logs')(logs_downloader_cli.download_logs)
    downloader_group.command('download_predictions')(tracks_downloader_cli.download_predictions)
    app.cli.add_command(downloader_group)

    reporter_group = AppGroup('reporter')
    reporter_group.command('run-fiji')(reporter_fiji_cli.run)
    reporter_group.command('run-pro-tracks')(reporter_pro_tracks_cli.run)
    reporter_group.command('run-pro')(reporter_pro_cli.run)
    reporter_group.command('lifecycle_controller')(lifecycle_controller_cli.run)
    app.cli.add_command(reporter_group)

    sender_group = AppGroup('sender')
    sender_group.command('camcom_predictor')(camcom_sender_cli.camcom_sender)
    app.cli.add_command(sender_group)

    depth_group = AppGroup('detections')
    depth_group.command('localize')(tracks_detections_localization_cli.run)
    app.cli.add_command(depth_group)

    reloader_group = AppGroup('reloader')
    reloader_group.command('reload_tracks')(tracks_reload_cli.reload_tracks)
    app.cli.add_command(reloader_group)

    utilities_group = AppGroup('s3_utilities')
    utilities_group.command('update_s3_buckets_lifecycle')(s3_bucket_lifecycle_cli.update_buckets_lifecycle)
    utilities_group.command('list_s3_buckets_lifecycle')(s3_bucket_lifecycle_cli.list_buckets_lifecycle)
    app.cli.add_command(utilities_group)

    cvat_group = AppGroup('cvat')
    cvat_group.command('upload_cvat_tasks')(cvat_uploader_cli.run)
    app.cli.add_command(cvat_group)

    saver_group = AppGroup('saver')
    saver_group.command('video_frames_save')(video_frames_saver_cli.video_frames_save)
    app.cli.add_command(saver_group)

    mm_group = AppGroup('map_matching')
    mm_group.command('match')(tracks_map_matching_cli.map_matcher)
    app.cli.add_command(mm_group)


app = create_app()

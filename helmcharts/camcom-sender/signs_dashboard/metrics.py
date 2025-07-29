import logging
import os

from flask import Flask, request
from prometheus_flask_exporter.multiprocess import GunicornPrometheusMetrics

logger = logging.getLogger(__name__)


def setup_metrics(app: Flask, app_name: str):
    metrics = GunicornPrometheusMetrics(
        app,
        group_by='url_rule',
        excluded_paths=['/static/.*', '/favicon.ico'],
        path='/healthz/metrics',
        export_defaults=False,
    )

    build_timestamp, app_version = _get_build_info()
    logger.warning(f'Setting up metrics for {app_name} {app_version} with build timestamp {build_timestamp}')
    app_info = metrics.info(
        name='app_info',
        description='App info',
        labelnames=['app_name', 'app_version'],
        labelvalues=[app_name, app_version],
    )
    app_info.set(build_timestamp)

    metrics.register_default(
        metrics.counter(
            name='app_request',
            description='Total requests',
            labels={
                'app_name': app_name,
                'http_path': lambda: request.url_rule,
                'http_method': lambda: request.method,
                'http_code': lambda response: response.status_code,
            },
        ),
        metrics.histogram(
            name='app_request_time',
            description='Request latency',
            labels={
                'app_name': app_name,
                'http_path': lambda: request.url_rule,
                'http_method': lambda: request.method,
                'http_code': lambda response: response.status_code,
            },
        ),
    )


def _get_build_info():
    return os.environ.get('BUILD_TIMESTAMP', 0), os.environ.get('BUILD_VERSION', 'undefined')

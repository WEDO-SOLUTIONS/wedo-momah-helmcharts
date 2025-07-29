import logging

import boto3
import click
from dependency_injector.wiring import Provide, inject

from signs_dashboard.containers.application import Application
from signs_dashboard.services.s3_client import S3ClientService
from signs_dashboard.services.s3_service import S3Service

logger = logging.getLogger(__name__)


@inject
@click.option(
    '--force',
    is_flag=True,
    required=False,
    default=False,
)
@click.option(
    '--debug',
    is_flag=True,
    required=False,
    default=False,
)
@click.argument(
    'buckets',
    nargs=-1,
    required=True,
)
def update_buckets_lifecycle(
    force: bool,
    debug: bool,
    buckets: tuple[str, ...],
    s3_service: S3Service = Provide[Application.services.s3_service],
    s3_client_service: S3ClientService = Provide[Application.services.s3_client],
):
    if debug:
        boto3.set_stream_logger('', logging.DEBUG)

    lifecycle_configuration = s3_service.buckets.get_frame_bucket_lifecycle()
    if not lifecycle_configuration:
        raise ValueError('Bucket lifecycle configuration not defined: missing frame_depth_map_ttl_days')
    logger.info(f'Got bucket list: {buckets}, lifecycle configuration {lifecycle_configuration}')

    click.confirm(f'Set buckets {buckets} lifecycle configuration to this?', abort=True)

    for bucket in buckets:
        logger.info(f'Setting up lifecycle configuration for {bucket} to {lifecycle_configuration}')
        existing = s3_client_service.get_bucket_lifecycle_configuration(bucket)

        if existing:
            logger.info(f'Bucket {bucket} already has lifecycle policy: {existing}')
            if not force:
                logger.info('Aborting!')
                return

        s3_client_service.set_bucket_lifecycle_configuration(bucket, lifecycle_configuration)

        updated = s3_client_service.get_bucket_lifecycle_configuration(bucket)
        logger.info(f'Bucket {bucket} now has lifecycle policy {updated}')

    logger.info('Done setting up lifecycle configuration')


@inject
@click.option(
    '--debug',
    is_flag=True,
    required=False,
    default=False,
)
@click.argument(
    'buckets',
    nargs=-1,
    required=True,
)
def list_buckets_lifecycle(
    debug: bool,
    buckets: tuple[str, ...],
    s3_client_service: S3ClientService = Provide[Application.services.s3_client],
):
    if debug:
        boto3.set_stream_logger('', logging.DEBUG)

    logger.info(f'Got bucket list: {buckets}')

    for bucket in buckets:
        existing = s3_client_service.get_bucket_lifecycle_configuration(bucket)

        if existing:
            logger.info(f'Bucket {bucket} has lifecycle policy {existing}')
        else:
            logger.info(f'Bucket {bucket} has no lifecycle configuration')

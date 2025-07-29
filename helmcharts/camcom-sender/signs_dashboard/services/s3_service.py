import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import BinaryIO, Optional

from signs_dashboard.services.s3_client import S3ClientService
from signs_dashboard.services.s3_keys import S3KeysService

logger = logging.getLogger(__name__)


@dataclass
class Buckets:
    frames_bucket_template: str
    videos_bucket_template: str
    frames_bucket_depth_map_ttl_days: Optional[int]
    legacy_frames_bucket: Optional[str]
    legacy_frames_bucket_timestamp: datetime
    logs_bucket_template: str
    crops_dump_bucket: Optional[str]

    def get_log_bucket(self, log_date: date) -> str:
        return self.logs_bucket_template.format(
            partition=log_date.strftime('%Y%m'),
        )

    def get_frame_bucket(self, frame_date: date) -> str:
        legacy_bucket = self.legacy_frames_bucket
        legacy_bucket_timestamp = self.legacy_frames_bucket_timestamp
        if frame_date <= legacy_bucket_timestamp and legacy_bucket:
            return legacy_bucket

        return self.frames_bucket_template.format(
            partition=frame_date.strftime('%Y%m'),
        )

    def get_videos_bucket(self, track_date: date):
        return self.videos_bucket_template.format(
            partition=track_date.strftime('%Y%m'),
        )

    def get_frame_bucket_lifecycle(self) -> Optional[dict]:
        # https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
        days = self.frames_bucket_depth_map_ttl_days
        if days is None:
            return None

        return {
            'Rules': [
                {
                    'ID': f'delete-depth-map-after-{days}-days',
                    'Expiration': {
                        'Days': days,
                    },
                    'Filter': {
                        'Tag': {
                            'Key': 'type',
                            'Value': 'depth_map',
                        },
                    },
                    'Status': 'Enabled',
                },
            ],
        }


def _as_bucket_name_template(bucket_prefix: str) -> str:
    return f'{bucket_prefix}{{partition}}'


class S3Service:
    def __init__(self, s3_client: S3ClientService, s3_config: dict, s3_keys: S3KeysService):
        self._s3_client = s3_client
        legacy_bucket_tstamp = s3_config.get('legacy_bucket_timestamp', 0)
        self.buckets = Buckets(
            frames_bucket_template=_as_bucket_name_template(s3_config['bucket_prefix']),
            videos_bucket_template=_as_bucket_name_template(s3_config['videos_bucket_prefix']),
            legacy_frames_bucket=s3_config.get('legacy_bucket'),
            legacy_frames_bucket_timestamp=datetime.utcfromtimestamp(legacy_bucket_tstamp / 1000),
            logs_bucket_template=_as_bucket_name_template(s3_config['logs_bucket_prefix']),
            crops_dump_bucket=s3_config.get('dump_bucket'),
            frames_bucket_depth_map_ttl_days=s3_config.get('frame_depth_map_ttl_days'),
        )
        self.keys = s3_keys
        self.base_url = s3_config['client_params']['endpoint_url']

    def list_objects_if_bucket_exists(self, bucket: str, prefix: str):
        if not self._s3_client.is_bucket_exist(bucket):
            return []

        return self._s3_client.list_objects(
            prefix=prefix,
            bucket=bucket,
        )

    def download_fileobj(self, bucket: str, key: str, fileobj: BinaryIO):
        return self._s3_client.download_fileobj(bucket=bucket, key=key, fileobj=fileobj)

    def upload_fileobj(
        self,
        bucket: str,
        key: str,
        content_type: str,
        fileobj: BinaryIO,
        extra_args: Optional[dict] = None,
    ):
        self._s3_client.create_bucket_if_not_exists(bucket)

        self._s3_client.upload_fileobj(
            fileobj=fileobj,
            bucket=bucket,
            key=key,
            content_type=content_type,
            extra_args=extra_args,
        )

    def upload_to_frames_bucket(
        self,
        bucket: str,
        key: str,
        content_type: str,
        fileobj: BinaryIO,
        extra_args: Optional[dict] = None,
    ):
        self._create_bucket_for_frames(bucket)

        self._s3_client.upload_fileobj(
            fileobj=fileobj,
            bucket=bucket,
            key=key,
            content_type=content_type,
            extra_args=extra_args,
        )

    def _create_bucket_for_frames(self, bucket: str):
        created = self._s3_client.create_bucket_if_not_exists(bucket)
        if not created:
            return

        configuration = self.buckets.get_frame_bucket_lifecycle()
        if configuration:
            self._s3_client.set_bucket_lifecycle_configuration(bucket, configuration)

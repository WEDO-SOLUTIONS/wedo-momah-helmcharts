import json
import logging
from functools import lru_cache
from typing import BinaryIO, List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3ClientService:
    def __init__(self, s3_config: dict):
        raw_config = s3_config['client_params'].pop('config', {})
        config = Config(max_pool_connections=int(raw_config.get('max_pool_connections', 10)))

        self._client = boto3.client('s3', config=config, **s3_config['client_params'])
        self.base_url = s3_config['client_params']['endpoint_url']
        self._set_public_read = s3_config.get('set_public_read_acl', False)

    @lru_cache
    def is_bucket_exist(self, bucket: str) -> bool:
        try:
            self._client.head_bucket(Bucket=bucket)
            return True
        except ClientError as error:
            logger.debug('Bucket %s is not exist because of %s', bucket, error)
        return False

    def create_bucket(self, bucket: str):
        logger.info('Create bucket %s', bucket)

        try:
            self._client.create_bucket(
                Bucket=bucket,
            )
        except ClientError as error:
            # There is no way to create bucket in transaction or other synchronization context,
            # race condition occurred. So, just skip this error.
            if type(error).__name__ in {'BucketAlreadyOwnedByYou', 'BucketAlreadyExists'}:
                logger.debug('Unable to create bucket %s, already created', bucket)
                return
            raise

        if self._set_public_read:
            permissions = ['s3:GetObject']
            principal = '*'
            logger.info('Set bucket %s policy %s for principal %s', bucket, permissions, principal)
            policy = {
                'Version': '2012-10-17',
                'Statement': [
                    {
                        'Sid': 'Allow',
                        'Effect': 'Allow',
                        'Principal': principal,
                        'Action': permissions,
                        'Resource': f'arn:aws:s3:::{bucket}/*',
                    },
                ],
            }
            self._client.put_bucket_policy(
                Bucket=bucket,
                Policy=json.dumps(policy),
            )

    def create_bucket_if_not_exists(
        self,
        bucket: str,
    ) -> bool:
        if not self.is_bucket_exist(bucket):
            self.create_bucket(bucket)
            self.is_bucket_exist.cache_clear()
            return True
        return False

    def list_objects(self, bucket: str, prefix: str) -> List[dict]:
        return self._client.list_objects(
            Prefix=prefix,
            Bucket=bucket,
        ).get('Contents', [])

    def download_fileobj(self, bucket: str, key: str, fileobj: BinaryIO):
        return self._client.download_fileobj(Bucket=bucket, Key=key, Fileobj=fileobj)

    def upload_fileobj(
        self,
        bucket: str,
        key: str,
        content_type: str,
        fileobj: BinaryIO,
        extra_args: Optional[dict] = None,
    ):
        extra_args = {
            'ContentType': content_type,
            **(extra_args or {}),
        }
        if self._set_public_read:
            extra_args.update({'ACL': 'public-read'})
        self._client.upload_fileobj(
            Fileobj=fileobj,
            Bucket=bucket,
            Key=key,
            ExtraArgs=extra_args,
        )

    def get_bucket_lifecycle_configuration(self, bucket: str) -> Optional[dict]:  # noqa: WPS615
        try:
            return self._client.get_bucket_lifecycle_configuration(
                Bucket=bucket,
            )
        except ClientError as error:
            if type(error).__name__ == 'NoSuchLifecycleConfiguration':
                return None
            raise error

    def set_bucket_lifecycle_configuration(self, bucket: str, configuration: dict) -> dict:  # noqa: WPS615
        return self._client.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration=configuration,
        )

import io
import logging
from datetime import date, datetime
from typing import List, Optional

from dateutil.relativedelta import relativedelta

from signs_dashboard.models.track_upload_status import TrackUploadStatus
from signs_dashboard.schemas.track_log import LogFileOnS3, TrackLog
from signs_dashboard.services.s3_service import S3Service


class TrackLogsService:

    def __init__(self, s3_service: S3Service):
        self._s3_service = s3_service

    def find_track_logs_around_date(self, track_uuid: str, track_date: date) -> List[LogFileOnS3]:
        return [
            *self._find_track_logs(track_uuid, track_date - relativedelta(months=1)),
            *self._find_track_logs(track_uuid, track_date),
            *self._find_track_logs(track_uuid, track_date + relativedelta(months=1)),
        ]

    def find_track_log(
        self,
        track_uuid: str,
        track_date: date,
        log_filename: str,
    ) -> Optional[LogFileOnS3]:
        track_logs = self.find_track_logs_around_date(track_uuid=track_uuid, track_date=track_date)
        for log_file in track_logs:
            if log_file.filename == log_filename:
                return log_file
        return None

    def upload_track_log(self, log: TrackLog):
        bucket = self._s3_service.buckets.get_log_bucket(log.date)
        key = self._s3_service.keys.get_log_key(log)
        logging.debug('Upload log bucket=%s, key=%s', bucket, key)

        self._s3_service.upload_fileobj(
            fileobj=io.BytesIO(log.log_data),
            bucket=bucket,
            key=key,
            content_type='text/plain',
        )

    def download_track_log(self, track_uuid: str, track_date: date, log_filename: str) -> Optional[bytes]:
        s3_file = self.find_track_log(
            track_uuid=track_uuid,
            track_date=track_date,
            log_filename=log_filename,
        )
        if not s3_file:
            return None

        log_file_bytes = io.BytesIO()
        self._s3_service.download_fileobj(
            bucket=s3_file.bucket,
            key=s3_file.key,
            fileobj=log_file_bytes,
        )
        log_file_bytes.seek(0)
        return log_file_bytes.read()

    def _find_track_logs(self, track_uuid: str, track_date: date) -> List[LogFileOnS3]:
        bucket = self._s3_service.buckets.get_log_bucket(track_date)
        prefix = self._s3_service.keys.get_log_key_prefix(track_uuid=track_uuid, track_date=track_date)
        objects = self._s3_service.list_objects_if_bucket_exists(
            prefix=prefix,
            bucket=bucket,
        )
        return [
            LogFileOnS3.from_s3_obj(obj, bucket=bucket)
            for obj in objects
            if obj['Size'] > 0
        ]


def find_logs_date(
    upload_status: Optional[TrackUploadStatus],
    fallback: datetime,
) -> date:
    if upload_status:
        if upload_status.complete_time:
            return upload_status.complete_time.date()

        if upload_status.gps_time:
            return upload_status.gps_time.date()

        if upload_status.init_time:
            return upload_status.init_time.date()

    return fallback.date()

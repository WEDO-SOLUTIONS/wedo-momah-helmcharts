import json
import logging
import os
import traceback
from datetime import datetime
from io import BytesIO, TextIOWrapper

from signs_dashboard.models.track import Track
from signs_dashboard.services.s3_service import S3Service

logger = logging.getLogger(__name__)

LOG_MAP_MATCHING_INPUT_OUTPUT = os.environ.get('LOG_MAP_MATCHING_INPUT_OUTPUT', True)


class MapMatchingLoggingService:

    def __init__(self, s3_service: S3Service, track: Track):
        self._s3_service = s3_service
        self._prefix = 'map_matching_'
        self.run_id = datetime.now().isoformat()
        self.bucket = self._s3_service.buckets.get_log_bucket(track.uploaded or track.recorded)
        self.track_uuid = track.uuid
        self.enabled = LOG_MAP_MATCHING_INPUT_OUTPUT

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            filename = f'{self._prefix}_{self.run_id}_error.json'
            data = {
                'traceback': traceback.format_exception(exc_type, exc_val, exc_tb),
                'exc_type': str(exc_type),
                'exc_val': str(exc_val),
            }
            self._upload_json(data, filename)
        return False

    def save_input(self, gps_points: list[dict], url: str):
        if not self.enabled:
            return

        filename = f'{self._prefix}_{self.run_id}_input.json'
        logger.info(f'Saving input to {filename}')
        data = {
            'gps_points': gps_points,
            'url': url,
        }
        self._upload_json(data, filename)

    def save_output(
        self,
        status_code: int,
        headers,
        body: str,
    ):
        if not self.enabled:
            return

        filename = f'{self._prefix}_{self.run_id}_output.json'
        logger.info(f'Saving output to {filename}')
        data = {
            'status_code': status_code,
            'headers': dict(headers),
            'body': str(body),
        }
        self._upload_json(data, filename)

    def _upload_json(self, data: dict, filename: str):
        buffer = BytesIO()
        text_buffer = TextIOWrapper(buffer, encoding='utf-8')
        json.dump(data, text_buffer)
        text_buffer.seek(0)
        self._s3_service.upload_fileobj(
            bucket=self.bucket,
            key=f'{self.track_uuid}/{filename}',
            content_type='application/json',
            fileobj=buffer,
        )

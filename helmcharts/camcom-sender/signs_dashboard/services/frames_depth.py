from io import BytesIO
from typing import Optional
from urllib.request import urlopen

import cv2
import numpy as np

from signs_dashboard.models.frame import Frame
from signs_dashboard.services.s3_service import S3Service


class FramesDepthService:
    def __init__(self, s3_service: S3Service):
        self._s3_service = s3_service

    def save_frame_depth_map(
        self,
        frame: Frame,
        depth_prediction_data_uri: str,
        theta: Optional[int],
    ):
        bucket = self._s3_service.buckets.get_frame_bucket(frame.date)
        if theta is None:
            key = self._s3_service.keys.get_depth_map_key(frame)
        else:
            key = self._s3_service.keys.get_crop_depth_map_key(frame, theta=theta)

        if not depth_prediction_data_uri.startswith('data:'):
            raise ValueError('Invalid prediction data uri format!')
        with urlopen(depth_prediction_data_uri) as data_uri:  # noqa: S310
            self._s3_service.upload_to_frames_bucket(
                bucket=bucket,
                key=key,
                content_type=data_uri.headers['content-type'],
                fileobj=BytesIO(data_uri.read()),
                extra_args={'Tagging': 'type=depth_map'},
            )

    def download_depth_map(self, frame: Frame, theta: Optional[int] = None) -> bytes:
        bucket = self._s3_service.buckets.get_frame_bucket(frame.date)
        if theta is None:
            key = self._s3_service.keys.get_depth_map_key(frame)
        else:
            key = self._s3_service.keys.get_crop_depth_map_key(frame, theta=theta)

        depth_map_data = BytesIO()
        self._s3_service.download_fileobj(
            bucket=bucket,
            key=key,
            fileobj=depth_map_data,
        )
        depth_map_data.seek(0)
        return depth_map_data.read()

    def get_frame_depth_map(self, frame: Frame) -> np.array:
        depth_map = self.download_depth_map(frame)
        byte_array = np.frombuffer(depth_map, dtype=np.uint8)
        return cv2.imdecode(byte_array, cv2.IMREAD_UNCHANGED)

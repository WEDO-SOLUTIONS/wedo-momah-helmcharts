import io
import logging
import os
import typing as tp

import cv2
import numpy as np

from signs_dashboard.models.frame import Frame
from signs_dashboard.services.s3_service import S3Service

logger = logging.getLogger(__name__)


class ImageService:
    def __init__(self, s3_service: S3Service):
        self._s3_service = s3_service

    def find_crops_by_prefix(self, prefix: str):
        return self._s3_service.list_objects_if_bucket_exists(
            prefix=prefix,
            bucket=self._s3_service.buckets.crops_dump_bucket,
        )

    def download_crop(self, key: str) -> bytes:
        img_bytes = io.BytesIO()
        self._s3_service.download_fileobj(
            bucket=self._s3_service.buckets.crops_dump_bucket,
            key=key,
            fileobj=img_bytes,
        )
        img_bytes.seek(0)
        return img_bytes.read()

    def get_s3_crop_path(self, key):
        return os.path.join(self._s3_service.base_url, self._s3_service.buckets.crops_dump_bucket, key)

    def get_s3_track360_crop_path(self, frame: Frame, theta: int) -> str:
        bucket = self._s3_service.buckets.get_frame_bucket(frame.date)
        key = self._s3_service.keys.get_crop_frame_key(frame, theta)
        return os.path.join(self._s3_service.base_url, bucket, key)

    def get_s3_path(self, frame: Frame) -> str:
        bucket = self._s3_service.buckets.get_frame_bucket(frame.date)
        key = self._s3_service.keys.get_frame_key(frame)
        return os.path.join(self._s3_service.base_url, bucket, key)

    def get_s3_location_info(self, frame: Frame) -> dict:
        return {
            'image_key': self._s3_service.keys.get_frame_key(frame),
            'image_bucket': self._s3_service.buckets.get_frame_bucket(frame.date),
            'image_url': self.get_s3_path(frame),
        }

    def upload_frame(self, frame: Frame, image: bytes, theta: tp.Optional[int] = None):
        img_byte_arr = io.BytesIO(image)

        bucket = self._s3_service.buckets.get_frame_bucket(frame.date)
        if theta is None:
            key = self._s3_service.keys.get_frame_key(frame)
        else:
            key = self._s3_service.keys.get_crop_frame_key(frame, theta=theta)

        logging.debug('Upload frame bucket=%s, key=%s', bucket, key)

        self._s3_service.upload_to_frames_bucket(
            fileobj=img_byte_arr,
            bucket=bucket,
            key=key,
            content_type='image/jpeg',
        )

    def download_image(self, frame: Frame, theta: tp.Optional[int] = None) -> tp.Optional[bytes]:
        if not frame.uploaded_photo:
            return None

        if theta is None:
            frame_key = self._s3_service.keys.get_frame_key(frame)
        else:
            frame_key = self._s3_service.keys.get_crop_frame_key(frame, theta=theta)

        img_bytes = io.BytesIO()
        self._s3_service.download_fileobj(
            bucket=self._s3_service.buckets.get_frame_bucket(frame.date),
            key=frame_key,
            fileobj=img_bytes,
        )
        img_bytes.seek(0)
        return img_bytes.read()

    def download_image_as_ndarray(self, frame: Frame, theta: tp.Optional[int] = None) -> tp.Optional[np.ndarray]:
        image_bytes = self.download_image(frame, theta=theta)
        if not image_bytes:
            return None

        buffer = bytearray(image_bytes)
        byte_array = np.frombuffer(buffer, dtype=np.uint8)
        return cv2.imdecode(byte_array, cv2.IMREAD_COLOR | cv2.IMREAD_UNCHANGED)

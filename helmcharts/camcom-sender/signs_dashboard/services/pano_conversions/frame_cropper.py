import gc
import io
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, wait

import cv2
import numpy as np
import piexif

from signs_dashboard.models.frame import Frame
from signs_dashboard.services.image import ImageService
from signs_dashboard.services.pano_conversions.common import CropsParams
from signs_dashboard.services.pano_conversions.from_equirectangular import equirectal_image_to_perspective_crop
from signs_dashboard.services.s3_client import S3ClientService
from signs_dashboard.services.s3_keys import S3KeysService
from signs_dashboard.services.s3_service import S3Service

image_service: ImageService = None
s3_executor: ThreadPoolExecutor = None
CAMERA_ROTATION_ANGLE = 0

logger = logging.getLogger(__name__)


def initialize_cropping_worker(s3_config: dict):
    global image_service  # pylint: disable=W0603
    global s3_executor  # pylint: disable=W0603
    image_service = ImageService(
        s3_service=S3Service(
            s3_client=S3ClientService(s3_config=s3_config),
            s3_keys=S3KeysService(s3_config=s3_config),
            s3_config=s3_config,
        ),
    )
    s3_executor = ThreadPoolExecutor(max_workers=s3_config.get('executor_max_workers', 10))


def generate_crops_from_frame(  # noqa: WPS210
    source_azimuth: float,
    frame: Frame,
    image_path: str,
) -> bool:
    t1 = time.monotonic()
    with open(image_path, 'rb') as file:
        img = file.read()

    futures = []
    np_image = cv2.imdecode(np.frombuffer(img, np.uint8), cv2.IMREAD_COLOR)
    for theta in CropsParams.CROPS_Z_POSITIONS:
        crop, focal_length_x, focal_length_y = equirectal_image_to_perspective_crop(
            img=np_image,
            theta=theta + CAMERA_ROTATION_ANGLE,
        )

        success, crop_bytes = cv2.imencode('.jpg', crop)
        if success:
            crop_with_exif = _update_crop_exif(
                crop_bytes=crop_bytes.tobytes(),
                source_image_bytes=img,
                source_azimuth=source_azimuth,
                theta=theta,
                focal_length_x=focal_length_x,
                focal_length_y=focal_length_y,
                image_width=np_image.shape[1],
                image_height=np_image.shape[0],
            )
            futures.append(s3_executor.submit(
                image_service.upload_frame,
                frame=frame,
                image=crop_with_exif,
                theta=theta,
            ))
        else:
            raise RuntimeError(
                f'Could crop with theta {theta} for frame from track {frame.track_uuid} @ {frame.date}',
            )

    t2 = time.monotonic()

    wait(futures)

    t3 = time.monotonic()

    logger.debug(f'Spend {t2 - t1:.4f}s cropping , {t3 - t2:.4f}s waiting for upload')  # noqa: E501
    os.remove(image_path)
    gc.collect()
    return True


def _update_crop_exif(
    crop_bytes: bytes,
    source_image_bytes: bytes,
    source_azimuth: float,
    theta: float,
    focal_length_x: float,
    focal_length_y: float,
    image_width: int,
    image_height: int,
) -> bytes:
    exif_dict = piexif.load(source_image_bytes)
    azimuth = source_azimuth + theta
    focal_length_x_mm = focal_length_x * CropsParams.PIXEL_SIZE_X
    focal_length_y_mm = focal_length_y * CropsParams.PIXEL_SIZE_Y  # noqa: F841 - NOT USED

    # Calculate camera sensor dimensions in mm
    sensor_width_mm = image_width * CropsParams.PIXEL_SIZE_X
    sensor_height_mm = image_height * CropsParams.PIXEL_SIZE_Y

    exif_dict['GPS'][piexif.GPSIFD.GPSImgDirection] = _as_fraction(azimuth, precision=4)
    exif_dict['Exif'][piexif.ExifIFD.FocalLength] = _as_fraction(focal_length_x_mm, precision=2)
    exif_dict['Exif'][piexif.ExifIFD.FocalPlaneXResolution] = _as_fraction(sensor_width_mm, precision=2)
    exif_dict['Exif'][piexif.ExifIFD.FocalPlaneYResolution] = _as_fraction(sensor_height_mm, precision=2)

    exif_bytes = piexif.dump(exif_dict)

    crop_img_byte_arr = io.BytesIO()
    piexif.insert(exif_bytes, crop_bytes, crop_img_byte_arr)
    crop_img_byte_arr.seek(0)
    return crop_img_byte_arr.read()


def _as_fraction(num: float, precision: int) -> tuple[int, int]:
    divider = 10 ** precision
    return int(num * divider), divider

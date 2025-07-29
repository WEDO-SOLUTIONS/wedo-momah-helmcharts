import os
import typing as tp
import zipfile
from io import BytesIO

import cv2
import numpy as np

from signs_dashboard.models.bbox_detection import BBOXDetection
from signs_dashboard.models.frame import Frame
from signs_dashboard.services.image import ImageService


class ImageArchiverService:
    def __init__(
        self,
        image_service: ImageService,
    ):
        self._image_service = image_service

    def get_zip_with_frames(self, frames: tp.List[Frame]) -> BytesIO:
        bytes_io = BytesIO()
        with zipfile.ZipFile(bytes_io, 'w') as zip_archive:
            for frame in frames:
                img = self._image_service.download_image(frame)
                if img is None:
                    continue
                _save_image(zip_archive, img_bytes=img, frame=frame)
        bytes_io.seek(0)
        return bytes_io

    def get_zip_with_crops(
        self, frames: tp.List[Frame], needed_signs_ids: tp.Optional[tp.List[int]],
    ) -> BytesIO:
        if needed_signs_ids is not None:
            needed_signs_ids = set(needed_signs_ids)
        bytes_io = BytesIO()
        with zipfile.ZipFile(bytes_io, 'w') as zip_archive:
            self._save_needed_crops_for_frames(zip_archive, frames=frames, needed_signs_ids=needed_signs_ids)
        bytes_io.seek(0)
        return bytes_io

    def _save_needed_crops_for_frames(
        self,
        zip_file: zipfile.ZipFile,
        frames: tp.List[Frame],
        needed_signs_ids: tp.Optional[tp.Set[int]],
    ) -> None:
        for frame in frames:
            if _is_frame_needed(frame, needed_signs_ids):
                self._save_needed_crops_for_frame(zip_file, frame, needed_signs_ids)

    def _save_needed_crops_for_frame(
        self,
        zip_file: zipfile.ZipFile,
        frame: Frame,
        needed_signs_ids: tp.Optional[tp.Set[int]],
    ) -> None:
        img = self._image_service.download_image_as_ndarray(frame=frame)
        if img is None:
            return

        for sign in frame.detections:
            if sign.id in needed_signs_ids:
                _save_crop(zip_file, img=img, sign=sign, frame=frame)


def _save_image(zip_file: zipfile.ZipFile, img_bytes: bytes, frame: Frame) -> None:
    filename = os.path.join(frame.track_uuid, frame.image_name)
    _write_to_zip(zip_file, img_bytes, fname=filename)


def _is_frame_needed(frame: Frame, needed_signs_ids: tp.Optional[tp.Set[int]]):
    for sign in frame.detections:
        if sign.id in needed_signs_ids:
            return True
    return False


def _save_crop(zip_file: zipfile.ZipFile, img: np.ndarray, sign: BBOXDetection, frame: Frame) -> None:
    fname = frame.image_name
    fname = fname[:fname.rfind('.')]
    fname = '_'.join([frame.track_uuid, fname, str(sign.id)])
    fname = f'{fname}.png'
    fpath = os.path.join(sign.label_with_value, fname)
    crop = _get_crop(img, sign)
    if crop.size != 0:
        _, buf = cv2.imencode('.png', crop)
        _write_to_zip(zip_file, buf, fname=fpath)


def _get_crop(img: np.ndarray, sign: BBOXDetection) -> np.ndarray:
    return img[
        sign.y_from:sign.y_from + sign.height,
        sign.x_from:sign.x_from + sign.width,
    ]


def _write_to_zip(zip_file: zipfile.ZipFile, obj_bytes: bytes, fname: str):
    with zip_file.open(fname, 'w') as file_in_zip:
        file_in_zip.write(obj_bytes)

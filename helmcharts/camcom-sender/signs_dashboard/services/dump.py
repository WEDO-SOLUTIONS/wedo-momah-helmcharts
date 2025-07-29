import datetime
import logging
import typing as tp
import zipfile
from dataclasses import dataclass
from io import BytesIO

from signs_dashboard.query_params.dump import DumpQueryParameters
from signs_dashboard.services.image import ImageService

logger = logging.getLogger(__name__)

DUMP_DATE_FORMAT = '%Y_%m_%d'  # noqa: WPS323


@dataclass
class DumpCrop:
    date: datetime.date
    label: str
    is_tmp: bool
    key: str
    url: str


class CropsDumpService:
    def __init__(self, image_service: ImageService):
        self._image_service = image_service

    def find(self, query_params: DumpQueryParameters) -> tp.List[DumpCrop]:
        crops = []
        for date in query_params.dates:
            for prefix in _build_prefixes(date, query_params.label, query_params.is_tmp):
                crops.extend(self._get_crops_by_prefix(prefix))
        return crops

    def get_zip_with_crops(self, crops: tp.List[DumpCrop]) -> BytesIO:
        bytes_io = BytesIO()
        with zipfile.ZipFile(bytes_io, 'w') as zip_archive:
            for crop in crops:
                img_bytes = self._image_service.download_crop(crop.key)
                with zip_archive.open(crop.key, 'w') as file_in_zip:
                    file_in_zip.write(img_bytes)
        bytes_io.seek(0)
        return bytes_io

    def _get_crops_by_prefix(self, prefix: str) -> tp.List[DumpCrop]:
        crops = []
        s3_date_contents = self._image_service.find_crops_by_prefix(prefix)
        for s3_content in s3_date_contents:
            key = s3_content['Key']
            try:
                dump_crop = self._build_crop_from_s3_key(key)
            except ValueError:
                logger.warning(f'Unexpected key format: {key}')
            else:
                crops.append(dump_crop)
        return crops

    def _build_crop_from_s3_key(self, key: str) -> tp.Optional[DumpCrop]:
        date_str, label_with_tmp, _ = key.split('/')
        date = datetime.datetime.strptime(date_str, DUMP_DATE_FORMAT).date()
        is_tmp = label_with_tmp.endswith('_tmp')
        label = label_with_tmp.rstrip('_tmp')
        return DumpCrop(date, label, is_tmp, key, self._image_service.get_s3_crop_path(key))


def _build_prefixes(
    date: datetime.date,
    label: tp.Optional[str],
    is_tmp: tp.Optional[str],
) -> tp.List[str]:
    if not label:
        return [f'{date.strftime(DUMP_DATE_FORMAT)}/']
    prefix_with_tmp = f'{date.strftime(DUMP_DATE_FORMAT)}/{label}_tmp/'
    prefix_without_tmp = f'{date.strftime(DUMP_DATE_FORMAT)}/{label}/'
    if is_tmp is None:
        return [prefix_with_tmp, prefix_without_tmp]
    if is_tmp:
        return [prefix_with_tmp]
    return [prefix_without_tmp]

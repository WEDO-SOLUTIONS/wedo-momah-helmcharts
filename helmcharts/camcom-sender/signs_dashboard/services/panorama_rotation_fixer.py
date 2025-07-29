import logging
from functools import partial
from io import BytesIO
from typing import Iterator, Optional

import requests
from PIL import Image
from yarl import URL

from signs_dashboard.schemas.video_frames_saver import VideoFrame

logger = logging.getLogger(__name__)


class PanoramaRotationFixer:
    def __init__(self, config: dict):
        self.enabled = config.get('enabled', False)
        self._base_url = config.get('base_url')
        if self._base_url:
            self._base_url = URL(self._base_url)
        self._batch_size = config.get('batch_size', 5)

    def wrap_iterator(self, iterator: Iterator[VideoFrame]) -> Iterator[VideoFrame]:
        batch = []
        holded_frames = []
        rotation_angle = None
        for video_frame in iterator:
            if rotation_angle is not None:
                yield self._rotate_panorama(video_frame, rotation_angle=rotation_angle)
                continue

            batch.append(video_frame)
            if len(batch) == self._batch_size:
                rotation_angle = self._get_rotation_angle(batch)
                if rotation_angle is None:
                    logger.warning(f'Failed to get rotation angle for batch {len(batch)}, holding batch')
                    holded_frames += batch
                else:
                    logger.warning(f'Got rotation angle {rotation_angle} for batch {len(batch)}, processing batch')
                    yield from map(partial(self._rotate_panorama, rotation_angle=rotation_angle), holded_frames)
                    holded_frames = []
                    yield from map(partial(self._rotate_panorama, rotation_angle=rotation_angle), batch)
                batch = []
        if batch or holded_frames:
            logger.warning('Processing remaining frames')
            yield from map(partial(self._rotate_panorama, rotation_angle=rotation_angle), holded_frames)
            yield from map(partial(self._rotate_panorama, rotation_angle=rotation_angle), batch)

    def _get_rotation_angle(self, batch: list[VideoFrame]) -> Optional[float]:
        resp = requests.post(
            self._base_url / 'api/1.0/correct_panoramas',
            files=[
                ('pano', ('pano.jpg', vframe.image, 'image/jpeg'))
                for vframe in batch
            ],
        )
        if resp.status_code != 200:
            logger.warning(f'Panorama-fixer responded with non-200: {resp.status_code} {resp.text}')
            return None

        try:
            return resp.json()['angle_shift']
        except Exception:
            logger.warning(f'Unable to parse panorama-fixer response: {resp.text}')
            return None

    def _rotate_panorama(self, vframe: VideoFrame, rotation_angle: Optional[float]) -> VideoFrame:
        if rotation_angle is None:
            logger.warning(f'Did not get rotation angle for frame {vframe.timestamp_ms}, skipping rotation')
            return vframe

        image = Image.open(BytesIO(vframe.image))

        width, height = image.size

        rotation_line = (width / 2) - (width * (rotation_angle / 360)) % width
        crop_x = int((rotation_line + width / 2) % width)

        left_cropped_section = image.crop((0, 0, crop_x, height))
        right_shifted_part = image.crop((crop_x, 0, width, height))

        result_image = Image.new('RGB', (width, height))
        result_image.paste(right_shifted_part, (0, 0))
        result_image.paste(left_cropped_section, (width - crop_x, 0))

        buffer = BytesIO()
        result_image.save(buffer, exif=image.info.get('exif'), format='jpeg')

        vframe.image = buffer.getvalue()
        logger.debug(f'Frame {vframe.timestamp_ms} rotated')
        return vframe

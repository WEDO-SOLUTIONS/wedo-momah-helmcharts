import re
from datetime import date
from typing import Optional

from signs_dashboard.models.frame import Frame
from signs_dashboard.schemas.track_log import TrackLog
from signs_dashboard.small_utils import correct_round

DATE_FORMAT = '%Y-%m-%d'
template_placeholders_re = re.compile('(?<={).*?(?=})', flags=re.DOTALL)  # from '{placeholder}' matches 'placeholder'
placeholder_key_re = re.compile('^[a-z_]+$')


class S3KeysService:
    def __init__(self, s3_config: dict):
        self._log_key_template = validate_key_template(s3_config['key_templates']['log'])
        self._log_key_prefix_template = validate_key_template(s3_config['key_templates']['log_prefix'])
        self._frame_key_template = validate_key_template(s3_config['key_templates']['frame'])
        self._crop_frame_key_template = validate_key_template(s3_config['key_templates']['crop_frame'])
        self._videos_key_template = validate_key_template(s3_config['key_templates']['videos'])
        self._frame_depth_map_key_template = validate_key_template(
            s3_config['key_templates'].get('depth_map'),
            allow_none=True,
        )
        self._frame_crop_depth_map_key_template = validate_key_template(
            s3_config['key_templates'].get('crop_depth_map'),
            allow_none=True,
        )

    def get_frame_key(self, frame: Frame) -> str:
        return self._frame_key_template.format(
            track_email=frame.track_email,
            track_uuid=frame.track_uuid,
            frame_date=frame.date.strftime(DATE_FORMAT),
            frame_timestamp_ms=frame.timestamp,
            lat=_coord_as_str(correct_round(frame.lat)),
            lon=_coord_as_str(correct_round(frame.lon)),
        )

    def get_crop_frame_key(self, frame: Frame, theta: int) -> str:
        return self._crop_frame_key_template.format(
            theta=theta,
            track_email=frame.track_email,
            track_uuid=frame.track_uuid,
            frame_date=frame.date.strftime(DATE_FORMAT),
            frame_timestamp_ms=frame.timestamp,
            lat=_coord_as_str(correct_round(frame.lat)),
            lon=_coord_as_str(correct_round(frame.lon)),
        )

    def get_depth_map_key(self, frame: Frame) -> str:
        return self._frame_depth_map_key_template.format(
            track_email=frame.track_email,
            track_uuid=frame.track_uuid,
            frame_date=frame.date.strftime(DATE_FORMAT),
            frame_timestamp_ms=frame.timestamp,
            lat=correct_round(frame.lat),
            lon=correct_round(frame.lon),
        )

    def get_crop_depth_map_key(self, frame: Frame, theta: int) -> str:
        return self._frame_crop_depth_map_key_template.format(
            track_email=frame.track_email,
            track_uuid=frame.track_uuid,
            frame_date=frame.date.strftime(DATE_FORMAT),
            frame_timestamp_ms=frame.timestamp,
            lat=correct_round(frame.lat),
            lon=correct_round(frame.lon),
            theta=theta,
        )

    def get_log_key(self, log: TrackLog) -> str:
        return self._log_key_template.format(
            track_uuid=log.track_uuid,
            log_date=log.date.strftime(DATE_FORMAT),
            log_timestamp_ms=log.timestamp_ms,
        )

    def get_log_key_prefix(self, track_uuid: str, track_date: date) -> str:
        return self._log_key_prefix_template.format(
            track_uuid=track_uuid,
            log_date=track_date.strftime(DATE_FORMAT),
        )

    def get_videos_key(self, track_uuid: str, resource_type: str) -> str:
        return self._videos_key_template.format(
            track_uuid=track_uuid,
            resource_type=resource_type,
        )


def _coord_as_str(coord: float) -> str:
    return f'{coord:.12f}'


def validate_key_template(template: Optional[str], allow_none: bool = False) -> Optional[str]:
    if not template:
        if allow_none:
            return template
        raise ValueError('Key template required!')

    placeholders = template_placeholders_re.findall(template)

    if not placeholders:
        raise ValueError(f'Key template "{template}" not supported: no placeholders found')

    for placeholder in placeholders:
        if not placeholder_key_re.match(placeholder):
            raise ValueError(f'Key template "{template}" not supported: not valid placeholder "{placeholder}"')

    return template

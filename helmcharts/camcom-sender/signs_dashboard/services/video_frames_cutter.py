import io
import logging
from datetime import datetime, timezone
from fractions import Fraction
from typing import Any, Iterator

import cv2
import piexif
from haversine import Unit, haversine

from signs_dashboard.models.track import Track
from signs_dashboard.schemas.video_frames_saver import VideoFrame
from signs_dashboard.services.gps_interpolation import GPSInterpolationService
from signs_dashboard.small_utils import correct_round

logger = logging.getLogger(__name__)

TpLatLonExif = tuple[tuple[int, int], tuple[int, int], tuple[int, int]]
EXIF_DT_FORMAT = '%Y:%m:%d %H:%M:%S'  # noqa: WPS323
UTF8 = 'utf-8'
MIN_FRAMES_DISTANCE_METERS = 1
MAX_FRAME_GPS_TIMESTAMP_DIFF_MS = 3000


def _deg_to_dms_rational(deg: float) -> TpLatLonExif:
    d = int(deg)
    m = int((deg - d) * 60)
    s = (deg - d - m / 60) * 3600
    return (d, 1), (m, 1), (int(s * 10000), 10000)


def float_value_as_rational(value: float) -> tuple[int, int]:
    fraction = Fraction(value).limit_denominator(1000)
    return fraction.numerator, fraction.denominator


class VideoFramesCutterService:

    def __init__(self, gps_interpolation_service: GPSInterpolationService):
        self._gps_interpolation_service = gps_interpolation_service

    @staticmethod
    def distance_between_frames_ok(
        frame1_gps_data: dict,
        frame2_gps_data: dict,
        min_distance: int = MIN_FRAMES_DISTANCE_METERS,
    ) -> bool:
        point1 = (frame1_gps_data['latitude'], frame1_gps_data['longitude'])
        point2 = (frame2_gps_data['latitude'], frame2_gps_data['longitude'])
        frames_distance = haversine(
            point1=point1,
            point2=point2,
            unit=Unit.METERS,
        )
        logger.debug(f'Distance between frames: {frames_distance}, ({point1}), ({point2})')
        return frames_distance >= min_distance

    def iterate_frames(
        self,
        track: Track,
        video_path: str,
    ) -> Iterator[VideoFrame]:
        logger.info(f'Iterating frames for track {track.uuid}')
        yield from self._extract_frames_from_video(
            video_path=video_path,
            video_recording_start_timestamp_ms=track.upload.video_recording_start_timestamp_ms,
            fps=5,
            gps_data=track.upload.gps_points,
            user_email=track.user_email,
            software=track.app_version,
            model=track.upload.init_metadata.get('camera_model', 'unknown'),
        )

    def _extract_frames_from_video(  # noqa: C901
        self,
        video_path: str,
        video_recording_start_timestamp_ms: float,
        gps_data: list[dict[str, Any]],
        fps: float,
        software: str,
        user_email: str,
        model: str,
    ) -> Iterator[VideoFrame]:
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            logger.error('Cannot open video from path %s', video_path)
            return

        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if frame_count <= 1:
            logger.error('Video %s has only %d frames', video_path, frame_count)
            return

        frame_rate = cap.get(cv2.CAP_PROP_FPS)
        if fps > 0:
            interval = int(frame_rate / fps)
        else:
            interval = 1

        frame_count = 0
        saved_frame_count = 0
        last_frame = None

        while True:
            ret, image = cap.read()
            if not ret:
                logger.warning(f'From {frame_count} raw frames preselected {saved_frame_count}')
                break

            if frame_count % interval == 0:
                sample_time_ms = cap.get(cv2.CAP_PROP_POS_MSEC)
                if sample_time_ms < 0:
                    logger.error(f'Skipping frame {frame_count} with negative sample time {sample_time_ms}')
                    continue
                timestamp_ms = video_recording_start_timestamp_ms + sample_time_ms
                frame_gps_data, time_difference = self._find_closest_gps_data(timestamp_ms, gps_data=gps_data)

                if abs(time_difference) > MAX_FRAME_GPS_TIMESTAMP_DIFF_MS:
                    logger.error(f'Frame {frame_count} has too big time difference {time_difference} ms')
                    continue

                if last_frame and not self.distance_between_frames_ok(last_frame.gps_data, frame_gps_data):
                    continue

                image_bytes, meta = self._prepare_frame_exif_and_meta(
                    image=cv2.imencode('.jpg', image)[1].tobytes(),
                    sample_time_ms=sample_time_ms,
                    gps_data=frame_gps_data,
                    user_email=user_email,
                    model=model,
                    software=software,
                )

                frame = VideoFrame(
                    image=image_bytes,
                    timestamp_ms=timestamp_ms,
                    sample_time_ms=sample_time_ms,
                    gps_data=frame_gps_data,
                    meta=meta,
                )
                last_frame = frame
                saved_frame_count += 1
                yield frame

            frame_count += 1

        cap.release()

    def _find_closest_gps_data(
        self,
        frame_time: float,
        gps_data: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], float]:
        point = self._gps_interpolation_service.interpolate_frame_point(
            frame_time,
            gps_points=gps_data,
        )
        ts_distance = point['timestamp'] - frame_time
        logger.debug(f'Closest point is in {ts_distance} ms')
        return point, ts_distance

    @staticmethod
    def _prepare_frame_exif_and_meta(
        image: bytes,
        sample_time_ms: float,
        gps_data: dict,
        user_email: str,
        model: str,
        software: str,
    ) -> tuple[bytes, dict]:
        lat_deg = float(gps_data.get('latitude'))
        lon_deg = float(gps_data.get('longitude'))
        alt = float(gps_data.get('altitude'))
        speed = float(gps_data.get('speed'))
        direction = float(gps_data.get('bearing'))

        ts = gps_data.get('timestamp')
        frame_date = datetime.fromtimestamp(ts / 1000).replace(tzinfo=timezone.utc)
        frame_subsec = int(frame_date.microsecond / 1000)

        exif_dict = {
            '0th': {
                piexif.ImageIFD.Model: model.encode(UTF8),
                piexif.ImageIFD.Orientation: 1,
                piexif.ImageIFD.Software: software.encode(UTF8),
                piexif.ImageIFD.DateTime: frame_date.strftime(EXIF_DT_FORMAT).encode(UTF8),
                piexif.ImageIFD.Artist: user_email.encode(UTF8),
            },
            'Exif': {
                piexif.ExifIFD.SubSecTime: str(frame_subsec).encode(UTF8),
            },
            'GPS': {
                piexif.GPSIFD.GPSLatitudeRef: 'N' if lat_deg >= 0 else 'S',
                piexif.GPSIFD.GPSLatitude: _deg_to_dms_rational(abs(lat_deg)),
                piexif.GPSIFD.GPSLongitudeRef: 'E' if lon_deg >= 0 else 'W',
                piexif.GPSIFD.GPSLongitude: _deg_to_dms_rational(abs(lon_deg)),
                piexif.GPSIFD.GPSAltitudeRef: 0 if alt >= 0 else 1,
                piexif.GPSIFD.GPSAltitude: (int(abs(alt) * 100), 100),
                piexif.GPSIFD.GPSSpeedRef: 'K'.encode('utf-8'),
                piexif.GPSIFD.GPSSpeed: (int(speed * 100), 100),
                piexif.GPSIFD.GPSImgDirectionRef: 'T'.encode('utf-8'),
                piexif.GPSIFD.GPSImgDirection: (int(direction * 10000), 10000),
                piexif.GPSIFD.GPSDateStamp: frame_date.date().strftime(EXIF_DT_FORMAT.split(' ')[0]),
                piexif.GPSIFD.GPSTimeStamp: ((frame_date.hour, 1), (frame_date.minute, 1), (frame_date.second, 1)),
                piexif.GPSIFD.GPSDestDistanceRef: 'K'.encode('utf-8'),
                piexif.GPSIFD.GPSDestDistance: float_value_as_rational(sample_time_ms),
            },
        }

        exif_bytes = piexif.dump(exif_dict)
        image_exif = io.BytesIO()
        piexif.insert(exif_bytes, image, image_exif)
        image_exif.seek(0)

        return image_exif.read(), {
            'ts': ts,
            'timezone_offset': '+00:00',
            'lat': correct_round(lat_deg),
            'lon': correct_round(lon_deg),
            'azimuth': direction,
            'speed': speed,
            'track_email': user_email,
        }

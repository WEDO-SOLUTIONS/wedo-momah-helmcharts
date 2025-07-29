import logging
import math
from io import BytesIO

import cv2
import numpy as np
import piexif
import utm
from PIL import Image
from shapely.geometry import Polygon

from signs_dashboard.models.bbox_detection import BBOXDetection
from signs_dashboard.models.frame import Frame
from signs_dashboard.services.frames_depth import FramesDepthService
from signs_dashboard.services.image import ImageService
from signs_dashboard.small_utils import detection_polygon_as_points

logger = logging.getLogger(__name__)


class DepthMapExtractionError(Exception):
    """Ошибка получения карты глубины."""


class FrameImageExtractionError(Exception):
    """Ошибка получения кадра."""


class DetectionLocalizationService:

    def __init__(
        self,
        image_service: ImageService,
        depth_map_service: FramesDepthService,
        s3_config: dict,
        naive_localization: bool,
    ):
        self._image_service = image_service
        self._depth_map_service = depth_map_service
        self.distance_min_thresh = 3
        self.distance_max_thresh = 40
        self._naive_localization = naive_localization

    def localize_frames(self, frames: list[Frame]) -> list[BBOXDetection]:
        if self._naive_localization:
            return sum(
                [
                    self._copy_frame_location_to_detections(frame)
                    for frame in frames
                ],
                [],
            )
        return self._localize_frames_via_depth_map(frames)

    def _localize_frames_via_depth_map(self, frames: list[Frame]) -> list[BBOXDetection]:
        updated_detections = []
        for frame in frames:
            img_pil, depth_map = self._download_frame_with_depth(frame)

            try:
                detections = self._localize(
                    frame=frame,
                    img=img_pil,
                    depth_map=depth_map,
                    detections=frame.detections,
                    distance_range=(self.distance_min_thresh, self.distance_max_thresh),
                )
            except Exception as exc:
                logger.exception(f'Frame {frame.id} not localized: {exc.__class__} {exc}')
                raise

            logger.debug(f'Frame {frame.id}: updating {len(updated_detections)} detections')
            updated_detections += detections
        return updated_detections

    def _download_frame_with_depth(self, frame: Frame) -> tuple[Image, np.ndarray]:
        try:
            img_pil = self._get_frame_pil_img(frame)
        except Exception as exc:
            raise FrameImageExtractionError from exc

        try:
            depth_map = self._depth_map_service.get_frame_depth_map(frame)
        except Exception as exc:
            raise DepthMapExtractionError from exc

        return img_pil, depth_map

    def _get_camera_params(self, img: Image) -> dict[str, float]:
        all_exif = piexif.load(img.info['exif']) if 'exif' in img.info else {}
        exif = all_exif.get('Exif')
        if not exif:
            raise ValueError('Unable to process image without exif!')

        focal_length_rational = exif[piexif.ExifIFD.FocalLength]
        sensor_w_rational = exif[piexif.ExifIFD.FocalPlaneXResolution]
        sensor_h_rational = exif[piexif.ExifIFD.FocalPlaneYResolution]

        focal_length = focal_length_rational[0] / focal_length_rational[1]
        sensor_w = sensor_w_rational[0] / sensor_w_rational[1]
        sensor_h = sensor_h_rational[0] / sensor_h_rational[1]
        return {
            'focal_length': focal_length,
            'sensor_w': sensor_w,
            'sensor_h': sensor_h,
        }

    def _get_detection_depth(self, depth_map: np.ndarray, polygon_points: list[int]) -> tuple[float, float, float]:
        poly_coords = detection_polygon_as_points(polygon_points)
        poly = Polygon(poly_coords)
        poly_center = poly.centroid

        map_h, map_w = depth_map.shape
        map_center_x, map_center_y = map_w // 2, map_h // 2

        trans_x = poly_center.x - map_center_x
        trans_y = poly_center.y - map_center_y
        mask = np.zeros_like(depth_map, dtype=np.uint8)

        cv2.fillPoly(mask, np.array([poly_coords]), 1)

        depth_centimeters = np.median(depth_map[np.where(mask == 1)]).item()

        return depth_centimeters / 100, trans_x, trans_y

    def _get_detection_angle(
        self,
        focal_length: float,
        sensor_w: float,
        sensor_h: float,
        img_w: int,
        img_h: int,
        trans_x: float,
        trans_y: float,
    ) -> tuple[float, float]:
        det_sensor_size_w = sensor_w * trans_x / img_w
        det_sensor_size_h = sensor_h * trans_y / img_h
        det_rel_angle_x = -math.atan2(det_sensor_size_w, focal_length)
        det_rel_angle_y = -math.atan2(det_sensor_size_h, focal_length)
        return det_rel_angle_x, det_rel_angle_y

    def _get_detection_lat_lon(
        self,
        depth: float,
        angle: float,
        lat: float,
        lon: float,
    ) -> tuple[float, float]:
        # TODO: check if zone changed
        utm_coords = utm.from_latlon(lat, lon)
        new_utm1 = utm_coords[0] + math.cos(angle) * depth
        new_utm2 = utm_coords[1] + math.sin(angle) * depth
        return utm.to_latlon(new_utm1, new_utm2, utm_coords[2], utm_coords[3])

    def _copy_frame_location_to_detections(self, frame: Frame) -> list[BBOXDetection]:
        detections_to_save = []

        for detection in frame.detections:
            if (detection.lat, detection.lon) == (frame.lat, frame.lon):
                continue
            detection.lat = frame.current_lat
            detection.lon = frame.current_lon
            detections_to_save.append(detection)

        return detections_to_save

    def _localize(  # noqa: WPS210
        self,
        frame: Frame,
        img: Image,
        depth_map: np.array,
        detections: list[BBOXDetection],
        distance_range: tuple[int, int],
    ) -> list[BBOXDetection]:
        detections_to_save = []
        camera_params = self._get_camera_params(img)
        logger.debug(f'Frame {frame.id} has {len(detections)} detections')
        for detection in detections:
            updated = self._localize_detection(
                frame=frame,
                img=img,
                detection=detection,
                depth_map=depth_map,
                distance_range=distance_range,
                camera_params=camera_params,
            )
            if updated:
                detections_to_save.append(detection)
        return detections_to_save

    def _localize_detection(
        self,
        frame: Frame,
        img: Image,
        detection: BBOXDetection,
        depth_map: np.array,
        distance_range: tuple[int, int],
        camera_params: dict[str, float],
    ) -> bool:
        if detection.lat and detection.lon:
            logger.debug(f'Localizing already localized detection {detection.id}')

        poly = self._build_poly_coords(detection)

        detection_depth, trans_x, trans_y = self._get_detection_depth(depth_map, poly)
        if detection_depth < distance_range[0] or detection_depth > distance_range[1]:
            logger.debug(f'Detection {detection.id} has {detection_depth}m not in {distance_range}')
            if detection.lat or detection.lon:
                detection.lat, detection.lon = None, None  # noqa: WPS414
                detection.detected_object_id = None
                return True
            return False

        det_angle_x, det_angle_y = self._get_detection_angle(
            img_w=img.width,
            img_h=img.height,
            trans_x=trans_x,
            trans_y=trans_y,
            **camera_params,
        )

        azimuth_rads = (90 - frame.azimuth) * math.pi / 180
        detection_abs_angle = azimuth_rads + det_angle_x
        detection_lat, detection_lon = self._get_detection_lat_lon(
            depth=detection_depth,
            angle=detection_abs_angle,
            lat=frame.current_lat,
            lon=frame.current_lon,
        )
        detection.lat = detection_lat
        detection.lon = detection_lon
        return True

    def _build_poly_coords(self, detection: BBOXDetection) -> list[int]:
        if detection.polygon:
            return detection.polygon
        x0, y0 = detection.x_from, detection.y_from
        x1, y1 = detection.x_to, detection.y_to
        return [x0, y0, x0, y1, x1, y1, x1, y0]

    def _get_frame_pil_img(self, frame: Frame) -> Image:
        img_data = self._image_service.download_image(frame)
        if not img_data:
            raise ValueError('Trying to download not uploaded frame image')
        return Image.open(BytesIO(img_data))

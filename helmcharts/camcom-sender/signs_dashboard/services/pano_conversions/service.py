import logging
from functools import lru_cache
from typing import Optional

import numpy as np
import shapely

from signs_dashboard.models.bbox_detection import BBOXDetection
from signs_dashboard.models.frame import Frame
from signs_dashboard.services.pano_conversions.common import CropsParams
from signs_dashboard.services.pano_conversions.from_equirectangular import equirectal_coords_to_perspective
from signs_dashboard.services.pano_conversions.from_perspective import perspective_coords_to_equirectal
from signs_dashboard.services.prediction_answer_parser import BBox
from signs_dashboard.small_utils import detection_polygon_as_points, detection_polygon_points_as_polygon

logger = logging.getLogger(__name__)
BBoxes = dict[str, list[BBox]]


class PanoramicConversionsService:
    def find_detections_from_crop(self, frame: Frame, theta: int, detector_name: str) -> list[BBOXDetection]:
        detections_from_crop = []
        for detection in frame.detections:
            if detection.detector_name != detector_name:
                continue
            if self._intersects_outline(detection, theta=theta):
                logger.info(
                    f'Frame {frame.id} detection {detection.as_json()} intersects crop outline {theta=}',
                )
                detections_from_crop.append(detection)
        return detections_from_crop

    def convert_detections_to_perspective_projection(
        self,
        detections: list[BBOXDetection],
        theta: int,
        convert_polygon: bool,
    ) -> list[BBOXDetection]:
        converted_detections = []
        for detection in detections:
            converted_coords = equirectal_coords_to_perspective(
                [
                    (detection.x_from, detection.y_from),
                    (detection.x_to, detection.y_to),
                ],
                theta=theta,
            )

            if len(converted_coords) != 2 or self._isnan_coords(converted_coords):
                continue

            point_from, point_to = converted_coords
            detection.x_from, detection.y_from = point_from[0], point_from[1]
            detection.width = point_to[0] - point_from[0]
            detection.height = point_to[1] - point_from[1]

            if convert_polygon and detection.polygon:
                detection.polygon = self._convert_polygon_points_to_perspective(detection.polygon, theta=theta)
                detection.polygon_cv2 = [detection_polygon_as_points(detection.polygon)]

            converted_detections.append(detection)
        return converted_detections

    def prepare_detections_for_equirectal_render(self, detections: list[BBOXDetection]):
        crop_width, crop_height = CropsParams.crop_size
        seam_width, seam_position = 1, crop_width / 2
        equirect_image_seam = shapely.Polygon([
            (seam_position - seam_width, 0),
            (seam_position - seam_width, crop_height),
            (seam_position + seam_width, crop_height),
            (seam_position + seam_width, 0),
            (seam_position - seam_width, 0),
        ])
        for detection in detections:
            if not detection.polygon:
                continue
            detection_polygon = shapely.Polygon(
                shell=equirectal_coords_to_perspective(
                    coords=detection_polygon_as_points(detection.polygon),
                    theta=CropsParams.SEAM_THETA,
                ),
            )
            if equirect_image_seam.intersects(detection_polygon):
                difference = detection_polygon.difference(equirect_image_seam)
                logger.debug(f'Frame {detection.frame_id} detection {detection.id} split by seam, got {difference}')
                detection.polygon_cv2 = self._shapely_polygon_as_equirectal_cv2(difference)
            else:
                detection.polygon_cv2 = [detection_polygon_as_points(detection.polygon)]

    def convert_bboxes_to_equirectangle_projection(self, bboxes: BBoxes, theta: int) -> BBoxes:
        return {
            label: [
                self._convert_bbox_to_equirectangle_projection(detection, theta=theta)
                for detection in detections
            ]
            for label, detections in bboxes.items()
        }

    def _convert_bbox_to_equirectangle_projection(self, bbox: BBox, theta: int) -> BBox:
        source_coords = [
            (bbox.xmin, bbox.ymin),
            (bbox.xmax, bbox.ymax),
        ]
        if bbox.polygon:
            source_coords += detection_polygon_as_points(bbox.polygon)
        coords = perspective_coords_to_equirectal(
            coords=source_coords,
            theta=theta,
        )
        equirectangle_bbox = bbox.copy(update={
            'xmin': coords[0][0],
            'ymin': coords[0][1],
            'xmax': coords[1][0],
            'ymax': coords[1][1],
        })
        if len(coords) > 2:
            equirectangle_bbox.polygon = detection_polygon_points_as_polygon(coords[2:])
        return equirectangle_bbox

    def _convert_polygon_points_to_perspective(self, polygon: list[int], theta: int) -> Optional[list[int]]:
        converted_polygon = equirectal_coords_to_perspective(
            detection_polygon_as_points(polygon),
            theta=theta,
        )
        if len(converted_polygon) > 2:
            return detection_polygon_points_as_polygon(converted_polygon)
        return None

    def _shapely_polygon_as_equirectal_cv2(self, difference: shapely.geometry.base.BaseGeometry):
        if isinstance(difference, shapely.geometry.base.BaseMultipartGeometry):
            return [
                perspective_coords_to_equirectal(polygon.exterior.coords, theta=CropsParams.SEAM_THETA)
                for polygon in difference.geoms
            ]
        if isinstance(difference, shapely.geometry.Polygon):
            return [
                perspective_coords_to_equirectal(difference.exterior.coords, theta=CropsParams.SEAM_THETA),
            ]
        logger.debug(f'Unsupported geometry type {type(difference)}')
        return None

    def _isnan_coords(self, coords: list[tuple[int, int]]) -> bool:
        return any(np.isnan(coord) or coord < 0 for point in coords for coord in point)

    def _intersects_outline(self, detection: BBOXDetection, theta: int) -> bool:
        outline = self._get_crop_boundary()
        polygon_coords = equirectal_coords_to_perspective(
            coords=[
                (detection.x_from, detection.y_from),
                (detection.x_to, detection.y_from),
                (detection.x_to, detection.y_to),
                (detection.x_from, detection.y_to),
                (detection.x_from, detection.y_from),
            ],
            theta=theta,
        )
        if self._isnan_coords(polygon_coords):
            return False

        det_polygon = shapely.Polygon(shell=polygon_coords)
        return shapely.intersects(outline, det_polygon)

    @lru_cache
    def _get_crop_boundary(self) -> shapely.Polygon:
        width, height = CropsParams.crop_size
        return shapely.Polygon(
            shell=[  # noqa: WPS317
                *[(0, height_i) for height_i in range(height)],
                *[(width_i, height) for width_i in range(width)],
                *[(width, height - height_i) for height_i in range(height)],
                *[(width - width_i, 0) for width_i in range(width)],
                (0, 0),
            ],
        )

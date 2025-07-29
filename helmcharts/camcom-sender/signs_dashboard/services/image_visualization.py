import base64
import os
from enum import Enum
from typing import Optional

import cv2
import numpy as np
from PIL import Image, ImageDraw, ImageFont

from signs_dashboard.models.bbox_detection import BBOXDetection
from signs_dashboard.models.frame import Frame
from signs_dashboard.models.twogis_pro_filters import Locale
from signs_dashboard.services.image import ImageService
from signs_dashboard.services.pano_conversions.service import PanoramicConversionsService
from signs_dashboard.services.translations import TranslationsService
from signs_dashboard.small_utils import detection_polygon_as_points

INFO_PANEL_WIDTH = 560
HSHIFT = 32
FONT_SIZE = 16
APPROXIMATE_TEXT_VSIZE = 80
VSHIFT_LABELS_STEP = 5
LINE_TRANSPARENCY = 0.7
POLYGON_OVERLAY_TRANSPARENCY = 0.3
IMAGE_FONT_PATH = os.getenv('IMAGE_FONT_PATH', '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf')
PANEL_FONT_PATH = os.getenv('PANEL_FONT_PATH', '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf')

image_font = ImageFont.truetype(IMAGE_FONT_PATH, FONT_SIZE)
panel_font = ImageFont.truetype(PANEL_FONT_PATH, FONT_SIZE)


class ColorBGRFormat(Enum):
    WHITE = (255, 255, 255)
    RED = (0, 0, 255)
    GREEN = (0, 255, 0)
    BLUE = (255, 191, 0)
    YELLOW = (0, 255, 255)


TextLine = tuple[str, tuple[int, int], ImageFont.FreeTypeFont, ColorBGRFormat]


class ImageVisualizationService:
    def __init__(
        self,
        image_service: ImageService,
        translations_service: TranslationsService,
        panoramic_conversions_service: PanoramicConversionsService,
    ):
        self._image_service = image_service
        self._translations_service = translations_service
        self._panoramic_conversions_service = panoramic_conversions_service

        self._vshift = 0
        self._curr_horizontal = 0

    def get_crop_base64(self, detection: BBOXDetection) -> Optional[str]:
        img = self._image_service.download_image_as_ndarray(detection.frame)
        if img is None:
            return None

        crop = _get_crop(img, detection)
        return _encode_img_base64(crop)

    def get_visualized_image(
        self,
        frame: Frame,
        locale: Optional[Locale],
        show_info: bool = True,
        render_translations: bool = True,
        detection_ids: Optional[list[int]] = None,
        theta: Optional[int] = None,
        render_polygons: bool = False,
    ) -> Optional[bytes]:
        img = self._image_service.download_image_as_ndarray(frame, theta=theta)
        if img is None:
            return None

        detections = frame.detections
        if detection_ids:
            detections = [
                detection
                for detection in detections
                if detection.id in detection_ids
            ]

        detections = self._prepare_detections(
            detections,
            theta=theta,
            render_polygons=render_polygons,
            panoramic=frame.panoramic,
        )

        img = self._visualize_detections(
            img,
            detections=detections,
            locale=locale,
            show_info=show_info,
            render_translations=render_translations,
            render_polygons=render_polygons,
        )
        return cv2.imencode('.jpg', img)[1]

    def _prepare_detections(
        self,
        detections: list[BBOXDetection],
        theta: Optional[int],
        render_polygons: bool,
        panoramic: bool,
    ) -> list[BBOXDetection]:
        if render_polygons and theta is None:
            if panoramic:
                self._panoramic_conversions_service.prepare_detections_for_equirectal_render(detections)
            else:
                self._convert_detections_polygons(detections)
        elif theta is not None:
            detections = self._panoramic_conversions_service.convert_detections_to_perspective_projection(
                detections=detections,
                theta=theta,
                convert_polygon=render_polygons,
            )
        return detections

    def _convert_detections_polygons(self, detections: list[BBOXDetection]):
        for detection in detections:
            if not detection.polygon:
                continue
            detection.polygon_cv2 = [detection_polygon_as_points(detection.polygon)]

    def _visualize_detections(
        self,
        img: np.ndarray,
        detections: list[BBOXDetection],
        locale: Optional[Locale],
        show_info,
        render_translations,
        render_polygons,
    ) -> np.ndarray:
        if show_info:
            lines, info_panel = self._construct_lines_and_info_panel(img, detections, render_polygons=render_polygons)
            img = np.hstack((img, info_panel))
            img = cv2.addWeighted(img, 1, lines, LINE_TRANSPARENCY, 0)
        else:
            signs_texts = []
            for detection in detections:
                text = self._get_sign_text(detection, locale, render_translations=render_translations)
                if text is None:
                    continue

                img, textline = self._draw_detection(
                    img,
                    detection=detection,
                    text=text,
                    render_polygons=render_polygons,
                )
                signs_texts.append(textline)

            img = self._draw_texts(img, signs_texts)

        return img

    def _draw_detection(
        self,
        img: np.ndarray,
        detection: BBOXDetection,
        text: str,
        render_polygons: bool,
    ) -> tuple[np.ndarray, TextLine]:
        if detection.is_ai is False:
            box_color = ColorBGRFormat.BLUE
            font_color = ColorBGRFormat.BLUE
        else:
            box_color = ColorBGRFormat.GREEN
            font_color = ColorBGRFormat.YELLOW

        img = self._highlight_detection(img, detection, render_polygons=render_polygons, color=box_color)

        text_coords = detection.x_from, detection.y_from - 6
        return img, (text, text_coords, image_font, font_color)

    def _highlight_detection(
        self,
        img: np.ndarray,
        detection: BBOXDetection,
        render_polygons: bool,
        color: ColorBGRFormat,
    ) -> np.ndarray:
        if render_polygons and detection.polygon_cv2:
            img = _draw_polygon(img, detection, color)
        else:
            _draw_box(img, detection, color)
        return img

    def _get_sign_text(
        self,
        detection: BBOXDetection,
        locale: Locale,
        render_translations: bool,
    ) -> Optional[str]:
        if locale and render_translations:
            return self._translations_service.get_translation_for_type(detection.label, locale)
        prob = f'{detection.prob:.3f}'.rstrip('0')
        return f'{detection.label} {prob}'

    def _construct_lines_and_info_panel(
        self,
        img: np.ndarray,
        detections: list[BBOXDetection],
        render_polygons: bool,
    ) -> tuple[np.ndarray, np.ndarray]:
        img_height, img_width, _ = img.shape
        lines = np.zeros((img_height, img_width + INFO_PANEL_WIDTH, 3), dtype=img.dtype)
        info_panel = np.zeros((img_height, INFO_PANEL_WIDTH, 3), dtype=img.dtype)
        self._vshift = 0
        texts = []
        for detection in detections:
            self._curr_horizontal = HSHIFT
            lines = self._highlight_detection(
                lines,
                detection=detection,
                render_polygons=render_polygons,
                color=ColorBGRFormat.GREEN,
            )
            self._draw_line(lines, detection, img_width=img_width)
            panel_text = self._put_info_panel_text(detection.get_info_as_str(), img_height)
            texts.extend(panel_text)
            if self._vshift + APPROXIMATE_TEXT_VSIZE > img_height:
                break
        info_panel = self._draw_texts(info_panel, texts)
        return lines, info_panel

    def _draw_line(self, lines: np.ndarray, detection: BBOXDetection, img_width: int) -> None:
        line_coords_from = (
            detection.x_from + detection.width,
            detection.y_from + detection.height // 2,
        )
        line_coords_to = (img_width + HSHIFT, self._vshift + (APPROXIMATE_TEXT_VSIZE // 2))
        cv2.line(lines, line_coords_from, line_coords_to, ColorBGRFormat.GREEN.value, 2)

    def _put_info_panel_text(self, text: str, img_height: float) -> list[TextLine]:
        texts = []
        for line in text.split('\n'):
            self._vshift += FONT_SIZE
            if self._vshift + APPROXIMATE_TEXT_VSIZE // 2 > img_height:
                break

            segm_text_coords = (self._curr_horizontal, self._vshift)
            texts.append((line, segm_text_coords, panel_font, ColorBGRFormat.WHITE))
            self._vshift += VSHIFT_LABELS_STEP
        self._vshift += 3 * VSHIFT_LABELS_STEP
        return texts

    def _draw_texts(self, img: np.ndarray, texts: list[TextLine]) -> np.ndarray:
        img_pil = Image.fromarray(img)
        draw = ImageDraw.Draw(img_pil)
        for text, coords, font, color in texts:
            draw.text((coords[0], coords[1] - FONT_SIZE), text, font=font, fill=color.value)
        return np.array(img_pil)


def _get_crop(img: np.ndarray, detection: BBOXDetection) -> np.ndarray:
    return img[
        detection.y_from:detection.y_to,
        detection.x_from:detection.x_to,
    ]


def _draw_box(img: np.ndarray, detection: BBOXDetection, color: ColorBGRFormat) -> None:
    x_from, y_from, x_to, y_to = detection.x_from, detection.y_from, detection.x_to, detection.y_to
    y_max, x_max, _ = img.shape
    draw_args = {
        'img': img,
        'color': color.value,
        'thickness': 2,
    }
    if x_from > x_to:
        lines = [
            ((x_from, y_from), (x_max, y_from)),
            ((x_from, y_from), (x_from, y_to)),
            ((x_from, y_to), (x_max, y_to)),
            ((0, y_from), (x_to, y_from)),
            ((0, y_to), (x_to, y_to)),
            ((x_to, y_from), (x_to, y_to)),
        ]
        for pt1, pt2 in lines:
            cv2.line(pt1=pt1, pt2=pt2, **draw_args)
    else:
        cv2.rectangle(
            pt1=(x_from, y_from),
            pt2=(x_to, y_to),
            **draw_args,
        )


def _draw_polygon(
    img: np.ndarray,
    detection: BBOXDetection,
    color: ColorBGRFormat = ColorBGRFormat.GREEN,
    alpha: float = POLYGON_OVERLAY_TRANSPARENCY,
) -> np.ndarray:
    overlay = img.copy()
    for polygon in detection.polygon_cv2:
        cv2.fillPoly(overlay, np.array([polygon], dtype=np.int32), color.value)
    return cv2.addWeighted(overlay, alpha, img, 1 - alpha, 0)


def _encode_img_base64(img: np.ndarray) -> str:
    jpg_encoded = cv2.imencode('.jpg', img)[1]
    return base64.b64encode(jpg_encoded).decode()

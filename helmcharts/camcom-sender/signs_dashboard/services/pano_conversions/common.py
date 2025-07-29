from dataclasses import dataclass
from functools import lru_cache

import cv2
import numpy as np


@dataclass(frozen=True)
class CropsParams:
    FOV_X: float = 90
    FOV_Y: float = 90
    PHI: float = 15
    SCALE_FACTOR: float = 1000
    VIDEO360_WIDTH: int = 3840
    VIDEO360_HEIGHT: int = 1920
    PIXEL_SIZE_X = 0.05
    PIXEL_SIZE_Y = 0.05
    SEAM_THETA: int = 180
    CROPS_Z_POSITIONS: tuple[int] = (0, 90, SEAM_THETA, 270)

    @classmethod
    @property
    def crop_size(cls) -> tuple[int, int]:
        """
        Calculate dimensions for the perspective image

        Returns:
            Width and height for perspective image.
        """
        wd = int(2 * np.tan(np.radians(cls.FOV_X / 2.0)) * cls.SCALE_FACTOR)
        hd = int(2 * np.tan(np.radians(cls.FOV_Y / 2.0)) * cls.SCALE_FACTOR)
        return wd, hd


@lru_cache
def calculate_equirectal_params(
    theta: float,
    fov_x: float = CropsParams.FOV_X,
    fov_y: float = CropsParams.FOV_Y,
    phi: float = CropsParams.PHI,
) -> tuple[int, int, np.ndarray, np.ndarray, float, float, float, float]:
    """
    Calculate conversion params for perspective and equirectangular conversions.

    Args:
        fov_x: Horizontal field of view in degrees.
        fov_y: Vertical field of view in degrees.
        theta: Rotation around the Z-axis in degrees.
        phi: Rotation around the Y-axis in degrees.

    Returns:
        Perspective crop dimensions,
        rotation matrices for THETA and PHI,
        center coordinates for the perspective image,
        width and height intervals based on FOV.
    """
    # Calculate dimensions for the perspective image
    wd, hd = CropsParams.crop_size

    # Calculate the center coordinates of the perspective image
    c_x = wd / 2.0
    c_y = hd / 2.0

    # Calculate width and height intervals based on FOV
    w_len = 2 * np.tan(np.radians(fov_x / 2.0))
    w_interval = w_len / wd
    h_len = 2 * np.tan(np.radians(fov_y / 2.0))
    h_interval = h_len / hd

    # Define rotation axes
    y_axis = np.array([0.0, 1.0, 0.0], np.float32)
    z_axis = np.array([0.0, 0.0, 1.0], np.float32)

    # Calculate rotation matrices for THETA and PHI
    R1, _ = cv2.Rodrigues(z_axis * np.radians(theta))
    R2, _ = cv2.Rodrigues(np.dot(R1, y_axis) * np.radians(-phi))

    return wd, hd, R1, R2, c_x, c_y, w_interval, h_interval

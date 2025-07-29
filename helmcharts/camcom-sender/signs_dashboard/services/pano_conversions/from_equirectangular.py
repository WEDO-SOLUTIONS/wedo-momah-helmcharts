from functools import lru_cache

import cv2
import numpy as np

from signs_dashboard.services.pano_conversions.common import CropsParams, calculate_equirectal_params


@lru_cache(maxsize=4)
def _crop_remap_params(
    theta: float,
    equ_h: int,
    equ_w: int,
) -> tuple[np.ndarray, np.ndarray, float, float]:
    """
    Generates remap parameters for projecting the equirectangular image to the perspective image at given rotation.

    Args:
        equ_h: Equirectangular image height
        equ_w: Equirectangular image width
        theta: Rotation around the Z-axis in degrees.

    Returns:
        Remap parameters.
    """
    wd, hd, R1, R2, c_x, c_y, w_interval, h_interval = calculate_equirectal_params(theta=theta)
    # Calculate the center coordinates of the equirectangular image
    equ_cx = equ_w / 2.0
    equ_cy = equ_h / 2.0
    # Create mapping grids
    x_map = np.zeros([hd, wd], np.float32) + 1
    y_map = np.tile((np.arange(0, wd) - c_x) * w_interval, [hd, 1])
    z_map = -np.tile((np.arange(0, hd) - c_y) * h_interval, [wd, 1]).T
    # Calculate distance to each pixel
    D = np.sqrt(x_map ** 2 + y_map ** 2 + z_map ** 2)
    # Create XYZ coordinates in the camera space
    xyz = np.zeros([hd, wd, 3], np.float32)
    xyz[:, :, 0] = x_map / D
    xyz[:, :, 1] = y_map / D
    xyz[:, :, 2] = z_map / D
    # Apply the rotations to the XYZ coordinates
    xyz = xyz.reshape([hd * wd, 3]).T
    xyz = np.dot(R1, xyz)
    xyz = np.dot(R2, xyz).T
    # Map to spherical coordinates
    lat = np.arcsin(xyz[:, 2])
    lon = np.arctan2(xyz[:, 1], xyz[:, 0])
    # Convert spherical coordinates to image coordinates
    lon = lon.reshape([hd, wd]) / np.pi * 180
    lat = -lat.reshape([hd, wd]) / np.pi * 180
    lon = lon / 180 * equ_cx + equ_cx
    lat = lat / 90 * equ_cy + equ_cy
    # Remap the equirectangular image to the perspective image
    return lat.astype(np.float32), lon.astype(np.float32), wd, hd


def equirectal_image_to_perspective_crop(
    img: np.ndarray,
    theta: float,
    fov_x: float = CropsParams.FOV_X,
    fov_y: float = CropsParams.FOV_Y,
) -> tuple[np.ndarray, float, float]:
    """
    Projects the equirectangular image to the perspective image at given rotation.

    Args:
        img: Equirectangular image.
        theta: Rotation around the Z-axis in degrees.

    Returns:
        Perspective image.
    """
    # Get the height and width of the equirectangular image
    equ_h, equ_w = img.shape[:2]
    # Get the remap parameters
    lat, lon, wd, hd = _crop_remap_params(theta=theta, equ_h=equ_h, equ_w=equ_w)
    # Remap the equirectangular image to the perspective image
    persp = cv2.remap(
        img,
        lon.astype(np.float32),
        lat.astype(np.float32),
        cv2.INTER_CUBIC,
        borderMode=cv2.BORDER_WRAP,
    )

    focal_length_x = (wd / 2.0) / np.tan(np.radians(fov_x / 2.0))
    focal_length_y = (hd / 2.0) / np.tan(np.radians(fov_y / 2.0))

    return persp, focal_length_x, focal_length_y


def equirectal_coords_to_perspective(
    coords: list[tuple[int, int]],
    theta: float,
    equ_w: int = CropsParams.VIDEO360_WIDTH,
    equ_h: int = CropsParams.VIDEO360_HEIGHT,
) -> list[tuple[int, int]]:
    """
    Projects coordinates from the equirectangular image to the perspective image.

    Args:
        coords: List of (x_e, y_e) coordinates in the equirectangular image.
        theta: Rotation around the Z-axis in degrees.
        equ_w: Width of the equirectangular image.
        equ_h: Height of the equirectangular image.

    Returns:
        List of (x_p, y_p) coordinates in the perspective image.
    """
    wd, hd, R1, R2, c_x, c_y, w_interval, h_interval = calculate_equirectal_params(theta=theta)

    # Prepare the combined rotation matrix
    R = np.dot(R2, R1)

    # Compute inverse rotation matrix
    R_inv = R.T

    # Equirectangular image center coordinates
    equ_cx = equ_w / 2.0
    equ_cy = equ_h / 2.0

    # List to hold mapped coordinates
    mapped_coords = []

    for x_e, y_e in coords:
        # Convert equirectangular coordinates to spherical coordinates
        lon = (x_e - equ_cx) / equ_cx * 180.0
        lat = -(y_e - equ_cy) / equ_cy * 90.0

        # Convert degrees to radians
        lon_rad = np.radians(lon)
        lat_rad = np.radians(lat)

        # Convert spherical coordinates to Cartesian coordinates
        x = np.cos(lat_rad) * np.cos(lon_rad)
        y = np.cos(lat_rad) * np.sin(lon_rad)
        z = np.sin(lat_rad)

        # Apply inverse rotation
        xyz_rot = np.dot(R_inv, np.array([x, y, z]))

        # Check if point is in front of the camera (x > 0)
        if xyz_rot[0] <= 0:
            # The point is behind the camera, cannot be projected
            mapped_coords.append((np.nan, np.nan))
            continue

        # Compute image coordinates
        y_map = xyz_rot[1] / xyz_rot[0]
        z_map = xyz_rot[2] / xyz_rot[0]

        x_p = int(c_x + y_map / w_interval)
        y_p = int(c_y - z_map / h_interval)

        mapped_coords.append((x_p, y_p))

    return mapped_coords

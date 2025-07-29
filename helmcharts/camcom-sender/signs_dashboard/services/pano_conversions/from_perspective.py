import numpy as np

from signs_dashboard.services.pano_conversions.common import CropsParams, calculate_equirectal_params


def perspective_coords_to_equirectal(
    coords: list[tuple[int, int]],
    theta: float,
    equ_w: int = CropsParams.VIDEO360_WIDTH,
    equ_h: int = CropsParams.VIDEO360_HEIGHT,
) -> list[tuple[int, int]]:
    """
    Projects coordinates from the perspective image back to the equirectangular image.

    Args:
        coords: List of (x_p, y_p) coordinates in the perspective image.
        theta: Rotation around the Z-axis in degrees.
        equ_w: Width of the equirectangular image.
        equ_h: Height of the equirectangular image.

    Returns:
        List of (x_e, y_e) coordinates in the equirectangular image.
    """
    wd, hd, R1, R2, c_x, c_y, w_interval, h_interval = calculate_equirectal_params(theta=theta)

    # Prepare the combined rotation matrix
    R = np.dot(R2, R1)

    # Equirectangular image center coordinates
    equ_cx = equ_w / 2.0
    equ_cy = equ_h / 2.0

    # List to hold mapped coordinates
    mapped_coords = []

    for x_p, y_p in coords:
        # Compute x_map, y_map, z_map for the given perspective coordinates
        x_map = 1.0  # Since x_map is always 1 in the perspective image
        y_map = (x_p - c_x) * w_interval
        z_map = -(y_p - c_y) * h_interval

        # Calculate the distance D to the pixel
        D = np.sqrt(x_map ** 2 + y_map ** 2 + z_map ** 2)

        # Normalize the coordinates
        x = x_map / D
        y = y_map / D
        z = z_map / D

        # Apply the combined rotation matrix
        xyz = np.dot(R, np.array([x, y, z]))

        # Convert to spherical coordinates
        lat = np.arcsin(xyz[2])  # Latitude
        lon = np.arctan2(xyz[1], xyz[0])  # Longitude

        # Convert spherical coordinates to image coordinates
        lon = lon / np.pi * 180
        lat = -lat / np.pi * 180

        x_e = int(lon / 180 * equ_cx + equ_cx)
        y_e = int(lat / 90 * equ_cy + equ_cy)

        mapped_coords.append((x_e, y_e))

    return mapped_coords

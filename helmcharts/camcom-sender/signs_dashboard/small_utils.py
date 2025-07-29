import logging
import math
import os
import typing as tp
from datetime import datetime, timedelta

from flask import url_for

FORM_DATE_FORMAT = '%d-%m-%Y'  # noqa: WPS323 это формат, который принимает datetime
T = tp.TypeVar('T')  # noqa: WPS111
PointsList = list[tuple[int, int]]
logger = logging.getLogger(__name__)


def get_value(dict_, key, default):
    # нельзя менять на стандартный .get дикта, т.к. обрабатывает nullable значения
    return dict_[key] if dict_.get(key) else default


def get_form_date_from(date_from: tp.Optional[str] = None) -> tp.Optional[datetime.date]:
    if date_from is None:
        return (datetime.now() - timedelta(days=1)).date()
    try:
        return datetime.strptime(date_from, FORM_DATE_FORMAT).date()
    except Exception:
        return None


def get_form_date_to(date_to: tp.Optional[str] = None) -> tp.Optional[datetime.date]:
    if date_to is None:
        return (datetime.now() + timedelta(days=1)).date()
    try:
        return (datetime.strptime(date_to, FORM_DATE_FORMAT) + timedelta(days=1)).date()
    except Exception:
        return None


def get_str_date_from(dt_from: tp.Optional[datetime] = None):
    if not dt_from:
        return ''
    return dt_from.strftime(FORM_DATE_FORMAT)


def get_str_date_to(dt_to: tp.Optional[datetime] = None):
    if not dt_to:
        return ''
    return (dt_to - timedelta(days=1)).strftime(FORM_DATE_FORMAT)


def correct_round(float_value, rank: int = 12):
    big_value = float_value * (10 ** rank)

    if big_value - math.floor(big_value) < 0.5:  # noqa: WPS459
        rounded = math.floor(big_value)
    else:
        rounded = math.ceil(big_value)
    return round(rounded * (10 ** (-rank)), rank)


def batch_iterator(elements: tp.List[T], batch_size: int) -> tp.Iterator[tp.List[T]]:
    yield from (
        elements[idx:idx + batch_size] for idx in range(0, len(elements), batch_size)
    )


def timezone_offset_str(offset: timedelta) -> str:
    seconds = abs(offset.total_seconds())
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    sign = '+' if offset.total_seconds() >= 0 else '-'
    return f'{sign}{hours:02.0f}:{minutes:02.0f}'


def placeholder_url_for(
    endpoint: str,
    placeholder: str,
    placeholder_field: str,
    **values: tp.Any,
) -> str:
    internal_placeholder = '123456789'
    return url_for(
        endpoint,
        **{placeholder_field: internal_placeholder},
        **values,
    ).replace(internal_placeholder, placeholder)


def detection_polygon_as_points(polygon: list[int]) -> tp.Optional[PointsList]:
    return [
        (polygon[idx], polygon[idx + 1])
        for idx in range(0, len(polygon), 2)
    ]


def detection_polygon_points_as_polygon(polygon: PointsList) -> tp.Optional[list[int]]:
    return [
        coord_part
        for coord in polygon
        for coord_part in coord
    ]


def uniques_preserving_order(iterable: tp.Iterable[str]) -> list[str]:
    result = []
    for item in iterable:
        if item not in result:
            result.append(item)
    return result


def parse_timedelta_from_minutes_env_var(env_var_name: str, default: timedelta) -> timedelta:
    try:
        return timedelta(minutes=int(os.environ.get(env_var_name)))
    except Exception:
        return default

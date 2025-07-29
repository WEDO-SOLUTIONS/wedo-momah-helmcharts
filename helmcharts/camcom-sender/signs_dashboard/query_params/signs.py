import typing as tp
from dataclasses import dataclass
from datetime import datetime

from flask import Request

from signs_dashboard.small_utils import get_form_date_from, get_form_date_to, get_str_date_from, get_str_date_to


class RangeParam:
    def __init__(
        self,
        *,
        from_value: tp.Union[int, float, None],
        to_value: tp.Union[int, float, None],
        min_value: tp.Union[int, float, None] = None,
        max_value: tp.Union[int, float, None] = None,
    ):
        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError('Max permitted value should be greater than min permitted value')

        self._min_value = min_value
        self._max_value = max_value

        self._from_value = self._set_from_value(from_value)
        self._to_value = self._set_to_value(to_value)

    @property
    def from_value(self) -> tp.Union[int, float, None]:
        if self._from_value == self._min_value:
            return None
        return self._from_value

    @property
    def to_value(self) -> tp.Union[int, float, None]:
        if self._to_value == self._max_value:
            return None
        return self._to_value

    def _set_from_value(
        self, from_value: tp.Union[int, float, None],
    ) -> tp.Union[int, float, None]:
        if from_value is not None:
            if self._min_value is not None:
                self._from_value = max(from_value, self._min_value)
            if self._max_value is not None:
                from_value = min(from_value, self._max_value)
        return from_value

    def _set_to_value(
        self, to_value: tp.Union[int, float, None],
    ) -> tp.Union[int, float, None]:
        if to_value is not None:
            if self._from_value is not None:
                to_value = max(to_value, self._from_value)
            if self._max_value is not None:
                to_value = min(to_value, self._max_value)
        return to_value


@dataclass
class SignsQueryParameters:
    label: tp.Optional[str]
    detector_name: tp.Optional[str]
    is_tmp: tp.Optional[bool]
    sign_value: tp.Optional[float]
    prob_range: RangeParam
    is_side_prob_range: RangeParam
    width_range: RangeParam
    height_range: RangeParam
    x_from_range: RangeParam
    y_from_range: RangeParam
    from_dt: tp.Optional[datetime] = None
    to_dt: tp.Optional[datetime] = None
    is_regex_on: bool = False

    @property
    def from_date(self):
        return get_str_date_from(self.from_dt)

    @property
    def to_date(self):
        return get_str_date_to(self.to_dt)

    @classmethod
    def from_request(cls, request: Request) -> 'SignsQueryParameters':
        label = request.args.get('label')
        label = label if label else None

        detector_name = request.args.get('detector_name')
        detector_name = detector_name if detector_name else None

        is_tmp = request.args.get('is_tmp')
        is_tmp = is_tmp.lower() == 'true' if is_tmp else None

        prob_range = _get_range(request, key_suffix='prob', min_value=0, max_value=1, dtype=float)
        is_side_prob_range = _get_range(request, key_suffix='is_side_prob', min_value=0, max_value=1, dtype=float)
        width_range = _get_range(request, key_suffix='width', min_value=1, dtype=int)
        height_range = _get_range(request, key_suffix='height', min_value=1, dtype=int)
        x_from_range = _get_range(request, key_suffix='x_from', min_value=0, dtype=int)
        y_from_range = _get_range(request, key_suffix='y_from', min_value=0, dtype=int)

        return cls(
            label=label,
            is_tmp=is_tmp,
            sign_value=request.args.get('sign_value', type=float),
            prob_range=prob_range,
            is_side_prob_range=is_side_prob_range,
            width_range=width_range,
            height_range=height_range,
            x_from_range=x_from_range,
            y_from_range=y_from_range,
            from_dt=get_form_date_from(request.args.get('from_date')),
            to_dt=get_form_date_to(request.args.get('to_date')),
            is_regex_on=request.args.get('is_regex', default=False, type=bool),
            detector_name=detector_name,
        )


def _get_range(
    request: Request,
    *,
    key_suffix: str,
    min_value: tp.Union[int, float, None] = None,
    max_value: tp.Union[int, float, None] = None,
    dtype: tp.Type = float,
) -> RangeParam:
    from_value = request.args.get(f'from_{key_suffix}', type=dtype)
    to_value = request.args.get(f'to_{key_suffix}', type=dtype)
    return RangeParam(
        from_value=from_value,
        to_value=to_value,
        min_value=min_value,
        max_value=max_value,
    )

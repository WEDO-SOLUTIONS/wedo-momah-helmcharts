import datetime
import typing as tp
from dataclasses import dataclass

from flask import Request

from signs_dashboard.small_utils import get_form_date_from, get_form_date_to, get_str_date_from, get_str_date_to


@dataclass
class DumpQueryParameters:
    label: tp.Optional[str]
    is_tmp: tp.Optional[bool]
    from_dt: tp.Optional[datetime.date] = None
    to_dt: tp.Optional[datetime.date] = None

    @property
    def from_date(self):
        return get_str_date_from(self.from_dt)

    @property
    def to_date(self):
        return get_str_date_to(self.to_dt)

    @property
    def dates(self) -> tp.List[datetime.date]:
        if (self.to_dt - self.from_dt).days == 1:
            return [self.from_dt]
        return [
            self.from_dt + datetime.timedelta(days=n_days)
            for n_days in range((self.to_dt - self.from_dt).days - 1)
        ]

    @classmethod
    def from_request(cls, request: Request) -> 'DumpQueryParameters':
        label = request.args.get('label')
        label = label if label else None

        is_tmp = request.args.get('is_tmp')
        is_tmp = is_tmp.lower() == 'true' if is_tmp else None

        return cls(
            label=label,
            is_tmp=is_tmp,
            from_dt=get_form_date_from(request.args.get('from_date')),
            to_dt=get_form_date_to(request.args.get('to_date')),
        )

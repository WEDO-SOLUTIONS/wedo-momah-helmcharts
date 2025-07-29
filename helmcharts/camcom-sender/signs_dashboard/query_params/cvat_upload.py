import typing as tp
from dataclasses import dataclass
from datetime import datetime

from flask import Request

from signs_dashboard.small_utils import get_form_date_from, get_form_date_to, get_str_date_from, get_str_date_to


@dataclass
class CVATUploadQueryParams:
    from_dt: tp.Optional[datetime]
    to_dt: tp.Optional[datetime]

    @property
    def from_date(self):
        return get_str_date_from(self.from_dt)

    @property
    def to_date(self):
        return get_str_date_to(self.to_dt)

    @classmethod
    def from_request(cls, request: Request) -> 'CVATUploadQueryParams':
        args = request.args
        return cls(
            from_dt=get_form_date_from(args.get('from_date')),
            to_dt=get_form_date_to(args.get('to_date')),
        )

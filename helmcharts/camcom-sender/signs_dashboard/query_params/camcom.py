import datetime
from dataclasses import dataclass

from signs_dashboard.small_utils import get_form_date_from, get_form_date_to, get_str_date_from, get_str_date_to


@dataclass
class CamComStatsQueryParams:
    from_dt: datetime.date
    to_dt: datetime.date

    @property
    def from_date(self):
        return get_str_date_from(self.from_dt)

    @property
    def to_date(self):
        return get_str_date_to(self.to_dt)

    @classmethod
    def from_request(cls, request):
        return cls(
            from_dt=get_form_date_from(request.args.get('from_date')),
            to_dt=get_form_date_to(request.args.get('to_date')),
        )

import typing as tp
from dataclasses import dataclass
from datetime import datetime

from signs_dashboard.small_utils import get_form_date_from, get_form_date_to, get_str_date_from, get_str_date_to


@dataclass
class ForeignTrackReloadQueryParams:
    endpoint: str
    uuids: tp.List[str]
    fiji_host: str
    verify_ssl: bool
    show_fiji_host_input: bool
    show_predictions_reload_toggle: bool
    with_detections: bool


@dataclass
class TracksReloadQueryParams:
    from_dt: tp.Optional[datetime] = None
    to_dt: tp.Optional[datetime] = None
    status: str = 'all'

    @classmethod
    def from_request(cls, request):
        return cls(
            from_dt=get_form_date_from(request.args.get('from_date')),
            to_dt=get_form_date_to(request.args.get('to_date')),
            status=request.args.get('upload_status'),
        )

    @property
    def from_date(self):
        return get_str_date_from(self.from_dt)

    @property
    def to_date(self):
        return get_str_date_to(self.to_dt)

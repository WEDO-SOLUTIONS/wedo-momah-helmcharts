from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from signs_dashboard.small_utils import get_form_date_from, get_form_date_to, get_str_date_from, get_str_date_to


@dataclass
class TrackQueryParameters:
    interest_zone_regions: list[int]
    date_type: str = 'uploaded'
    from_dt: Optional[datetime] = None
    to_dt: Optional[datetime] = None
    email: str = ''
    type: str = ''
    status: list[int] = field(default_factory=list)
    reloaded: str = ''
    upload_status: str = ''
    status_field: str = 'fiji_status'
    format: str = 'html'
    app_version: str = ''
    map_matching_status: list[int] = field(default_factory=list)
    localization_status: list[int] = field(default_factory=list)

    @property
    def from_date(self):
        return get_str_date_from(self.from_dt)

    @property
    def to_date(self):
        return get_str_date_to(self.to_dt)

    def __str__(self):
        return '_'.join((self.email, self.from_date, self.to_date))

    def to_uri(self, **kwargs):
        uri_params = {
            'date_type': self.date_type,
            'from_date': self.from_date,
            'to_date': self.to_date,
            'email': self.email,
            'track_type': self.type,
            'reloaded': self.reloaded,
            'upload_status': self.upload_status,
            'status_field': self.status_field,
            'format': self.format,
            'interest_zone_regions': self.interest_zone_regions,
            **kwargs,
        }
        status = uri_params.get('status', self.status)
        uri_params['status'] = ','.join(map(str, status))
        mm_status = uri_params.get('map_matching_status', self.map_matching_status)
        uri_params['map_matching_status'] = ','.join(map(str, mm_status))

        localization_status = uri_params.get('localization_status', self.localization_status)
        uri_params['localization_status'] = ','.join(map(str, localization_status))

        return uri_params

    @classmethod
    def from_request(cls, request):
        date_type = request.args.get('date_type', 'uploaded')
        if date_type not in {'uploaded', 'recorded'}:
            date_type = 'uploaded'

        return cls(
            format=request.args.get('format', 'html'),
            date_type=date_type,
            from_dt=get_form_date_from(request.args.get('from_date')),
            to_dt=get_form_date_to(request.args.get('to_date')),
            email=request.args.get('email', ''),
            type=request.args.get('track_type', 'mobile'),
            status=_status_str_to_list(request.args.get('status')),
            reloaded=request.args.get('reloaded', ''),
            upload_status=request.args.get('upload_status', ''),
            status_field=request.args.get('status_field', 'fiji_status'),
            app_version=request.args.get('app_version', ''),
            interest_zone_regions=request.args.getlist('interest_zone_regions', type=int),
            map_matching_status=_status_str_to_list(request.args.get('map_matching_status')),
            localization_status=_status_str_to_list(request.args.get('localization_status')),
        )


def _status_str_to_list(status: str) -> list[int]:
    return list(map(int, status.split(','))) if status else []

import typing as tp
from dataclasses import dataclass
from datetime import datetime

from flask import Request

from signs_dashboard.small_utils import get_form_date_from, get_form_date_to, get_str_date_from, get_str_date_to


@dataclass
class FramesQueryParameters:
    track_uuid: tp.Optional[str]
    frame_ids: tp.Optional[list[int]]
    frame_ids_raw: tp.Optional[str]
    predictor: tp.Optional[str]
    label: tp.Optional[str]
    prob_min: tp.Optional[float]
    prob_max: tp.Optional[float]
    from_dt: tp.Optional[datetime]
    to_dt: tp.Optional[datetime]
    interest_zone_regions: list[int]
    moderation_status: tp.Optional[int]
    limit: int = 1000

    @property
    def from_date(self):
        return get_str_date_from(self.from_dt)

    @property
    def to_date(self):
        return get_str_date_to(self.to_dt)

    @classmethod
    def from_request(cls, request: Request) -> 'FramesQueryParameters':
        args = request.args
        track_uuid = args.get('track_uuid') or None
        prob_min = args.get('prob_min', '')
        prob_max = args.get('prob_max', '')
        moderation_status = args.get('moderation_status', '')

        frame_ids_raw, frame_ids = request.args.get('frame_ids', None), None
        if frame_ids_raw:
            try:
                frame_ids = list(map(int, frame_ids_raw.strip().split(',')))
            except ValueError:
                frame_ids = []

        return cls(
            track_uuid=track_uuid,
            predictor=args.get('predictor') or None,
            label=args.get('label') or None,
            prob_min=None if prob_min == '' else float(prob_min),
            prob_max=None if prob_max == '' else float(prob_max),
            from_dt=get_form_date_from(args.get('from_date')),
            to_dt=get_form_date_to(args.get('to_date')),
            frame_ids=frame_ids,
            frame_ids_raw=frame_ids_raw,
            interest_zone_regions=request.args.getlist('interest_zone_regions', type=int),
            moderation_status=None if moderation_status == '' else int(moderation_status),
        )

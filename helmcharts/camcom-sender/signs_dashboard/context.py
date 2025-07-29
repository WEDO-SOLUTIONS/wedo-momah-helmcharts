from contextvars import ContextVar
from typing import TYPE_CHECKING, Iterable, Optional

if TYPE_CHECKING:
    from signs_dashboard.models.track import Track

default_track_uuid = None
c_track_uuid: ContextVar[Optional[str]] = ContextVar('track_uuid', default=default_track_uuid)


class ContextService:

    def __init__(self, track_uuid: Optional[str]):
        self._track_uuid = track_uuid

    def __enter__(self):
        c_track_uuid.set(self._track_uuid)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        c_track_uuid.set(default_track_uuid)
        return False


def context_aware_track_iterator(tracks: Iterable['Track']) -> Iterable['Track']:
    for track in tracks:
        with ContextService(track_uuid=track.uuid):
            yield track


def get_context_log_fields() -> dict:
    if c_track_uuid.get() is None:
        return {}
    return {
        'track_uuid': c_track_uuid.get(),
    }

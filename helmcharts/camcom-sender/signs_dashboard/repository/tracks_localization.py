from datetime import datetime
from typing import Optional

from sqlalchemy import func
from sqlalchemy.dialects import postgresql

from signs_dashboard.models.track_localization_status import TrackLocalizationStatus


class TracksLocalizationRepository:
    def __init__(self, session_factory):
        self._session_factory = session_factory

    def create_or_update(self, track_uuid: str, detector_name: str, status: int, last_done: Optional[datetime] = None):
        insert = postgresql.insert(TrackLocalizationStatus).values(
            uuid=track_uuid,
            detector_name=detector_name,
            status=status,
            last_done=last_done,
            updated=func.now(),
        )
        set_if_exists = {
            TrackLocalizationStatus.status: status,
            TrackLocalizationStatus.updated: func.now(),
        }
        if last_done:
            set_if_exists.update({TrackLocalizationStatus.last_done: last_done})
        upsert = insert.on_conflict_do_update(
            index_elements=[TrackLocalizationStatus.uuid, TrackLocalizationStatus.detector_name],
            set_=set_if_exists,
        )

        with self._session_factory(expire_on_commit=False) as session:
            session.execute(upsert)
            session.commit()

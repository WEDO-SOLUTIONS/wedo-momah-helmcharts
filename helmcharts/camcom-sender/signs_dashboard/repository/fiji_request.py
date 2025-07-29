from datetime import datetime
from typing import Optional

from sqlalchemy.dialects import postgresql

from signs_dashboard.models.fiji_request import FijiRequest


class FijiRequestRepository:
    def __init__(self, session_factory):
        self.session_factory = session_factory

    def upsert(
        self,
        track_uuid: str,
        last_request_time: datetime,
        retries: int,
        last_response: Optional[str],
        last_response_status: Optional[int],
        last_fiji_status: Optional[int],
    ):
        insert = postgresql.insert(FijiRequest).values(
            track_uuid=track_uuid,
            last_response=last_response,
            last_response_status=last_response_status,
            last_fiji_status=last_fiji_status,
            last_request_time=last_request_time,
            retries=retries,
        )

        upsert = insert.on_conflict_do_update(
            index_elements=[
                FijiRequest.track_uuid,
            ],
            set_={
                FijiRequest.last_response: last_response,
                FijiRequest.last_response_status: last_response_status,
                FijiRequest.last_fiji_status: last_fiji_status,
                FijiRequest.last_request_time: last_request_time,
                FijiRequest.retries: retries,
            },
        )

        with self.session_factory(expire_on_commit=False) as session:
            session.execute(upsert)
            session.commit()

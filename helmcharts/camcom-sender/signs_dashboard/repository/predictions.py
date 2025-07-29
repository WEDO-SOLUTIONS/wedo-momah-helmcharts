from datetime import datetime
from typing import List, Optional

from sqlalchemy import func
from sqlalchemy.dialects import postgresql

from signs_dashboard.models.frame import Frame
from signs_dashboard.models.frames_attributes import FrameAttribute
from signs_dashboard.models.prediction import Prediction


class PredictionsRepository:

    def __init__(self, session_factory):
        self.session_factory = session_factory

    def find(
        self,
        frame_ids: List[int],
        predictors: Optional[List[str]] = None,
        min_date: Optional[datetime] = None,
        max_date: Optional[datetime] = None,
    ) -> List[Prediction]:
        conditions = [
            Prediction.frame_id.in_(frame_ids),
        ]

        if predictors is not None:
            conditions.append(Prediction.detector_name.in_(predictors))

        if min_date:
            conditions.append(Prediction.date >= min_date)
        if max_date:
            conditions.append(Prediction.date <= max_date)

        with self.session_factory() as session:
            return session.query(Prediction).filter(*conditions).all()

    def save_with_raw(self, prediction: Prediction):
        insert = postgresql.insert(Prediction).values(
            frame_id=prediction.frame_id,
            date=prediction.date,
            detector_name=prediction.detector_name,
            error=prediction.error,
            raw_data=prediction.raw_data,
            created=func.now(),
            updated=func.now(),
        )
        upsert = insert.on_conflict_do_update(
            index_elements=[Prediction.frame_id, Prediction.detector_name, Prediction.date],
            set_={
                Prediction.error: prediction.error,
                Prediction.raw_data: prediction.raw_data,
                Prediction.updated: func.now(),
            },
        )

        with self.session_factory(expire_on_commit=False) as session:
            session.execute(upsert)
            session.commit()

    def save_frame_attributes(self, frame: Frame, detector_name: str, attributes: dict):
        insert = postgresql.insert(FrameAttribute).values(
            frame_id=frame.id,
            date=frame.date,
            detector_name=detector_name,
            attributes=attributes,
        )
        upsert = insert.on_conflict_do_update(
            index_elements=[
                FrameAttribute.frame_id,
                FrameAttribute.date,
                FrameAttribute.detector_name,
            ],
            set_={
                FrameAttribute.attributes: attributes,
            },
        )

        with self.session_factory(expire_on_commit=False) as session:
            session.execute(upsert)
            session.commit()

    def get_frames_attributes(
        self,
        frame_ids: list[int],
        detector_names: list[str],
        min_frame_date: Optional[datetime] = None,
        max_frame_date: Optional[datetime] = None,
    ) -> list[FrameAttribute]:
        with self.session_factory() as session:
            query = session.query(FrameAttribute)
            query = query.filter(FrameAttribute.frame_id.in_(frame_ids))
            if min_frame_date and max_frame_date:
                query = query.filter(FrameAttribute.date >= min_frame_date, FrameAttribute.date <= max_frame_date)
            query = query.filter(FrameAttribute.detector_name.in_(detector_names))
            return query.all()

import json
import math
import typing as tp
from datetime import datetime

from sqlalchemy import and_, delete, update
from sqlalchemy.orm import InstrumentedAttribute, Query, joinedload

from signs_dashboard.models.bbox_detection import BBOXDetection
from signs_dashboard.models.frame import Frame
from signs_dashboard.query_params.signs import RangeParam, SignsQueryParameters
from signs_dashboard.schemas.prediction import BBox

MAX_RECORDS_IN_LIST = 500


class BBOXDetectionsRepository:
    def __init__(self, session_factory):
        self.session_factory = session_factory

    def create(
        self,
        frame: Frame,
        detector_name: str,
        bbox: BBox,
        label: tp.Optional[str] = None,
        base_bbox: tp.Optional[BBOXDetection] = None,
        polygon: list[int] = None,
    ) -> BBOXDetection:
        x_from, y_from, width, height = _transform_bbox(bbox.xmin, bbox.ymin, bbox.xmax, bbox.ymax)

        sign = BBOXDetection(
            frame_id=frame.id,
            base_bbox_detection_id=base_bbox.id if base_bbox else None,
            date=frame.date,
            label=label or bbox.label,
            x_from=x_from,
            y_from=y_from,
            width=width,
            height=height,
            prob=bbox.probability or 1,
            is_side=bbox.is_side or False,
            is_side_prob=bbox.is_side_prob or 0,
            directions=bbox.directions,
            directions_prob=bbox.directions_prob,
            is_tmp=bbox.is_tmp,
            sign_value=bbox.sign_value,
            detector_name=detector_name,
            attributes=bbox.attributes,
            polygon=polygon,
        )

        with self.session_factory(expire_on_commit=False) as session:
            session.add(sign)
            session.commit()

        return sign

    def get_detection(self, detection_id: int) -> BBOXDetection:
        with self.session_factory() as session:
            return session.query(BBOXDetection).options(joinedload('frame')).get(detection_id)

    def find(self, query_params: SignsQueryParameters) -> tp.List[BBOXDetection]:
        with self.session_factory() as session:
            query = session.query(BBOXDetection)

            query = query.filter(BBOXDetection.date > query_params.from_dt) if query_params.from_dt else query
            query = query.filter(BBOXDetection.date < query_params.to_dt) if query_params.to_dt else query

            if query_params.label is not None:
                if query_params.is_regex_on:
                    query = query.filter(BBOXDetection.label.regexp_match(query_params.label))
                else:
                    query = query.filter(BBOXDetection.label == query_params.label)
            if query_params.sign_value is not None:
                query = query.filter(BBOXDetection.sign_value == query_params.sign_value)
            if query_params.detector_name is not None:
                query = query.filter(BBOXDetection.detector_name == query_params.detector_name)
            if query_params.is_tmp is not None:
                query = query.filter(BBOXDetection.is_tmp == query_params.is_tmp)

            query = _filter_by_range(query, BBOXDetection.prob, query_params.prob_range)
            query = _filter_by_range(query, BBOXDetection.is_side_prob, query_params.is_side_prob_range)
            query = _filter_by_range(query, BBOXDetection.width, query_params.width_range)
            query = _filter_by_range(query, BBOXDetection.height, query_params.height_range)
            query = _filter_by_range(query, BBOXDetection.x_from, query_params.x_from_range)
            query = _filter_by_range(query, BBOXDetection.y_from, query_params.y_from_range)

            return query.limit(MAX_RECORDS_IN_LIST).all()

    def get_signs_by_frame_id(self, frame_id: int) -> tp.List[BBOXDetection]:
        with self.session_factory() as session:
            sign_query = session.query(BBOXDetection)
            sign_query = sign_query.join(Frame).filter(Frame.id == frame_id)
            return sign_query.all()

    def get_by_frame_and_detector(self, frame: Frame, detector_name: str) -> tp.List[BBOXDetection]:
        with self.session_factory() as session:
            return session.query(BBOXDetection).filter(
                BBOXDetection.frame_id == frame.id,
                BBOXDetection.detector_name == detector_name,
                BBOXDetection.date == frame.date,
            ).all()

    def set_signs_status_by_frame(self, frame_id: int, status: int):
        with self.session_factory() as session:
            session.execute(
                update(BBOXDetection).
                where(BBOXDetection.frame_id == frame_id).
                values(status=status),
            )
            session.commit()

    def save_location_info(self, detection: BBOXDetection):
        with self.session_factory() as session:
            session.execute(
                update(BBOXDetection).
                where(
                    BBOXDetection.id == detection.id,
                    BBOXDetection.date == detection.date,
                ).
                values(
                    lat=detection.lat,
                    lon=detection.lon,
                ),
            )
            session.commit()

    def delete_by_frame_id_and_detector_name(
        self,
        frame_id: int,
        detector_name: str,
        frame_filter_date: tp.Optional[datetime] = None,
    ):
        conditions = [BBOXDetection.frame_id == frame_id, BBOXDetection.detector_name == detector_name]

        if frame_filter_date:
            conditions.append(BBOXDetection.date == frame_filter_date)

        stmt = delete(BBOXDetection).where(and_(*conditions))

        with self.session_factory() as session:
            session.execute(stmt)
            session.commit()

    def delete_by_id(
        self,
        bbox_detection_ids: list[int],
    ):
        stmt = delete(BBOXDetection).where(BBOXDetection.id.in_(bbox_detection_ids))

        with self.session_factory() as session:
            session.execute(stmt)
            session.commit()

    def update_attributes(self, detection_id: int, attributes: dict):
        sql = """
            UPDATE bbox_detections
            SET attributes=(COALESCE(NULLIF(attributes, 'null'::JSONB), '{}'::JSONB) || (:new_attributes)::JSONB)
            WHERE id = :detection_id
        """

        with self.session_factory() as session:
            session.execute(sql, {'detection_id': detection_id, 'new_attributes': json.dumps(attributes)})
            session.commit()


def _transform_bbox(x_min: int, y_min: int, x_max: int, y_max: int) -> tp.Tuple[int, int, int, int]:
    x_from = math.floor(x_min)
    y_from = math.floor(y_min)
    width = math.ceil(x_max) - x_from
    height = math.ceil(y_max) - y_from
    return x_from, y_from, width, height


def _filter_by_range(query: Query, field: InstrumentedAttribute, range_param: RangeParam) -> Query:
    if range_param.from_value is not None:
        query = query.filter(field >= range_param.from_value)
    if range_param.to_value is not None:
        query = query.filter(field <= range_param.to_value)
    return query

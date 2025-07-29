import typing as tp
from datetime import timedelta

from sqlalchemy import between, distinct, func, inspect, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Query, joinedload, lazyload, noload
from sqlalchemy.sql.expression import and_, or_

from signs_dashboard.models.bbox_detection import BBOXDetection
from signs_dashboard.models.frame import Frame
from signs_dashboard.models.interest_zones import SRID4326_ID, InterestZoneRegion
from signs_dashboard.models.prediction import Prediction
from signs_dashboard.models.track import Track
from signs_dashboard.models.track_localization_status import TrackLocalizationStatus
from signs_dashboard.query_params.frames import FramesQueryParameters


class FramesRepository:

    _options_track_app_version = joinedload('track').load_only('uuid', 'app_version').options(
        noload(Track.errors),
        noload(Track.clarifications),
        noload(Track.upload),
    )

    def __init__(self, session_factory):
        self.session_factory = session_factory

    def get(self, utc_dt, uuid):
        with self.session_factory(expire_on_commit=False) as session:
            query = session.query(Frame).options(lazyload(Frame.detections))
            query = query.filter(
                Frame.track_uuid == uuid,
            ).filter(
                Frame.date == utc_dt,
            )
            return query.first()

    def upsert(self, frame: Frame):
        changes_new = self._get_model_changes(frame)
        if not changes_new:
            return

        frame_fields_values = self._get_frame_field_values(frame)
        insert_stmt = postgresql.insert(Frame).values(**frame_fields_values)
        do_update_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['track_uuid', 'date'],
            set_=changes_new,
        ).returning(Frame.id)

        with self.session_factory(expire_on_commit=False) as session:
            cursor = session.execute(do_update_stmt)
            session.commit()
            # we should set frame id via side effect
            # because manual statements do not synchronize ORM objects
            frame.id = cursor.fetchone()['id']

    def bulk_insert(self, frames: tp.List[Frame]) -> None:
        if not frames:
            return

        frame_fields_values = [self._get_frame_field_values(frame) for frame in frames]
        insert_stmt = postgresql.insert(Frame).values(frame_fields_values)
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['track_uuid', 'date'],
            set_={'id': insert_stmt.excluded.id},
        ).returning(Frame.id)

        with self.session_factory(expire_on_commit=False) as session:
            result = session.execute(upsert_stmt)
            session.commit()

            for frame, row in zip(frames, result):
                frame.id = row['id']

    def find(self, query_params: FramesQueryParameters) -> tp.List[Frame]:
        with self.session_factory(expire_on_commit=False) as session:
            query = session.query(Frame).options(joinedload(Frame.detections))

            if query_params.from_dt:
                query = query.filter(Frame.date > query_params.from_dt)

            if query_params.to_dt:
                query = query.filter(Frame.date < query_params.to_dt)

            if query_params.frame_ids:
                query = query.filter(Frame.id.in_(query_params.frame_ids))

            if query_params.interest_zone_regions:
                query = self._regions_filters(query, query_params.interest_zone_regions)

            if query_params.moderation_status is not None:
                query = query.filter(Frame.moderation_status == query_params.moderation_status)

            query = _filter_by_predicts(query, query_params)

            if query_params.track_uuid:
                query = query.filter(Frame.track_uuid == query_params.track_uuid)
                query = _add_filter_by_frame_date(session, query, query_params)
                query = query.order_by(Frame.date.asc())
            else:
                query = query.order_by(Frame.date.asc())
                query = query.limit(query_params.limit)

            return query.all()

    def find_by_bbox(self, point1, point2, limit: int, scope: tp.Optional[str]) -> tp.List[Frame]:
        with self.session_factory() as session:
            query = session.query(Frame).options(
                lazyload(Frame.detections),
            )
            query = _filter_by_position(query, [*point1, *point2])
            query = query.filter(Frame.uploaded_photo.is_(True))

            if scope is not None:
                query = query.join(Track, Frame.track_uuid == Track.uuid)
                query = query.filter(Track.type == scope)

            query = query.limit(limit)
            return query.all()

    def find_similar_frames(self, frame: Frame, distance: float, direction: float, limit: int) -> tp.List[Frame]:
        with self.session_factory(expire_on_commit=False) as session:
            similar_track_frames_ids = (
                select(distinct(func.first_value(Frame.id).over(
                    partition_by=Frame.track_uuid,
                    order_by=func.ST_Distance(
                        func.ST_MakePoint(Frame.lon, Frame.lat),
                        func.ST_MakePoint(frame.lon, frame.lat),
                    ),
                ))).where(
                    between(Frame.lat, frame.lat - distance, frame.lat + distance),
                ).where(
                    between(Frame.lon, frame.lon - distance, frame.lon + distance),
                ).where(
                    between(Frame.azimuth, frame.azimuth - direction, frame.azimuth + direction),
                )
            )
            return session.query(Frame).filter(
                Frame.id.in_(similar_track_frames_ids),
            ).filter(
                Frame.track_uuid != frame.track_uuid,
            ).limit(limit).all()

    def get_frame(
        self,
        frame_id: int,
        include_detections: bool,
        include_app_version: bool,
        include_api_user: bool,
        from_detector: tp.Optional[str] = None,
    ) -> tp.Optional[Frame]:
        with self.session_factory(expire_on_commit=False) as session:
            query = session.query(Frame)
            if include_detections and from_detector:
                query = query.options(joinedload(Frame.detections.and_(BBOXDetection.detector_name == from_detector)))
            if not include_detections:
                query = query.options(noload('detections'))
            if include_app_version:
                query = query.options(self._options_track_app_version)
            if include_api_user:
                query = query.options(joinedload(Frame.api_user))
            return query.get(frame_id)

    def get_next(self, frame: Frame) -> tp.Optional[Frame]:
        with self.session_factory() as session:
            query = session.query(Frame)
            query = query.filter(Frame.track_uuid == frame.track_uuid)
            query = query.filter(Frame.date > frame.date)
            query = query.order_by(Frame.date.asc())
            next_frame = query.first()
            if next_frame:
                return next_frame

            query = session.query(Frame)
            query = query.filter(Frame.track_email == frame.track_email)
            query = query.filter(Frame.track_uuid != frame.track_uuid)
            query = query.filter(Frame.date > frame.date)
            max_date_shift = frame.date + timedelta(hours=1)
            query = query.filter(Frame.date < max_date_shift)
            query = query.order_by(Frame.date.asc())
            return query.first()

    def get_prev(self, frame: Frame) -> tp.Optional[Frame]:
        with self.session_factory() as session:
            query = session.query(Frame)
            query = query.filter(Frame.track_uuid == frame.track_uuid)
            query = query.filter(Frame.date < frame.date)
            query = query.order_by(Frame.date.desc())
            prev_frame = query.first()
            if prev_frame:
                return prev_frame

            query = session.query(Frame)
            query = query.filter(Frame.track_email == frame.track_email)
            query = query.filter(Frame.track_uuid != frame.track_uuid)
            query = query.filter(Frame.date < frame.date)
            max_date_shift = frame.date - timedelta(hours=1)
            query = query.filter(Frame.date > max_date_shift)
            query = query.order_by(Frame.date.desc())
            return query.first()

    def get_frames(self, frames_ids: tp.List[int]) -> tp.List[Frame]:
        with self.session_factory() as session:
            return session.query(Frame).filter(Frame.id.in_(frames_ids)).all()

    def get_by_track(self, track: Track, include_app_version: bool, include_api_user: bool) -> tp.List[Frame]:
        with self.session_factory() as session:
            query = session.query(Frame)
            query = query.filter(Frame.track_uuid == track.uuid)
            if track.recorded:
                query = query.filter(and_(
                    Frame.date >= track.recorded,
                    Frame.date <= track.recorded + timedelta(hours=12),
                ))
            if include_app_version:
                query = query.options(self._options_track_app_version)
            if include_api_user:
                query = query.options(joinedload(Frame.api_user))
            frames = query.all()
        return frames

    def get_by_track_for_localization(self, track: Track, ignore_predictions_status: bool) -> tp.List[Frame]:
        with self.session_factory() as session:
            query = session.query(Frame).filter(
                Frame.track_uuid == track.uuid,
            )
            if not ignore_predictions_status:
                query = query.join(
                    Prediction,
                    and_(
                        Prediction.frame_id == Frame.id,
                        Prediction.date == Frame.date,
                    ),
                ).options(
                    joinedload(Frame.detections.and_(Prediction.detector_name == BBOXDetection.detector_name)),
                ).outerjoin(
                    TrackLocalizationStatus,
                    and_(
                        Frame.track_uuid == TrackLocalizationStatus.uuid,
                        Prediction.detector_name == TrackLocalizationStatus.detector_name,
                    ),
                ).filter(
                    or_(
                        TrackLocalizationStatus.last_done.is_(None),
                        Prediction.updated >= TrackLocalizationStatus.last_done,
                    ),
                )
            if track.recorded:
                query = query.filter(and_(
                    Frame.date >= track.recorded,
                    Frame.date <= track.recorded + timedelta(hours=12),
                ))
            frames = query.all()
        return frames

    def get_by_track_uuids(self, track_uuids: list[str]) -> tp.List[Frame]:
        with self.session_factory() as session:
            query = session.query(Frame).filter(Frame.track_uuid.in_(track_uuids))
            frames = query.all()
        return frames

    def count_by_track(self, track_uuid: str) -> int:
        with self.session_factory() as session:
            query = session.query(Frame)
            count = query.filter(Frame.track_uuid == track_uuid).count()
        return count

    def _get_model_changes(self, model):
        state = inspect(model)
        changes = {}
        for attr in state.attrs:
            hist = state.get_history(attr.key, True)  # noqa: WPS425

            if hist.has_changes() and hist.added:
                changes[attr.key] = hist.added[0]

        return changes

    def _get_frame_field_values(self, frame: Frame) -> dict:
        field2value = {}
        state_mapper = vars(frame)  # noqa: WPS421
        for attr in frame.__mapper__.columns.keys():
            if attr != 'id' and attr in state_mapper:
                field2value[attr] = state_mapper[attr]
        return field2value

    def _regions_filters(self, query: Query, region_ids: list[int]) -> Query:
        conditions = []
        for region_id in region_ids:
            region_query = select(InterestZoneRegion.region)
            region_query = region_query.select_from(InterestZoneRegion)
            region_query = region_query.filter(InterestZoneRegion.id == region_id)
            region_query = region_query.limit(1)
            region_query = region_query.subquery()
            conditions.append(func.ST_Contains(
                region_query,
                func.ST_SetSRID(func.ST_MakePoint(Frame.lon, Frame.lat), SRID4326_ID),
            ))

        return query.filter(or_(*conditions))


def _add_filter_by_frame_date(session, query: Query, query_params: FramesQueryParameters) -> Query:
    if not query_params.from_dt and not query_params.to_dt:
        track_recorded_query = select(
            Track.recorded,
        ).select_from(
            Track,
        ).where(
            Track.uuid == query_params.track_uuid,
        )
        track_recorded = session.execute(track_recorded_query).scalar()
        if track_recorded:
            query = query.filter(and_(
                Frame.date >= track_recorded,
                Frame.date <= track_recorded + timedelta(hours=12),
            ))
    return query


def _filter_by_position(query, bbox: tp.List[tp.Optional[float]]):
    if bbox[0]:
        query = query.filter(Frame.lat > bbox[0])
    if bbox[1]:
        query = query.filter(Frame.lon > bbox[1])
    if bbox[2]:
        query = query.filter(Frame.lat < bbox[2])
    if bbox[3]:
        query = query.filter(Frame.lon < bbox[3])
    return query


def _filter_by_predicts(query, query_params: FramesQueryParameters):
    if any((query_params.predictor, query_params.label, query_params.prob_min, query_params.prob_max)):
        query = query.filter(
            Frame.detections.any(
                and_(
                    BBOXDetection.detector_name == query_params.predictor if query_params.predictor else True,
                    BBOXDetection.label == query_params.label if query_params.label else True,
                    query_params.prob_min <= BBOXDetection.prob if query_params.prob_min is not None else True,
                    query_params.prob_max >= BBOXDetection.prob if query_params.prob_max is not None else True,
                ),
            ),
        )
    return query

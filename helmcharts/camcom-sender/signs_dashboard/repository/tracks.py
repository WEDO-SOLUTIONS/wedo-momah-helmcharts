import typing as tp
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import Date, Float, Interval, String, distinct, exists, literal_column, select, update
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, aggregate_order_by, insert
from sqlalchemy.engine import Row
from sqlalchemy.orm import Query, contains_eager, joinedload, noload, undefer
from sqlalchemy.sql.expression import and_, case, cast, func, join, or_, text, true

from signs_dashboard.models.clarification import Clarification
from signs_dashboard.models.error import Error
from signs_dashboard.models.fiji_request import FijiRequest
from signs_dashboard.models.frame import Frame
from signs_dashboard.models.interest_zones import SRID4326_ID, InterestZoneRegion
from signs_dashboard.models.prediction import Prediction
from signs_dashboard.models.track import Track, TrackStatuses
from signs_dashboard.models.track_localization_status import TrackLocalizationStatus
from signs_dashboard.models.track_upload_status import (
    STATUS_NOT_UPLOADED,
    STATUS_PROCESSING_FAILED,
    STATUS_UPLOADED,
    TrackUploadStatus,
)
from signs_dashboard.query_params.drivers import DriversQueryParams
from signs_dashboard.query_params.tracks import TrackQueryParameters


def _filter_by_reloaded(query: Query, reloaded) -> Query:
    if not reloaded:
        return query
    if reloaded == '0':
        return query.filter(Track.reloaded.is_(False))
    return query.filter(Track.reloaded.is_(True))


def _regions_filters(query: Query, region_ids: list[int]) -> Query:
    conditions = []
    for region_id in region_ids:
        region_query = select(InterestZoneRegion.region)
        region_query = region_query.select_from(InterestZoneRegion)
        region_query = region_query.filter(InterestZoneRegion.id == region_id)
        region_query = region_query.limit(1)
        region_query = region_query.subquery()
        conditions.append(
            func.ST_Contains(
                region_query,
                func.ST_SetSRID(func.ST_MakePoint(Frame.lon, Frame.lat), SRID4326_ID),
            ),
        )

    return query.filter(
        exists(
            select(Frame.id).filter(
                or_(*conditions),
                Frame.track_uuid == Track.uuid,
            ),
        ),
    )


class TracksRepository:
    def __init__(self, session_factory):
        self.session_factory = session_factory

    def find(self, query_params: TrackQueryParameters, tracks_only: bool = False) -> list[Track]:
        with self.session_factory() as session:
            date_type = Track.uploaded if query_params.date_type == 'uploaded' else Track.recorded
            loader_type = noload if tracks_only else joinedload

            options = [
                loader_type(Track.errors),
                loader_type(Track.clarifications),
                loader_type(Track.upload).undefer('init_metadata'),
            ]

            query = session.query(
                Track,
            ).options(
                *options,
            ).order_by(
                date_type.desc(),
            )

            query = query.filter(date_type > query_params.from_dt) if query_params.from_dt else query
            query = query.filter(date_type < query_params.to_dt) if query_params.to_dt else query
            query = query.filter(Track.type == query_params.type) if query_params.type else query
            query = _filter_by_reloaded(query, query_params.reloaded)
            query = _filter_by_statuses(query_params=query_params, query=query)

            if query_params.email:
                query_expr = '%{email}%'.format(email=query_params.email)
                query = query.filter(Track.user_email.ilike(query_expr))

            if query_params.app_version:
                app_ver_expr = '%{version}%'.format(version=query_params.app_version)
                query = query.filter(Track.app_version.ilike(app_ver_expr))

            if query_params.interest_zone_regions:
                query = _regions_filters(query, query_params.interest_zone_regions)

            return query.all()

    def find_with_retries_requests_by_fiji_status(
        self, statuses: list[int], max_retries: int, retries_timeout: int,
    ) -> tp.List[Track]:
        with self.session_factory() as session:
            query = (
                session.query(Track).outerjoin(FijiRequest, Track.uuid == FijiRequest.track_uuid).options(
                    joinedload(Track.upload).undefer_group('meta_bodies').defer('gps_points'),
                    contains_eager(Track.fiji_request),
                ).filter(
                    and_(
                        Track.fiji_status.in_(statuses),
                        or_(
                            FijiRequest.id.is_(None),
                            and_(
                                FijiRequest.id.isnot(None),
                                FijiRequest.retries < max_retries,
                                FijiRequest.last_request_time + timedelta(seconds=retries_timeout) <= func.now(),
                            ),
                        ),
                    ),
                )
            )
            return query.all()

    def find_by_pro_status(self, statuses: tp.List[int]) -> tp.List[Track]:
        with self.session_factory() as session:
            return session.query(Track).options(
                joinedload(Track.upload).undefer('init_metadata'),
                noload(Track.clarifications),
                noload(Track.errors),
            ).filter(Track.pro_status.in_(statuses)).all()

    def find_localization_pending(
        self,
        expected_track_types: tp.Sequence[str],
        skip_localization_statuses: tp.Sequence[int],
        localization_requires_detections_from: tp.Optional[str],
        scheduled_processing_timeout: timedelta,
        track_upload_timeout: timedelta,
        uploading_track_localization_interval: timedelta,
        uploaded_track_localization_interval: timedelta,
    ) -> list[tuple[Track, list]]:
        detectors_with_new_detections_list = self._detectors_with_new_detections_list_query(
            localization_requires_detections_from,
        )
        detectors_with_new_detections_exists = exists(detectors_with_new_detections_list.with_only_columns([1]))

        last_tls_updated = select(
            func.max(TrackLocalizationStatus.updated),
        ).select_from(TrackLocalizationStatus).filter(
            Track.uuid == TrackLocalizationStatus.uuid,
        )
        not_localized_before = ~exists(last_tls_updated.with_only_columns([1]))
        track_last_update_date = func.coalesce(
            TrackUploadStatus.complete_time,
            TrackUploadStatus.gps_time,
            TrackUploadStatus.init_time,
            Track.uploaded,
            Track.recorded,
        )

        track_upload_timeout_expired = track_last_update_date <= func.now() - track_upload_timeout

        with self.session_factory() as session:
            query = session.query(Track).join(
                Track.upload,
                isouter=True,
            ).options(
                noload(Track.upload),
                noload(Track.localizations),
                noload(Track.clarifications),
                noload(Track.errors),
            ).add_columns(
                detectors_with_new_detections_list.label('new_detections_exists_for'),
            ).filter(
                and_(
                    Track.localization_status.notin_(skip_localization_statuses),
                    Track.type.in_(expected_track_types),
                    # загружен до конца или уже нет шансов что будет загружен до конца
                    or_(
                        Track.upload_status == STATUS_UPLOADED,
                        track_upload_timeout_expired,
                    ),
                    or_(
                        # forced
                        Track.localization_status == TrackStatuses.LOCALIZATION_FORCED,
                        # зависшие в обработке (упавшие в ошибку не ретраим)
                        and_(
                            Track.localization_status.in_([
                                TrackStatuses.LOCALIZATION_SCHEDULED,
                                TrackStatuses.LOCALIZATION_IN_PROGRESS,
                            ]),
                            last_tls_updated <= func.now() - scheduled_processing_timeout,
                        ),
                        # новые детекции
                        and_(
                            # не в обработке и не в очереди на обработку
                            Track.localization_status.notin_([
                                TrackStatuses.LOCALIZATION_SCHEDULED,
                                TrackStatuses.LOCALIZATION_IN_PROGRESS,
                            ]),
                            detectors_with_new_detections_exists,
                            or_(
                                # не обрабатывавшиеся
                                not_localized_before,
                                # прошло n времени с прошлой обработки
                                last_tls_updated <= func.now() - case(
                                    [(
                                        track_upload_timeout_expired,
                                        uploaded_track_localization_interval,
                                    )],
                                    else_=uploading_track_localization_interval,
                                ),
                            ),
                        ),
                    ),
                ),
            )
            return query.all()

    def fetch_track_detectors(self, track_uuid: str) -> tuple[list[str], list[str]]:
        with self.session_factory() as session:
            query = session.query(
                func.array_agg(distinct(Prediction.detector_name)).label('all_detectors'),
                func.array_agg(distinct(Prediction.detector_name)).filter(
                    exists(
                        select(1).select_from(TrackLocalizationStatus).filter(
                            TrackLocalizationStatus.uuid == Track.uuid,
                            TrackLocalizationStatus.detector_name == Prediction.detector_name,
                            or_(
                                TrackLocalizationStatus.last_done.is_(None),
                                TrackLocalizationStatus.last_done < TrackLocalizationStatus.updated,
                            ),
                        ),
                    ),
                ).label('not_localized_detectors'),
            ).select_from(Prediction).join(
                Frame,
                and_(Frame.id == Prediction.frame_id, Frame.date == Prediction.date),
            ).join(
                Track, Frame.track_uuid == Track.uuid,
            ).filter(
                Track.uuid == track_uuid,
                or_(
                    Track.recorded.is_(None),
                    and_(
                        Frame.date >= Track.recorded,
                        Frame.date <= Track.recorded + timedelta(hours=12),
                    ),
                ),
            )
            return query.first()

    def get_by_uuid(
        self,
        track_uuid: str,
        with_localization_statuses: bool = False,
        with_api_user: bool = False,
    ) -> tp.Optional[Track]:
        with self.session_factory() as session:
            query = session.query(Track).options(
                joinedload(Track.upload).undefer_group('meta_bodies'),
            ).filter(Track.uuid == track_uuid)
            if with_localization_statuses:
                query = query.options(joinedload(Track.localizations))
            if with_api_user:
                query = query.options(joinedload(Track.api_user))
            return query.first()

    def get_by_uuids(self, track_uuids: list[str]) -> tp.List[Track]:
        with self.session_factory() as session:
            query = session.query(Track).options(
                joinedload(Track.upload).undefer_group('meta_bodies'),
            ).filter(Track.uuid.in_(track_uuids))
        return query.all()

    def get_fields_values(self, track_uuid: str, model_fields: list[tp.Any]) -> tp.Optional[Row]:
        with self.session_factory() as session:
            return session.query(*model_fields).filter_by(uuid=track_uuid).first()

    def create_track_from_init_request(
        self,
        request_data: dict,
        track_uuid: str,
        event_dt: datetime,
        track_type: str,
        localization_status: int,
        map_matching_status: int,
        recorded: tp.Optional[str],
    ) -> tp.Optional[str]:
        recorded_dtime = None
        timezone_offset = '+00:00'
        if recorded:
            recorded_raw = datetime.fromisoformat(recorded)
            timezone_offset = recorded_raw.tzinfo.utcoffset(recorded_raw)
            recorded_dtime = recorded_raw.astimezone(timezone.utc).replace(tzinfo=None)

        track_data = {
            'uuid': track_uuid,
            'user_email': request_data['user_email'],
            'app_version': request_data['app_version'].split('_')[0],
            'distance': request_data['distance_in_meters'] / 1000,  # meters to km
            'duration': request_data['duration_in_millis'] / (60 * 60 * 1000),  # in hours
            'uploaded': event_dt,
            'type': track_type,
            'upload_status': STATUS_NOT_UPLOADED,
            'fiji_status': TrackStatuses.UPLOADING,
            'pro_status': TrackStatuses.UPLOADING,
            'localization_status': localization_status,
            'map_matching_status': map_matching_status,
            'reloaded': 'reload' in request_data['app_version'],
            'recorded': recorded_dtime,
            'timezone_offset': timezone_offset,
        }
        with self.session_factory() as session:
            track = session.query(Track).filter_by(uuid=track_uuid).first()
            if not track:
                track = Track(**track_data)
                session.add(track)
                session.commit()
                return track.user_email
        return None

    def update_track_fiji_status(self, uuid: str, status: int):
        with self.session_factory() as session:
            track = session.query(Track).filter_by(uuid=uuid).first()
            track.fiji_status = status
            session.add(track)
            session.commit()

    def update_track_pro_status(self, uuid: str, status: int):
        with self.session_factory() as session:
            track = session.query(Track).filter_by(uuid=uuid).first()
            track.pro_status = status
            session.add(track)
            session.commit()

    def bulk_update_track_field(self, uuids: list[str], **kwargs):
        with self.session_factory() as session:
            query = update(Track).where(Track.uuid.in_(uuids)).values(**kwargs)
            session.execute(query)
            session.commit()

    def update_track_localization_status(self, uuid: str, status: int):
        with self.session_factory() as session:
            track = session.query(Track).filter_by(uuid=uuid).first()
            track.localization_status = status
            session.add(track)
            session.commit()

    def update_track_map_matching_status(self, uuid: str, status: int):
        with self.session_factory() as session:
            track = session.query(Track).filter_by(uuid=uuid).first()
            track.map_matching_status = status
            session.add(track)
            session.commit()

    def set_uploaded(self, uuid: str) -> tp.Optional[str]:
        upload_status = self.get_upload_status(uuid)
        upload_status.status = STATUS_UPLOADED

        track_email = None
        with self.session_factory() as session:
            session.add(upload_status)

            track = session.query(Track).filter_by(uuid=uuid).first()
            if track.upload_status == STATUS_NOT_UPLOADED:
                track_email = track.user_email

            track.upload_status = STATUS_UPLOADED

            if track.fiji_status == TrackStatuses.NOT_COMPLETE:
                track.fiji_status = TrackStatuses.UPLOADING

            session.add(track)
            session.commit()
        return track_email

    def set_processing_failed_status(self, track: Track):
        track.upload_status = STATUS_PROCESSING_FAILED
        track.upload.status = STATUS_PROCESSING_FAILED

        with self.session_factory() as session:
            session.add(track)
            session.commit()

    def get_upload_status(self, uuid: str) -> TrackUploadStatus:
        with self.session_factory() as session:
            upload_status = session.query(
                TrackUploadStatus,
            ).options(
                undefer(TrackUploadStatus.init_metadata),
                undefer(TrackUploadStatus.gps_points),
            ).filter_by(uuid=uuid).first()
            if not upload_status:
                upload_status = TrackUploadStatus(uuid=uuid)
            return upload_status

    def save_upload_status(self, upload_status):
        with self.session_factory(expire_on_commit=False) as session:
            session.add(upload_status)
            session.commit()

    def save_track(self, track: Track):
        with self.session_factory() as session:
            for error in track.errors:
                error_insert = insert(Error).values(
                    id=error.id,
                    created=error.created,
                    updated=error.updated,
                    assignee=error.assignee,
                    sign_id=error.sign_id,
                    sign_type=error.sign_type,
                    track_uuid=error.track_uuid,
                    status=error.status,
                    type=error.type,
                    resolution=error.resolution,
                    deleted=error.deleted,
                ).on_conflict_do_nothing()
                session.execute(error_insert)
            for clarification in track.clarifications:
                clarification_insert = insert(Clarification).values(
                    id=clarification.id,
                    created=clarification.created,
                    updated=clarification.updated,
                    type=clarification.type,
                    sign_id=clarification.sign_id,
                    sign_type=clarification.sign_type,
                    track_uuid=clarification.track_uuid,
                    status=clarification.status,
                    deleted=clarification.deleted,
                    is_new_sign=clarification.is_new_sign,
                ).on_conflict_do_nothing()
                session.execute(clarification_insert)
            session.add(track)
            session.commit()

    def set_track_recorded_time_and_distance(
        self,
        track_uuid: str,
        distance_km: float,
        recorded_time: datetime,
    ) -> str:
        with self.session_factory() as session:
            query = update(Track).filter(
                Track.uuid == track_uuid,
            ).values(
                distance=distance_km,
                recorded=func.coalesce(Track.recorded, recorded_time),
            ).returning(Track.user_email)
            user_email = session.execute(query).scalar()
            session.commit()
        return user_email

    def update_track_frames_count(self, track_uuid: str, frames_count: int):
        with self.session_factory() as session:
            query = update(TrackUploadStatus).where(
                TrackUploadStatus.uuid == track_uuid,
            ).values(
                init_metadata=func.jsonb_set(
                    TrackUploadStatus.init_metadata,
                    '{frames_count}',
                    func.to_jsonb(frames_count, type_=JSONB),
                ),
            )
            session.execute(query)
            session.commit()

    def get_active_drivers(self, query_params: DriversQueryParams) -> tp.List[dict]:
        with self.session_factory() as session:
            query = select(
                cast(Track.recorded, Date).label('date_for'),
                Track.user_email.label('email'),
                func.array_agg(Track.uuid, type_=ARRAY(String))[1].label('example_track_uuid'),
            ).select_from(
                Track,
            ).group_by(
                cast(Track.recorded, Date),
                Track.user_email,
            )

            if query_params.emails:
                query = query.filter(Track.user_email.in_(query_params.emails))

            if query_params.from_dt:
                query = query.filter(Track.recorded >= query_params.from_dt)

            if query_params.to_dt:
                query = query.filter(Track.recorded < query_params.to_dt)

            return session.execute(query).fetchall()

    def get_tracks_summary_at_date(self, user_email: str, target_date: date) -> dict:
        with self.session_factory() as session:
            one_hour = text("'1 hour'::interval")

            query = select(
                func.array_agg(aggregate_order_by(Track.uuid, Track.recorded)).label('tracks_uuids'),
                func.sum(Track.distance).label('distance_km'),
                func.sum(func.coalesce(Track.duration, 0)).op('*', return_type=Interval)(one_hour).label('duration'),
                func.array_agg(func.distinct(Track.app_version)).label('app_versions'),
                func.array_agg(func.distinct(Track.pro_status)).label('pro_statuses'),
                func.array_agg(func.distinct(Track.upload_status)).label('upload_statuses'),
            ).select_from(
                Track,
            ).where(
                Track.recorded >= target_date,
                Track.recorded < target_date + timedelta(days=1),
                Track.user_email == user_email,
                Track.pro_status != TrackStatuses.HIDDEN_PRO,
            )
            row = session.execute(query).first()
            return dict(row)

    def get_daily_gps_track_info(self, track_uuid: str):
        with self.session_factory() as session:
            point = literal_column('points.value', type_=JSONB)
            linestring = literal_column('gps_tracks.linestring', type_=JSONB)
            linestring_recorded = literal_column('gps_tracks.recorded_time', type_=JSONB)

            gps_tracks_subquery = select(
                func.jsonb_agg(
                    aggregate_order_by(
                        func.jsonb_build_array(
                            func.jsonb_build_array(
                                _jsonb_field_variants_as_float(point, 'longitude', 'Longitude'),
                                _jsonb_field_variants_as_float(point, 'latitude', 'Latitude'),
                            ),
                            _jsonb_field_variants_as_float(point, 'speed', 'Speed'),
                        ),
                        _jsonb_field_variants_as_float(point, 'timestamp', 'Timestamp'),
                    ),
                ).label('linestring'),
                func.array_agg(TrackUploadStatus.recorded_time).label('recorded_time'),
            ).select_from(
                join(
                    TrackUploadStatus,
                    func.jsonb_array_elements(TrackUploadStatus.current_gps_points).alias('points'),
                    true(),
                ),
            ).where(
                TrackUploadStatus.current_gps_points.isnot(None),
                TrackUploadStatus.uuid == track_uuid,
            ).group_by(
                TrackUploadStatus.uuid,
            ).subquery()

            query = select(
                linestring,
            ).select_from(
                gps_tracks_subquery.alias('gps_tracks'),
            ).order_by(
                linestring_recorded,
            )
            return session.execute(query).scalar()

    def _detectors_with_new_detections_list_query(
        self,
        localization_requires_detections_from: tp.Optional[str],
    ) -> Query:
        frame_with_detections_conditions = []
        if localization_requires_detections_from:
            frame_with_detections_conditions.append(exists(
                select(1).select_from(Prediction).correlate(Frame).filter(
                    Prediction.frame_id == Frame.id,
                    Prediction.date == Frame.date,
                    Prediction.detector_name == localization_requires_detections_from,
                ),
            ))

        return select(
            func.array_agg(distinct(Prediction.detector_name)),
        ).select_from(
            Frame,
        ).join(
            Prediction,
            and_(Frame.id == Prediction.frame_id, Frame.date == Prediction.date),
            isouter=False,
        ).outerjoin(
            TrackLocalizationStatus,
            and_(
                Prediction.detector_name == TrackLocalizationStatus.detector_name,
                TrackLocalizationStatus.uuid == Track.uuid,
            ),
        ).filter(
            and_(
                Frame.track_uuid == Track.uuid,
                or_(
                    Track.recorded.is_(None),
                    and_(
                        Frame.date >= Track.recorded,
                        Frame.date <= Track.recorded + timedelta(hours=12),
                    ),
                ),
                Prediction.updated.isnot(None),
                Prediction.detector_name.isnot(None),
                or_(
                    TrackLocalizationStatus.uuid.is_(None),
                    Prediction.updated > TrackLocalizationStatus.last_done,
                ),
                *frame_with_detections_conditions,
            ),
        )


def _jsonb_field_variants_as_float(jsonb_field, *name_variants):
    return cast(
        func.coalesce(
            *[
                jsonb_field.op('->>')(name_variant)
                for name_variant in name_variants
            ],
        ),
        Float,
    )


def _filter_by_statuses(query_params: TrackQueryParameters, query: Query) -> Query:
    if query_params.status:
        status_field = Track.pro_status if query_params.status_field == 'pro_status' else Track.fiji_status
        query = query.filter(status_field.in_(query_params.status))

    if query_params.map_matching_status:
        query = query.filter(Track.map_matching_status.in_(query_params.map_matching_status))

    if query_params.localization_status:
        query = query.filter(Track.localization_status.in_(query_params.localization_status))

    if query_params.upload_status:
        query = query.filter(Track.upload_status == int(query_params.upload_status))

    return query

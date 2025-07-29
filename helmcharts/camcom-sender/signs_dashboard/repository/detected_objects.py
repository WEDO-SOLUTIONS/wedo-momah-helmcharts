import logging
from dataclasses import dataclass, field
from datetime import datetime

from geoalchemy2 import Geography
from sqlalchemy import Boolean, Float, String, and_, case, column, delete, func, insert, or_, select, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import values

from signs_dashboard.models.bbox_detection import BBOXDetection
from signs_dashboard.models.detected_object import COMMON_DETECTION_FIELDS, DetectedObject
from signs_dashboard.models.interest_zones import SRID4326_ID, InterestZoneRegion
from signs_dashboard.query_params.detected_objects import DetectedObjectsQueryParams

logger = logging.getLogger(__name__)
DETECTIONS_BATCH_SIZE = 100


@dataclass
class ClusterizationResult:
    created_ids: list[int] = field(default_factory=list)
    updated_ids: list[int] = field(default_factory=list)
    deleted_ids: list[int] = field(default_factory=list)


class DetectedObjectsRepository:
    def __init__(self, session_factory):
        self.session_factory = session_factory

    def find(self, query_params: DetectedObjectsQueryParams) -> list[DetectedObject]:
        with self.session_factory(expire_on_commit=False) as session:
            query = session.query(DetectedObject)
            if query_params.label:
                query = query.filter(DetectedObject.label == query_params.label)

            if query_params.region_ids:
                conditions = []
                for region_id in query_params.region_ids:
                    region_query = select(InterestZoneRegion.region)
                    region_query = region_query.select_from(InterestZoneRegion)
                    region_query = region_query.filter(InterestZoneRegion.id == region_id)
                    region_query = region_query.limit(1)
                    region_query = region_query.subquery()
                    conditions.append(func.ST_Contains(
                        region_query,
                        func.ST_SetSRID(func.ST_MakePoint(DetectedObject.lon, DetectedObject.lat), SRID4326_ID),
                    ))
                query = query.filter(or_(*conditions))

            return query.all()

    def find_near_detections(
        self,
        detections: list[BBOXDetection],
        radius_meters: float,
    ) -> list[DetectedObject]:
        results = []
        seen_ids = set()

        with self.session_factory(expire_on_commit=False) as session:
            query_base = session.query(DetectedObject).options(joinedload(DetectedObject.detections))

            for batch_num in range(0, len(detections), DETECTIONS_BATCH_SIZE):
                batch = detections[batch_num:batch_num + DETECTIONS_BATCH_SIZE]

                points_cte = select(
                    values(
                        column('point', Geography),
                        *[
                            column(field_name, getattr(DetectedObject, field_name).type)
                            for field_name in COMMON_DETECTION_FIELDS
                        ],
                        name='points',
                    ).data([
                        (
                            func.cast(
                                func.ST_SetSRID(func.ST_MakePoint(detection.lon, detection.lat), SRID4326_ID),
                                Geography,
                            ),
                            func.cast(detection.detector_name, String),
                            func.cast(detection.label, String),
                            func.cast(detection.sign_value, Float),
                            func.cast(detection.is_tmp, Boolean),
                            func.cast(detection.directions, JSONB),
                        )
                        for detection in batch
                    ]),
                ).cte()

                query = query_base.select_from(
                    DetectedObject.__table__.join(
                        points_cte,
                        and_(
                            func.ST_DWithin(
                                func.cast(
                                    func.ST_SetSRID(
                                        func.ST_MakePoint(DetectedObject.lon, DetectedObject.lat),
                                        SRID4326_ID,
                                    ),
                                    Geography,
                                ),
                                points_cte.c.point,
                                radius_meters,
                            ),
                            DetectedObject.detector_name == points_cte.c.detector_name,
                            DetectedObject.label == points_cte.c.label,
                            case(
                                [
                                    (points_cte.c.sign_value.is_(None), DetectedObject.sign_value.is_(None)),
                                ],
                                else_=DetectedObject.sign_value == func.cast(points_cte.c.sign_value, Float),
                            ),
                            DetectedObject.is_tmp == points_cte.c.is_tmp,
                            case(
                                [
                                    (points_cte.c.directions.is_(None), DetectedObject.directions.is_(None)),
                                    (
                                        func.jsonb_typeof(func.cast(points_cte.c.directions, JSONB)) == 'null',
                                        func.jsonb_typeof(DetectedObject.directions) == 'null',
                                    ),
                                ],
                                else_=DetectedObject.directions == points_cte.c.directions,
                            ),
                        ),
                    ),
                )

                for detected_object in query.all():
                    if detected_object.id not in seen_ids:
                        seen_ids.add(detected_object.id)
                        results.append(detected_object)

        return results

    def get(self, detected_object_id: int) -> DetectedObject:
        with self.session_factory(expire_on_commit=False) as session:
            query = session.query(DetectedObject).options(joinedload(DetectedObject.detections))
            query = query.filter(DetectedObject.id == detected_object_id)
            return query.one_or_none()

    def get_by_id_list(self, detected_objects_ids: list[int]) -> list[DetectedObject]:
        if not detected_objects_ids:
            return []
        with self.session_factory(expire_on_commit=False) as session:
            query = session.query(DetectedObject).options(joinedload(DetectedObject.detections))
            query = query.filter(DetectedObject.id.in_(detected_objects_ids))
            return query.all()

    def find_by_bbox(self, point1, point2, limit: int) -> list[DetectedObject]:
        with self.session_factory() as session:
            query = session.query(DetectedObject).options(joinedload(DetectedObject.detections))
            query = query.filter(
                func.ST_Contains(
                    func.ST_MakeEnvelope(*reversed(point1), *reversed(point2)),  # latlon -> lonlat
                    func.ST_MakePoint(DetectedObject.lon, DetectedObject.lat),
                ),
            )
            query = query.limit(limit)
            return query.all()

    def save_state(
        self,
        expected_to_affect_detections: list[BBOXDetection],
        detections_to_unlink: list[BBOXDetection],
        objects_to_add: list[DetectedObject],
        objects_to_update: list[DetectedObject],
        objects_to_remove: list[DetectedObject],
        default_status: str,
    ) -> ClusterizationResult:
        affected_detections = []
        statistics = ClusterizationResult()
        with self.session_factory() as session:
            session.begin()

            if objects_to_add:
                logger.info(f'Creating {len(objects_to_add)} objects...')
                ins = insert(DetectedObject).values([
                    {
                        'lat': object_to_create.lat,
                        'lon': object_to_create.lon,
                        'detector_name': object_to_create.detector_name,
                        'updated': func.now(),
                        'label': object_to_create.label,
                        'is_tmp': object_to_create.is_tmp,
                        'sign_value': object_to_create.sign_value,
                        'directions': object_to_create.directions,
                        'status': object_to_create.status or default_status,
                    }
                    for object_to_create in objects_to_add
                ]).returning(DetectedObject.id)
                add_cursor = session.execute(ins)
                added = add_cursor.fetchall()
                added_detections = []
                for obj, added_obj in zip(objects_to_add, added):
                    if not obj.detections:
                        raise ValueError('Adding object without detections!')
                    for added_det in obj.detections:
                        added_detections.append(
                            {
                                'id': added_det.id,
                                'detected_object_id': added_obj.id,
                            },
                        )
                    affected_detections.extend(list(obj.detections))
                    statistics.created_ids.append(added_obj['id'])
                session.bulk_update_mappings(BBOXDetection, added_detections)

            if objects_to_update:
                logger.info(f'Updating {len(objects_to_update)} objects...')
                update_objects = []
                update_detections = []
                for object_to_update in objects_to_update:
                    if not object_to_update.detections:
                        raise ValueError(f'expected to update object without detections {object_to_update}')
                    update_objects.append(
                        {
                            'id': object_to_update.fast_id,
                            'lat': object_to_update.lat,
                            'lon': object_to_update.lon,
                            'updated': datetime.utcnow(),
                        },
                    )

                    for updated_det in object_to_update.detections:
                        update_detections.append(
                            {
                                'id': updated_det.fast_id,
                                'detected_object_id': object_to_update.fast_id,
                            },
                        )
                    affected_detections.extend(list(object_to_update.detections))
                    statistics.updated_ids.append(object_to_update.id)
                session.bulk_update_mappings(DetectedObject, update_objects)
                session.bulk_update_mappings(BBOXDetection, update_detections)

            if detections_to_unlink:
                logger.info(f'Unlinking {len(detections_to_unlink)} detections...')
                upd = update(BBOXDetection).filter(
                    BBOXDetection.id.in_([det.id for det in detections_to_unlink]),
                ).values(
                    detected_object_id=None,
                )
                session.execute(upd)
                affected_detections += detections_to_unlink

            if objects_to_remove:
                logger.info(f'Marking {len(objects_to_remove)} objects as deleted...')
                affected_detection_ids = {detection.id for detection in affected_detections}
                for removed_obj in objects_to_remove:
                    for r_d in removed_obj.detections:
                        if r_d.id not in affected_detection_ids:
                            raise ValueError(f'Removing object with existing detections {removed_obj.id} {r_d.id}')

                del_query = delete(DetectedObject).filter(DetectedObject.id.in_([
                    object_to_remove.id
                    for object_to_remove in objects_to_remove
                ]))
                session.execute(del_query)
                statistics.deleted_ids.extend([object_to_remove.id for object_to_remove in objects_to_remove])

            self._validate_affected_detections(affected_detections, expected_to_affect_detections)
            session.commit()

        return statistics

    def update_object_status(self, object_id: int, status: str):
        with self.session_factory() as session:
            query = update(DetectedObject).filter(DetectedObject.id == object_id).values(status=status)
            session.execute(query)
            session.commit()

    def _validate_affected_detections(
        self,
        affected_detections: list[BBOXDetection],
        expected_to_affect_detections: list[BBOXDetection],
    ):
        affected_detections_ids = [detection.id for detection in affected_detections]
        duplicated_ids_in_all = {
            detection_id
            for detection_id in affected_detections_ids
            if affected_detections_ids.count(detection_id) > 1
        }
        if duplicated_ids_in_all:
            # чтобы sentry схлопнул в один алерт
            logger.warning(f'duplicated detections ids in affected object detections: {duplicated_ids_in_all}')
            logger.error('duplicated detections ids in affected object detections')

        expected = {det.id for det in expected_to_affect_detections}
        affected = {det.id for det in affected_detections}
        unexpected_affected = affected - expected
        unexpected_unaffected = expected - affected
        if unexpected_affected or unexpected_unaffected:
            logger.warning(f'Not affected detections: {unexpected_affected}, unexpected: {unexpected_unaffected}')
            logger.error('Number of affected and expected to be affected detections does not match!')

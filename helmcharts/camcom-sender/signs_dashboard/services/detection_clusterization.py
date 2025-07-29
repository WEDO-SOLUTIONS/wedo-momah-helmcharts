import logging
import time
from collections import defaultdict
from typing import Dict, Iterable, Optional

import h3
import numpy as np
import utm
from pydantic import BaseModel, Field
from sklearn.cluster import DBSCAN

from signs_dashboard.models.bbox_detection import BBOXDetection, DetectedObjectTypeFields
from signs_dashboard.models.detected_object import DetectedObject
from signs_dashboard.models.track import Track
from signs_dashboard.services.detected_objects import DetectedObjectsService
from signs_dashboard.services.detection_clusterization_debug import DetectionClusterizationDebugService
from signs_dashboard.services.s3_service import S3Service

logger = logging.getLogger(__name__)

BBOXDetectionsPositions = list[tuple[float, float]]
H3_PREFILTER_RADIUS = 1


class ClusterizationThresholds(BaseModel):
    cluster_objects_distance_thr: float
    dbscan_eps: float


class ClusterizationParams(BaseModel):
    nearest_objects_select_radius: float
    default_thresholds: ClusterizationThresholds
    overloaded_thresholds: Dict[str, ClusterizationThresholds] = Field(default_factory=dict)

    def get_thresholds(self, label: str) -> ClusterizationThresholds:
        return self.overloaded_thresholds.get(label, self.default_thresholds)


class DetectionClusterizationService:

    def __init__(
        self,
        detected_objects_service: DetectedObjectsService,
        s3_service: S3Service,
        clusterization_params: ClusterizationParams,
        naive_localization: bool,
    ):
        self._s3_service = s3_service
        self._detected_objects_service = detected_objects_service
        self._clusterization_params = clusterization_params
        self._naive_localization = naive_localization

    def clusterize(self, detections: list[BBOXDetection], for_track: Track):
        t_start = time.monotonic()
        nearest_objects = self._detected_objects_service.find_near_detections(
            detections,
            radius_meters=self._clusterization_params.nearest_objects_select_radius,
        )
        t_find = time.monotonic() - t_start
        logger.info(f'found {len(nearest_objects)} nearest objects')

        with DetectionClusterizationDebugService(self._s3_service, track=for_track) as debug_service:
            debug_service.save_input(
                detections=detections,
                nearest_objects=nearest_objects,
                clustering_params=self._clusterization_params.dict(),
            )
            affected_detections = detections
            affected_detections += [
                detection
                for near_object in nearest_objects
                for detection in near_object.detections
                if detection not in affected_detections
            ]
            t_clusterize = time.monotonic()
            objects_to_add, objects_to_update, objects_to_remove, detections_to_unlink = self._do_clusterize(
                detections,
                nearest_objects,
            )
            t_clusterize = time.monotonic() - t_clusterize

            debug_service.save_output(
                objects_to_add=objects_to_add,
                objects_to_update=objects_to_update,
                objects_to_remove=objects_to_remove,
                detections_to_unlink=detections_to_unlink,
            )
        t_db_save = time.monotonic()
        self._detected_objects_service.save_state(
            expected_to_affect_detections=affected_detections,
            detections_to_unlink=detections_to_unlink,
            objects_to_add=objects_to_add,
            objects_to_update=objects_to_update,
            objects_to_remove=objects_to_remove,
        )
        t_db_save = time.monotonic() - t_db_save
        t_all = time.monotonic() - t_start
        logger.warning(f'track: {for_track.uuid}, t_all: {t_all:.3f},t_find: {t_find:.3f},t_clusterize: {t_clusterize:.3f}, t_db_save: {t_db_save:.3f}')  # noqa: E501

    def _prepare_input(
        self,
        detections: list[BBOXDetection],
        nearest_objects: list[DetectedObject],
    ) -> tuple[list[BBOXDetection], list[BBOXDetection]]:
        detections_to_unlink, detections_to_clusterize = {}, {}
        for input_detection in detections:
            if input_detection.location_not_none():
                detections_to_clusterize.update({input_detection.fast_id: input_detection})
            else:
                detections_to_unlink.update({input_detection.fast_id: input_detection})

        # Check nearest objects without location
        # This may happen during relocalization or changes in database
        for nearest_object in nearest_objects:
            good_detections = []
            for object_detection in nearest_object.detections:
                if object_detection.location_not_none():
                    good_detections.append(object_detection)
                else:
                    if object_detection.fast_id in detections_to_unlink:
                        continue
                    detections_to_unlink.update({object_detection.fast_id: object_detection})

            if len(good_detections) < len(nearest_object.detections):
                logger.debug(f'Found object with partially localized detections. Object id {nearest_object.fast_id}')

            nearest_object.detections = good_detections
            detections_to_clusterize.update({
                detection_w_latlon.fast_id: detection_w_latlon
                for detection_w_latlon in good_detections
                if detection_w_latlon.fast_id not in detections_to_clusterize
            })
        return list(detections_to_unlink.values()), list(detections_to_clusterize.values())

    def _do_clusterize(
        self,
        detections: list[BBOXDetection],
        nearest_objects: list[DetectedObject],
    ) -> tuple[list[DetectedObject], list[DetectedObject], list[DetectedObject], list[BBOXDetection]]:
        detections_to_unlink, detections_to_clusterize = self._prepare_input(
            detections,
            nearest_objects=nearest_objects,
        )

        detected_object_types = {detection.detected_object_fields for detection in detections_to_clusterize}
        object_type_to_detections = self._detections_as_object_type_mapping(
            detections=detections_to_clusterize,
            detected_object_types=detected_object_types,
        )

        objects_to_add, objects_to_update, objects_to_remove = [], {}, []
        object_idx = 0
        for object_type in detected_object_types:
            positions = object_type_to_detections[object_type]
            label = object_type[1]
            clustering_thresholds = self._clusterization_params.get_thresholds(label)

            dbscan = DBSCAN(eps=clustering_thresholds.dbscan_eps, min_samples=1).fit(positions)  # positions=X, eps = 5
            clusterized = dbscan.labels_  # Y

            object_type_detections = [
                detection
                for detection in detections_to_clusterize
                if detection.detected_object_fields == object_type
            ]

            new_objects = DetectedObject.create_from_detections(
                object_type_detections,
                clusterized.tolist(),
                from_latest_detection=self._naive_localization,
            )
            # objects here are not yet saved to db, so we need to temporarily fill id property
            for new_obj in new_objects:
                object_idx -= 1
                new_obj.id = object_idx
            (
                label_objects_to_add,
                label_objects_to_update,
                label_objects_to_remove,
            ) = self._merge_clusters(
                nearest_objects,
                new_objects,
                cluster_objects_distance_thr=clustering_thresholds.cluster_objects_distance_thr,
            )

            objects_to_add.extend(label_objects_to_add)
            objects_to_update.update(label_objects_to_update)
            objects_to_remove.extend(label_objects_to_remove)

        # skip temporary objects, not saved to db
        objects_to_remove = [
            object_to_remove
            for object_to_remove in objects_to_remove
            if object_to_remove.fast_id > 0
        ]
        for processed_object in nearest_objects:
            if not processed_object.detections and processed_object not in objects_to_remove:
                objects_to_remove.append(processed_object)
        return objects_to_add, list(objects_to_update.values()), objects_to_remove, detections_to_unlink

    def _detections_as_object_type_mapping(
        self,
        detections: list[BBOXDetection],
        detected_object_types: Iterable[DetectedObjectTypeFields],
    ) -> dict[DetectedObjectTypeFields, BBOXDetectionsPositions]:
        return {
            object_type: self._detections_as_utm_coords([
                detection
                for detection in detections
                if detection.detected_object_fields == object_type
            ])
            for object_type in detected_object_types
        }

    def _detections_as_utm_coords(self, detections: list[BBOXDetection]) -> BBOXDetectionsPositions:
        lats = np.array([detection.lat for detection in detections])
        lons = np.array([detection.lon for detection in detections])

        utm_results = utm.from_latlon(lats, lons)
        return [(float(result[0]), float(result[1])) for result in zip(*utm_results[:2])]

    def _merge_clusters(
        self,
        existing_objects: list[DetectedObject],
        new_objects: list[DetectedObject],
        cluster_objects_distance_thr: float,
    ) -> tuple[list[DetectedObject], dict[int, DetectedObject], list[DetectedObject]]:
        objects_to_add, objects_to_update, objects_to_remove = [], {}, []
        existing_detections = {
            detection.fast_id: known_obj
            for known_obj in existing_objects
            for detection in known_obj.detections
        }
        existing_object_id_to_object = {obj.fast_id: obj for obj in existing_objects}
        existing_object_ids = set(existing_object_id_to_object)
        existing_detections_ids = set(existing_detections.keys())
        intersected_removed_ids = set()
        h3_to_near_objects_ids = _get_h3_to_near_object_ids(existing_object_id_to_object)

        for new_obj in new_objects:
            if any(detection.fast_id in existing_detections_ids for detection in new_obj.detections):
                # if there are detections in new object which already exist in old obj
                intersected_dets_objects_ids = set()  # obj_ids of new_obj detections with existing obj_id
                for detection in new_obj.detections:
                    if detection.fast_id in existing_detections_ids:
                        intersected_dets_objects_ids.add(existing_detections[detection.fast_id].fast_id)

                # if we have some detections from different old obj in new, compare with nearest
                # for each old object check if it near the current and possibly merge
                updated_obj: DetectedObject = None

                for near_obj_id in set(h3_to_near_objects_ids.get(new_obj.h3, [])):
                    existing_obj = existing_object_id_to_object[near_obj_id]
                    if near_obj_id in intersected_removed_ids or near_obj_id not in intersected_dets_objects_ids:
                        continue
                    (  # noqa: WPS236
                        new_obj,
                        updated_obj,
                        objects_to_remove,
                        objects_to_update,
                        new_removed_ids,
                    ) = self._try_merge(
                        new_obj=new_obj,
                        existing_obj=existing_obj,
                        updated_obj=updated_obj,
                        objects_to_remove=objects_to_remove,
                        objects_to_update=objects_to_update,
                        cluster_objects_distance_thr=cluster_objects_distance_thr,
                    )
                    intersected_removed_ids.update(new_removed_ids.intersection(existing_object_ids))
                    if updated_obj is not None:
                        break

                if updated_obj is None:
                    objects_to_add.append(new_obj)
            else:
                objects_to_add.append(new_obj)

        return objects_to_add, objects_to_update, objects_to_remove

    def _try_merge(
        self,
        new_obj: DetectedObject,
        existing_obj: DetectedObject,
        updated_obj: Optional[DetectedObject],
        objects_to_remove: list[DetectedObject],
        objects_to_update: dict[int, DetectedObject],
        cluster_objects_distance_thr: float,
    ):
        n_removed_objects_start = len(objects_to_remove)
        if new_obj.is_near(existing_obj, cluster_objects_distance_thr, from_latest_detection=self._naive_localization):
            # if new obj near the old - merge objects into one
            (
                new_obj,
                updated_obj,
                objects_to_remove,
                objects_to_update,
            ) = self._merge_close_located_objects(
                updated_obj=updated_obj,
                existing_obj=existing_obj,
                new_obj=new_obj,
                objects_to_remove=objects_to_remove,
                objects_to_update=objects_to_update,
            )
        else:
            # if not near - complete detections transfer from existing object to new
            objects_to_remove, objects_to_update = self._complete_detections_transfer_from_object(
                objs_to_remove=objects_to_remove,
                objs_to_update=objects_to_update,
                obj_to_check=existing_obj,
                used_somewhere_else_detections=new_obj.detections,
            )
        new_removed_ids = {obj.fast_id for obj in objects_to_remove[n_removed_objects_start:]}
        return new_obj, updated_obj, objects_to_remove, objects_to_update, new_removed_ids

    def _merge_close_located_objects(
        self,
        updated_obj: Optional[DetectedObject],
        existing_obj: DetectedObject,
        new_obj: Optional[DetectedObject],
        objects_to_remove: list[DetectedObject],
        objects_to_update: dict[int, DetectedObject],
    ):
        if updated_obj is None:
            existing_obj.merge(new_obj, from_latest_detection=self._naive_localization)
            objects_to_remove.append(new_obj)
            updated_obj = existing_obj
            objects_to_update[updated_obj.fast_id] = updated_obj
            new_obj = updated_obj
        else:
            updated_obj.merge(existing_obj, from_latest_detection=self._naive_localization)
            objects_to_update[updated_obj.fast_id] = updated_obj
            objects_to_remove, objects_to_update = self._complete_detections_transfer_from_object(
                objs_to_remove=objects_to_remove,
                objs_to_update=objects_to_update,
                obj_to_check=existing_obj,
                used_somewhere_else_detections=updated_obj.detections,
            )
            new_obj = updated_obj
        return new_obj, updated_obj, objects_to_remove, objects_to_update

    def _complete_detections_transfer_from_object(
        self,
        objs_to_remove: list[DetectedObject],
        objs_to_update: dict[int, DetectedObject],
        obj_to_check: DetectedObject,
        used_somewhere_else_detections: list[BBOXDetection],
    ) -> tuple[list[int], dict[int, DetectedObject]]:
        used_somewhere_else_detections_mapping = {
            detection.fast_id: detection
            for detection in used_somewhere_else_detections
        }
        all_detections_mapping = {detection.fast_id: detection for detection in obj_to_check.detections}
        remaining_detections_ids = set(all_detections_mapping).difference(
            set(used_somewhere_else_detections_mapping.keys()),
        )
        remaining_detections = [all_detections_mapping[detection_id] for detection_id in remaining_detections_ids]
        if remaining_detections:
            obj_to_check.detections = remaining_detections
            obj_to_check.calculate_latlon_from_detections(from_latest_detection=self._naive_localization)
            objs_to_update[obj_to_check.fast_id] = obj_to_check
        else:
            obj_to_check.detections = []
            objs_to_remove.append(obj_to_check)
        return objs_to_remove, objs_to_update


def _get_h3_to_near_object_ids(existing_objects: dict[int, DetectedObject]):
    h3_to_objects = defaultdict(list)
    for obj_id, obj in existing_objects.items():
        h3_to_objects[obj.h3].append(obj_id)
    disk_h3s = set()
    for h3_id in h3_to_objects:
        for h3_neigb_idx in h3.grid_disk(h3_id, H3_PREFILTER_RADIUS):
            disk_h3s.add(h3_neigb_idx)
    h3_disk_to_objects_ids = defaultdict(list)
    for h3_obj_idx in disk_h3s:
        for h3_neigb_idx in h3.grid_disk(h3_obj_idx, H3_PREFILTER_RADIUS):
            h3_disk_to_objects_ids[h3_obj_idx].extend(h3_to_objects.get(h3_neigb_idx, []))
    return dict(h3_disk_to_objects_ids)

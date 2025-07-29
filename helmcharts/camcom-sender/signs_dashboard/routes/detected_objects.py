import logging

from dependency_injector.wiring import Provide, inject
from flask import Response, abort, render_template

from signs_dashboard.containers.application import Application
from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.services.detected_objects import DetectedObjectsService
from signs_dashboard.small_utils import placeholder_url_for


@inject
def detections_map(
    config: dict = Provide[Application.config],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
):
    if not modules_config.is_track_localization_enabled():
        abort(404)

    lon, lat = config['map']['projects'][0]['coords']
    return render_template(
        'detections_map.html',
        map_center=(lat, lon),
        base_layer_tileserver_url_template='http://tile2.maps.2gis.com/tiles?x={x}&y={y}&z={z}',
        frame_url_template=placeholder_url_for(
            'frame',
            placeholder='frame_id_placeholder',
            placeholder_field='frame_id',
        ),
        crop_url_template=placeholder_url_for(
            'crop',
            img_only='true',
            placeholder='detection_id_placeholder',
            placeholder_field='sign_id',
        ),
    )


@inject
def update_status(
    object_id: int,
    new_status: str,
    detected_objects_service: DetectedObjectsService = Provide[Application.services.detected_objects],
):
    detected_object = detected_objects_service.get(object_id)

    if not detected_object:
        return abort(404)

    try:
        detected_objects_service.update_status(detected_object, new_status)
    except ValueError as exc:
        logging.error(f'Unable to update object status to {new_status}: {exc}')
        return abort(400)

    return Response(status=200)

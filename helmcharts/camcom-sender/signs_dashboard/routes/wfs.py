from functools import lru_cache
from typing import List, Optional

from dependency_injector.wiring import Provide, inject
from flask import abort, jsonify, render_template, request, url_for
from pyproj import Proj, Transformer

from signs_dashboard.containers.application import Application
from signs_dashboard.models.detected_object import DetectedObject
from signs_dashboard.models.frame import Frame
from signs_dashboard.query_params.wfs import RequestValidationError, WfsQueryParams
from signs_dashboard.services.detected_objects import DetectedObjectsService
from signs_dashboard.services.frames import FramesService


@inject
def wfs_map(config: dict = Provide[Application.config]):
    return render_template(
        'map.html',
        mapgl=config['map']['mapgl'],
        projects=config['map']['projects'],
        initial_project=config['map']['initial_project'],
    )


@inject
def wfs(scope: Optional[str] = 'mobile'):  # pylint: disable=R1710
    try:
        query_params = WfsQueryParams.from_request(request)
    except RequestValidationError as error:
        response = jsonify({'error': error.message})
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response, 400

    if query_params.requested_type_name == 'by_bbox':
        return _list_by_bbox(query_params, scope)

    if query_params.requested_type_name == 'similar_tracks':
        return _similar_tracks(query_params)

    if query_params.requested_type_name == 'detected_objects_with_detections':
        return _detected_objects(query_params)

    abort(400)


def _bbox_to_points(query_params: WfsQueryParams):
    in_transformer, out_transformer = _get_transformers(query_params.srs_name, query_params.crs_name)

    min_x = min(query_params.bbox[0], query_params.bbox[2])
    max_x = max(query_params.bbox[0], query_params.bbox[2])
    min_y = min(query_params.bbox[1], query_params.bbox[3])
    max_y = max(query_params.bbox[1], query_params.bbox[3])

    point1 = in_transformer.transform(min_x, min_y)
    point2 = in_transformer.transform(max_x, max_y)
    return point1, point2, out_transformer


@inject
def _list_by_bbox(
    query_params: WfsQueryParams,
    scope: str,
    frames_service: FramesService = Provide[Application.services.frames],
):
    point1, point2, out_transformer = _bbox_to_points(query_params)

    return _decorate_frames_response(
        frames_service.find_by_bbox(point1, point2, query_params.max_features, scope),
        out_transformer,
        query_params.output_format,
    )


@inject
def _similar_tracks(
    query_params: WfsQueryParams,
    frames_service: FramesService = Provide[Application.services.frames],
):
    _, out_transformer = _get_transformers(query_params.srs_name, query_params.crs_name)

    frame = frames_service.get_frame(query_params.feature_id)
    if not frame:
        return _decorate_frames_response([], out_transformer, query_params.output_format)

    distance = 0.001
    direction = 10.0

    return _decorate_frames_response(
        frames_service.find_similar_frames(frame, distance, direction, query_params.max_features),
        out_transformer,
        query_params.output_format,
    )


@inject
def _detected_objects(
    query_params: WfsQueryParams,
    detected_objects_service: DetectedObjectsService = Provide[Application.services.detected_objects],
):
    point1, point2, out_transformer = _bbox_to_points(query_params)

    return _decorate_detected_objects_response(
        detected_objects_service.find_by_bbox(point1, point2, query_params.max_features),
        out_transformer,
        query_params.output_format,
    )


@lru_cache
def _get_transformers(srs: str, crs: str):
    in_coords = Proj(srs)
    out_coords = Proj(crs)
    internal_coords = Proj('EPSG:4326')

    return (
        Transformer.from_proj(in_coords, internal_coords),
        Transformer.from_proj(internal_coords, out_coords),
    )


@inject
def _decorate_frames_response(
    frames: List[Frame],
    out_transformer: Transformer,
    output_format: str,
):
    if output_format == 'application/json':
        response = jsonify({
            'type': 'FeatureCollection',
            'features': [
                {
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': list(out_transformer.transform(frame.lat, frame.lon)),
                    },
                    'properties': {
                        'id': frame.id,
                        'track_id': frame.track_uuid,
                        'azimuth': frame.azimuth,
                        'capture_time': frame.date.isoformat()[:-3],
                        'photo_url': url_for('get_frame_predictions', frame_id=frame.id),
                    },
                }
                for frame in frames
            ],
        })
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response

    template = render_template(
        'wfs_get_feature.xml',
        frames=frames,
        coord_transformer=out_transformer,
    )
    headers = {
        'Content-Type': 'application/xml',
        'Cache-Control': 'public, max-age=900',
        'Access-Control-Allow-Origin': '*',
    }
    return template, 200, headers


@inject
def _decorate_detected_objects_response(
    detected_objects: List[DetectedObject],
    out_transformer: Transformer,
    output_format: str,
):

    if output_format != 'application/json':
        abort(400)

    response = jsonify({
        'type': 'FeatureCollection',
        'features': [
            {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': out_transformer.transform(detected_object.lon, detected_object.lat),
                },
                'properties': detected_object.as_json(),
            }
            for detected_object in detected_objects
        ],
    })
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

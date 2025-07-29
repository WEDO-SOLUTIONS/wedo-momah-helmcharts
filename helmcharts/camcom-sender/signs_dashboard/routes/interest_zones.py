import logging
from io import BytesIO

import fiona
from dependency_injector.wiring import Provide, inject
from flask import abort, jsonify, redirect, render_template, request, send_file, url_for
from shapely.geometry import shape

from signs_dashboard.containers.application import Application
from signs_dashboard.models.interest_zones import INTEREST_ZONE_TYPE_REQUIRES_NAME, InterestZoneType
from signs_dashboard.query_params.interest_zones import AddInterestZoneRequest
from signs_dashboard.repository.interest_zones import PolygonAndName
from signs_dashboard.services.interest_zones import InterestZonesService

logger = logging.getLogger(__name__)


@inject
def list_zones(
    interest_zones_service: InterestZonesService = Provide[Application.services.interest_zones],
):
    zones = interest_zones_service.get_interest_zones()

    return render_template(
        'interest_zones.html',
        zones=zones,
        known_zone_types=InterestZoneType,
    )


@inject
def add_zone(
    interest_zones_service: InterestZonesService = Provide[Application.services.interest_zones],
):
    try:
        add_request = AddInterestZoneRequest.parse_obj(request.form)
    except Exception as exc:
        logging.error(f'Unable to parse interest zone creation request: {exc}')
        return abort(400, str(exc))

    regions_file = request.files.get('regions')
    if not regions_file:
        logging.error('No regions file provided')
        return abort(400)

    try:
        polygons, srid = _parse_regions_from_request(regions_file, zone_type=add_request.zone_type)
    except ValueError as exc:
        logging.error(exc)
        return abort(400)

    interest_zones_service.add_interest_zone(
        zone_name=add_request.zone_name,
        zone_type=add_request.zone_type,
        polygons=polygons,
        from_srid=srid,
    )

    return redirect(url_for('interest_zones'))


@inject
def delete_zone(
    zone_name: str,
    interest_zones_service: InterestZonesService = Provide[Application.services.interest_zones],
):
    zone = interest_zones_service.get_interest_zone(zone_name)
    if not zone:
        return abort(404)

    interest_zones_service.delete_interest_zone(zone)

    return redirect(url_for('interest_zones'))


@inject
def view_zone(
    zone_name: str,
    interest_zones_service: InterestZonesService = Provide[Application.services.interest_zones],
    config: dict = Provide[Application.config],
):
    zone = interest_zones_service.get_interest_zone(zone_name)
    if not zone:
        return abort(404)

    zone_regions = interest_zones_service.get_interest_zone_regions_as_geojson(zone)

    return render_template(
        'interest_zone.html',
        zone=zone,
        zone_regions=zone_regions,
        base_layer_tileserver_url_template=config['map']['tileserver_url_template'],
    )


@inject
def list_search_regions(
    interest_zones_service: InterestZonesService = Provide[Application.services.interest_zones],
):
    regions = interest_zones_service.get_regions_for_search()
    return jsonify([
        {
            'id': region.id,
            'region_name': region.region_name,
            'zone_name': region.zone_name,
        }
        for region in regions
    ])


@inject
def download_zone_geojson(
    zone_name: str,
    interest_zones_service: InterestZonesService = Provide[Application.services.interest_zones],
):
    zone = interest_zones_service.get_interest_zone(zone_name)
    if not zone:
        return abort(404)

    zone_regions = interest_zones_service.get_interest_zone_regions_as_geojson(zone)

    return send_file(
        BytesIO(zone_regions.encode('utf-8')),
        mimetype='application/geo+json',
        download_name=f'{zone_name}.geojson',
    )


@inject
def update_zone(
    zone_name: str,
    interest_zones_service: InterestZonesService = Provide[Application.services.interest_zones],
):
    zone = interest_zones_service.get_interest_zone(zone_name)
    if not zone:
        return abort(404)

    regions_file = request.files.get('regions')
    if not regions_file:
        return abort(400)

    try:
        polygons, srid = _parse_regions_from_request(regions_file, zone_type=zone.zone_type)
    except ValueError as exc:
        logging.error(exc)
        return abort(400)

    interest_zones_service.recreate_zone_polygons(zone, polygons, from_srid=srid)

    return redirect(url_for('view_interest_zone', zone_name=zone.name))


def _parse_regions_from_request(file, zone_type: InterestZoneType) -> tuple[list[PolygonAndName], str]:
    polygons = []
    with fiona.open(file, mode='r', driver='GeoJSON') as layer:
        crs = layer.crs
        logger.info(f'Layer: {layer}, CRS: {layer.crs}')
        for _, feature in layer.items():
            polygons.extend(_parse_polygons(feature, zone_type))

    if not polygons:
        raise ValueError('No polygons extracted from file')

    return polygons, crs['init']


def _get_polygon_name(polygon: fiona.model.Feature, zone_type: InterestZoneType) -> str:
    name = polygon.properties.get('name')
    if zone_type in INTEREST_ZONE_TYPE_REQUIRES_NAME and not name:
        raise ValueError(
            f'Zone type {zone_type} requires region names, feature has no name: {dict(polygon.properties)}',
        )
    return name


def _parse_polygons(polygon: fiona.model.Feature, zone_type: InterestZoneType) -> list[PolygonAndName]:
    shp = shape(polygon.geometry)
    name = _get_polygon_name(polygon, zone_type)

    polygons = []
    if shp.geom_type == 'Polygon':
        polygons.append((shp, name))
    elif shp.geom_type == 'MultiPolygon':
        for subpolygon in shp.geoms:
            polygons.append((subpolygon, name))
    else:
        raise ValueError(f'Got unsupported geometry type: {shp.type} with properties {dict(polygon.properties)}')

    return polygons

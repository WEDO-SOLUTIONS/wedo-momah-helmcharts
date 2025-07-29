import uuid

from dependency_injector.wiring import Provide, inject
from flask import Response, make_response, render_template, request

from signs_dashboard.containers.application import Application
from signs_dashboard.models.track import TrackStatuses
from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.query_params.tracks import TrackQueryParameters
from signs_dashboard.services.cvat.uploader import CVATUploader
from signs_dashboard.services.events.tracks_lifecycle import TracksLifecycleService
from signs_dashboard.services.interest_zones import InterestZonesService
from signs_dashboard.services.kml_generator import KMLGeneratorService
from signs_dashboard.services.predictors import PredictorsService
from signs_dashboard.services.tracks import TracksService
from signs_dashboard.small_utils import get_form_date_from, get_form_date_to


@inject
def drivers(
    tracks_service: TracksService = Provide[Application.services.tracks],
    interest_zones_service: InterestZonesService = Provide[Application.services.interest_zones],
    predictors_service: PredictorsService = Provide[Application.services.predictors],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
):
    query_params = TrackQueryParameters.from_request(request)
    drivers_stat = tracks_service.get_drivers_stats(query_params)
    regions = interest_zones_service.get_regions_for_search()

    return render_template(
        'drivers.html',
        drivers_stat=drivers_stat,
        query_params=query_params,
        search_regions=regions,
        track_statuses=TrackStatuses,
        total_distance=round(sum(dr.distance for dr in drivers_stat), 2),
        total_duration=sum(dr.duration for dr in drivers_stat),
        total_frames=sum(dr.frames_count for dr in drivers_stat),
        predictors_names=predictors_service.get_all_predictors(),
        predictor_has_prompt=predictors_service.has_prompt,
        cvat_uploading_enabled=modules_config.is_cvat_uploading_enabled(),
        map_matching_enabled=modules_config.is_map_matching_enabled(),
    )


@inject
def drivers_audit(
    tracks_service: TracksService = Provide[Application.services.tracks],
    interest_zones_service: InterestZonesService = Provide[Application.services.interest_zones],
):
    query_params = TrackQueryParameters.from_request(request)
    drivers_stat = tracks_service.get_drivers_stats(query_params)
    regions = interest_zones_service.get_regions_for_search()

    return render_template(
        'drivers_audit.html',
        drivers_stat=drivers_stat,
        search_regions=regions,
        query_params=query_params,
    )


@inject
def download_kml(
    service: KMLGeneratorService = Provide[Application.services.kml_generator],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
):
    query_params = TrackQueryParameters.from_request(request)
    kml = service.get_kml_from_email_and_date(query_params, modules_config.is_reporter_enabled('fiji'))

    return Response(
        kml,
        mimetype='application/vnd.google-earth.kml+xml',
        headers={
            'Content-Disposition': 'attachment; filename={filename}.kml'.format(filename=query_params),
        },
    )


@inject
def download_csv(tracks_service: TracksService = Provide[Application.services.tracks]):
    query_params = TrackQueryParameters.from_request(request)
    drivers_stat = tracks_service.get_drivers_stats(query_params)

    response = make_response(
        render_template(
            'drivers.csv',
            drivers_stat=drivers_stat,
        ),
        200,
    )
    response.mimetype = 'text/csv'
    response.headers.set(
        'Content-Disposition',
        'attachment; filename={filename}.csv'.format(filename=query_params),
    )
    return response


@inject
def predict_drivers(
    tracks_service: TracksService = Provide[Application.services.tracks],
    tracks_lifecycle_service: TracksLifecycleService = Provide[Application.services.tracks_lifecycle],
):
    query_params = TrackQueryParameters.from_request(request)
    predictor = request.json['predictor']
    drivers_for_prediction = request.json['drivers']
    all_tracks = []

    for driver in drivers_for_prediction:
        query_params.email = driver['email']
        query_params.from_dt = get_form_date_from(driver['date'])
        query_params.to_dt = get_form_date_to(driver['date'])
        all_tracks.extend(tracks_service.find_tracks_by_query_params(query_params))

    for track in all_tracks:
        tracks_lifecycle_service.produce_dashboard_predict_event(
            track.uuid,
            track.user_email,
            predictor=request.json['predictor'],
            prompt=request.json['prompt'],
        )
    return {'predictor': predictor, 'drivers': drivers_for_prediction}


@inject
def upload_drivers_to_cvat(
    tracks_service: TracksService = Provide[Application.services.tracks],
    cvat_uploader: CVATUploader = Provide[Application.services.cvat_uploader],
    tracks_lifecycle_service: TracksLifecycleService = Provide[Application.services.tracks_lifecycle],
):
    query_params = TrackQueryParameters.from_request(request)
    upload_uuid = str(uuid.uuid4())
    project_id = cvat_uploader.get_or_create_project_by_name(request.json['project_name']).id

    all_tracks = []

    for driver in request.json['drivers']:
        query_params.email = driver['email']
        query_params.from_dt = get_form_date_from(driver['date'])
        query_params.to_dt = get_form_date_to(driver['date'])
        all_tracks.extend(tracks_service.find_tracks_by_query_params(query_params))
    for track in all_tracks:
        tracks_lifecycle_service.produce_dashboard_cvat_upload_event(
            track.uuid,
            track.user_email,
            project_id,
            upload_uuid,
        )
    return {'upload_uuid': upload_uuid}

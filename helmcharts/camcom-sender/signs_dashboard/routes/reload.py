from dependency_injector.wiring import Provide, inject
from flask import abort, jsonify, render_template, request

from signs_dashboard.containers.application import Application
from signs_dashboard.models.track_upload_status import STATUS_UPLOADED
from signs_dashboard.query_params.tracks_reload import TracksReloadQueryParams
from signs_dashboard.repository.tracks import TracksRepository
from signs_dashboard.services.tracks import TracksService
from signs_dashboard.services.tracks_reload import TracksReloadService


@inject
def reload_statuses(tracks_reload_service: TracksReloadService = Provide[Application.services.tracks_reloader]):
    query_params = TracksReloadQueryParams.from_request(request)
    tasks = tracks_reload_service.find(query_params)
    return render_template('reload_statuses.html', query_params=query_params, tasks=tasks)


@inject
def get_track_reload_data(
    uuid: str,
    tracks_reload_service: TracksReloadService = Provide[Application.services.tracks_reloader],
    tracks_repository: TracksRepository = Provide[Application.pg_repositories.tracks],
    tracks_service: TracksService = Provide[Application.services.tracks],
):
    track = tracks_service.get(uuid)
    if not track:
        abort(404)

    upload_process = tracks_repository.get_upload_status(uuid)
    if not upload_process or upload_process.status != STATUS_UPLOADED:
        abort(404)

    data = tracks_reload_service.prepare_reload_request(uuid)
    return jsonify(data)


@inject
def reload_track(
    tracks_reload_service: TracksReloadService = Provide[Application.services.tracks_reloader],
):
    if request.method != 'POST':
        query = tracks_reload_service.parse_track_reload_query(request.args)
        return render_template('reload_track.html', query=query)

    query = tracks_reload_service.parse_track_reload_query(request.form)

    errors = []
    tracks_to_submit = []
    tracks_with_errors = []
    for uuid in query.uuids:
        track, error = tracks_reload_service.fetch_track_reload_info(uuid, query=query)
        if track:
            tracks_to_submit.append(track)
        else:
            tracks_with_errors.append(uuid)
            errors.append(error)

    upload_result = tracks_reload_service.reload_tracks_async(tracks_to_submit)

    for skipped_uuid in upload_result['skip_tracks']:
        tracks_with_errors.append(skipped_uuid)
        errors.append(f'Track {skipped_uuid} already in queue in status "pending" or "in progress"')

    return render_template(
        'reload_track.html',
        query=query,
        errors=errors,
        tracks_with_errors=tracks_with_errors,
        tracks_submitted=upload_result['upload_tracks'],
    )


@inject
def stop_pending_tasks(
    tracks_reload_service: TracksReloadService = Provide[Application.services.tracks_reloader],
):
    tasks_ids = request.args.get('tasks_ids')
    tasks_ids = [task_id for task_id in tasks_ids.split(',') if task_id]
    tracks_reload_service.stop_pending_tasks(tasks_ids)
    return 'ok'

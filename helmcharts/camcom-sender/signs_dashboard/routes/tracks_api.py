from dependency_injector.wiring import Provide, inject
from flask import abort, jsonify, request
from flask_babel import gettext

from signs_dashboard.containers.application import Application
from signs_dashboard.models.track import TrackStatuses
from signs_dashboard.models.track_upload_status import STATUS_UPLOADED
from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.services.frames import FramesService
from signs_dashboard.services.tracks import TracksService


@inject
def get_track(
    uuid: str,
    tracks_service: TracksService = Provide[Application.services.tracks],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
):
    track = tracks_service.get(uuid)
    if not track:
        abort(404)

    track_json = {
        'uuid': track.uuid,
        'upload_status': track.upload_status,
        'user_email': track.user_email,
        'app_version': track.app_version,
        'distance': track.distance,
        'duration': track.duration,
        'reloaded': track.reloaded,
        'type': track.type,
    }

    if modules_config.is_track_localization_enabled():
        track_json.update({
            'localization_status': track.localization_status,
            'localization_text_status': gettext(track.localization_text_status),
        })

    if modules_config.is_reporter_enabled('fiji'):
        track_json.update({
            'fiji_status': track.fiji_status,
            'fiji_text_status': gettext(track.fiji_text_status),
        })

    if modules_config.is_reporter_enabled('pro'):
        track_json.update({
            'pro_status': track.pro_status,
            'pro_text_status': gettext(track.pro_text_status),
        })

    return jsonify(track_json)


@inject
def update_track(
    uuid: str,
    tracks_service: TracksService = Provide[Application.services.tracks],
):
    track = tracks_service.get(uuid)
    if not track:
        abort(404)

    request_json = request.json

    localization_status = request_json.pop('localization_status', None)
    if localization_status:
        if localization_status == TrackStatuses.LOCALIZATION_FORCED and track.localization_can_be_forced():
            tracks_service.change_localization_status(track.uuid, TrackStatuses.LOCALIZATION_FORCED)
        else:
            abort(400)

    fiji_status = request_json.pop('fiji_status', None)
    if fiji_status:
        if fiji_status == TrackStatuses.FORCED_SEND and track.sending_to_fiji_can_be_forced():
            tracks_service.change_fiji_status(track.uuid, TrackStatuses.FORCED_SEND)
        else:
            abort(400)

    return get_track(uuid)


@inject
def track_frames(
    uuid: str,
    tracks_service: TracksService = Provide[Application.services.tracks],
    frames_service: FramesService = Provide[Application.services.frames],
):
    track = tracks_service.get(uuid)
    if not track:
        abort(404)

    return jsonify({
        'frames': [
            {
                'id': frame.id,
                'meta': frame.meta,
                'image_name': frame.image_name,
            }
            for frame in frames_service.get_by_track(track)
        ],
        'track_uploaded': track.upload_status == STATUS_UPLOADED,
    })

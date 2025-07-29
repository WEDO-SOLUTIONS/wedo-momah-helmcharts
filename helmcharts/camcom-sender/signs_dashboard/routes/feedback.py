from dependency_injector.wiring import Provide, inject
from flask import Response
from werkzeug.exceptions import BadRequest, NotFound

from signs_dashboard.containers.application import Application
from signs_dashboard.models.frame import ModerationStatus
from signs_dashboard.services.events.frames_lifecycle import FramesLifecycleService
from signs_dashboard.services.frames import FramesService


@inject
def moderation_feedback(
    frame_id: int,
    moderation_status: str,
    frames_service: FramesService = Provide[Application.services.frames],
    frames_lifecycle_service: FramesLifecycleService = Provide[Application.services.frames_lifecycle],
):
    if not frame_id:
        raise BadRequest('missing required parameter "frame_id"')

    if moderation_status not in {'ok', 'fail'}:
        raise BadRequest('invalid "moderation_status" value')

    frame = frames_service.get_frame(frame_id)
    if not frame:
        raise NotFound('requested frame not found')

    good_code, wrong_code = ModerationStatus.moderated_good.value, ModerationStatus.moderated_wrong.value
    frame.moderation_status = good_code if moderation_status == 'ok' else wrong_code

    frames_service.save(frame)

    frames_lifecycle_service.produce_moderation_saved_event(frame)

    return Response(status=200)

from dependency_injector.wiring import Provide, inject
from flask import abort

from signs_dashboard.containers.application import Application
from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.reporter_fiji_cli import create_fiji_request
from signs_dashboard.services.fiji_quality import FijiQualityChecker
from signs_dashboard.services.frames import FramesService
from signs_dashboard.services.image import ImageService
from signs_dashboard.services.prediction import PredictionService
from signs_dashboard.services.tracks import TracksService


@inject
def get_fiji_request(
    uuid: str,
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
    tracks_service: TracksService = Provide[Application.services.tracks],
    prediction_service: PredictionService = Provide[Application.services.prediction],
    frames_service: FramesService = Provide[Application.services.frames],
    image_service: ImageService = Provide[Application.services.image],
    fiji_quality: FijiQualityChecker = Provide[Application.services.fiji_quality],
):
    track = tracks_service.get(uuid)
    if not track or not modules_config.is_reporter_enabled('fiji'):
        abort(404)

    frames = frames_service.get_by_track(track)
    for frame in frames:
        frame.track_email = track.user_email
    needed_predictors = modules_config.get_predictors_for('fiji')
    predictions_status = prediction_service.get_frames_predictions_status(frames, needed_predictors)

    return create_fiji_request(
        prediction_service=prediction_service,
        fiji_quality=fiji_quality,
        predictions_status=predictions_status,
        image_service=image_service,
        modules_config=modules_config,
        track=track,
        frames=frames,
        forced_send=True,
    ).dict()

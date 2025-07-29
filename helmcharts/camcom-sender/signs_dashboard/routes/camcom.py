import datetime
import logging

from dependency_injector.wiring import Provide, inject
from flask import abort, redirect, render_template, request, url_for

from signs_dashboard.containers.application import Application
from signs_dashboard.query_params.camcom import CamComStatsQueryParams
from signs_dashboard.services.camcom.camcom_sender import CamcomSenderService
from signs_dashboard.services.camcom.statistics import CamcomStatisticsService
from signs_dashboard.services.predictors import PredictorsService

logger = logging.getLogger(__name__)


@inject
def statistics_page(
    statistics_service: CamcomStatisticsService = Provide[Application.services.camcom_statistics],
    predictors: PredictorsService = Provide[Application.services.predictors],
):
    if not predictors.is_camcom_predictor_enabled():
        abort(404)

    query_params = CamComStatsQueryParams.from_request(request)
    return render_template(
        'camcom_stats.html',
        send_stats=statistics_service.statistics(query_params),
        query_params=query_params,
    )


@inject
def resend_failed_by_date(
    date: str,
    sender_service: CamcomSenderService = Provide[Application.services.camcom_sender],
    predictors: PredictorsService = Provide[Application.services.predictors],
):
    if not predictors.is_camcom_predictor_enabled():
        abort(404)

    target_date = datetime.date.fromisoformat(date)
    logger.warning(f'Resending to CamCom failed frames for date {target_date}')

    frames_with_bad_statuses = sender_service.find_frames_with_errors(target_date)
    if frames_with_bad_statuses:
        sender_service.resend_frames(frames_with_bad_statuses)

    return redirect(url_for('camcom_stats'))

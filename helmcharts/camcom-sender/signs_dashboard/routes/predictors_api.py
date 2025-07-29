import logging

from dependency_injector.wiring import Provide, inject
from flask import jsonify, render_template, request
from sqlalchemy import func as alchemy_func

from signs_dashboard.containers.application import Application
from signs_dashboard.models.predictor import Predictor
from signs_dashboard.repository.predictors import PredictorsRepository
from signs_dashboard.services.predictors import PredictorsService
from signs_dashboard.services.translations import TranslationsService
from signs_dashboard.services.twogis_pro.filters import TwoGisProFiltersService
from signs_dashboard.services.twogis_pro.filters_update import TwoGisProFiltersUpdateService

logger = logging.getLogger(__name__)


@inject
def predictors_status(predictors_service: PredictorsService = Provide[Application.services.predictors]):
    predictors = predictors_service.get_faust_predictors()
    return render_template(
        'active_predictors.html',
        predictors=predictors,
    )


@inject
def register_predictor(
    predictors_repository: PredictorsRepository = Provide[Application.pg_repositories.predictors],
    translations_service: TranslationsService = Provide[Application.services.translations],
    pro_filters_service: TwoGisProFiltersService = Provide[Application.services.twogis_pro_filters],
    pro_filters_update: TwoGisProFiltersUpdateService = Provide[Application.services.twogis_pro_filters_update],
):
    labels = request.json['labels']
    predictor_name = request.json['predictor_name']
    persist_predictor = request.json.get('persist', True)

    if persist_predictor:
        predictor = Predictor(
            name=predictor_name,
            labels=labels,
            last_register_time=alchemy_func.now(),
        )
        predictors_repository.upsert(predictor)

    translations_service.upsert_translations_for_registered_predictor(labels)

    any_class_created = pro_filters_service.create_classes_from_predictor_if_not_exists(
        predictor=predictor_name,
        codes=[label['name'] for label in labels],
    )

    if any_class_created:
        logger.warning(f'Some detection classes were created for {predictor_name}, updating filters in Pro.')
        pro_filters_update.sync()

    return jsonify({'status': 'registered'})

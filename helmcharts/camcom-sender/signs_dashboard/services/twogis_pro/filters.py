import os
from operator import attrgetter

from cached_property import cached_property_with_ttl

from signs_dashboard.models.twogis_pro_filters import DetectionClass, Filter, FilterOption, ProFiltersType
from signs_dashboard.repository.twogis_pro_filters import TwogisProFiltersRepository
from signs_dashboard.services import detected_objects
from signs_dashboard.services.camcom.SA_administrative_division import SAAdministrativeDivisionNamesService
from signs_dashboard.services.predictors import PredictorsService
from signs_dashboard.services.twogis_pro.kafka.localization import TwoGisProKafkaLocalizerService

sort_by_id_key = attrgetter('id')
FiltersToOptions = dict[Filter, list[FilterOption]]
CACHE_PERIOD = float(os.getenv('PRO_FILTERS_CACHE_PERIOD', 300))


class TwoGisProFiltersService:
    def __init__(
        self,
        pro_filters_repository: TwogisProFiltersRepository,
        saudi_arabia_administrative_divisions_names: SAAdministrativeDivisionNamesService,
        predictors_service: PredictorsService,
        localization_service: TwoGisProKafkaLocalizerService,
    ):
        self._pro_filters_repository = pro_filters_repository
        self._sa_translations = saudi_arabia_administrative_divisions_names
        self._predictors_service = predictors_service
        self._localization_service = localization_service

    def create_classes_from_predictor_if_not_exists(self, predictor: str, codes: list[str]) -> bool:
        any_created = False
        existing_classes = self.detection_classes_map
        for code in codes:
            if code in existing_classes:
                continue
            self._pro_filters_repository.add_or_update_object(DetectionClass(code=code, predictor=predictor))
            any_created = True
        return any_created

    @cached_property_with_ttl(ttl=CACHE_PERIOD)
    def detection_classes(self) -> list[DetectionClass]:
        # кеш работает только в воркерах, где объект создаётся один раз
        # в api из-за того что в DI этот класс фабрика, кеш не работает
        return self._pro_filters_repository.get_objects(DetectionClass)

    @cached_property_with_ttl(ttl=CACHE_PERIOD)
    def pro_filters(self) -> list[Filter]:
        return self._pro_filters_repository.get_objects(Filter)

    @property
    def detection_classes_map(self) -> dict[str, DetectionClass]:
        return {
            detection_class.code: detection_class
            for detection_class in self.detection_classes  # pylint: disable=E1133
        }

    def get_ui_and_index_fields(self, labels: list[str]):
        matching_options = self._matching_filter_options(labels)

        ui_matching_options = {}
        index_matching_options = {}
        for pro_filter, options in matching_options.items():
            index_matching_options[pro_filter] = options
            if pro_filter.filter_type == ProFiltersType.show_in_card:
                ui_matching_options[pro_filter] = options

        return self._filters_ui_fields(ui_matching_options), self._filter_index_fields(index_matching_options)

    def get_frames_filters_update_payload(self) -> dict:
        extra_filters = []
        if self._predictors_service.is_camcom_predictor_enabled():
            extra_filters += self._sa_translations.get_municipalities_pro_filters()

        return {
            'items': [
                *extra_filters,
                *self._get_label_filters(),
            ],
        }

    def get_objects_filters_update_payload(self) -> dict:
        return {
            'items': [
                *self._get_label_filters(),
                self._get_objects_status_filter(),
            ],
        }

    def _get_label_filters(self) -> list[dict]:
        return [
            *(
                {
                    'tag': pro_filter.index_field,  # <---  поле в {index} кадра, на которое смотрит фильтр
                    'control_type': 'checkbox_list',
                    'data_type': 'string',
                    'value_type': 'list',
                    'is_required': False,
                    'operation': 'add_or_update',
                    **self._localization_service.get_caption_translations(
                        field=pro_filter.index_field,
                    ),
                    # чекбоксы
                    'items': [
                        {
                            'value': option.code,
                            **self._localization_service.get_caption_translations(
                                field=pro_filter.index_field,
                                key=option.code,
                            ),
                        }
                        for option in sorted(pro_filter.options, key=sort_by_id_key)
                    ],
                }
                for pro_filter in sorted(self.pro_filters, key=sort_by_id_key)
            ),
            *self._get_static_filters(detection_classes=self.detection_classes),
        ]

    def _get_objects_status_filter(self) -> dict:
        return {
            'tag': 'status',
            'control_type': 'checkbox_list',
            'data_type': 'string',
            'value_type': 'list',
            'is_required': False,
            'operation': 'add_or_update',
            **self._localization_service.get_caption_translations(field='object_status'),
            'items': [
                {
                    **self._localization_service.get_caption_translations(field='object_status', key=status),
                    'value': status,
                }
                for status in (
                    detected_objects.STATUS_NEW,
                    detected_objects.STATUS_IN_PROGRESS,
                    detected_objects.STATUS_COMPLETED,
                    detected_objects.STATUS_REJECTED,
                )
            ],
        }

    def _get_static_filters(self, detection_classes: list[DetectionClass]):
        return [
            {
                'tag': 'labels',  # <---  поле в {index} кадра, на которое смотрит фильтр
                'control_type': 'checkbox_list',
                'data_type': 'string',
                'value_type': 'list',
                'is_required': False,
                'operation': 'add_or_update',
                **self._localization_service.get_caption_translations(field='filter_types'),
                # чекбоксы
                'items': [
                    {
                        'value': det_class.code,
                        **self._localization_service.get_caption_translations(field='type', key=det_class.code),
                    }
                    for det_class in sorted(detection_classes, key=sort_by_id_key)
                ],
            },
        ]

    def _matching_filter_options(self, labels: list[str]) -> FiltersToOptions:
        fields = {}
        for pro_filter in self.pro_filters:  # pylint: disable=E1133
            field_value = [
                filter_option
                for filter_option in pro_filter.options
                if set(labels).intersection(filter_option.detection_classes_labels)
            ]
            fields.update({
                pro_filter: field_value,
            })
        return fields

    def _filter_index_fields(self, matching_options: FiltersToOptions) -> dict[str, list[str]]:
        return {
            pro_filter.index_field: [
                filter_option.code
                for filter_option in pro_filter_options
            ]
            for pro_filter, pro_filter_options in matching_options.items()
        }

    def _filters_ui_fields(self, matching_options: FiltersToOptions) -> list[dict]:
        fields = []
        for pro_filter in sorted(matching_options, key=sort_by_id_key):
            pro_filter_options = matching_options[pro_filter]
            if not pro_filter_options:
                continue

            fields.append({
                'type': 'string',
                **self._localization_service.get_value_translations(
                    field=pro_filter.index_field,
                    keys=[option.code for option in pro_filter_options],
                ),
                **self._localization_service.get_caption_translations(field=pro_filter.index_field),
            })

        return fields

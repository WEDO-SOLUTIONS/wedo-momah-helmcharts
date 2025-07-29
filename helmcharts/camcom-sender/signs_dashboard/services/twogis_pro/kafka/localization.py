import datetime
from typing import Optional, Union

from signs_dashboard.services.translations import TranslationsService
from signs_dashboard.small_utils import uniques_preserving_order


class TwoGisProKafkaLocalizerService:
    def __init__(self, translations_service: TranslationsService):
        self._translations_service = translations_service

    def get_caption_translations(
        self,
        field: str,
        key: Optional[str] = None,
        keys: Optional[list[str]] = None,
        default: Optional[str] = None,
    ) -> dict[str, str]:
        return self._get_multiple_keys_translations(
            field=field,
            key=key,
            keys=keys,
            default=default,
            as_field='caption',
        )

    def get_value_translations(
        self,
        field: str,
        key: Optional[str] = None,
        keys: Optional[list[str]] = None,
        default: Optional[str] = None,
    ) -> dict[str, str]:
        return self._get_multiple_keys_translations(field=field, key=key, keys=keys, default=default, as_field='value')

    def get_value_translations_as_strftime(
        self,
        dtime: Union[datetime.date, datetime.datetime],
        field: str,
        key: Optional[str] = None,
        keys: Optional[list[str]] = None,
        default: Optional[str] = None,
    ) -> dict[str, str]:
        value_dict = self.get_value_translations(field=field, key=key, keys=keys, default=default)
        return {
            field_name: dtime.strftime(strftime_template)
            for field_name, strftime_template in value_dict.items()
        }

    def _get_multiple_keys_translations(
        self,
        field: str,
        key: Optional[str],
        keys: Optional[list[str]],
        as_field: str,
        default: Optional[str],
    ) -> dict[str, str]:
        if keys is None:
            keys = [key]

        translation = self._get_translations_as_field(field=field, keys=keys, as_field=as_field)
        if translation:
            return translation
        return {as_field: default or ''}

    def _get_translations_as_field(self, field: str, keys: list[str], as_field: str):
        translations = self._translations_service.get_translations(field=field, keys=keys)
        fields = {}
        for locale, locale_translations in translations.items():
            field_name = f'{as_field}[{locale}]'
            if locale == self._translations_service.default_locale:
                field_name = as_field
            fields[field_name] = ', '.join(uniques_preserving_order(locale_translations))
        return fields

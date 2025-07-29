import logging
from collections import defaultdict
from typing import Optional

from cached_property import cached_property_with_ttl

from signs_dashboard.models.twogis_pro_filters import Locale
from signs_dashboard.repository.translations import TranslationsRepository

CACHE_PERIOD = 300


class DeepDict(defaultdict):
    def __call__(self):
        return DeepDict(self.default_factory)


class TranslationsService:
    def __init__(self, translations_repository: TranslationsRepository):
        self._translations_repository = translations_repository

    def get_closest_or_default_locale(self, identifier: Optional[str]) -> Optional[Locale]:
        # Example: locale = ru_RU, lang = ru
        locale, lang = None, None
        if isinstance(identifier, str):
            if len(identifier) == 2:
                locale, lang = None, identifier
            elif len(identifier) == 5:
                locale, lang = identifier, identifier[:2]
        return self._translations_repository.get_closest_or_default_locale(
            locale=locale,
            lang=lang,
        )

    def get_translation_for_type(self, label: str, locale: Locale) -> Optional[str]:
        return self.get_translation_for(field='type', key=label, locale=locale)

    def get_translation_for(self, field: str, key: str, locale: Locale) -> Optional[str]:
        translations = self._translations_repository.get(field=field, key=key)
        for translation in translations:
            if translation.locale_id == locale.id:
                return translation.value
        return None

    def get_translations(
        self,
        field: str,
        key: Optional[str] = None,
        keys: Optional[list[str]] = None,
    ) -> dict[str, list[str]]:
        if keys is None:
            keys = [key]

        unmerged = [
            (
                key,
                {
                    locale.locale: translation
                    for locale, translation in self._translations_map[field][key].items()
                },
            )
            for key in keys
        ]

        locales = list({
            t_locale
            for _, key_translations in unmerged
            for t_locale in key_translations.keys()
        })

        translations = {}
        for locale in locales:
            locale_translations = []
            for key, translation in unmerged:
                tr = translation.get(locale)
                if not tr:
                    tr = translation.get(self.default_locale)
                if tr:
                    locale_translations.append(tr.value)
                else:
                    logging.error(f'Got no translation for {field=} {key=} for {locale=}! Using key as translation.')
                    locale_translations.append(key)
            translations[locale] = locale_translations

        return translations

    def upsert_translations_for_registered_predictor(self, labels: list[dict]):
        for label in labels:
            for lang, translation in label['translations'].items():
                locale = self._translations_repository.get_closest_or_default_locale(lang)
                if locale is None or locale.locale != lang:
                    continue
                self._translations_repository.upsert_value(
                    key=label['name'],
                    locale_id=locale.id,
                    field='type',
                    value=translation,
                )

    @cached_property_with_ttl(ttl=CACHE_PERIOD)
    def default_locale(self) -> str:
        for locale in self.locales:  # pylint: disable=E1133
            if locale.default:
                return locale.locale
        raise ValueError('Default locale not found in database!')

    @property
    def locales(self) -> list[Locale]:
        return self._translations_repository.get_locales()

    @cached_property_with_ttl(ttl=CACHE_PERIOD)
    def _translations_map(self):
        translations = self._translations_repository.get()
        mapping = DeepDict(DeepDict(DeepDict(dict)))
        for translation in translations:
            mapping[translation.field][translation.key][translation.locale] = translation
        if not mapping:
            logging.error('Got no translations from DB!')
        return mapping

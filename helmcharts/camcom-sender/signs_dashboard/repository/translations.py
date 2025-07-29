import logging
from typing import Any, Optional

from sqlalchemy import func

from signs_dashboard.models.translations import Translation
from signs_dashboard.models.twogis_pro_filters import Locale

logger = logging.getLogger(__name__)


class TranslationsRepository:

    def __init__(self, session_factory):
        self.session_factory = session_factory

    def get(
        self, field: Optional[str] = None, key: Optional[str] = None, locale_id: Optional[int] = None,
    ) -> list[Translation]:
        with self.session_factory() as session:
            query = session.query(Translation)
            if field:
                query = query.filter(Translation.field == field)
            if key:
                query = query.filter(Translation.key == key)
            if locale_id is not None:
                query = query.filter(Translation.locale_id == locale_id)
            return query.all()

    def get_closest_or_default_locale(
        self,
        locale: Optional[str] = None,
        lang: Optional[str] = None,
    ) -> Optional[Locale]:
        with self.session_factory() as session:
            conditions = [
                Locale.default.is_(True),
            ]
            if lang:
                conditions.insert(0, _normalize(Locale.locale) == _normalize(lang))
            if locale:
                conditions.insert(0, _normalize(Locale.locale) == _normalize(locale))

            for condition in conditions:
                selected = session.query(Locale).filter(condition).first()
                if selected:
                    return selected
            return None

    def get_locales(self) -> list[Locale]:
        with self.session_factory() as session:
            return session.query(Locale).all()

    def upsert_value(
        self,
        key: str,
        locale_id: int,
        field: str,
        value: str,
    ):

        translations = self.get(key=key, locale_id=locale_id, field=field)

        if not translations:  # noqa: WPS504 мотивация линтера на это правило работает только для if-else без elif
            translation = Translation(key=key, locale_id=locale_id, field=field, value=value)
        elif len(translations) == 1:
            translation = translations[0]
            translation.value = value
        else:
            raise AssertionError(f'Multiple translations for key={key}, locale_id={locale_id}, field={field}')

        with self.session_factory() as session:
            session.add(translation)
            session.commit()


def _normalize(field: Any):
    return func.translate(func.lower(field), '-', '_')

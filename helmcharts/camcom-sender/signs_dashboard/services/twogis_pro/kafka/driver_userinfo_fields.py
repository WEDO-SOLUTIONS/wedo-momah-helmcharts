from typing import Any

from signs_dashboard.models.user import ApiUser
from signs_dashboard.services.twogis_pro.kafka.localization import TwoGisProKafkaLocalizerService
from signs_dashboard.services.users import USER_OIDC_META_TRANSLATIONS_FIELD


class DriverInformationRenderingMixin:

    _localization_service: TwoGisProKafkaLocalizerService

    def _driver_extra_ui_fields(self, api_user: ApiUser) -> list[dict]:
        if not api_user or not api_user.oidc_meta:
            return []
        return [
            {
                'type': 'string',
                **self._localization_service.get_caption_translations(field=USER_OIDC_META_TRANSLATIONS_FIELD, key=key),
                'value': str(api_user.oidc_meta[key]),
            }
            for key in sorted(api_user.oidc_meta)
        ]

    def _driver_extra_index_fields(self, api_user: ApiUser) -> dict[str, Any]:
        if not api_user or not api_user.oidc_meta:
            return {}
        return {
            f'driver_{key}': str(api_user.oidc_meta[key])
            for key in sorted(api_user.oidc_meta)
        }

    def _driver_extra_searchable_keywords(self, api_user: ApiUser) -> list[str]:
        if not api_user or not api_user.oidc_meta:
            return []
        return [
            str(api_user.oidc_meta[key])
            for key in sorted(api_user.oidc_meta)
        ]

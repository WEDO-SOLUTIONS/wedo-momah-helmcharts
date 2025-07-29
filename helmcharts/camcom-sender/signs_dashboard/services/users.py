from typing import List, Optional, Tuple

from signs_dashboard.models.user import ApiUser
from signs_dashboard.query_params.users import UsersCreateRequest, UsersListRequest
from signs_dashboard.repository.users import UsersRepository
from signs_dashboard.services.translations import TranslationsService

USER_OIDC_META_TRANSLATIONS_FIELD = 'driver_oidc_meta'


class UsersService:
    def __init__(
        self,
        users_repository: UsersRepository,
        translations_service: TranslationsService,
        config: dict,
    ):
        self._dashboard_locale = config.get('default_locale', 'ru')
        self._translations_service = translations_service
        self._users_repository = users_repository

    def create(self, req: UsersCreateRequest) -> ApiUser:
        return self._users_repository.create(req)

    def find(self, req: UsersListRequest) -> Tuple[List[ApiUser], int]:
        return self._users_repository.find(req)

    def get(self, user_id: int) -> ApiUser:
        return self._users_repository.get(user_id)

    def get_by_email(self, email: str) -> Optional[ApiUser]:
        return self._users_repository.get_by_email(email)

    def update(self, model: ApiUser) -> ApiUser:
        return self._users_repository.update(model)

    def delete(self, user_id: int):
        self._users_repository.delete(user_id)

    def render_api_user_info(self, user: ApiUser) -> list[tuple[str, str, str]]:
        fields = []
        if not user.oidc_meta:
            return fields

        locale = self._translations_service.get_closest_or_default_locale(self._dashboard_locale)
        for key in sorted(user.oidc_meta):
            fields.append((
                key,
                self._translations_service.get_translation_for(
                    field=USER_OIDC_META_TRANSLATIONS_FIELD,
                    key=key,
                    locale=locale,
                ),
                user.oidc_meta[key],
            ))
        return fields

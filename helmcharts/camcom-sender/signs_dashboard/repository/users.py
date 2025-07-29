from typing import List, Optional, Tuple

from sqlalchemy import delete, func

from signs_dashboard.models.user import ApiUser
from signs_dashboard.query_params.users import UsersCreateRequest, UsersListRequest


class UsersRepository:
    def __init__(self, session_factory):
        self._session_factory = session_factory

    def create(self, req: UsersCreateRequest) -> ApiUser:
        model = ApiUser(
            email=req.email,
            password_hash=req.password_hash,
            enabled=req.enabled,
        )

        with self._session_factory(expire_on_commit=False) as session:
            session.add(model)
            session.commit()

            return model

    def find(self, req: UsersListRequest) -> Tuple[List[ApiUser], int]:
        with self._session_factory() as session:
            query = session.query(ApiUser)

            if req.email:
                query = query.filter(ApiUser.email.ilike(f'%{req.email}%'))
            if req.enabled is not None:
                query = query.filter(ApiUser.enabled == req.enabled)

            total = query.count()

            query = query.limit(req.items_per_page).offset((req.page - 1) * req.items_per_page)
            return query.all(), total

    def get(self, user_id: int) -> ApiUser:
        with self._session_factory(expire_on_commit=False) as session:
            return session.query(ApiUser).get(user_id)

    def get_by_email(self, email: str) -> Optional[ApiUser]:
        with self._session_factory(expire_on_commit=False) as session:
            return session.query(ApiUser).filter(func.lower(ApiUser.email) == func.lower(email)).first()

    def update(self, user_model: ApiUser) -> ApiUser:
        with self._session_factory(expire_on_commit=False) as session:
            session.add(user_model)
            session.commit()

        return user_model

    def delete(self, user_id: int):
        with self._session_factory() as session:
            session.execute(delete(ApiUser).where(ApiUser.id == user_id))
            session.commit()

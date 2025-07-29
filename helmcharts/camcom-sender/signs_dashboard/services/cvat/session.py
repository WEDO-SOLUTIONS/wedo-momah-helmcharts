import typing as tp

from cachetools import func as cache_func
from cvat_sdk import Client, make_client
from pydantic import BaseModel


class CVATConfig(BaseModel):
    cvat_url: str
    login: str
    password: str
    organization: str

    @classmethod
    def create_empty(cls) -> 'CVATConfig':
        return CVATConfig(cvat_url='', login='', password='', organization='')  # noqa: S106


class CVATSession:
    """Обёртка над сессией цвата, которая ресетится раз в n минут, т.к. логин в цват небыстрый"""
    def __init__(self, cfg: CVATConfig):
        self._cfg = cfg
        self._client: tp.Optional[Client] = None

    @property
    def cvat_url(self) -> str:
        return self._cfg.cvat_url

    @property
    def client(self) -> tp.Optional[Client]:
        if not self._cfg.cvat_url:
            return None
        self._recreate()
        return self._client

    @cache_func.ttl_cache(maxsize=1, ttl=10 * 60)
    def _recreate(self):
        if self._client:
            self._client.close()
        self._client = make_client(
            host=self._cfg.cvat_url,
            credentials=(self._cfg.login, self._cfg.password),
        )
        self._client.organization_slug = self._cfg.organization

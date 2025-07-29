import logging
import typing as tp

import requests
from sentry_sdk import capture_message

from signs_dashboard.schemas.fiji.request import FijiRequest
from signs_dashboard.schemas.fiji.response import FijiResponse

logger = logging.getLogger(__name__)


class FijiClient:

    def __init__(self, config: dict):
        if not isinstance(config, dict):
            config = {}

        self.host = config.get('host')
        self.path = config.get('path')
        self.timeout = config.get('timeout')
        self.max_retries = config.get('max_retries')
        self.retries_timeout = config.get('retries_timeout')
        self.endpoint = f'http://{self.host}/{self.path}'
        self._session = requests.Session()
        self.valid_statuses = [200, 422]
        self.allow_forced_host = config.get('allow_forced_host')

    def __call__(
        self,
        request_data: FijiRequest,
        forced_fiji_host: tp.Optional[str] = None,
    ) -> tp.Tuple[requests.Response, tp.Optional[FijiResponse]]:
        data_dict = request_data.dict()

        id_log = f'[id]: {data_dict["id"]}'
        logger.warning(f'{id_log}, Request to Fiji.')

        if self.allow_forced_host and forced_fiji_host:
            endpoint = f'http://{forced_fiji_host}/{self.path}'
        else:
            endpoint = self.endpoint
        logger.warning(f'ENDPOINT {endpoint}, {forced_fiji_host}')

        response = self._session.post(
            endpoint,
            json=data_dict,
            timeout=self.timeout,
        )

        text_log = f'{id_log}, [status code]: {response.status_code}, [body]: {response.content}'
        logger.warning(text_log)

        if response.status_code not in self.valid_statuses:
            capture_message(text_log)

        if response.status_code == 200 and response.json() and isinstance(response.json(), dict):
            return response, FijiResponse(**response.json())
        return response, None

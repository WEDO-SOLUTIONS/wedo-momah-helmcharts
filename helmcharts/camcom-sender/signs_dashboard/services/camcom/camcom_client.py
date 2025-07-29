import logging
from datetime import datetime
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

logger = logging.getLogger(__name__)


class CamComClient:
    def __init__(
        self,
        endpoint_url: str,
        api_key: str,
        timeout: float,
        retries: int,
        retries_backoff: float,
        source_env: Optional[str],
    ):
        self._endpoint_url = endpoint_url
        self._api_key = api_key
        self._timeout = timeout
        self._retries = retries
        self._retries_backoff = retries_backoff
        self._source_env = source_env

        max_retries = Retry(  # pylint: disable=E1123
            total=self._retries,
            backoff_factor=self._retries_backoff,
            method_whitelist=['POST'],
            status_forcelist=[429, 502, 503, 504, 404],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=max_retries)
        self._session = requests.Session()
        self._session.mount('https://', adapter)
        self._session.mount('http://', adapter)

    def send(
        self,
        s3_info: dict,
        frame_id: int,
        azimuth: float,
        latitude: float,
        longitude: float,
        frame_datetime: datetime,
        extra_args: dict,
    ) -> tuple[str, int, str]:
        headers = {'x-api-auth': self._api_key}

        response = self._session.post(
            self._endpoint_url,
            headers=headers,
            **self._build_request_body(
                s3_info=s3_info,
                frame_id=frame_id,
                latitude=latitude,
                azimuth=azimuth,
                longitude=longitude,
                frame_datetime=frame_datetime,
                extra_args=extra_args,
            ),
            timeout=self._timeout,
        )

        if response.status_code == 401:
            raise CamComAuthorizationError(
                status_code=response.status_code,
                response_text=response.text,
            )

        response_info = f'{response.status_code}, {response.headers}, {response.text}'
        logger.warning(f'Got camcom response: {response_info}')

        if response.status_code not in {200, 409}:
            raise CamComResponseError(
                f'unexpected CamCom response {response_info}',
                status_code=response.status_code,
                response_text=response.text,
            )

        try:
            feedback = response.json()
        except Exception:
            raise CamComResponseError(
                f'non json CamCom response {response_info}',
                status_code=response.status_code,
                response_text=response.text,
            )

        job_id = feedback.get('citylens_id')
        if not job_id and _is_already_exists_response(response.status_code, feedback, frame_id):
            job_id = str(frame_id)

        if not job_id:
            msg = feedback.get('message', None)
            raise CamComResponseError(
                f'malformed CamCom response {response_info} msg: {msg}',
                status_code=response.status_code,
                response_text=response.text,
            )

        return job_id, response.status_code, response.text

    def _build_request_body(
        self,
        s3_info: dict,
        frame_id: int,
        azimuth: float,
        latitude: float,
        longitude: float,
        frame_datetime: datetime,
        extra_args: dict,
    ) -> dict[str, any]:
        data = {
            'latitude': latitude,
            'longitude': longitude,
            'azimuth': azimuth,
            'date': frame_datetime.date().strftime('%Y-%m-%d'),
            'datetime_utc': frame_datetime.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'citylens_id': frame_id,
            **_stringify(extra_args),
        }
        if s3_info:
            data.update({
                **s3_info,
            })

        if self._source_env:
            data.update({
                'source_env': self._source_env,
            })

        logger.warning(f'Prepared request for frame id {frame_id}: {data}')

        return {'data': data}


class CamComAuthorizationError(Exception):
    """
    CamComAuthorizationError.
    """
    def __init__(self, *args, status_code: int, response_text: Optional[str]):
        super().__init__(*args)
        self.status_code = status_code
        self.response_text = response_text


class CamComResponseError(Exception):
    """
    CamComResponseError. Unexpected CamCom response
    """

    def __init__(self, message, status_code: int, response_text: Optional[str]):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text


def _is_already_exists_response(status_code: int, response: dict, frame_id: int) -> bool:
    message = response.get('message') or ''
    return all([
        status_code == 409,
        'already exists' in message.lower(),
        str(frame_id) in message,
    ])


def _stringify(attributes: dict[str, Any]) -> dict[str, str]:  # noqa: WPS231
    final = {}
    for attribute, attribute_val in attributes.items():
        if isinstance(attribute_val, str):
            final[attribute] = attribute_val
        elif isinstance(attribute_val, bool):
            final[attribute] = str(attribute_val).lower()
        elif attribute_val is None:
            final[attribute] = ''
        else:
            raise ValueError(f'Unexpected attribute type: {attribute_val}')
    return final

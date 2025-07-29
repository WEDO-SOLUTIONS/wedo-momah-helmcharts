import logging
import typing as tp

import requests
from pydantic import BaseModel, Extra
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from yarl import URL

from signs_dashboard.models.track import Track
from signs_dashboard.services.map_matching.exceptions import MapMatchingUnavailableError, UnableToMatchTrackError
from signs_dashboard.services.map_matching.logging import MapMatchingLoggingService
from signs_dashboard.services.s3_service import S3Service

logger = logging.getLogger(__name__)

UNAVAILABLE_CODES = (403, 501, 502, 503)
UNABLE_TO_MATCH_CODES = (204, 400, 422)
RETRY_CODES = (501, 502, 503)


class MapMatchingResult(BaseModel):
    distance: float
    query: list[dict]

    class Config:
        extra = Extra.ignore


class MapMatchingAPIClient:
    def __init__(self, s3_service: S3Service, map_matching_config: tp.Optional[dict]):
        self._s3_service = s3_service
        if map_matching_config is None:
            map_matching_config = {}
        self.host = map_matching_config.get('host')
        if self.host:
            self.host = URL(self.host)
        self._key = map_matching_config.get('key')
        self._verify_ssl = map_matching_config.get('verify_ssl')

        self._timeout = map_matching_config.get('timeout') or 10
        retries = Retry(
            total=map_matching_config.get('retries_total') or 0,
            backoff_factor=map_matching_config.get('retries_backoff_factor') or 0,
            status_forcelist=RETRY_CODES,
        )
        self._session = requests.Session()
        self._session.mount('http://', HTTPAdapter(max_retries=retries))
        self._session.mount('https://', HTTPAdapter(max_retries=retries))

    def match(self, gps_points: list[dict], track: Track) -> MapMatchingResult:
        url = self.host / 'map_matching' / '1.0.0'
        params = {'key': self._key}
        with MapMatchingLoggingService(self._s3_service, track=track) as mm_logging:
            mm_logging.save_input(gps_points=gps_points, url=str(url))
            response = self._session.post(
                url,
                params=params,
                json={'query': gps_points},
                timeout=self._timeout,
                verify=self._verify_ssl,
            )
            mm_logging.save_output(
                status_code=response.status_code,
                headers=response.headers,
                body=response.text,
            )
        logger.info(f'Map matching response status code {response.status_code}')

        if response.status_code in UNAVAILABLE_CODES:
            raise MapMatchingUnavailableError(
                f"Map matching service is unavailable: {response.status_code}: '{response.text}'",
            )

        if response.status_code in UNABLE_TO_MATCH_CODES:
            raise UnableToMatchTrackError(
                f"Map matching service is unable to match track: {response.status_code}: '{response.text}'",
            )

        if response.status_code != 200:
            raise MapMatchingUnavailableError(f"Unexpected status code: {response.status_code}: '{response.text}'")

        try:
            resp = response.json()
        except Exception:
            raise MapMatchingUnavailableError(f"Unexpected response: {response.status_code}: '{response.text}'")

        logger.debug(f'Map matching results {resp}')

        if resp.get('status') != 'OK':
            raise UnableToMatchTrackError(f'Unable to match track: {resp}')

        return self._parse_response(resp)

    def _parse_response(self, resp: dict) -> MapMatchingResult:
        try:
            return MapMatchingResult.parse_obj(resp)
        except Exception as exc:
            raise UnableToMatchTrackError(f'Unable to parse response: {exc}: {resp}')

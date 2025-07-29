from requests import Session
from yarl import URL


class TwoGisProAPIClient:
    def __init__(self, config: dict):
        self._base_url = URL(config.get('base_url') or '')
        self._token = config.get('token')
        self._verify_ssl = config.get('verify_ssl', True)

        self._session = Session()

    def update_filters(self, asset_name: str, payload: dict):
        endpoint = self._base_url / asset_name / 'filters'
        resp = self._session.put(
            endpoint,
            json=payload,
            headers={
                'Authorization': f'Bearer {self._token}',
            },
            verify=self._verify_ssl,
        )
        return resp.status_code, resp.text

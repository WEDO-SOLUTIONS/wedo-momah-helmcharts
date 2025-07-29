import json
import logging
import os
from base64 import urlsafe_b64encode
from functools import wraps
from hashlib import sha256
from http import HTTPStatus
from typing import Any, Callable, Dict, Optional, Tuple
from urllib.parse import urlencode
from uuid import uuid4

import httpx
import requests
from cached_property import cached_property
from httpx import Timeout
from requests import HTTPError

from signs_dashboard.keycloak.structs import ClientConfig, OpenId

logger = logging.getLogger(__name__)

settings_path_env_var = 'KEYCLOAK_SETTINGS'


def auth_header(token_val: str) -> Dict:
    """method to generate authorization header"""
    return {'Authorization': f'Bearer {token_val}'}


def handle_exceptions(func: Callable) -> Any:
    """decorator to take care of HTTPError"""

    @wraps(func)
    def wrapper(*args: Tuple, **kwargs: Dict) -> Any:
        try:
            return func(*args, **kwargs)
        except HTTPError as ex:
            if hasattr(ex.response, "content"):
                logger.exception(ex.response.content)
            raise ex
        except Exception as ex:
            logger.exception("Error occurred:")
            raise ex

    return wrapper


class OpenIDClient:
    def __init__(
        self,
        callback_uri: str = 'http://localhost/kc/callback',
    ) -> None:
        self.callback_uri = callback_uri

    def login(self, scopes: Tuple = ('openid', 'profile')) -> Tuple:
        # OIDC Authorization Code Flow (with optional PKCE)
        state = uuid4().hex
        code_verifier = state * 2
        arguments_dict = {
            'state': state,
            'client_id': self.config.client_id,
            'response_type': 'code',
            'scope': ' '.join(scopes),
            'redirect_uri': self.callback_uri,
        }
        if self.config.pkce:
            code_challenge = urlsafe_b64encode(sha256(code_verifier.encode('utf-8')).digest())
            if code_challenge.endswith(b'='):
                code_challenge = code_challenge.rstrip(b'=')
            arguments_dict.update({
                'code_challenge': code_challenge.decode('utf-8'),
                'code_challenge_method': 'S256',
            })
        arguments = urlencode(arguments_dict)
        return f'{self.openid.authorization_endpoint}?{arguments}', state

    @handle_exceptions
    def callback(self, code: str, state: str) -> Dict:
        # OIDC Authorization Code Flow (with optional PKCE)
        payload = {
            'code': code,
            'grant_type': 'authorization_code',
            'redirect_uri': self.callback_uri,
            'client_id': self.config.client_id,
            'client_secret': self.config.client_secret,
        }
        if self.config.pkce:
            payload.update({'code_verifier': state * 2})
        logger.debug('Retrieving user tokens from server')
        response = httpx.post(
            self.openid.token_endpoint,
            data=payload,
            verify=self.config.verify_ssl,
            timeout=self.config.timeout,
        )
        if response.status_code == HTTPStatus.BAD_REQUEST:
            logger.error(f'Token request failed: {response.text}')
        response.raise_for_status()
        logger.debug('User tokens retrieved successfully')
        return response.json()

    @handle_exceptions
    def fetch_userinfo(self, access_token: str = None) -> Dict:
        headers = auth_header(access_token)
        logger.debug('Retrieving user info from server')
        response = httpx.post(
            self.openid.userinfo_endpoint,
            headers=headers,
            verify=self.config.verify_ssl,
            timeout=self.config.timeout,
        )
        response.raise_for_status()
        logger.debug('User info retrieved successfully')
        return response.json()

    def back_channel_logout(self, access_token: str, refresh_token: Optional[str]) -> None:
        # OIDC Back-Channel Logout
        payload = {
            'client_id': self.config.client_id,
            'client_secret': self.config.client_secret,
        }
        if refresh_token:
            payload.update({'refresh_token': refresh_token})
        headers = auth_header(access_token)
        logger.debug('Logging out user from server')
        response = httpx.post(
            self.openid.end_session_endpoint,
            data=payload,
            headers=headers,
            verify=self.config.verify_ssl,
            timeout=self.config.timeout,
        )
        if response.status_code not in (200, 204, 301, 302):
            logger.error(f'Unexpected response status code {response.status_code} on logout: {response.text}')
            response.raise_for_status()
        logger.debug('User logged out successfully')

    @cached_property
    def config(self) -> ClientConfig:
        logger.debug('Loading client config from the settings file')
        file_path = os.getenv(settings_path_env_var, 'keycloak.json')
        with open(file_path, 'rt') as stream:
            data = json.loads(stream.read())
            timeout = None
            if 'timeout' in data:
                timeout = Timeout(data['timeout'])
            return ClientConfig(
                realm=data.get('realm'),
                auth_server_url=data['auth-server-url'],
                client_id=data['resource'],
                client_secret=data['credentials']['secret'],
                verify_ssl=data['verify-ssl'],
                pkce=data.get('pkce'),
                timeout=timeout,
            )

    @cached_property
    def openid(self) -> OpenId:
        logger.debug('Loading openid config using well-known endpoint')
        response = requests.get(self._openid_endpoint, verify=self.config.verify_ssl)
        response.raise_for_status()
        data = response.json()
        return OpenId(
            authorization_endpoint=data['authorization_endpoint'],
            token_endpoint=data['token_endpoint'],
            userinfo_endpoint=data['userinfo_endpoint'],
            end_session_endpoint=data['end_session_endpoint'],
        )

    @property
    def _openid_endpoint(self) -> str:
        auth_server_url = self.config.auth_server_url.rstrip('/')
        endpoint = '/.well-known/openid-configuration'
        if self.config.realm:
            endpoint = '/realms/' + self.config.realm + endpoint
        return auth_server_url + endpoint

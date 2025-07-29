from dataclasses import dataclass
from typing import Dict, Optional

from httpx import Timeout


@dataclass
class ClientConfig:
    auth_server_url: str
    client_id: str
    client_secret: Dict
    verify_ssl: bool
    realm: Optional[str]
    pkce: bool
    timeout: Optional[Timeout]


@dataclass
class OpenId:
    authorization_endpoint: str
    token_endpoint: str
    userinfo_endpoint: str
    end_session_endpoint: str

# -*- coding: utf-8 -*-
import json
import logging
import os
from typing import Any, Callable, Dict

from flask import Config, Flask, Request, redirect
from flask.sessions import SessionInterface
from werkzeug.routing.map import Map
from werkzeug.routing.rules import Rule
from werkzeug.wrappers import Response

from signs_dashboard.keycloak.client import OpenIDClient

logger = logging.getLogger(__name__)

STATIC_AUTH_TOKEN_ENV = os.environ.get('DASHBOARD_STATIC_AUTH_TOKEN')
BEARER_SEPARATOR = ' '


class ProxyApp:
    def __init__(self, config: Config):
        self.config = config

    @property
    def secret_key(self) -> str:
        return self.config["SECRET_KEY"]

    @property
    def session_cookie_name(self) -> str:
        return self.config["SESSION_COOKIE_NAME"]

    @property
    def permanent_session_lifetime(self) -> int:
        return self.config["PERMANENT_SESSION_LIFETIME"]  # pragma: nocover


class AuthenticationMiddleware:
    session_interface: SessionInterface = None  # type: ignore

    token_auth_urlmap = Map([
        Rule('/api/tracks/<uuid>/reload', methods=['GET']),
        Rule('/frames/<frame_id>/image', methods=['GET']),
        Rule('/api/frames/predict_frames', methods=['GET']),
    ])
    without_auth_urlmap = Map([
        Rule('/api/frames/<frame_id>/predictions', methods=['GET']),
        Rule('/api/frames/<frame_id>/info', methods=['GET']),
        Rule('/api/frames/<frame_id>/moderate/<moderation_status>', methods=['PUT']),
    ])

    def __init__(
        self,
        app: Flask,
        config: Config,
        session_interface: SessionInterface,
        domain: str = "http://localhost:5000",
        callback_url: str = "/kc/callback",
        login_redirect_uri: str = "/",
        logout_uri: str = "/logout",
        logout_redirect_uri: str = "/logout/success",
    ) -> None:
        callback_url_path = callback_url
        callback_url = domain.rstrip('/') + callback_url_path

        self.app = app
        self.config = config
        self.session_interface = session_interface
        self.callback_url = callback_url
        self.callback_url_path = callback_url_path
        self.login_redirect_uri = login_redirect_uri
        self.logout_uri = logout_uri
        self.logout_redirect_uri = logout_redirect_uri
        self.kc = OpenIDClient(callback_uri=callback_url)
        self.proxy_app = ProxyApp(config)

    def _response(
        self, environ: Dict, start_response: Callable, session: Any, response: Callable
    ) -> Callable:
        self.session_interface.save_session(self.proxy_app, session, response)
        return response(environ, start_response)

    def __call__(self, environ: Dict, start_response: Callable) -> Callable:
        request = Request(environ)
        session = self.session_interface.open_session(  # type: ignore
            self.proxy_app, request
        )

        # callback request
        if request.path == self.callback_url_path:
            response = self.callback(session, request)
            return self._response(environ, start_response, session, response)

        # logout request
        if request.path == self.logout_uri:
            response = self.logout(session)
            return self._response(environ, start_response, session, response)

        if request.path == self.logout_redirect_uri:
            response = Response(  # type: ignore[misc]
                "<!doctype html>\n"
                "<html lang=en>\n"
                "<title>Logout success</title>\n"
                "<h1>Logout success</h1>\n"
                "<p>You have successfully logged out</p>\n"
                f'<p>Press <a href="{self.login_redirect_uri}">here</a> to login.</p>\n'
                '</html>\n',
                status=200,
                mimetype="text/html",
            )
            return self._response(environ, start_response, session, response)

        # для некоторых урлов - авторизация по статичному токену
        # для некоторых урлов - авторизации нет
        if self.auth_via_token(request) or self._request_matches(self.without_auth_urlmap, request.environ):
            return self.app(environ, start_response)

        # unauthorized request
        if "user" not in session:
            response = self.login(session)
            return self._response(environ, start_response, session, response)

        # authorized request
        return self.app(environ, start_response)

    def login(self, session: Dict) -> Response:
        """Initiate authentication"""
        url, state = self.kc.login()
        session["state"] = state
        return redirect(url)

    def callback(self, session: Dict, request: Request) -> Response:
        """Authentication callback handler"""

        # validate state
        state = request.args.get("state", "unknown")
        _state = session.pop("state", None)
        if state != _state:
            return Response("Invalid state", status=403)

        # fetch user tokens
        code: str = request.args.get("code", "unknown")
        tokens = self.kc.callback(code, state)
        session["tokens"] = json.dumps(tokens)

        # fetch user info
        access_token = tokens["access_token"]
        user = self.kc.fetch_userinfo(access_token)
        # Very long user info may overflow cookie length
        session["user"] = json.dumps({
            field: field_value
            for field, field_value in user.items()
            if field in {'email', 'preferred_username'}
        })

        return redirect(self.login_redirect_uri)

    def logout(self, session: Dict) -> Response:

        if "tokens" in session:
            tokens = json.loads(session["tokens"])
            access_token = tokens["access_token"]
            refresh_token = tokens.get("refresh_token")
            self.kc.back_channel_logout(access_token, refresh_token)
            del session["tokens"]

        if "user" in session:
            del session["user"]

        return redirect(self.logout_redirect_uri)

    def auth_via_token(self, request: Request):
        if not self._request_matches(self.token_auth_urlmap, request.environ):
            return False

        authorization = request.headers.get('Authorization')

        if STATIC_AUTH_TOKEN_ENV and authorization and BEARER_SEPARATOR in authorization:
            auth_type, token = authorization.split(BEARER_SEPARATOR, maxsplit=1)
            return auth_type.lower() == 'bearer' and token == STATIC_AUTH_TOKEN_ENV
        return False

    def _request_matches(self, urlmap: Map, environ: Dict) -> bool:
        urls = urlmap.bind_to_environ(environ)
        try:
            urls.match()
        except Exception:
            pass
        else:
            return True
        return False

from dataclasses import asdict, dataclass
from typing import Optional

from flask import Request
from passlib.hash import bcrypt


class UserValidationError(Exception):
    def __init__(self, field: str, message: str, *args):
        super().__init__(*args)

        self.field = field
        self.message = message


@dataclass
class UsersCreateRequest:
    email: str
    password: str
    enabled: bool

    @classmethod
    def from_request(cls, req: Request, is_update: bool = False) -> 'UsersCreateRequest':
        email = req.form.get('email', '')

        if not email or '@' not in email:
            raise UserValidationError(field='email', message='invalid email')

        password, password_confirm = req.form.get('password'), req.form.get('password_confirm')

        if not password and not is_update:
            raise UserValidationError(field='password', message='password required')

        if password != password_confirm:
            raise UserValidationError(field='password_confirm', message='passwords does not match')

        return cls(
            email=email,
            password=password,
            enabled=req.form.get('enabled', 'off') == 'on',
        )

    @property
    def password_hash(self) -> str:
        return str(bcrypt.hash(self.password))


@dataclass
class UsersListRequest:
    page: int = 1
    email: str = ''
    enabled: Optional[bool] = None
    items_per_page: int = 20

    def to_dict(self, **kwargs) -> dict:
        items = asdict(self)
        items.update(kwargs)

        return items

    @classmethod
    def from_request(cls, req: Request) -> 'UsersListRequest':
        enabled = req.args.get('enabled')

        return cls(
            page=int(req.args.get('page', 1)),
            email=req.args.get('email', ''),
            enabled=enabled == 'true' if enabled else None,
        )

import json
from datetime import datetime
from typing import Optional

from dependency_injector.wiring import Provide, inject
from flask import session
from flask_babel import format_datetime, gettext

from signs_dashboard.containers.application import Application
from signs_dashboard.modules_config import ModulesConfig


@inject
def prettify_hours(hr_time: Optional[float] = None) -> Optional[str]:
    if hr_time is None:
        return None

    n_hours = int(hr_time)
    n_minutes = round((hr_time - n_hours) * 60)
    hours_part = f'{n_hours}{gettext("ч")} ' if n_hours > 0 else ''
    return f'{hours_part}{n_minutes}{gettext("м")}'


def prettify_time_ms(date: datetime) -> str:
    return date.strftime('%H:%M:%S.%f')[:-3]


def prettify_date(date: Optional[datetime] = None) -> str:
    if date is None:
        return ''
    return format_datetime(date, 'short')


def prettify_date_ms(date: datetime) -> str:
    return date.isoformat()[:-3]


@inject
def is_feature_enabled(
    feature: str,
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
) -> bool:
    return modules_config.is_reporter_enabled(feature)


@inject
def template_context_processor(
    config: dict = Provide[Application.config],
    modules_config: ModulesConfig = Provide[Application.services.modules_config],
) -> dict:
    user_info = session.get('user')

    username = None
    if user_info and 'email' in user_info:
        username = json.loads(user_info)['preferred_username']

    return {
        'header_items': config['enabled_modules']['header_links'],
        'header_help_url': config['enabled_modules']['header_help_url'],
        'username': username,
        'pro_filters_enabled': modules_config.is_reporter_enabled('pro'),
    }

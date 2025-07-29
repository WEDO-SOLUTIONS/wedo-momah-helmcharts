import math

from dependency_injector.wiring import Provide, inject
from flask import redirect, render_template, request, url_for

from signs_dashboard.containers.application import Application
from signs_dashboard.query_params.users import UsersCreateRequest, UsersListRequest, UserValidationError
from signs_dashboard.services.users import UsersService


@inject
def list_users(users_service: UsersService = Provide[Application.services.users]):
    params = UsersListRequest.from_request(request)
    users, total = users_service.find(params)

    return render_template(
        'users.html',
        users=users,
        params=params,
        total_pages=math.ceil(total / params.items_per_page),
    )


@inject
def create_user(users_service: UsersService = Provide[Application.services.users]):
    if request.method.lower() == 'post':
        try:
            data = UsersCreateRequest.from_request(request)
        except UserValidationError as err:
            return render_template('users_form.html', user=request.form.to_dict(), error=err)

        user_model = users_service.create(data)

        return redirect(url_for('users_crud', user_id=user_model.id))

    return render_template('users_form.html', user={}, error={})


@inject
def crud_users(user_id: int, users_service: UsersService = Provide[Application.services.users]):
    user_model = users_service.get(user_id)
    error = {}

    if request.method.lower() == 'post':
        data = None

        try:
            data = UsersCreateRequest.from_request(request, is_update=True)
        except UserValidationError as err:
            error = err

        if data:
            user_model.email = data.email
            if data.password:
                user_model.password_hash = data.password_hash
            if data.enabled is not None:
                user_model.enabled = data.enabled

            user_model = users_service.update(user_model)

    return render_template('users_form.html', user=user_model, error=error)


@inject
def delete_user(user_id: int, users_service: UsersService = Provide[Application.services.users]):
    users_service.delete(user_id)
    return redirect(url_for('users'))

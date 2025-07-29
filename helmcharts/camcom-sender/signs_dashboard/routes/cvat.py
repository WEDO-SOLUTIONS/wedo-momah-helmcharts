import logging
from collections import defaultdict
from dataclasses import dataclass, field

from dependency_injector.wiring import Provide, inject
from flask import jsonify, render_template, request

from signs_dashboard.containers.application import Application
from signs_dashboard.query_params.cvat_upload import CVATUploadQueryParams
from signs_dashboard.repository.cvat_upload import CVATUploadTaskRepository
from signs_dashboard.services.cvat.uploader import CVATUploader

logger = logging.getLogger(__name__)


@dataclass
class CVATUploadTaskStatusCounter:
    uuid: str
    total: int = 0
    statuses: dict = field(default_factory=lambda: defaultdict(int))


@dataclass
class CVATUploadTaskData:
    name: str
    status: str
    url: str
    project_id: int


@inject
def cvat_upload_statistics(
    cvat_upload_repository: CVATUploadTaskRepository = Provide[Application.pg_repositories.cvat_upload_task_repo],
):
    query_params = CVATUploadQueryParams.from_request(request)
    tasks = cvat_upload_repository.find_by_params(query_params)

    statistics = {task.upload_uuid: CVATUploadTaskStatusCounter(task.upload_uuid) for task in tasks}

    for task in tasks:
        statistics[task.upload_uuid].total += 1
        statistics[task.upload_uuid].statuses[task.status] += 1

    return render_template(
        'cvat_upload_statistics.html',
        stats=list(statistics.values()),
        query_params=query_params,
    )


@inject
def cvat_uuid_upload_status(
    upload_uuid: str,
    cvat_uploader: CVATUploader = Provide[Application.services.cvat_uploader],
    cvat_upload_repository: CVATUploadTaskRepository = Provide[Application.pg_repositories.cvat_upload_task_repo],
):
    raw_tasks = cvat_upload_repository.get_tasks_by_uuid(upload_uuid)

    tasks = [
        CVATUploadTaskData(
            name=task.name,
            status=task.status,
            url=cvat_uploader.build_task_url(task.cvat_task_id),
            project_id=task.project_id,
        )
        for task in raw_tasks
    ]

    project_url = cvat_uploader.build_project_url(tasks[0].project_id) if tasks else None

    return render_template(
        'cvat_upload.html',
        cvat_upload_tasks=tasks,
        project_url=project_url,
    )


@inject
def api_get_projects_list(cvat_uploader: CVATUploader = Provide[Application.services.cvat_uploader]):
    projects = cvat_uploader.get_projects_with_status()
    return jsonify(projects.dict())

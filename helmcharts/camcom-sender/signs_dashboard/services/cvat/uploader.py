import logging
import time
import uuid
from urllib.parse import urljoin

from cvat_sdk import models
from cvat_sdk.core.proxies.projects import Project
from cvat_sdk.core.proxies.tasks import ResourceType, Task as CVATSDKTask
from pydantic import BaseModel

from signs_dashboard.models.cvat_upload_task import CVATUploadStatus
from signs_dashboard.models.frame import Frame
from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.repository.cvat_upload import CVATUploadTaskRepository
from signs_dashboard.services.cvat.session import CVATSession
from signs_dashboard.services.frames import FramesService
from signs_dashboard.services.kafka_service import KafkaService
from signs_dashboard.small_utils import batch_iterator

MAX_FRAMES_IN_TASK = 200
logger = logging.getLogger(__name__)


class CVATProject(BaseModel):
    id: int
    name: str
    url: str

    @classmethod
    def from_cvat_api_project(cls, proj: Project) -> 'CVATProject':
        return cls(
            id=proj.id,
            name=proj.name,
            url=proj.url,
        )


class CVATProjectsWithStatus(BaseModel):
    projects: list[CVATProject]
    is_ok: bool

    @classmethod
    def unavailable(cls) -> 'CVATProjectsWithStatus':
        return cls(projects=[], is_ok=False)


class CVATUploader:
    def __init__(
        self,
        cvat_session: CVATSession,
        kafka_service: KafkaService,
        cvat_upload_task_repo: CVATUploadTaskRepository,
        frames_service: FramesService,
        modules_config: ModulesConfig,
        dashboard_domain: str,
    ):
        self._cvat_session = cvat_session
        self._kafka_service = kafka_service
        self._cvat_task_repository = cvat_upload_task_repo
        self._cvat_url = self._cvat_session.cvat_url
        self._dashboard_domain = dashboard_domain
        self._frames_service = frames_service
        self._modules_config = modules_config

    def build_task_url(self, task_id: int) -> str:
        return urljoin(self._cvat_url, f'/tasks/{task_id}')

    def build_project_url(self, project_id: int) -> str:
        return urljoin(self._cvat_url, f'/projects/{project_id}')

    def get_projects_with_status(self) -> CVATProjectsWithStatus:
        # Чтобы фронт не разваливался, если не можем достать проекты из CVAT'а
        if not self._modules_config.is_cvat_uploading_enabled():
            return CVATProjectsWithStatus.unavailable()

        t_start = time.monotonic()
        projects = self._get_projects_with_status()
        logger.warning(f'Spent {time.monotonic() - t_start}s getting CVAT projects & CVAT status')
        return projects

    def create_upload_tasks(
        self,
        frames: list[Frame],
        project_name: str,
    ) -> str:
        project = self.get_or_create_project_by_name(project_name)
        upload_uuid = str(uuid.uuid4())
        uploaded_frames = [frame for frame in frames if frame.uploaded_photo]
        producer = self._kafka_service.get_producer()
        for batch_num, frames_batch in enumerate(batch_iterator(uploaded_frames, MAX_FRAMES_IN_TASK)):
            frames_ids = [frame.id for frame in frames_batch]
            task_name = f'{upload_uuid}-{batch_num}'
            producer.send(
                self._kafka_service.topics.cvat_upload,
                key=f'{task_name}'.encode(),
                value={
                    'task_name': task_name,
                    'project_id': project.id,
                    'upload_uuid': upload_uuid,
                    'frame_ids': frames_ids,
                },
            )
        producer.close()
        return upload_uuid

    def upload_task(self, task_name: str, project_id: int, upload_uuid: str, frame_ids: list[int]):
        task_id = self._cvat_task_repository.create(upload_uuid=upload_uuid, project_id=project_id, task_name=task_name)
        self._cvat_task_repository.update(task_id, status=CVATUploadStatus.PROCESSING)
        try:
            cvat_task_id = self._upload_task_to_cvat(task_name, project_id, frame_ids)
        except Exception:
            self._cvat_task_repository.update(task_id, status=CVATUploadStatus.ERROR)
            raise
        self._cvat_task_repository.update(
            task_id,
            cvat_task_id=cvat_task_id,
            status=CVATUploadStatus.COMPLETED,
        )

    def get_or_create_project_by_name(self, name: str) -> CVATProject:
        try:
            return [project for project in self.get_projects_with_status().projects if project.name == name][0]
        except IndexError:
            client = self._cvat_session.client
            return client.projects.create(models.ProjectWriteRequest(name=name))

    def _get_projects_with_status(self) -> CVATProjectsWithStatus:
        try:
            return CVATProjectsWithStatus(
                projects=[
                    CVATProject.from_cvat_api_project(proj) for proj in self._cvat_session.client.projects.list()
                ],
                is_ok=True,
            )
        except Exception as exc:
            logger.debug(f'Unable to get CVAT projects list: {exc}')
            return CVATProjectsWithStatus.unavailable()

    def _upload_task_to_cvat(self, task_name: str, project_id: int, frame_ids: list[int]):

        frames_names = [f'{frame_id}.jpg' for frame_id in frame_ids]
        # нужно формировать через url дашборда, т.к. если через s3, то потом не достать frame_id
        frame_urls = [urljoin(self._dashboard_domain, f'/frames/{frame_id}.jpg') for frame_id in frame_ids]
        cvat_task = self._cvat_session.client.tasks.create_from_data(
            spec=models.TaskWriteRequest(
                name=task_name,
                project_id=project_id,
            ),
            resources=frame_urls,
            resource_type=ResourceType.REMOTE,
            data_params={
                'job_file_mapping': [frames_names],
            },
        )

        self._upload_annotations_if_exists(frame_ids, cvat_task)

        return cvat_task.id

    def _upload_annotations_if_exists(self, frame_ids: list[int], cvat_task: CVATSDKTask):
        project_labels = self._cvat_session.client.projects.retrieve(cvat_task.project_id).get_labels()
        label_name2label = {label.name: label for label in project_labels}
        frames = self._frames_service.get_frames(frame_ids)
        shapes = []
        for frame_num, frame in enumerate(frames):
            for detection in frame.detections:
                if detection.label not in label_name2label:
                    continue
                x0, y0, width, height = detection.x_from, detection.y_from, detection.width, detection.height
                shapes.append(
                    {
                        'type': 'rectangle',
                        'frame': frame_num,
                        'source': 'auto',
                        'label_id': label_name2label[detection.label].id,
                        'points': [x0, y0, x0 + width, y0 + height],
                    },
                )
        if shapes:
            cvat_task.update_annotations(models.PatchedLabeledDataRequest(shapes=shapes))

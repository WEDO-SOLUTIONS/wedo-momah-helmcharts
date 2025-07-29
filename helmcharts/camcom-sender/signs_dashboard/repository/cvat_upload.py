from signs_dashboard.models.cvat_upload_task import CVATUploadStatus, CVATUploadTask
from signs_dashboard.query_params.cvat_upload import CVATUploadQueryParams


class CVATUploadTaskRepository:
    def __init__(self, session_factory):
        self.session_factory = session_factory

    def create(self, upload_uuid: str, project_id: int, task_name: str) -> int:
        with self.session_factory() as session:
            task = CVATUploadTask(
                upload_uuid=upload_uuid,
                project_id=project_id,
                name=task_name,
                status=CVATUploadStatus.PRENDING,
            )
            session.add(task)
            session.commit()
            return task.id

    def get(self, task_id: int) -> CVATUploadTask:
        with self.session_factory() as session:
            return session.query(CVATUploadTask).get(task_id)

    def get_tasks_by_uuid(self, upload_uuid: str) -> list[CVATUploadTask]:
        with self.session_factory() as session:
            return session.query(CVATUploadTask).filter(CVATUploadTask.upload_uuid == upload_uuid).all()

    def find_by_params(self, query_params: CVATUploadQueryParams) -> list[CVATUploadTask]:
        with self.session_factory() as session:
            return session.query(CVATUploadTask).filter(
                CVATUploadTask.created > query_params.from_dt,
                CVATUploadTask.created < query_params.to_dt,
            ).all()

    def update(self, task_id: int, **updates):
        with self.session_factory() as session:

            task = session.query(CVATUploadTask).get(task_id)
            if not task:
                raise ValueError(f'Task with id {task_id} not found')

            for field_name, field_value in updates.items():
                setattr(task, field_name, field_value)
                session.commit()

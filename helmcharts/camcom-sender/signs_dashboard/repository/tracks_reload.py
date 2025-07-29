import typing as tp
from datetime import datetime

from signs_dashboard.models.track_reload_status import TrackReloadStatus
from signs_dashboard.query_params.tracks_reload import TracksReloadQueryParams


class ReloadedTracksRepository:
    def __init__(self, session_factory):
        self._session_factory = session_factory

    def create_pending_task(self, uid: str, new_uid: str, task_hash: str):
        with self._session_factory() as session:
            session.add(
                TrackReloadStatus(
                    task_hash=task_hash,
                    uuid=uid,
                    uuid_after_upload=new_uid,
                    status='pending',
                    created=datetime.now(),
                ),
            )
            session.commit()

    def get_original_uuid(self, reloaded_uuid: str) -> str:
        with self._session_factory() as session:
            query = session.query(TrackReloadStatus).filter_by(uuid_after_upload=reloaded_uuid)
            status = query.first()
        return status.uuid if status else reloaded_uuid

    def get_task(self, task_hash: str):
        with self._session_factory() as session:
            query = session.query(TrackReloadStatus).filter_by(task_hash=task_hash)
            return query.first()

    def get_tasks_by_track_ids(self, track_ids: tp.List[str]):
        with self._session_factory() as session:
            tasks = session.query(TrackReloadStatus).filter(
                TrackReloadStatus.uuid.in_(track_ids),
            ).order_by(
                TrackReloadStatus.created.desc(),
            ).all()
        return tasks

    def find(self, query_params: TracksReloadQueryParams):
        with self._session_factory() as session:
            query = session.query(TrackReloadStatus).order_by(TrackReloadStatus.created.desc())

            if query_params.from_dt:
                query = query.filter(TrackReloadStatus.created > query_params.from_dt)

            if query_params.to_dt:
                query = query.filter(TrackReloadStatus.created < query_params.to_dt)

            if query_params.status and query_params.status != 'all':
                query = query.filter(TrackReloadStatus.status == query_params.status)

            return query.all()

    def mark_pending_tasks_as_stopped(self, tasks_hashes: tp.List[str]):
        with self._session_factory() as session:
            for task_hash in tasks_hashes:
                task = session.query(TrackReloadStatus).filter_by(task_hash=task_hash).filter_by(
                    status='pending',
                ).first()
                if task:
                    task.status = 'stop'
                    session.add(task)
            session.commit()

    def set_task_status(self, task_hash: str, status: str):
        self._update(task_hash, status=status)

    def set_task_n_frames(self, task_hash: str, n_frames: int):
        self._update(task_hash, n_frames=n_frames)

    def set_task_n_complete_frames(self, task_hash: str, n_complete_frames: int):
        self._update(task_hash, n_complete_frames=n_complete_frames)

    def _update(self, task_hash: str, **kwargs):
        with self._session_factory() as session:
            task = session.query(TrackReloadStatus).filter_by(task_hash=task_hash).first()
            if task:
                for attr_name, attr_value in kwargs.items():
                    setattr(task, attr_name, attr_value)
                session.add(task)
                session.commit()

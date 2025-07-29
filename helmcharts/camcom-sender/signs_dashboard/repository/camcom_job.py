from datetime import datetime
from typing import Optional

from sqlalchemy import Integer, case, func, literal_column, or_, select, update
from sqlalchemy.orm import noload

from signs_dashboard.models.camcom_job import CAMCOM_JOB_BAD_STATUSES, CamcomJob, CamcomJobLog, CamcomJobStatus
from signs_dashboard.models.frame import Frame
from signs_dashboard.query_params.camcom import CamComStatsQueryParams


class CamcomJobRepository:
    def __init__(self, session_factory):
        self.session_factory = session_factory

    def create(self, job_id: str, frame_id: int, status: CamcomJobStatus) -> bool:
        with self.session_factory(expire_on_commit=False) as session:
            job = session.query(CamcomJob).filter_by(frame_id=frame_id).first()
            if job:
                return False
            job = CamcomJob(
                frame_id=frame_id,
                job_id=job_id,
                status=status.value,
                sent_date=datetime.now(),
                response_status=None,
            )
            session.add(job)
            session.commit()
            return True

    def create_log(
        self,
        job_id: str,
        sent_date: datetime,
        response_status: Optional[int],
        response_text: Optional[str],
    ):
        with self.session_factory() as session:
            log = CamcomJobLog(
                job_id=job_id,
                sent_date=sent_date,
                response_status=response_status,
                response_text=response_text,
            )
            session.add(log)
            session.commit()

    def set_status(
        self,
        job_id: str,
        status: CamcomJobStatus,
        response_status: Optional[int] = -1,
    ) -> Optional[CamcomJob]:
        with self.session_factory(expire_on_commit=False) as session:
            job = session.query(CamcomJob).filter_by(job_id=job_id).first()
            if job:
                job.status = status.value
                if response_status != -1:
                    job.response_status = response_status
                session.add(job)
                session.commit()
        return job

    def complete(self, job_id: str) -> Optional[CamcomJob]:
        return self.set_status(job_id, CamcomJobStatus.CAMCOM_COMPLETE)

    def mark_jobs_as_resend(self, frame_ids: list[int]):
        query = update(CamcomJob).where(
            CamcomJob.frame_id.in_(frame_ids),
        ).values(
            status=CamcomJobStatus.WILL_BE_SENT.value,
        )
        with self.session_factory() as session:
            session.execute(query)
            session.commit()

    def get_statistics_by_frame_ids(self, frame_ids: list[int]) -> list[tuple]:
        with self.session_factory() as session:
            query = select(
                CamcomJob.status,
                func.count(CamcomJob.frame_id),
            )
            query = query.select_from(CamcomJob)
            query = query.where(CamcomJob.frame_id.in_(frame_ids))
            query = query.group_by(CamcomJob.status)
            return session.execute(query).all()

    def statistics(self, query_params: CamComStatsQueryParams):
        with self.session_factory() as session:
            simplified_status_column = case(
                [(CamcomJob.status.in_(CAMCOM_JOB_BAD_STATUSES), CamcomJobStatus.CAMCOM_ERROR.value)],
                else_=CamcomJob.status,
            )
            http_status_column = case(
                [(CamcomJob.status.notin_(CAMCOM_JOB_BAD_STATUSES), None)],
                else_=CamcomJob.response_status,
            )
            date_column = func.date(CamcomJob.sent_date)

            query = select(
                date_column.label('date'),
                simplified_status_column.label('status'),
                http_status_column.label('http_code'),
                func.count(CamcomJob.frame_id).label('frames_count'),
                func.array_agg(CamcomJob.frame_id).label('frame_ids'),
            )
            query = query.select_from(CamcomJob)
            query = query.filter(CamcomJob.sent_date > query_params.from_dt) if query_params.from_dt else query
            query = query.filter(CamcomJob.sent_date < query_params.to_dt) if query_params.to_dt else query
            query = query.group_by(
                date_column,
                simplified_status_column,
                http_status_column,
            )
            query = query.order_by(
                date_column.desc(),
                simplified_status_column.asc(),
                http_status_column.asc(),
            )

            sample_resp_query = session.query(CamcomJobLog)
            sample_resp_query = sample_resp_query.with_entities(CamcomJobLog.response_text)
            sample_resp_query = sample_resp_query.filter(
                func.cast(CamcomJobLog.job_id, Integer) == func.any(literal_column('frame_ids')),
                case(
                    [(literal_column('http_code').is_(None), CamcomJobLog.response_status.is_(None))],
                    else_=literal_column('http_code') == CamcomJobLog.response_status,
                ),
            )
            sample_resp_query = sample_resp_query.order_by(CamcomJobLog.sent_date.desc()).limit(1)

            statistics = session.query(
                literal_column('status'),
                literal_column('frames_count'),
                literal_column('date'),
                literal_column('http_code'),
                sample_resp_query.label('sample_response'),
            ).select_from(query)
            return statistics.all()

    def find_frames_with_errors(self, target_date: datetime.date) -> list[Frame]:
        with self.session_factory() as session:
            query = session.query(Frame)
            query = query.join(CamcomJob, Frame.id == CamcomJob.frame_id)
            query = query.filter(func.date(CamcomJob.sent_date) == target_date)
            query = query.filter(CamcomJob.status.in_(CAMCOM_JOB_BAD_STATUSES))
            query = query.filter(
                or_(
                    CamcomJob.response_status != 409,
                    CamcomJob.response_status.is_(None),
                ),
            )
            query = query.filter(Frame.uploaded_photo.is_(True))
            query = query.options(noload('detections'))
            return query.all()

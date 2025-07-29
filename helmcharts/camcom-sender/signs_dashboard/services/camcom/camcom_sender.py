import logging
import os
from datetime import date, datetime
from typing import Optional, Union

from ratelimiter import RateLimiter

from signs_dashboard.models.camcom_job import CamcomJob, CamcomJobStatus
from signs_dashboard.models.frame import Frame
from signs_dashboard.repository.camcom_job import CamcomJobRepository
from signs_dashboard.services.camcom.camcom_client import CamComAuthorizationError, CamComClient, CamComResponseError
from signs_dashboard.services.image import ImageService
from signs_dashboard.services.interest_zones import InterestZonesService
from signs_dashboard.services.predictors import PredictorsService
from signs_dashboard.services.users import UsersService

logger = logging.getLogger(__name__)

MOMRAH_DRIVER_ID_OIDC_FIELD = os.environ.get('CAMCOM_INSPECTOR_ID_USER_OIDC_FIELD', 'id_no')


class CamcomSenderService:
    def __init__(
        self,
        config: dict,
        camcom_job_repository: CamcomJobRepository,
        frames_image_service: ImageService,
        interest_zones_service: InterestZonesService,
        predictors_service: PredictorsService,
        users_service: UsersService,
    ):
        if not isinstance(config, dict):
            config = {}

        self._client = CamComClient(
            endpoint_url=config.get('endpoint_url'),
            api_key=config.get('api_key'),
            timeout=config.get('request_timeout'),
            retries=config.get('request_retries'),
            retries_backoff=config.get('request_retries_backoff'),
            source_env=config.get('source_env'),
        )

        rate_limit = config.get('request_rate_limit', {})
        self._rate_limiter = RateLimiter(
            max_calls=rate_limit.get('calls', 1),
            period=rate_limit.get('period', 1),
        )
        self._camcom_job_repository = camcom_job_repository
        self._predictors_service = predictors_service
        self._frames_image_service = frames_image_service
        self._interest_zones_service = interest_zones_service
        self._users_service = users_service

    def send(self, frame: Frame, frame_attributes: dict[str, Union[str, int, None]]):
        job_id = str(frame.id)
        self._camcom_job_repository.create(job_id, frame.id, CamcomJobStatus.CREATED)
        response_status = None

        try:
            with self._rate_limiter:
                logger.debug(f'Sends CamCom job for frame {frame.id}')
                returned_job_id, response_status, response_text = self._send(
                    frame=frame,
                    zones_attributes=frame_attributes,
                )
                if returned_job_id == job_id:
                    status = CamcomJobStatus.SENT
                else:
                    logger.error(f'CamCom returned not matching job_id: {job_id} vs {returned_job_id}')
                    status = CamcomJobStatus.CAMCOM_ERROR
        except CamComAuthorizationError as auth_exc:
            logger.error(f"Can't schedule CamCom job for frame {frame.id}, authorization error")
            status = CamcomJobStatus.CAMCOM_ERROR
            response_status = auth_exc.status_code
            response_text = auth_exc.response_text
        except CamComResponseError as other_error:
            logger.exception(f'Unable to schedule CamCom job for frame {frame.id}, error response')
            status = CamcomJobStatus.CAMCOM_ERROR
            response_status = other_error.status_code
            response_text = other_error.response_text
        except Exception as exc:
            logger.exception(f'Unable to schedule CamCom job for frame {frame.id}')
            status = CamcomJobStatus.NOT_PROCESSED
            response_text = str(exc)

        logger.debug(f'Records CamCom job {job_id} for frame {frame.id}')
        self._camcom_job_repository.create_log(job_id, datetime.now(), response_status, response_text)

        if status == CamcomJobStatus.SENT and response_status == 409:
            response_status = 200

        self._camcom_job_repository.set_status(
            job_id,
            status,
            response_status=response_status,
        )

    def complete(self, job_id: str) -> Optional[CamcomJob]:
        return self._camcom_job_repository.complete(job_id)

    def find_frames_with_errors(self, target_date: date) -> list[Frame]:
        return self._camcom_job_repository.find_frames_with_errors(target_date)

    def resend_frames(self, frames: list[Frame]):
        logger.warning(f'Sending resend events for {len(frames)} frames')
        self._predictors_service.send_frames_to_predictor(
            frames,
            predictor='camcom',
            prompt=None,
            recalculate_interest_zones=False,
        )
        self._camcom_job_repository.mark_jobs_as_resend([frame.id for frame in frames])

    def _send(
        self,
        frame: Frame,
        zones_attributes: dict[str, Union[str, int, None]],
    ):
        s3_info = self._frames_image_service.get_s3_location_info(frame)

        inspector_info = {}
        api_user = self._users_service.get_by_email(frame.track_email)
        if api_user and api_user.oidc_meta and api_user.oidc_meta.get(MOMRAH_DRIVER_ID_OIDC_FIELD):
            inspector_info = {'inspector_id': api_user.oidc_meta[MOMRAH_DRIVER_ID_OIDC_FIELD]}

        logger.info(f'Zones attributes for frame {frame.id}: {zones_attributes}')

        return self._client.send(
            s3_info=s3_info,
            latitude=frame.current_lat,
            longitude=frame.current_lon,
            azimuth=frame.azimuth,
            frame_id=frame.id,
            frame_datetime=frame.date,
            extra_args={
                **zones_attributes,
                **inspector_info,
            },
        )

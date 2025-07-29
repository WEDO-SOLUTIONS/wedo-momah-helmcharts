import logging
import typing as tp
from datetime import datetime, timedelta

from signs_dashboard.models.frame import Frame
from signs_dashboard.repository.predictors import PredictorsRepository
from signs_dashboard.services.events.frames_lifecycle import FramesLifecycleService
from signs_dashboard.services.pano_conversions.common import CropsParams

logger = logging.getLogger(__name__)


class PredictorsService:
    def __init__(
        self,
        cfg: dict,
        frames_lifecycle_service: FramesLifecycleService,
        predictors_repository: PredictorsRepository,
    ):
        self._predictors = cfg['enabled_modules']['predictors']
        self._reporters = cfg['enabled_modules']['reporters']
        self._frames_lifecycle_service = frames_lifecycle_service
        self._predictors_repository = predictors_repository
        self._register_ttl = cfg['register_predictor_ttl_seconds']

    def get_active_predictors(self) -> list[str]:
        return list({predictor for reporter in self._reporters for predictor in reporter['predictors']})

    def get_all_predictors(self) -> list[str]:
        return sorted(
            {
                *self.get_active_predictors(),
                *[predictor.name for predictor in self.get_faust_predictors()],
                *[predictor['name'] for predictor in self._predictors],
            },
        )

    def get_faust_predictors(self):
        predictors = self._predictors_repository.all()
        t_now = datetime.now()
        return [
            predictor
            for predictor in predictors
            if t_now - predictor.last_register_time < timedelta(seconds=self._register_ttl)
        ]

    def get_prompt(self) -> tp.Optional[str]:
        active_predictors = self.get_active_predictors()
        try:
            return [
                predictor['prompt'] for predictor in self._predictors
                if self.has_prompt(predictor) and predictor['name'] in active_predictors
            ][0]
        except IndexError:
            return None

    def has_prompt(self, predictor_name: str) -> bool:
        return predictor_name == 'prompt-detection'

    def is_camcom_predictor_enabled(self) -> bool:
        return 'camcom' in self.get_active_predictors()

    def send_frames_to_predictor(
        self,
        frames: list[Frame],
        predictor: str,
        prompt: tp.Optional[str],
        recalculate_interest_zones: bool = False,
    ) -> tuple[list[Frame], list[Frame]]:
        sended_frames = []
        error_frames = []
        for frame in frames:
            try:
                self._produce_event(
                    frame,
                    predictor=predictor,
                    prompt=prompt,
                    recalculate_interest_zones=recalculate_interest_zones,
                )
                sended_frames.append(frame)
            except Exception:
                logger.exception('Failed send frame %s to predictor %s', frame.id, predictor)
                error_frames.append(frame)
        return sended_frames, error_frames

    def _produce_event(self, frame: Frame, predictor: str, prompt: tp.Optional[str], recalculate_interest_zones: bool):
        if frame.panoramic:
            for theta in CropsParams.CROPS_Z_POSITIONS:
                self._frames_lifecycle_service.produce_prediction_required_event(
                    frame=frame,
                    required_predictors=[predictor],
                    prompt=prompt,
                    theta=theta,
                    recalculate_interest_zones=recalculate_interest_zones,
                )
        else:
            self._frames_lifecycle_service.produce_prediction_required_event(
                frame=frame,
                required_predictors=[predictor],
                prompt=prompt,
                recalculate_interest_zones=recalculate_interest_zones,
            )

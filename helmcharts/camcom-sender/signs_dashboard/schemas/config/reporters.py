from typing import Optional

from pydantic import BaseModel, Field

DEFAULT_DAYS_TO_UPLOAD_EXPIRED = 1


class DetectionsLocalizerConfig(BaseModel):
    name: str = Field('detections-localizer', const=True)
    predictors: list[str] = Field(min_length=1)
    timeout: Optional[int] = Field(ge=0, default=None)

    depth_detector_name: str = 'depth-detection'
    force_naive_localization: bool = False

    @property
    def naive_localization(self) -> bool:
        if self.force_naive_localization:
            return True
        return self.depth_detector_name not in self.predictors

    @property
    def requires_detection_from_detector(self) -> Optional[str]:
        if self.naive_localization:
            return None
        return self.depth_detector_name

    @property
    def track_upload_timeout_days(self) -> int:
        if self.timeout is None:
            return DEFAULT_DAYS_TO_UPLOAD_EXPIRED
        return self.timeout

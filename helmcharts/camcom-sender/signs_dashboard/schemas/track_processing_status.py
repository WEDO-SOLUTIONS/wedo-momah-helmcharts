from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from signs_dashboard.models.frame import Frame
from signs_dashboard.models.track import Track
from signs_dashboard.services.prediction import PredictionStatusProxy


@dataclass
class TrackProcessingState:

    track: Track
    frames: list[Frame]
    predictions_status: Optional[PredictionStatusProxy] = None

    @property
    def timedelta_since_last_upload_action(self) -> timedelta:
        if not self.track.upload:
            return timedelta()
        if self.track.upload.complete_time:
            return datetime.now() - self.track.upload.complete_time
        return datetime.now() - self.track.upload.init_time

    @property
    def all_frames_uploaded(self) -> bool:
        return (
            self.track.upload
            and self.track.upload.is_ready_to_send()
            and len(self.frames) == self.track.upload.expected_frames_count
            and len(self.frames) == len([frame for frame in self.frames if frame.uploaded_photo])
        )

    @property
    def all_frames_uploaded_and_predicted(self) -> bool:
        return self.all_frames_uploaded and len(self.frames) == self.predictions_status.count_ready_frames()

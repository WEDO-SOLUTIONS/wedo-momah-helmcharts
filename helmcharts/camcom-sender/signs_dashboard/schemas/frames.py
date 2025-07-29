from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from signs_dashboard.models.frame import Frame


@dataclass
class FramesOperations:
    frames_map: Dict[int, Frame] = field(default_factory=dict)

    @property
    def list_ids(self) -> List[int]:
        return list(self.frames_map.keys())

    @property
    def min_max_frames_date(self) -> (Optional[datetime], Optional[datetime]):
        min_frames_date, max_frames_date = None, None

        if self.frames_map:
            frames = list(self.frames_map.values())
            min_frames_date, max_frames_date = frames[0].date, frames[0].date
            for frame in frames:
                if min_frames_date > frame.date:
                    min_frames_date = frame.date
                if max_frames_date < frame.date:
                    max_frames_date = frame.date

        return min_frames_date, max_frames_date

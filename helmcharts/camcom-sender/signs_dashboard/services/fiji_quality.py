from collections import defaultdict

from signs_dashboard.services.prediction import FramesBatchAttributes

IMAGE_QUALITY_LABELS = 'quality_labels'


class FijiQualityCheckResult:
    def __init__(self):
        self.checks = []
        self.passed = True

    def add_check(self, label: str, threshold: float, value: float):
        self.checks.append({
            'label': label,
            'threshold': threshold,
            'value': value,
        })
        self.passed = self.passed and value < threshold


class FijiQualityChecker:
    def __init__(
        self,
        config: dict,
    ):
        fiji_config = config.get('fiji_client') or {}
        labels_thresholds_raw = fiji_config.get('quality_checks') or {}
        self._labels_thresholds = {
            label: float(label_thr)
            for label, label_thr in labels_thresholds_raw.items()
        }

    def check_frames_quality(self, frame_attributes: FramesBatchAttributes) -> FijiQualityCheckResult:
        result = FijiQualityCheckResult()

        if not self._labels_thresholds:
            return result

        frames_count = len(frame_attributes.frame_ids)
        labels_count: dict[str, int] = defaultdict(int)

        for frame_id in frame_attributes.frame_ids:
            frame_labels = frame_attributes.get_frame_attribute(frame_id, IMAGE_QUALITY_LABELS, [])
            for label in frame_labels:
                labels_count[label] += 1

        for label, threshold in self._labels_thresholds.items():
            if label not in labels_count:
                continue

            result.add_check(label, threshold, labels_count[label] / frames_count)

        return result

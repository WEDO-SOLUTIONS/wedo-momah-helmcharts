import typing as tp
from functools import cached_property

from signs_dashboard.schemas.config.reporters import DetectionsLocalizerConfig


def _topics_as_list(topics):
    return [topics] if isinstance(topics, str) else topics


class ModulesConfig:
    def __init__(self, config: dict):
        self._config = config

        self._enabled_modules = self._config['enabled_modules']

    def get_predictors_for(self, reporter_name: str):
        reporter = self._get_reporter(reporter_name)
        return reporter['predictors']

    def get_predictor_by_topic(self, topic_name: str) -> tp.Optional[str]:
        for predictor_conf in self._enabled_modules['predictors']:
            if predictor_conf.get('topic') == topic_name:
                return predictor_conf['name']
        raise ValueError(f'Got unknown prediction topic: {topic_name}')

    def get_unified_predictions_topic(self) -> tp.Optional[str]:
        return self._enabled_modules['track_downloader']['unified_predictions_topic']

    def get_all_predictions_topics(self):
        topics = [
            predictor['topic']
            for predictor in self._enabled_modules['predictors']
            if predictor.get('topic')
        ]
        topics.append(self.get_unified_predictions_topic())
        return topics

    def is_reporter_enabled(self, reporter_name: str) -> bool:
        return self._get_reporter(reporter_name) is not None

    def get_pro_reporter_timeout(self) -> tp.Optional[int]:
        return self._get_reporter_timeout('pro')

    def get_fiji_reporter_timeout(self) -> tp.Optional[int]:
        return self._get_reporter_timeout('fiji')

    def get_reload_topic(self):
        return self._config['tracks_uploader']['topic']

    def get_frames_saver_topics(self):
        return _topics_as_list(self._enabled_modules['track_downloader']['frames_topics'])

    def get_track_metadata_saver_topics(self):
        return _topics_as_list(self._enabled_modules['track_downloader']['metadata_topics'])

    def get_logs_saver_topics(self):
        return _topics_as_list(self._enabled_modules['logs_saver']['logs_topic'])

    def get_lifecycle_frames_topic(self):
        return self._enabled_modules['track_downloader']['frames_lifecycle_topic']

    def get_lifecycle_objects_topic(self):
        return self._enabled_modules['track_downloader'].get('objects_lifecycle_topic')

    def get_lifecycle_tracks_topic(self):
        return self._enabled_modules['track_downloader']['tracks_lifecycle_topic']

    def is_track_localization_enabled(self):
        return bool(self.detections_localizer)

    def is_track_localization_naive(self) -> str:
        localizer = self._enabled_modules.get('localizer')
        return localizer.get('naive_localization') if localizer else None

    def is_additional_maps_enabled(self):
        return bool(self._enabled_modules.get('additional_maps_enabled'))

    def is_map_matching_enabled(self):
        if self.is_visual_localization_enabled():
            return False
        map_matching_config = self._enabled_modules.get('matching') or {}
        return map_matching_config.get('enabled')

    def is_interpolating_map_matching_enabled(self):
        if self.is_visual_localization_enabled():
            return False
        map_matching_config = self._enabled_modules.get('matching') or {}
        return map_matching_config.get('interpolation')

    def is_visual_localization_enabled(self):
        return self._enabled_modules.get('visual_localization', {}).get('enabled')

    def get_pro_frames_topic(self):
        pro_topics = self._config['pro']['topics']
        return pro_topics['frames']

    def get_pro_objects_topic(self):
        pro_topics = self._config['pro']['topics']
        return pro_topics.get('objects')

    def get_pro_drivers_topic(self):
        pro_topics = self._config['pro']['topics']
        return pro_topics['drivers']

    def get_cvat_upload_topic(self):
        return self._enabled_modules.get('cvat_uploader', {}).get('cvat_upload_topic')

    def is_cvat_uploading_enabled(self):
        return bool(self._config.get('cvat', {}).get('cvat_url'))

    def is_detections_localizer_naive(self) -> tp.Optional[bool]:
        return self.detections_localizer.naive_localization if self.detections_localizer else None

    @cached_property
    def detections_localizer(self) -> tp.Optional[DetectionsLocalizerConfig]:
        if reporter := self._get_reporter('detections-localizer'):
            return DetectionsLocalizerConfig(**reporter)
        return None

    def is_signboard_text_recognition_enabled(self):
        return bool(self._enabled_modules.get('signboard_text_recognition_enabled'))

    def _get_reporter(self, reporter_name: str) -> tp.Optional[dict]:
        for reporter_conf in self._enabled_modules['reporters']:
            if reporter_conf['name'] == reporter_name:
                return reporter_conf
        return None

    def _get_reporter_timeout(self, reporter_name: str) -> tp.Optional[int]:
        reporter_conf = self._get_reporter(reporter_name)
        timeout = reporter_conf.get('timeout') if reporter_conf else None
        if timeout is not None:
            return int(timeout)
        return None

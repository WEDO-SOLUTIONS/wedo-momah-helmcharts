import hashlib
import io
import json
import logging
import os
import typing as tp
from dataclasses import asdict, dataclass
from datetime import datetime
from http import HTTPStatus
from itertools import zip_longest
from uuid import UUID, uuid4

import piexif
import requests
from flask import url_for
from PIL import Image

from signs_dashboard.modules_config import ModulesConfig
from signs_dashboard.query_params.tracks_reload import ForeignTrackReloadQueryParams, TracksReloadQueryParams
from signs_dashboard.repository.tracks_reload import ReloadedTracksRepository
from signs_dashboard.services.fiji_client import FijiClient
from signs_dashboard.services.frames import FramesService
from signs_dashboard.services.kafka_service import KafkaService
from signs_dashboard.services.tracks import TracksService

_JPG_EXTENSIONS = ('.jpg', '.jpeg', '.JPG', '.JPEG')
logger = logging.getLogger(__name__)
TARGET_API_STATIC_AUTH_TOKEN_ENV = os.environ.get('TRACK_UPLOADER_API_AUTH_TOKEN')
SOURCE_WEB_STATIC_AUTH_TOKEN_ENV = os.environ.get('TRACK_UPLOADER_SOURCE_AUTH_TOKEN')


@dataclass
class TrackReloadRequest:
    uuid: str
    frames: tp.List[str]
    gps_points: tp.List[dict]
    init_metadata: dict
    frames_detections: list[dict]


class TracksReloadService:
    def __init__(  # noqa: WPS211
        self,
        config: dict,
        tracks_service: TracksService,
        frames_service: FramesService,
        reloaded_tracks_repository: ReloadedTracksRepository,
        kafka_service: KafkaService,
        modules_config: ModulesConfig,
        fiji_client: FijiClient,
    ):
        if not isinstance(config, dict):
            config = {}

        self._api = config.get('api')
        self._source = config.get('source')
        self._verify_ssl = config.get('verify_ssl', True)
        self.reload_track_timeout_seconds = config.get('reload_track_timeout_seconds', 900)  # default 15 minutes

        self._reloaded_tracks_repository = reloaded_tracks_repository
        self._tracks_service = tracks_service
        self._frames_service = frames_service
        self._kafka_service = kafka_service

        self._allow_fiji_host_input = modules_config.is_reporter_enabled('fiji') and fiji_client.allow_forced_host
        self._allow_predictions_reload = config.get('predictions_reload_allowed', False)

    @property
    def api_auth_headers(self) -> tp.Optional[dict]:
        if TARGET_API_STATIC_AUTH_TOKEN_ENV:
            return {'Authorization': f'Bearer {TARGET_API_STATIC_AUTH_TOKEN_ENV}'}
        return None

    @property
    def web_auth_headers(self) -> tp.Optional[dict]:
        if SOURCE_WEB_STATIC_AUTH_TOKEN_ENV:
            return {'Authorization': f'Bearer {SOURCE_WEB_STATIC_AUTH_TOKEN_ENV}'}
        return None

    def find(self, query_params: TracksReloadQueryParams):
        return self._reloaded_tracks_repository.find(query_params)

    def get_original_uuid(self, reloaded_uuid: str) -> tp.Optional[str]:
        return self._reloaded_tracks_repository.get_original_uuid(reloaded_uuid)

    def parse_track_reload_query(self, request: dict) -> ForeignTrackReloadQueryParams:
        uuids = request.get('uuids', '').split('\n')
        uuids = [uuid.strip() for uuid in uuids if uuid.strip()]
        return ForeignTrackReloadQueryParams(
            endpoint=request.get('endpoint', self._source),
            uuids=uuids,
            fiji_host=request.get('fiji_host', 'uk-test3-iis.2gis.local'),
            show_fiji_host_input=self._allow_fiji_host_input,
            show_predictions_reload_toggle=self._allow_predictions_reload,
            verify_ssl=self._verify_ssl,
            with_detections=request.get('with_detections', 'off') == 'on',
        )

    def prepare_reload_request(self, track_id: str) -> TrackReloadRequest:
        track = self._tracks_service.get(track_id)
        frames = self._frames_service.get_by_track(track)
        return TrackReloadRequest(
            uuid=track_id,
            frames=[
                url_for('frame_proxy', frame_id=frame.id, _external=True)
                for frame in frames
            ],
            gps_points=track.upload.gps_points,
            init_metadata=track.upload.init_metadata,
            frames_detections=[frame.detections_as_manual_prediction for frame in frames],
        )

    def reload_tracks_async(self, tracks: tp.List[TrackReloadRequest]):
        track_ids = [track.uuid for track in tracks]
        skipped_ids: tp.Set[str] = self._get_skip_upload_track_ids(track_ids)
        ids_for_work = [track_id for track_id in track_ids if track_id not in skipped_ids]
        track_new_ids = [_replace_uuid_prefix_with_timestamp(track_id) for track_id in ids_for_work]
        tasks_hashes = [_get_task_hash(track_id) for track_id in ids_for_work]
        tracks = [track for track in tracks if track.uuid in ids_for_work]

        producer = self._kafka_service.get_producer()
        for track, task_hash, track_new_id in zip(tracks, tasks_hashes, track_new_ids):
            self._reloaded_tracks_repository.create_pending_task(track.uuid, track_new_id, task_hash)
            future = producer.send(
                self._kafka_service.topics.tracks_reload,
                value={
                    'track': asdict(track),
                    'task_hash': task_hash,
                    'track_new_id': track_new_id,
                },
                key=track_new_id.encode(),
            )
            try:
                future.get(self._kafka_service.producer_timeout_seconds)
            except Exception as err:
                logger.error(f'Fail send message with error {err}')
                self._reloaded_tracks_repository.set_task_status(task_hash, 'error')

        producer.close()
        return {
            'upload_tracks': ids_for_work,
            'skip_tracks': list(skipped_ids),
        }

    def stop_pending_tasks(self, tasks_hashes: tp.List[str]):
        self._reloaded_tracks_repository.mark_pending_tasks_as_stopped(tasks_hashes)

    def mark_task_as_failed(self, task_hash: str):
        self._reloaded_tracks_repository.set_task_status(task_hash, status='error')

    def reload_track(self, track: TrackReloadRequest, new_track_uid: str, task_hash: str):
        task = self._reloaded_tracks_repository.get_task(task_hash)
        if task and task.status != 'pending':
            return
        try:
            self._upload_track(track, new_track_uid, task_hash)
        except Exception as error:
            logging.exception(f'Unable to reload track {new_track_uid}: {error}')
            self._reloaded_tracks_repository.set_task_status(task_hash, 'error')
            return
        self._reloaded_tracks_repository.set_task_status(task_hash, 'complete')

    def fetch_image_as_pil(self, url: str) -> tp.Optional[Image.Image]:
        response = requests.get(
            url,
            headers=self.web_auth_headers,
            verify=self._verify_ssl,
        )
        if not response.content:
            return None
        return Image.open(io.BytesIO(response.content))

    def fetch_track_reload_info(
        self,
        uuid: str,
        query: ForeignTrackReloadQueryParams,
    ) -> tuple[tp.Optional[TrackReloadRequest], tp.Optional[str]]:
        response = requests.get(
            f'{query.endpoint}/api/tracks/{uuid}/reload',
            verify=query.verify_ssl,
            headers=self.web_auth_headers,
        )
        if response.status_code == 200:
            track = TrackReloadRequest(**response.json())
            if query.fiji_host:
                track.init_metadata['fiji_host'] = query.fiji_host
            if not query.with_detections:
                track.frames_detections = []

            return track, None

        return None, response.text

    def _get_skip_upload_track_ids(self, track_ids: tp.List[str]) -> tp.Set[str]:
        tasks = self._reloaded_tracks_repository.get_tasks_by_track_ids(track_ids)
        track_id2status = {}
        for task in tasks:
            if task.uuid not in track_id2status:
                track_id2status[task.uuid] = task.status
        res = set()
        for track_id in track_ids:
            if track_id2status.get(track_id) in {'pending', 'in_progress'}:
                res.add(track_id)
        return res

    def _upload_track(self, track: TrackReloadRequest, new_uuid: str, task_hash: str):
        self._reloaded_tracks_repository.set_task_status(task_hash, 'in_progress')
        self._init_query(track.init_metadata, new_uuid)
        self._gps_query(track.gps_points, new_uuid)
        self._reloaded_tracks_repository.set_task_n_frames(task_hash, len(track.frames))
        n_uploaded = 0
        for frame_img_url, frame_detections in zip_longest(track.frames, track.frames_detections):
            self._frame_query(frame_img_url, new_uuid)
            if frame_detections:
                self._frame_detections_query(frame_detections, new_uuid)
            n_uploaded += 1
            if n_uploaded % 10 == 0:
                self._reloaded_tracks_repository.set_task_n_complete_frames(task_hash, n_uploaded)
        self._reloaded_tracks_repository.set_task_n_complete_frames(task_hash, n_uploaded)
        self._complete_query(new_uuid)

    def _init_query(self, metadata: tp.Dict, new_uuid: str):
        if 'type' in metadata:
            # not part of API init request, but stored from Kafka message payload
            metadata.pop('type')

        if not self._allow_fiji_host_input:
            metadata.pop('fiji_host', None)

        if metadata['app_version'][-7:] != '_reload':
            metadata['app_version'] = f"{metadata['app_version']}_reload"
        response = requests.post(
            self._get_target_api_endpoint(new_uuid, 'init'),
            json=metadata,
            headers=self.api_auth_headers,
            verify=self._verify_ssl,
        )
        if response.status_code != HTTPStatus.OK:
            raise ValueError(f'init response code: {response.status_code}: {response.text}')

    def _gps_query(self, gps_track: tp.List, new_uuid: str):
        response = requests.post(
            self._get_target_api_endpoint(new_uuid, 'gps_track'),
            data=json.dumps(gps_track),
            headers=self.api_auth_headers,
            verify=self._verify_ssl,
        )
        if response.status_code != HTTPStatus.OK:
            raise ValueError(f'gps response code: {response.status_code}: {response.text}')

    def _frame_query(self, frame_url: str, new_uuid: str):
        img = self.fetch_image_as_pil(frame_url)
        if not img:
            raise ValueError(f'failed read frame: {frame_url}')

        response = requests.post(
            self._get_target_api_endpoint(new_uuid, 'frames'),
            files={
                'file': ('frame.jpg', _mark_image_exif_as_reloaded(img)),
            },
            headers=self.api_auth_headers,
            verify=self._verify_ssl,
        )
        if response.status_code != HTTPStatus.OK:
            raise ValueError(f'frame response code: {response.status_code}: {response.text}')

    def _frame_detections_query(self, frame_detections: dict, new_uuid: str):
        response = requests.post(
            self._get_target_api_endpoint(new_uuid, 'manual_predictions'),
            json=[frame_detections],
            headers=self.api_auth_headers,
            verify=self._verify_ssl,
        )
        if response.status_code != HTTPStatus.OK:
            raise ValueError(f'frame manual predictions response code: {response.status_code}: {response.text}')

    def _complete_query(self, new_uuid: str):
        response = requests.post(
            self._get_target_api_endpoint(new_uuid, 'complete'),
            headers=self.api_auth_headers,
            verify=self._verify_ssl,
        )
        if response.status_code != HTTPStatus.OK:
            raise ValueError(f'complete response code: {response.status_code}: {response.text}')

    def _get_target_api_endpoint(self, uuid: str, operation: str) -> str:
        return f'{self._api}api/1.0/routes/{uuid}/{operation}'


def _replace_uuid_prefix_with_timestamp(uuid: str, n_symb: int = 8) -> str:
    try:
        UUID(uuid)
    except ValueError as exc:
        logger.warning(f'Failed to parse uuid: {uuid}: {exc}')
        uuid = str(uuid4())
    now_dt = int(datetime.now().timestamp())
    new_prefix = str(now_dt)[-n_symb:]
    return new_prefix + uuid[n_symb:]


def _get_task_hash(track_id: str) -> str:
    timestamp_str = str(int(datetime.now().timestamp()))
    task_id = f'{timestamp_str}{track_id}'
    return hashlib.md5(task_id.encode('utf-8')).hexdigest()  # noqa: S303 (insecure)


def _mark_image_exif_as_reloaded(img: Image):
    exif_dict = piexif.load(img.info['exif'])
    app_version = exif_dict['0th'][piexif.ImageIFD.Software].decode('utf-8')
    if 'reload' not in app_version:
        app_version = f'{app_version}_reload'
    exif_dict['0th'][piexif.ImageIFD.Software] = app_version.encode('utf-8')
    exif_bytes = piexif.dump(exif_dict)
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, exif=exif_bytes, format=img.format)
    return img_byte_arr.getvalue()

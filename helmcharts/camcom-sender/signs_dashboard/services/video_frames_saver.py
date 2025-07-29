import gc
import logging
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait
from tempfile import TemporaryDirectory
from typing import Iterator, Optional

from signs_dashboard.models.frame import Frame
from signs_dashboard.models.track import Track
from signs_dashboard.repository.frames import FramesRepository
from signs_dashboard.repository.tracks import TracksRepository
from signs_dashboard.schemas.video_frames_saver import VideoFrame
from signs_dashboard.services.image import ImageService
from signs_dashboard.services.interest_zones import InterestZonesService
from signs_dashboard.services.pano_conversions.frame_cropper import (
    generate_crops_from_frame,
    initialize_cropping_worker,
)
from signs_dashboard.services.panorama_rotation_fixer import PanoramaRotationFixer
from signs_dashboard.services.s3_service import S3Service
from signs_dashboard.services.video_frames_cutter import VideoFramesCutterService

logger = logging.getLogger(__name__)

SCRATCH_DIR = os.getenv('SCRATCH_DIR', '/scratch')  # noqa: S108
VIDEO_FRAMES_CROPPER_WORKERS = int(os.getenv('VIDEO_FRAMES_CROPPER_WORKERS') or 4)
VIDEO_FRAMES_PROCESSING_BATCH_SIZE = int(os.getenv('VIDEO_FRAMES_PROCESSING_BATCH_SIZE') or 10)
VIDEO_FRAMES_MINIMAL_DISTANCE_METERS = int(os.getenv('VIDEO_FRAMES_MINIMAL_DISTANCE_METERS') or 5)


class VideoFramesSaverService:
    def __init__(
        self,
        frames_repository: FramesRepository,
        tracks_repository: TracksRepository,
        video_frames_cutter_service: VideoFramesCutterService,
        interest_zones_service: InterestZonesService,
        image_service: ImageService,
        s3_service: S3Service,
        s3_config: dict,
        panorama_rotation_config: Optional[dict],
    ):
        self._frames_repository = frames_repository
        self._tracks_repository = tracks_repository
        self._s3_service = s3_service
        self._video_frames_cutter_service = video_frames_cutter_service
        self._image_service = image_service
        self._interest_zones_service = interest_zones_service

        self._frame_upload_executor = ThreadPoolExecutor(max_workers=s3_config.get('executor_max_workers', 10))
        self._crops_executor = ProcessPoolExecutor(
            max_workers=VIDEO_FRAMES_CROPPER_WORKERS,
            initializer=initialize_cropping_worker,
            initargs=(s3_config,),
        )

        self._rotation_fixer = PanoramaRotationFixer(panorama_rotation_config or {})

    def save_video_frames(self, track: Track) -> list[Frame]:
        logger.info(f'Start processing video for track {track.uuid}')

        if not track.upload.gps_points:
            logger.error(f'Track {track.uuid} has no gps points')
            self._tracks_repository.set_processing_failed_status(track)
            return []

        with TemporaryDirectory(dir=SCRATCH_DIR) as video_dir:
            video_file_name = os.path.join(video_dir, 'video.mp4')
            with open(video_file_name, 'wb') as video_file:
                try:
                    self._download_video360(track, video_file)
                except Exception as exc:
                    logger.exception(f'Error while downloading video for track {track.uuid}: {exc}')
                    self._tracks_repository.set_processing_failed_status(track)
                    return []

                video_file.flush()

                return self._process_video(track, video_file_name, directory=video_dir)

    def _process_video(self, track: Track, video_path: str, directory: str) -> list[Frame]:  # noqa: WPS213
        video_frames_iterator = self._video_frames_cutter_service.iterate_frames(
            track=track,
            video_path=video_path,
        )

        if self._rotation_fixer.enabled:
            video_frames_iterator = self._rotation_fixer.wrap_iterator(video_frames_iterator)

        video_frames_iterator = self._limit_frames_frequency(video_frames_iterator)

        logger.info(f'Start saving pano frames for track {track.uuid}')
        frames = []
        upload_futures, cropper_futures = [], []
        frame_idx = 0
        frames_batch = []
        for video_frame in video_frames_iterator:
            frame = Frame(
                lat=video_frame.meta['lat'],
                lon=video_frame.meta['lon'],
                azimuth=video_frame.meta['azimuth'],
                speed=video_frame.meta['speed'],
                date=video_frame.date_from_meta,
                track_uuid=track.uuid,
                track_email=track.user_email,
                panoramic=True,
                uploaded_photo=True,
            )

            cropper_futures.append(self._crops_executor.submit(
                generate_crops_from_frame,
                source_azimuth=frame.azimuth,
                frame=frame,
                image_path=_write_image_to_disk(video_frame.image, idx=frame_idx, directory=directory),
            ))
            upload_futures.append(self._frame_upload_executor.submit(
                self._image_service.upload_frame,
                frame=frame,
                image=video_frame.image,
            ))

            frames_batch.append(frame)
            if len(frames_batch) > VIDEO_FRAMES_PROCESSING_BATCH_SIZE:
                logger.info(f'Begin inserting to DB {len(frames_batch)} frames')
                self._frames_repository.bulk_insert(frames_batch)
                logger.info('Done inserting to DB, calculating frames interest zones')
                for new_frame in frames_batch:
                    self._interest_zones_service.update_frame_interest_zones(new_frame)
                logger.info('Done calculating frames interest zones')
                frames_batch = []

            frames.append(frame)
            frame_idx += 1

        if frames_batch:
            logger.info('Begin inserting to DB last batch')
            self._frames_repository.bulk_insert(frames_batch)
            logger.info('Done inserting to DB last batch')

        logger.info(f'Done saving frames for track {track.uuid}: {len(frames)} frames total')

        logger.info('Begin waiting for s3 upload executor')
        wait(upload_futures)
        logger.info('Done waiting s3 upload executor')
        logger.info('Begin waiting for crops executor')
        wait(cropper_futures)
        logger.info('Done waiting crops executor')

        self._tracks_repository.set_uploaded(track.uuid)
        self._tracks_repository.update_track_frames_count(track.uuid, len(frames))
        logger.info(f'Finished saving all frames for track {track.uuid}')

        if not frames:
            logger.error(f'No frames extracted from video for track {track.uuid}')
            self._tracks_repository.set_processing_failed_status(track)
            return []

        gc.collect()

        return frames

    def _limit_frames_frequency(self, iterator: Iterator[VideoFrame]) -> Iterator[VideoFrame]:
        prev_frame = None
        total = 0
        for frame in iterator:
            if prev_frame:
                enough_distance = self._video_frames_cutter_service.distance_between_frames_ok(
                    frame1_gps_data=prev_frame.gps_data,
                    frame2_gps_data=frame.gps_data,
                    min_distance=VIDEO_FRAMES_MINIMAL_DISTANCE_METERS,
                )
                if not enough_distance:
                    logger.debug(f'Frame with ts {frame.timestamp_ms} skipped as distance from prev frame is too small')
                    continue

            yield frame
            total += 1
            prev_frame = frame

        logger.warning(f'Based of distances between frames {total} frames were selected')

    def _download_video360(self, track: Track, video_file):
        self._s3_service.download_fileobj(
            bucket=self._s3_service.buckets.get_videos_bucket(track.uploaded),
            key=self._s3_service.keys.get_videos_key(track.uuid, resource_type='video_360'),
            fileobj=video_file,
        )


def _write_image_to_disk(image: bytes, directory: str, idx: int) -> str:
    path = os.path.join(directory, f'frame_{idx}.jpg')
    with open(path, 'wb') as file:
        file.write(image)
    return path

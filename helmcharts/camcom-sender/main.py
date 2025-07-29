import os
import sys

os.environ['PROMETHEUS_MULTIPROC_DIR'] = '.cache'
os.makedirs('.cache', exist_ok=True)
os.environ['CONFIG_PATH'] = 'config/config_tests.yaml'

# pylint: disable=C0411
from signs_dashboard import (
    camcom_sender_cli,
    cvat_uploader_cli,
    logs_downloader_cli,
    reporter_fiji_cli,
    reporter_pro_cli,
    tracks_downloader_cli,
    tracks_reload_cli,
    video_frames_saver_cli,
)
from wsgi import create_app

if __name__ == '__main__':
    app = create_app()  # resolve app dependencies

    command = sys.argv[-1] if sys.argv else None
    command = {
        'frames': tracks_downloader_cli.download_frames,
        'tracks': tracks_downloader_cli.download_tracks,
        'logs': logs_downloader_cli.download_logs,
        'predictions': tracks_downloader_cli.download_predictions,
        'reporter-fiji': reporter_fiji_cli.run,
        'reporter-pro': reporter_pro_cli.run,
        'camcom': camcom_sender_cli.camcom_sender,
        'reloader': tracks_reload_cli.reload_tracks,
        'cvat-uploader': cvat_uploader_cli.run,
        'video-frames-saver': video_frames_saver_cli.video_frames_save,
    }.get(command, lambda: app.run(debug=True))
    command()

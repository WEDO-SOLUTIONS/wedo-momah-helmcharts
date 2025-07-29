from flask import Flask

from signs_dashboard.routes import (
    camcom as camcom_routes,
    cvat as cvat_routes,
    detected_objects as detected_objects_routes,
    driver as driver_routes,
    feedback as feedback_routes,
    fiji_request as fiji_request_routes,
    frames as frames_routes,
    interest_zones as interest_zones_routes,
    predictors_api as predictors_api_routes,
    reload as reload_routes,
    signs as signs_routes,
    static as static_routes,
    tracks as tracks_routes,
    tracks_api as tracks_api_routes,
    twogis_pro as twogis_pro_routes,
    users as user_routes,
    wfs as wfs_routes,
)


def set_routes(app: Flask):
    app.add_url_rule('/favicon.ico', 'favicon', static_routes.favicon)

    app.add_url_rule('/', 'drivers', driver_routes.drivers)
    app.add_url_rule('/drivers/download_kml', 'drivers_download_kml', driver_routes.download_kml)
    app.add_url_rule('/drivers/download_csv', 'drivers_download_csv', driver_routes.download_csv)

    app.add_url_rule('/tracks', 'tracks', tracks_routes.tracks)
    app.add_url_rule('/tracks/download_kml', 'tracks_download_kml__old', tracks_routes.download_kml)
    app.add_url_rule('/tracks/<uuid>', 'tracks_view', tracks_routes.track_view)
    app.add_url_rule('/tracks/<uuid>/kml', 'tracks_download_kml', tracks_routes.download_kml)
    app.add_url_rule('/tracks/<uuid>/map', 'tracks_map_view', tracks_routes.track_map)
    app.add_url_rule('/tracks/<uuid>/logs/<log_id>', 'tracks_download_log', tracks_routes.track_log)
    app.add_url_rule('/tracks/<uuid>/lidar', 'tracks_view_lidar', tracks_routes.track_lidar_data)
    app.add_url_rule('/tracks/<uuid>/fiji_request', 'fiji_request', fiji_request_routes.get_fiji_request)

    app.add_url_rule('/audit', 'audit', driver_routes.drivers_audit)

    app.add_url_rule('/reload_statuses', 'reload_statuses', reload_routes.reload_statuses)
    app.add_url_rule('/stop_pending_tasks', 'stop_pending_tasks', reload_routes.stop_pending_tasks)
    app.add_url_rule('/reload_track', 'reload_track', reload_routes.reload_track, methods=['GET', 'POST'])

    app.add_url_rule('/frames', 'frames', frames_routes.search_frames)
    app.add_url_rule('/frames/<int:frame_id>', 'frame', frames_routes.frame_page)
    app.add_url_rule('/frames/<int:frame_id>/theta/<int:theta>', 'frame_360_crop', frames_routes.frame_page)
    app.add_url_rule('/frames/<int:frame_id>/image', 'frame_proxy', frames_routes.frame_proxy)
    app.add_url_rule('/frames/<int:frame_id>.jpg', 'frame_cvat_proxy', frames_routes.frame_proxy)  # специально для CVAT
    app.add_url_rule(
        '/frames/<int:frame_id>/theta/<int:theta>/image',
        'frame_360_crop_proxy',
        frames_routes.frame_proxy,
    )
    app.add_url_rule('/frames/<int:frame_id>/depth', 'frame_depth_proxy', frames_routes.frame_depth_proxy)
    app.add_url_rule(
        '/frames/<int:frame_id>/theta/<int:theta>/depth',
        'frame_360_crop_depth_proxy',
        frames_routes.frame_depth_proxy,
    )

    app.add_url_rule('/signs', 'signs', signs_routes.search_signs)
    app.add_url_rule('/crop', 'crop', signs_routes.crop)
    app.add_url_rule('/download_frames', 'download_frames', signs_routes.download_frames)
    app.add_url_rule('/download_crops', 'download_crops', signs_routes.download_crops)
    app.add_url_rule('/download_predictions', 'download_predictions', signs_routes.download_predictions)

    app.add_url_rule('/wfs', 'wfs', wfs_routes.wfs)
    app.add_url_rule('/wfs/scoped/<scope>', 'wfs_scoped', wfs_routes.wfs)
    app.add_url_rule('/map', 'map', wfs_routes.wfs_map)

    app.add_url_rule('/detections_map', 'detections_map', detected_objects_routes.detections_map)
    app.add_url_rule(
        '/api/objects/<int:object_id>/<new_status>',
        'api_update_object_status',
        detected_objects_routes.update_status,
        methods=['PUT'],
    )

    app.add_url_rule('/camcom', 'camcom_stats', camcom_routes.statistics_page, methods=['GET'])
    app.add_url_rule(
        '/api/camcom/resend/<date>',
        'api_camcom_resend_by_date',
        camcom_routes.resend_failed_by_date,
        methods=['POST'],
    )

    app.add_url_rule('/pro', 'pro', twogis_pro_routes.filters_page)
    app.add_url_rule(
        '/api/pro/sync_filters',
        'api_pro_sync_filters',
        twogis_pro_routes.api_update_filters_in_pro,
        methods=['POST'],
    )
    app.add_url_rule(
        '/api/pro/resend/tracks',
        'api_pro_resend_tracks',
        twogis_pro_routes.resend_tracks,
        methods=['POST'],
    )
    app.add_url_rule(
        '/api/pro/resend/tracks_by_drivers',
        'api_pro_resend_tracks_by_drivers',
        twogis_pro_routes.resend_tracks_by_drivers,
        methods=['POST'],
    )
    app.add_url_rule(
        '/api/pro/resend/frames',
        'api_pro_resend_frames',
        twogis_pro_routes.resend_frames,
        methods=['POST'],
    )
    app.add_url_rule(
        '/api/pro/resend/objects',
        'api_pro_resend_objects',
        twogis_pro_routes.resend_detected_objects,
        methods=['POST'],
    )
    app.add_url_rule(
        '/api/pro/resend/drivers',
        'api_pro_resend_drivers',
        twogis_pro_routes.resend_drivers,
        methods=['POST'],
    )

    app.add_url_rule('/api/detection_classes', 'api_det_classes', twogis_pro_routes.api_detection_classes)

    app.add_url_rule('/api/tracks/<uuid>', 'api_get_track', tracks_api_routes.get_track, methods=['GET'])
    app.add_url_rule('/api/tracks/<uuid>', 'api_update_track', tracks_api_routes.update_track, methods=['PUT'])
    app.add_url_rule('/api/tracks/<uuid>/reload', 'track_reload_data', reload_routes.get_track_reload_data)
    app.add_url_rule('/api/tracks/<uuid>/frames', 'api_track_frames', tracks_api_routes.track_frames)

    app.add_url_rule(
        '/api/tracks/predict_tracks',
        'predict_tracks',
        tracks_routes.predict_tracks,
        methods=['POST'],
    )

    app.add_url_rule(
        '/api/tracks/upload_cvat',
        'upload_tracks_to_cvat',
        tracks_routes.upload_tracks_to_cvat,
        methods=['POST'],
    )

    app.add_url_rule(
        '/api/tracks/localization',
        'api_bulk_change_localization_status',
        tracks_routes.localization,
        methods=['POST'],
    )

    app.add_url_rule('/api/frames/<int:frame_id>/info', 'get_frame_info', frames_routes.frame_info)
    app.add_url_rule(
        '/api/frames/<int:frame_id>/predictions',
        'get_frame_predictions',
        frames_routes.get_frame_predictions,
    )
    app.add_url_rule(
        '/api/frames/<int:frame_id>/theta/<int:theta>/predictions',
        'get_frame_360_crop_predictions',
        frames_routes.get_frame_predictions,
    )

    app.add_url_rule(
        '/api/frames/<int:frame_id>/moderate/<moderation_status>',
        'moderation_feedback',
        feedback_routes.moderation_feedback,
        methods=('PUT',),
    )

    app.add_url_rule(
        '/api/frames/predict_frames',
        'predict_frames',
        frames_routes.predict_frames,
        methods=['POST'],
    )
    app.add_url_rule(
        '/api/frames/upload_cvat',
        'upload_frames_to_cvat',
        frames_routes.cvat_frames_upload,
        methods=['POST'],
    )
    app.add_url_rule(
        '/api/drivers/predict_drivers',
        'predict_drivers',
        driver_routes.predict_drivers,
        methods=['POST'],
    )
    app.add_url_rule(
        '/api/drivers/upload_cvat',
        'upload_drivers_to_cvat',
        driver_routes.upload_drivers_to_cvat,
        methods=['POST'],
    )
    app.add_url_rule('/users', 'users', user_routes.list_users)
    app.add_url_rule('/users/new', 'create_user', user_routes.create_user, methods=('GET', 'POST'))
    app.add_url_rule('/users/<user_id>', 'users_crud', user_routes.crud_users, methods=('GET', 'POST'))
    app.add_url_rule('/users/<user_id>/delete', 'delete_user', user_routes.delete_user)

    app.add_url_rule('/interest_zones', 'interest_zones', interest_zones_routes.list_zones, methods=['GET'])
    app.add_url_rule('/interest_zones', 'add_interest_zone', interest_zones_routes.add_zone, methods=['POST'])
    app.add_url_rule(
        '/interest_zones/<zone_name>',
        'view_interest_zone',
        interest_zones_routes.view_zone,
        methods=['GET'],
    )
    app.add_url_rule(
        '/interest_zones/<zone_name>/geojson',
        'download_zone_geojson',
        interest_zones_routes.download_zone_geojson,
        methods=['GET'],
    )
    app.add_url_rule(
        '/interest_zones/<zone_name>',
        'update_interest_zone',
        interest_zones_routes.update_zone,
        methods=['POST'],
    )
    app.add_url_rule(
        '/interest_zones/<zone_name>/delete',
        'delete_interest_zone',
        interest_zones_routes.delete_zone,
        methods=['POST'],
    )
    app.add_url_rule(
        '/api/search_regions',
        'list_search_regions',
        interest_zones_routes.list_search_regions,
        methods=['GET'],
    )

    app.add_url_rule(
        '/cvat_upload/',
        'cvat_upload',
        cvat_routes.cvat_upload_statistics,
    )
    app.add_url_rule(
        '/cvat_upload_status/<upload_uuid>',
        'cvat_uuid_upload_status',
        cvat_routes.cvat_uuid_upload_status,
    )
    app.add_url_rule(
        '/api/cvat/projects',
        'api_cvat_get_projects',
        cvat_routes.api_get_projects_list,
    )

    app.add_url_rule(
        '/api/predictors/register',
        'api_register_predictor',
        predictors_api_routes.register_predictor,
        methods=['PUT'],
    )

    app.add_url_rule(
        '/active_predictors',
        'active_predictors',
        predictors_api_routes.predictors_status,
        methods=['GET'],
    )

from flask import send_from_directory


def favicon():
    return send_from_directory(
        directory='signs_dashboard/templates/',
        path='favicon.ico',
        mimetype='image/vnd.microsoft.icon',
    )

# -*- coding: utf-8 -*-
from werkzeug.wsgi import wrap_file
from flask import request, current_app

def send_gridfs_file(fileobj, cache_for=31536000):
    '''
    flask.send_file para ficheros de gridfs
    '''
    response = current_app.response_class(
        wrap_file(request.environ, fileobj, buffer_size=1024 * 256),
        mimetype=(fileobj.content_type or "application/octet-stream"),
        direct_passthrough=True)
    response.content_length = fileobj.length
    response.last_modified = fileobj.upload_date
    response.set_etag(fileobj.md5)
    response.cache_control.max_age = cache_for
    response.cache_control.s_max_age = cache_for
    response.cache_control.public = True
    response.make_conditional(request)
    return response

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import functools
import json
import pprint
import zlib
import base64

from flask import Blueprint, current_app, request, jsonify, url_for, make_response, abort
from flask.ext.babelex import gettext as _

from foofind.utils.downloader import downloader_url
from foofind.utils import logging

logger = Blueprint("logger", __name__)

@logger.route("/logger", methods=("GET", "POST"))
@downloader_url
def handler():
    try:
        if request.method == "POST":
            data = request.form.to_dict()
        elif request.method == "GET":
            data = request.args.to_dict()
        else:
            abort(404)

        rdata = zlib.decompress(base64.b64decode(str(data["records"]), "-_"))

        data["records"] = json.loads(rdata)
        data["remote_addr"] = request.remote_addr

        logging.warn("Downloader error received", extra=data)

        response = make_response("OK")
        response.status_code = 202
    except BaseException as e:
        logging.exception(e)
        response = make_response("ERROR")
        response.status_code = 500
    response.mimetype = "text/plain"
    return response

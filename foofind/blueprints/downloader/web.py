#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os.path

from foofind.services.extensions import cache

from flask import Blueprint, render_template, g, current_app, request, send_file
from flask.ext.babelex import gettext as _

from foofind.utils.downloader import get_file_metadata

web = Blueprint("web", __name__)

@web.route('/favicon.ico')
def favicon():
    return send_file(
        os.path.join(current_app.static_folder, 'favicon.ico'),
        mimetype='image/vnd.microsoft.icon'
        )

@web.route('/robots.txt')
def robots():
    return send_file(
        os.path.join(current_app.static_folder, 'robots.txt')
        )

@web.route('/')
@web.route('/<lang>')
@cache.cached(60)
def index():
    downloader_files = current_app.config["DOWNLOADER_FILES"]
    installer_available = os.path.exists(downloader_files["installer.exe"])
    setup_metadata = get_file_metadata(downloader_files["setup.exe"])
    source_available = os.path.exists(downloader_files["source.zip"])

    properties = {
        "available": installer_available and setup_metadata,
        "source_available": source_available
        }

    try:
        if properties["available"]:
            properties["version_code"] = setup_metadata["version"]
            properties["length"] = os.path.getsize(downloader_files["installer.exe"])
            properties["filename"] = "installer.exe"
    except KeyError:
        properties["available"] = False

    try:
        if source_available:
            properties["source_length"] = os.path.getsize(downloader_files["source.zip"])
            properties["source_filename"] = "source.zip"
    except KeyError:
        properties["source_available"] = False

    g.title = "Foofind download manager"
    return render_template(
        "microsite/foodownloader.html",
        properties = properties,
        mode = "download",
        style_alternative = request.args.get("a", 2, int)
        )

@web.route("/success")
@cache.cached()
def foodownloader_success():
    return render_template(
        "microsite/foodownloader.html",
        mode = "success",
        style_alternative = 0
        )

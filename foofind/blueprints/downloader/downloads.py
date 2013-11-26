#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mimetypes
import os.path

from foofind.services.extensions import cache
from foofind.utils import logging

from flask import Blueprint, current_app, request, jsonify, url_for, abort, send_file, g, redirect
from flask.ext.babelex import gettext as _

from foofind.utils.downloader import downloader_url, is_downloader_useragent, get_file_metadata

downloads = Blueprint("downloads", __name__)

@downloads.route("/update")
@downloader_url
@cache.cached_GET(60)
def update():
    '''

    JSON SPEC
        {
        ?"update": {
            ?"title": "Update available...",
            ?"text": "New version...",
            "files":[
                {"url": "http://...", "version": "xyz", "argv": ["/arg1", ... ]},
                ...
                ]
            },
        ?"messages": [{
            ?"title": "title",
            ?"icon": "wxART_INFORMATION" // Icon name to display, defaults to wxART_INFORMATION

            ?"priority": 0, // Higher priority means first shown on multiple messages, otherwhise alphabetical order by title is used
            ?"id": "unique_identifier", // For non repeateable messages, if not specified, message will be shown on every session

            ?"text": "Text...",
            ?"url": "http://...",
            ?"size": [-1,-1], // Size for embeded objects like url

            ?"go_url": "http//:", // Implies Go, Cancel buttons
            ?"go_text": ""

            ?"start_url": "http://...", // Implies Download, Cancel buttons
            ?"start_filename": "...", // Filename wich file should have on disk, if not given, last part of url will be used
            ?"start_argv": ["/arg1", ...]
            ?"start_text"
            ?"start_close": true // True if app needs to be closed when run. Defaults to false,
            }, ...]
        }

    ICONS:
        wxART_ERROR                 wxART_FOLDER_OPEN
        wxART_QUESTION              wxART_GO_DIR_UP
        wxART_WARNING               wxART_EXECUTABLE_FILE
        wxART_INFORMATION           wxART_NORMAL_FILE
        wxART_ADD_BOOKMARK          wxART_TICK_MARK
        wxART_DEL_BOOKMARK          wxART_CROSS_MARK
        wxART_HELP_SIDE_PANEL       wxART_MISSING_IMAGE
        wxART_HELP_SETTINGS         wxART_NEW
        wxART_HELP_BOOK             wxART_FILE_OPEN
        wxART_HELP_FOLDER           wxART_FILE_SAVE
        wxART_HELP_PAGE             wxART_FILE_SAVE_AS
        wxART_GO_BACK               wxART_DELETE
        wxART_GO_FORWARD            wxART_COPY
        wxART_GO_UP                 wxART_CUT
        wxART_GO_DOWN               wxART_PASTE
        wxART_GO_TO_PARENT          wxART_UNDO
        wxART_GO_HOME               wxART_REDO
        wxART_PRINT                 wxART_CLOSE
        wxART_HELP                  wxART_QUIT
        wxART_TIP                   wxART_FIND
        wxART_REPORT_VIEW           wxART_FIND_AND_REPLACE
        wxART_LIST_VIEW             wxART_HARDDISK
        wxART_NEW_DIR               wxART_FLOPPY
        wxART_FOLDER                wxART_CDROM
        wxART_REMOVABLE

    '''
    # TODO(felipe): counter
    version = request.args.get("version", "")
    lang = request.args.get("lang", "en")
    platform = request.args.get("platform", None)

    # Updates
    update_files = []
    downloader_files = current_app.config["DOWNLOADER_FILES"]
    setup_version = get_file_metadata(downloader_files["update.exe"]).get("version", "")

    if version < setup_version:
        update_files.append({
            "url": url_for(".download", instfile="update.exe", _external=True, **request.args),
            "version": setup_version,
            "argv": ["/SILENT", "/NORESTART", "/RESTARTAPPLICATIONS", "/LAUNCH", "/VERSION=%s" % version],
            })

    response = {}
    if update_files:
        # Custom update message
        message_version = max(i["version"] for i in update_files)
        response["update"] = {
            "text": _("downloader_update_message",
                      appname = current_app.config["DOWNLOADER_APPNAME"],
                      version = message_version
                      ),
            "title": _("Update available"),
            "files": update_files,

            }

    # Messages
    # TODO(felipe): remove before release
    response["messages"] = [
        #{
            #"title": "Test",
            #"icon": "wxART_WARNING",
            #"text": "This is a test server message",
            #"url": "http://foofind.is",
            #"go_url": "http://foofind.is"
        #},
        #{
            #"url": "http://foofind.is",
        #},
        #{
            #"title": "Simple message",
            #"icon": "wxART_FIND",
            #"text": "Simple text"
        #},
        #{
            #"title": "Test",
            #"text": "This is another server message\nin two lines",
            #"go_url": "http://foofind.is",
            #"go_text": "Open URL"
        #},
        #{
            #"title": "Download test",
            #"text": "Download test\n<b>What if we add this before next release?</b>",
            #"start_url": "http://foofind.is/en/downloader/foofind_download_manager_installer.exe",
            #"start_text": "Download installer",
            #"start_close": True,
            #"id":"test1",
        #},
        ]

    return jsonify(response)

@downloads.route("/download/<instfile>")
def download(instfile):
    downloader_files = current_app.config["DOWNLOADER_FILES"]
    downloader_files_aliases = current_app.config["DOWNLOADER_FILES_ALIASES"]

    if instfile in downloader_files_aliases:
        instfile = downloader_files_aliases[instfile]
    if not instfile in downloader_files:
        abort(404)

    version = request.args.get("version", "")
    lang = request.args.get("lang", "en")
    platform = request.args.get("platform", None)

    path = downloader_files[instfile]

    # Redirect downloads on static directories to static_download endpoint
    # hoping server will handle request and serve it directly
    prefix = os.path.join(current_app.root_path, "downloads") + os.sep
    if path.startswith(prefix):
        relative_path = path[len(prefix):]
        return redirect(url_for('.static_download', instfile=relative_path))

    # All downloads should be inside downloads static dir
    logging.warn("Download %r served dinamically because not in static directory %r" % (path, prefix))
    return send_file(path, mimetypes.guess_type(path)[0])


@downloads.route("/download/static/<path:instfile>") # Should be served statically
def static_download(instfile):

    # Check if instfile is inside downloads (seccurity stuff)
    prefix = os.path.join(current_app.root_path, "downloads") + os.sep
    path = os.path.abspath(prefix + path)
    if path.startswith(prefix):
        return send_file(path, instfile)

    # Path outside static dir
    abort(404)

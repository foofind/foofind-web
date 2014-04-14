#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os.path
import mimetypes
import requests
from flask import g, render_template, current_app, request, send_file, jsonify, url_for, abort, redirect, make_response
from flask.ext.babelex import gettext as _
from foofind.utils.downloader import downloader_url
from foofind.utils.fooprint import Fooprint
from foofind.utils import logging

downloader = Fooprint("downloader", __name__, template_folder="template", dup_on_startswith="/<lang>")

def get_downloader_properties(base_path, downloader_files_builds):
    # source code information
    properties = {"common":{"base_path": base_path}}

    # builds information
    for build, info in downloader_files_builds.iteritems():
        try:
            with open(os.path.join(base_path, info["metadata"]), "r") as f:
                metadata = json.load(f)

            properties[build] = info.copy()
            properties[build].update(metadata)
            properties[build]["length"] = os.path.getsize(os.path.join(base_path, properties[build]["main"]))
        except:
            logging.error("Error checking downloader files.")

    return properties

def parse_version(info):
    if info:
        try:
            info_parts = info.split("-",3)
            info_parts.reverse()
            version = info_parts.pop().split(".")
            version2 = info_parts.pop() if info_parts else ""
            build = info_parts.pop() if info_parts else "W32"
            return [[int(v) for v in version], version2], build
        except:
            pass

    # Default return value: no version, W32 build
    return 0, "W32"

@downloader.route("/<lang>/downloader")
def index():
    g.title = "Foofind download manager"
    g.page_description = _("downloader_meta_description")
    return render_template(
        'downloader/index.html',
        zone = "downloader"
        )

@downloader.route("/<lang>/downloader/success")
def success():
    g.title = "Foofind download manager"
    g.page_description = _("downloader_meta_description")
    return render_template(
        'downloader/success.html',
        zone = "downloader"
        )


@downloader.route("/<lang>/downloader/logger")
@downloader_url
def logger():
    return ""

@downloader.route("/<lang>/downloader/update")
@downloader_url
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
    platform = request.args.get("platform", None)
    version_raw = request.args.get("version", None)
    if not version_raw and "/" in request.user_agent.string: # tries to get version from user agent
        version_raw = request.user_agent.string.split("/")[-1]

    version, build = parse_version(version_raw)

    # Updates
    update = g.downloader_properties[build].get("update",None)
    response = {}
    if update and version<update["min"]:
        # Custom update message
        new_version = g.downloader_properties[build]["version"]
        response["update"] = {
            "text": _("downloader_update_message",
                      appname = current_app.config["DOWNLOADER_APPNAME"],
                      version = new_version
                      ),
            "title": _("Update available"),
            "files": {
                        "url": url_for('.download', build=build, instfile=update["file"], _external=True),
                        "version": new_version,
                        "argv": [],
                    },
            }

    # Messages
    response["messages"] = []

    return jsonify(response)

@downloader.route("/<lang>/downloader/foofind_download_manager_proxy.exe")
@downloader.route("/<lang>/downloader/<build>/foofind_download_manager_proxy.exe")
def download_proxy(build="W32"):
    if build[0]=="W":
        data = {'geturl':'1', 'name':"Foofind Download Manager",'version':g.downloader_properties[build]["version"],
                'url':url_for('downloader.download', build=build, instfile=g.downloader_properties[build]["main"], _external=True), 'id':"foofind.com", 'img':'http://foofind.com/static/img/downloader/foofind.png'}
        headers = {'Content-Type':'application/x-www-form-urlencoded', 'Connection':'close', 'Referer':request.referrer}

        resp = requests.post("http://download.oneinstaller.com/installer/", headers=headers, data=data)

        return redirect(resp.text, 302)
    else:
        return redirect(url_for('downloader.download', build=build, instfile=g.downloader_properties[build]["main"], _external=True), 302)

@downloader.route("/<lang>/downloader/<instfile>")
@downloader.route("/<lang>/downloader/<build>/<instfile>")
def download(instfile, build="W32"):
    return send_instfile(instfile, build)

def send_instfile(instfile, build):
    downloader_files = g.downloader_properties.get(build, None)
    if not downloader_files:
        abort(404)

    downloader_files_aliases = downloader_files.get("aliases",{})
    if instfile in downloader_files_aliases:
        path = downloader_files[downloader_files_aliases[instfile]]
    else:
        # check that can be downloaded
        for downloadable in downloader_files["downloadables"]:
            if downloader_files.get(downloadable, None)==instfile:
                path = instfile
                break
        else:
            abort(404)

    return send_file(os.path.join(g.downloader_properties["common"]["base_path"], path), mimetypes.guess_type(path)[0])


# Old URLs
installer_dependencies = ("setup.exe", "vcredist_x86.exe")

@downloader.route("/<lang>/downloader/version")
@downloader.route("/<lang>/downloader/installer/<version>/version")
@downloader_url
def version(version=None):
    build = "W32"
    new_version = g.downloader_properties[build]["version"]
    if version:
        response = make_response("%s redist.exe\n"%new_version)
        response.headers["Content-Type"] = "text/plain; charset=utf-8"
        return response
    else:
        return new_version

@downloader.route("/<lang>/downloader/download/<instfile>")
@downloader.route("/<lang>/downloader/installer/<version>/<instfile>")
@downloader_url
def downloader_dependency_download(instfile, version=None):
    '''
    Old downloader dependencies
    '''
    return send_instfile(instfile, "W32")

# -*- coding: utf-8 -*-
"""
    API pública
"""

from flask import Blueprint, abort, request, render_template, current_app, jsonify, url_for, g
from foofind.utils import mid2url, url2mid
from foofind.services.search import search_files, get_ids
from foofind.services import *
from foofind.utils import u
from base64 import b64encode, b64decode

# Blueprints
from files import fill_data

import logging, zlib

api = Blueprint('api', __name__)


def file_embed_link(data):
    '''
    Obtiene el enlace del iframe de embed para el archivo dado
    '''
    return url_for( "api.api_embed", _external=True, fileid=mid2url(data["file"]["_id"]), nameid=data["view"]["fnid"])

@api.route("/api")
@api.route("/api/")
@api.route("/api/1")
def api_v1():
    method = request.args.get("method", None)
    results = ()
    success = False
    try:
        if method == "getSearch":
            files, query = search_files(request.args["q"], request.args)
            results = enumerate(fill_data(file_data) for file_data in filesdb.get_files(get_ids(files),True))
            success = True
    except BaseException as e:
        logging.debug(e)

    return render_template("api/v1.xml",
        api_method=method,
        results=results,
        success = success
        )

_api_v2_md_parser = {
    "created": str,
    }
@api.route("/api/2")
def api_v2():
    method = request.args.get("method", None)
    success = True
    result = None
    if method == "search":
        result = []
        files, query = search_files(request.args["q"], request.args)
        result = [{
            "size": f["file"]["z"] if "z" in f["file"] else 0,
            "type": f["view"]["file_type"],
            "link": url_for("files.download", file_id=f["view"]["url"], _external=True),
            "metadata": {k: (_api_v2_md_parser[k](v) if k in _api_v2_md_parser else v)
                for k, v in f["view"]["md"].iteritems()},
            } for f in (fill_data(file_data) for file_data in filesdb.get_files(get_ids(files), True))]
        success = True
    return jsonify(
        method = method,
        success = success,
        result = result
        )

@api.route("/api/embed/<fileid>/<nameid>")
@cache.cached(
    unless=lambda:True,
    key_prefix=lambda: "api/embed_%s_%%s" % g.lang
    )
def api_embed(fileid, nameid):

    data = filesdb.get_file(url2mid(fileid))

    if "torrent:name" in data["md"]:
        filename = data["md"]["torrent:name"]
    elif nameid in data["fn"]:
        filename = data["fn"][nameid]['n']
    elif data["fn"]:
        filename = data["fn"].values()[0]['n']
    else:
        filename = ""

    if data.get("z", 0):
        size = data['z']

    return render_template("api/embed.html",
        blocked = data.get("bl", True),
        filename = filename,
        size = size,
        download_url = url_for("files.download", file_id=mid2url(data["_id"]), file_name="%s.html" % filename)
        )

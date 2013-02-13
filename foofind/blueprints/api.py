# -*- coding: utf-8 -*-
"""
    API pública
"""

import zlib
from flask import Blueprint, abort, request, render_template, current_app, jsonify, url_for, g
from foofind.utils import mid2url, url2mid, u, logging
from foofind.services import *
from foofind.blueprints.files import secure_fill_data, get_file_metadata, DatabaseError, FileNotExist, FileRemoved, FileUnknownBlock



api = Blueprint('api', __name__)

def file_embed_link(data, size="m"):
    '''
    Obtiene el enlace del iframe de embed para el archivo dado
    '''
    return url_for( "api.api_embed", _external=True, embed_size=size, fileid=mid2url(data["file"]["_id"]), nameid=data["view"]["fnid"])

@api.route("/api")
@api.route("/api/")
@api.route("/api/1")
def api_v1():
    method = request.args.get("method", None)
    results = ()
    success = False
    try:
        if method == "getSearch":
            query = request.args["q"]
            s = searchd.search({"type":"text", "text":query}, request.args, 1000)
            ids = list(s.get_results([], 100, 100))
            stats = s.get_stats()
            results = enumerate(filter(None, [secure_fill_data(f,text=query) for f in filesdb.get_files(ids,True)]))
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
        query = request.args["q"]
        s = searchd.search({"type":"text", "text":query}, request.args, 1000)
        ids = list(s.get_results([], 100, 100))
        stats = s.get_stats()
        result = [{
            "size": f["file"]["z"] if "z" in f["file"] else 0,
            "type": f["view"]["file_type"],
            "link": url_for("files.download", file_id=f["view"]["url"], file_name=f["view"]["qfn"]+".htm", _external=True),
            "metadata": {k: (_api_v2_md_parser[k](v) if k in _api_v2_md_parser else v)
                for k, v in f["view"]["md"].iteritems()},
            } for f in filter(None, [secure_fill_data(f,text=query) for f in filesdb.get_files(ids,True)])]
        success = True
    return jsonify(
        method = method,
        success = success,
        result = result
        )

@api.route("/api/embed/<embed_size>/<fileid>/<nameid>")
@cache.cached(
    unless=lambda:True,
    key_prefix=lambda: "api/embed_%s_%%s" % g.lang
    )
def api_embed(embed_size, fileid, nameid):
    if not embed_size in ("s","m","b"):
        embed_size = "m"

    file_id = url2mid(fileid)
    filename = ""

    download_url = None
    size = 0

    if embed_size == "b":
        try:
            data = get_file_metadata(file_id, nameid)
            download_url = data["view"]["url"]
        except DatabaseError:
            abort(503)
        except FileNotExist:
            flash("link_not_exist", "error")
            abort(404)
        except FileRemoved:
            flash("error_link_removed", "error")
            abort(404)
        except FileUnknownBlock:
            abort(404)

    else:
        data = filesdb.get_file(file_id)

        size = None
        if "z" in data:
            size = data['z']

        if "torrent:name" in data["md"]:
            filename = data["md"]["torrent:name"]
        elif nameid in data["fn"]:
            filename = data["fn"][nameid]['n']
        elif data["fn"]:
            filename = data["fn"].values()[0]['n']
        else:
            filename = ""

        download_url = url_for("files.download", file_id=mid2url(data["_id"]), file_name="%s.html" % filename)

    return render_template("api/embed.html",
        filename = filename,
        size = size,
        file = data,
        embed_size = embed_size,
        download_url = download_url
        )

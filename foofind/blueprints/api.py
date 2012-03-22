# -*- coding: utf-8 -*-
"""
    API pública
"""

from flask import Blueprint, abort, request, render_template, current_app, jsonify, url_for

from foofind.services.search import search_files, get_ids
from foofind.services import *

# Blueprints
from files import fill_data

import logging

api = Blueprint('api', __name__)

@api.route("/api")
@api.route("/api/")
@api.route("/api/<int:version>")
def api_gateway(version = 1):
    if version == 1: return api_v1()
    elif version == 2: return api_v2()
    return abort(502)

def api_v1():
    method = request.args.get("method", None)
    results = ()
    success = False
    try:
        if method == "getSearch":
            files = search_files(request.args["q"], request.args)
            results = enumerate(fill_data(file_data) for file_data in filesdb.get_files(get_ids(files),True))
            success = True
    except BaseException as e:
        logging.debug(e)

    return render_template("api/v1.xml",
        api_method=method,
        results=results,
        success = succeeded
        )

_api_v2_md_parser = {
    "created": str,
    }
def api_v2():
    method = request.args.get("method", None)
    success = True
    result = None

    '''
    {'file': {u'md': {u'video:category': u'comedy', u'video:duration': 6287L, u'video:description': u'asdf'}, u'c': -1, u'fs': datetime.datetime(2011, 4, 7, 7, 19, 49), 'name': u'http://www.veoh.com/veohplayer.swf?permalinkId=v19638583pbHaJywJ', u'src': {u'6d25345e0114c0424a06af6c': {u'url': u'http://www.veoh.com/watch/v19638583pbHaJywJ', u'm': 1, u't': 17, u'fn': {u'1361703869': {u'm': 1, u'l': 1}}, u'l': 1}, u'fe6d3bedce035eebe2a1e051': {u'url': u'http://www.veoh.com/veohplayer.swf?permalinkId=v19638583pbHaJywJ', u'm': 1, u't': 17, u'fn': {u'1361703869': {u'm': 1, u'l': 1}}, u'l': 1}}, u'bl': 0, u'tt': 1, u'm': 1, u's': 3, u'ls': datetime.datetime(2011, 4, 7, 7, 19, 49), u'_id': ObjectId('fe6d3bedce035eebe2a1e051'), 'id': '-m077c4DXuvioeBR', u'fn': {u'1361703869': {u'x': u'', 'c': 2, 'tht': 0, u'n': u'asdf'}}, u'ct': 2}, 'view': {'md': {}, 'file_type': 'video', 'source': u'veoh.com', 'url': '-m077c4DXuvioeBR', 'nfn': u'asdf', 'fnx': '', 'sources': {u'veoh.com': {'count': 2, 'join': False, 'tip': u'veoh.com', 'parts': [], 'urls': [u'http://www.veoh.com/watch/v19638583pbHaJywJ', u'http://www.veoh.com/veohplayer.swf?permalinkId=v19638583pbHaJywJ', u'magnet:?dn=asdf&'], 'icon': 'web'}}, 'efn': u'asdf', 'action': 'Download', 'fn': u'asdf', 'fnh': u'<strong>asdf</strong>'}}
    '''


    if method == "search":
        result = []
        files = search_files(request.args["q"], request.args)
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
        result = result)

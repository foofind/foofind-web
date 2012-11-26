# -*- coding: utf-8 -*-
from foofind.utils.fooprint import Fooprint
from flask import jsonify, request
from foofind.services import *
from foofind.blueprints.files import url2filters

files_test = Fooprint('files_test', __name__, template_folder="template")

@files_test.route('/<lang>/search_stats/')
def search_stats():
    return jsonify(searchd.proxy.stats)

@files_test.route('/<lang>/search_info/<query>')
@files_test.route('/<lang>/search_info/<query>/<path:filters>/')
def search_info(query, filters=None):
    query = query.replace("_"," ")
    dict_filters, has_changed = url2filters(filters)

    info = searchd.get_search_info({"type":"text", "text":query}, dict_filters)
    return jsonify({"stats":[{"-":info["query"]}, {"-":info["filters"]}, {"-":info["temp"]}, {"-":info["locked"]}]})

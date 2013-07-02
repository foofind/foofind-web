# -*- coding: utf-8 -*-
from foofind.utils.fooprint import Fooprint
from flask import jsonify, request
from foofind.services import *
from foofind.blueprints.files import url2filters

files_test = Fooprint('files_test', __name__, template_folder="template")

@files_test.route('/<lang>/search_stats/')
def search_stats():
    return jsonify(searchd.proxy.stats)

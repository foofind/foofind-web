# -*- coding: utf-8 -*-
from flask import request, render_template, g, jsonify, flash, redirect, url_for, abort, current_app
from flask.ext.login import current_user
from foofind.forms.files import SearchForm
from foofind.services.search import search_related, get_ids
from foofind.services import *
from foofind.utils import mid2bin, url2mid, u
from foofind.utils.splitter import split_file
from foofind.utils.fooprint import Fooprint
from foofind.blueprints.files import download2,searcha
from werkzeug.urls import url_encode

files_ajax = Fooprint('files_ajax', __name__, template_folder="template")

#@download.alternative("ajax")
def downloada(file_id=None,file_name=None):
    if request.args.get("alt"): #FIXME problema de codificacion con ! en los id al hacer la redireccion para
        return redirect(u"%s?%s" % (url_for("files.download",file_id=file_id,file_name=file_name), url_encode(i for i in request.args.iteritems() if not i[0] in params_blacklisted)), 302)

    return searcha(file_id=file_id,file_name=file_name)

@files_ajax.route('/<lang>/download_ajax/<file_id>')
@files_ajax.route('/<lang>/download_ajax/<file_id>/<path:file_name>',methods=['GET','POST'])
def download_ajax(file_id,file_name=None):
    #validar id de download
    try:
        file_mid=url2mid(file_id)
    except:
        file_mid=file_id=None

    download=download2(file_mid,file_name)
    return jsonify(html=download["html"],play=download["play"])

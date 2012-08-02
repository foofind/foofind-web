# -*- coding: utf-8 -*-
from flask import Blueprint, request, render_template, g, current_app, jsonify, flash, redirect, url_for, abort
from flaskext.login import current_user
from foofind.forms.files import SearchForm
from foofind.services.search import search_files, get_ids
from foofind.services import *
from foofind.utils import url2mid, u
from foofind.utils.fooprint import ManagedSelect
from foofind.blueprints.files import taming_search, vote, files, search, init_data, choose_filename, choose_file_type, get_images, format_metadata, build_source_links
from urlparse import urlparse
from werkzeug.urls import url_encode, url_decode
import urllib

isma_blacklisted = ("alt",)

def redirect_to_ajax():
    #return redirect(u(request.url).replace(u(request.path)+"?", u(request.path)+"/#!/"), 302)
    return redirect(u"%s#!/%s" % (
        url_for("files_ajax.searcha"),
        url_encode(i for i in request.args.iteritems()
            if not i[0] in isma_blacklisted)
        ), 302)

@search.alternative("ajax")
def leaf_search():
    return redirect_to_ajax()

files_ajax = Blueprint('files_ajax', __name__, template_folder="template")

@files_ajax.context_processor
def file_var():
    return {"zone":"files","search_form":SearchForm(request.args),"args":request.args}

@files_ajax.route('/<lang>/search/')
def searcha():
    if leaf_search.select("files.search") != "ajax":
        return redirect(url_for("files.search", **request.args), 302)
    if request.args:
        return redirect_to_ajax()
    return render_template('files_ajax/search_ajax.html',results = {"total_found":0,"total":0,"time":0})

_api_v2_md_parser = {
    "created": str,
    }
_filters={
    "q":"",
    "type":["audio","video","image","document","software"],
    "src":{"w":'direct_downloads',"s":"Streaming","p":"P2P"},
    "size":['all_sizes','smaller_than','larger_than'],
    "quality":['all_qualities',['128 kbps ','or_better'],['192 kbps ','or_better'],['256 kbps ','or_better'],['320 kbps ','or_better']],
    "year":['all_years',['before'," 1960"],"60's","70's","80's","90's","00's",'last_year'],
    }
@files_ajax.route('/<lang>/search_ajax')
def search_ajax():
    arg=dict(request.args)
    query = request.args.get("q", None)
    result = search_files(query, request.args,int(request.args.get("page", 1))) or {"total_found":0,"total":0,"time":0}
    taming, dym = taming_search(query, request.args.get("type", None))
    try:
        tags = tags.next()
    except:
        tags = []

    files = [{
        "size": f["file"]["z"] if "z" in f["file"] else 0,
        "type": f["view"]["file_type"] if "file_type" in f["view"] else None,
        "link": url_for("files.download", file_id=f["view"]["url"], _external=True),
        "metadata": {k: (_api_v2_md_parser[k](v) if k in _api_v2_md_parser else v)
            for k, v in f["view"]["md"].iteritems()},

        "html": render_template('files_ajax/file_ajax.html',file=f)
        } for f in (fill_data(file_data,text=query) for file_data in filesdb.get_files(get_ids(result), True))]

    return jsonify(
        result=result,
        files=files,
        tags=render_template('files_ajax/tags.html',tags=tags),
        no_results=render_template('files_ajax/no_results.html',filters=_filters)
    )

@files_ajax.route('/<lang>/download_ajax/<file_id>')
def download_ajax(file_id):
    file_id=url2mid(file_id)
    try:
        data = filesdb.get_file(file_id, bl = None)
    except filesdb.BogusMongoException as e:
        logging.exception(e)
        abort(503)

    if data:
        if not data["bl"] in (0, None):
            if data["bl"] == 1:
                flash("link_not_exist", "error")
            elif data["bl"] == 3:
                flash("error_link_removed", "error")

            goback = True
            abort(404)
    else:
        flash("link_not_exist", "error")
        abort(404)

    #si el usuario esta logueado se comprueba si ha votado el archivo para el idioma activo
    vote=None
    if current_user.is_authenticated():
        vote=usersdb.get_file_vote(file_id,current_user,g.lang)

    if vote is None:
        vote={"k":0}

    return render_template('files_ajax/download_ajax.html',file=fill_data(data,True),vote=vote)

def embed_info(f):
    # embed de ficheros
    source = f['view']['source']
    tip = f['view']['sources'][source]['tip']
    if tip in {"youtube.com", "4shared.com"}:
        vid = None

        for url in f['view']['sources'][source]["urls"]:
            urlparts = urlparse(url)
            if tip=="youtube.com":
                if urlparts.path.startswith("/v/"):
                    vid = urlparts.path[3:]
                else:
                    querystring = url_decode(urlparts.query)
                    if "v" in querystring: vid = querystring["v"]
                if vid: f["view"]["embed"] = "<embed src='http://www.youtube.com/v/%s?version=3&amp;hl=es_ES' type='application/x-shockwave-flash' width='420' height='315' allowscriptaccess='always' allowfullscreen='true' wmode='opaque'></embed>" % vid
                f['view']['sources'][source]["logo"] = "/static/img/youtube.png"
            elif tip=="4shared.com":
                path_parts = urlparts.path.split("/")
                if path_parts[1] in {"mp3", "video"}:
                    vid = path_parts[2]
                    f["view"]["embed"] = "<embed wmode='opaque' src='http://www.4shared.com/embed/%s/%s' width='420' height='315' allowfullscreen='true' allowscriptaccess='always' type='application/x-shockwave-flash'></embed>" % (vid, vid)

def fill_data(file_data,details=False,text=False):
    '''
    Añade los datos necesarios para mostrar los archivos
    '''
    f=init_data(file_data)
    choose_filename(f,text)
    build_source_links(f, unify_torrents=True)
    embed_info(f)
    choose_file_type(f)
    get_images(f)
    format_metadata(f,details,text)
    
    return f

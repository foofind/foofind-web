# -*- coding: utf-8 -*-
from flask import Blueprint, request, render_template, g, current_app, jsonify, flash, redirect, url_for, abort
from flask.ext.login import current_user
from foofind.forms.files import SearchForm
from foofind.services.search import search_files, get_ids, get_id_server_from_search
from foofind.services import *
from foofind.utils import url2mid, u
from foofind.utils.fooprint import ManagedSelect
from foofind.blueprints.files import taming_search, vote, files, search, init_data, choose_filename, choose_file_type, get_images, format_metadata, build_source_links, save_visited
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

@search.alternative("ajax2")
def history():
    print "activa ajax2"
    return redirect(u"%s?%s" % (url_for("files_ajax.searcha"), url_encode(i for i in request.args.iteritems() if not i[0] in isma_blacklisted)), 302)

files_ajax = Blueprint('files_ajax', __name__, template_folder="template")

streaming_icons = download_icons = None
def sources_icons():
    global streaming_icons, download_icons
    streaming_icons = list({(searchd.proxy.sources[src["_id"]], src["d"]) for src in filesdb.get_sources(group="s")})
    download_icons = list({(searchd.proxy.sources[src["_id"]], src["d"]) for src in filesdb.get_sources(group="w")})
    streaming_icons.sort(reverse=True)
    download_icons.sort(reverse=True)

    streaming_icons = u"<ul>%s</ul><a href='#'>%d más</a>"%("".join("<li><img title='%s' src='http://www.google.com/s2/u/0/favicons?domain=%s'></li>"%(src,src) for weight, src in streaming_icons[:10]), len(streaming_icons)-10)
    download_icons = u"<ul>%s</ul><a href='#'>%d más</a>"%("".join("<li><img title='%s' src='http://www.google.com/s2/u/0/favicons?domain=%s'></li>"%(src,src) for weight, src in download_icons[:10]), len(download_icons)-10)

@files_ajax.context_processor
def file_var():
    if not streaming_icons: sources_icons()
    return {"zone":"files","search_form":SearchForm(request.args),"args":request.args, "streaming_icons":streaming_icons, "download_icons":download_icons}


@files_ajax.route('/<lang>/search/')
def searcha():
    ajax=leaf_search.select("files.search")
    if ajax not in ("ajax","ajax2"):
        return redirect(url_for("files.search", **request.args), 302)
    #if request.args:
        #return redirect_to_ajax()

    if ajax=="ajax":
        return render_template('files_ajax/search_ajax.html')
    elif ajax=="ajax2":
        static_results=search_ajax()
        return render_template('files_ajax/search_ajax.html',
            search_info=static_results["result_number"],
            static_results=static_results["files"] if static_results["total_found"]>0 else static_results["no_results"],
            total_found=static_results["total_found"],
        )

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
    query = request.args.get("q", None)
    profiler.checkpoint(opening=["sphinx"])
    result, query_info = search_files(query, request.args,int(request.args.get("page", 1))) or ({"total_found":0,"total":0,"time":0}, {"q":query, "s":[]})
    ids = get_ids(result)
    profiler.checkpoint(opening=["mongo"], closing=["sphinx"])
    files = [fill_data(f,text=query_info["q"]) for f in filesdb.get_files(ids,True)]
    profiler.checkpoint(opening=["visited"], closing=["mongo"])
    save_visited(files)
    profiler.checkpoint(closing=["visited"])

    ajax=leaf_search.select("files.search")
    if ajax=="ajax" or (ajax=="ajax2" and request.args.get("page", 1)>1):
        return jsonify(
            files_ids=[f["file"]["id"] for f in files],
            files=[render_template('files_ajax/file_ajax.html',file=f) for f in files],
            no_results=render_template('files_ajax/no_results.html',filters=_filters),
            result_number=render_template('files_ajax/results_number.html',results=result,search=query),
            total_found=result["total_found"],
        )
    else:
        return {
            "files_ids":[f["file"]["id"] for f in files],
            "files":"".join([render_template('files_ajax/file_ajax.html',file=f) for f in files]),
            "no_results":render_template('files_ajax/no_results.html',filters=_filters),
            "result_number":render_template('files_ajax/results_number.html',results=result,search=query),
            "total_found":result["total_found"],
        }

@files_ajax.route('/<lang>/download_ajax/<file_id>')
@files_ajax.route('/<lang>/download_ajax/<file_id>/<path:file_name>',methods=['GET','POST'])
def download_ajax(file_id,file_name=None):
    file_id=url2mid(file_id)

    try:
        data = filesdb.get_file(file_id, bl = None)
    except filesdb.BogusMongoException as e:
        logging.exception(e)
        abort(503)

    # intenta sacar el id del servidor de sphinx,
    # resuelve inconsistencias de los datos
    if not data:
        sid = get_id_server_from_search(file_id, file_name)
        if sid:
            try:
                data = filesdb.get_file(file_id, sid = sid, bl = None)
            except filesdb.BogusMongoException as e:
                logging.exception(e)
                abort(503)

    if data:
        if not data["bl"] in (0, None):
            if data["bl"] == 1: flash("link_not_exist", "error")
            elif data["bl"] == 3: flash("error_link_removed", "error")
            goback = True
            block_files( mongo_ids=((data["_id"],file_name),) )
            abort(404)
    else:
        flash("link_not_exist", "error")
        abort(404)

    #obtener los datos
    file_data=fill_data(data, True, file_name)
    if file_data["view"]["sources"]=={}: #si tiene todos los origenes bloqueados
        flash("error_link_removed", "error")
        abort(404)

    #si el usuario esta logueado se comprueba si ha votado el archivo para el idioma activo
    vote=None
    if current_user.is_authenticated():
        vote=usersdb.get_file_vote(file_id,current_user,g.lang)

    if vote is None:
        vote={"k":0}

    return jsonify(html=render_template('files_ajax/download_ajax.html',file=file_data,vote=vote),play=file_data["view"]["play"] if "play" in file_data["view"] else "")

def embed_info(f):
    # embed de ficheros
    source = f['view']['source']
    tip = f['view']['sources'][source]['tip']
    if tip in {"youtube.com", "4shared.com"}:
        video_id = None
        for url in f['view']['sources'][source]["urls"]:
            urlparts = urlparse(url)
            if tip=="youtube.com":
                #se extrae el id de los dos tipos distintos de url que vienen
                if urlparts.path.startswith("/v/"):
                    video_id = urlparts.path[3:]
                else:
                    querystring = url_decode(urlparts.query)
                    if "v" in querystring:
                        video_id = querystring["v"]

                if video_id:
                    f["view"]["embed"] = '<iframe type="text/html" width="512" height="288" src="https://www.youtube.com/embed/%s?showinfo=0&autoplay=0&&rel=0&color=white&theme=light&wmode=opaque" frameborder="0" allowfullscreen="allowfullscreen" />' % video_id
                    f["view"]["play"] = ("&autoplay=0","&autoplay=1")

            elif tip=="4shared.com":
                path_parts = urlparts.path.split("/")
                if path_parts[1] in {"mp3", "video"}:
                    video_id = path_parts[2]
                    f["view"]["embed"] = '<embed wmode="opaque" src="http://www.4shared.com/embed/%s/%s" width="512" height="288" allowfullscreen="true" allowscriptaccess="always" type="application/x-shockwave-flash" flashvars="autostart=false"></embed>' % (video_id, video_id)
                    f["view"]["play"] = ("autostart=false","autostart=true")

def fill_data(file_data,details=False,text=False):
    '''
    Añade los datos necesarios para mostrar los archivos
    '''
    if not hasattr(g,"sources"):
        g.sources = {s["_id"]:s for s in filesdb.get_sources(blocked=None)}
        g.image_servers = filesdb.get_image_servers()

    f=init_data(file_data)
    choose_filename(f,text)
    build_source_links(f, unify_torrents=True)
    embed_info(f)
    choose_file_type(f)
    get_images(f)
    format_metadata(f,details,text)
    return f

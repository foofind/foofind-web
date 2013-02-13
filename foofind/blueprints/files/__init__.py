# -*- coding: utf-8 -*-
"""
    Controladores de las páginas de búsqueda y de fichero.
"""
import urllib, json, unicodedata, random, sys, bson
from flask import request, render_template, g, current_app, jsonify, flash, redirect, url_for, abort, Markup
from flask.ext.login import login_required, current_user
from flask.ext.babel import gettext as _
from flaskext.babel import format_datetime
from datetime import datetime
from timelib import strtotime
from struct import pack, unpack
from collections import OrderedDict
from copy import deepcopy
from base64 import b64encode, b64decode

from foofind.blueprints.files.fill_data import secure_fill_data, get_file_metadata, init_data, choose_filename
from foofind.blueprints.files.helpers import *
from foofind.services import *
from foofind.forms.files import SearchForm, CommentForm
from foofind.utils import url2mid, mid2bin, mid2hex, mid2url, bin2hex, u, canonical_url, logging
from foofind.utils.content_types import *
from foofind.utils.splitter import split_phrase
from foofind.utils.pagination import Pagination
from foofind.utils.fooprint import Fooprint
from foofind.utils.seo import seoize_text

files = Fooprint('files', __name__, dup_on_startswith="/<lang>")

@files.context_processor
def file_var():
    if request.args.get("error",None)=="error":
        abort(404)

    share=[
        (_("send_by_email"),"email","mailto:?subject=%(title)s&amp;body=%(url)s"),
        ("Facebook","facebookcom","http://www.facebook.com/share.php?u=%(url)s"),
        ("Twitter","twittercom","http://twitter.com/home?status=%(url)s"),
        ("Google+","plusgooglecom","https://plus.google.com/share?url=%(url)s"),
        ("Myspace","myspacecom","http://www.myspace.com/Modules/PostTo/Page/?u=%(url)s"),
        ("Delicious","deliciouscom","http://delicious.com/post?url=%(url)s"),
        ("Digg","diggcom","http://digg.com/submit?phase2&url=%(url)s&title=%(title)s"),
        ("Evernote","evernotecom","http://www.evernote.com/clip.action?url=%(url)s"),
        ("Friendfeed","friendfeedcom","http://www.friendfeed.com/share?title=%(title)s&url=%(url)s"),
        ("Google","googlecom","http://www.google.com/bookmarks/mark?op=edit&btmk=%(url)s"),
        ("Newsvine","newsvinecom","http://www.newsvine.com/_tools/seed&save?u=%(url)s"),
        ("Reddit","redditcom","http://www.reddit.com/submit?url=%(url)s"),
        ("Stumbleupon","stumbleuponcom","http://www.stumbleupon.com/submit?url=%(url)s"),
        ("Technorati","technoraticom","http://technorati.com/faves?add=%(url)s"),
        ("Menéame","meneamenet","http://meneame.net/submit.php?url=%(url)s&title=%(title)s"),
        ("Tuenti","tuenticom","http://www.tuenti.com/share?url=%(url)s"),
        ("Pinterest","pinterestcom","http://pinterest.com/pin/create/button/?url=%(url)s"),
        ("Netvibes","netvibescom","http://www.netvibes.com/share?title=%(title)s&url=%(url)s"),
        ("Linkedin","linkedincom","http://www.linkedin.com/shareArticle?mini=true&url=%(url)s&title=%(title)s"),
        ("Posterous","posterouscom","http://posterous.com/share?linkto=%(url)s"),
        ("Yahoo","yahoocom","http://bookmarks.yahoo.com/toolbar/savebm?u=%(url)s&t=%(title)s"),
    ]
    return {"zone":"files","search_form":SearchForm(request.args),"args":g.args,"extra_sources":g.extra_sources,"share":share}

@unit.observe
@files.route('/<lang>/search') #para soportar las URL antiguas
@files.route('/<lang>/search/<query>')
@files.route('/<lang>/search/<query>/download/<file_id>')
@files.route('/<lang>/search/<query>/<path:filters>/')
@files.route('/<lang>/search/<query>/<path:filters>/download/<file_id>')
def search(query=None,filters=None,file_id=None,file_name=None):
    '''
    Gestiona las URL de busqueda de archivos
    '''
    canonical_query=query #la busqueda canonica es la misma por defecto
    full_browser=is_full_browser()
    search_bot=is_search_bot()
    url_with_get_params=False
    if "_escaped_fragment_" in request.args: #si la URL tiene que ser una "captura de pantalla" de una ajax se redirige a la normal
        return redirect(url_for(".search",query=query,filters=filters,file_id=file_id,file_name=file_name),301 if search_bot else 302)
    elif query is None and file_id is None: #si puede venir una URL antigua
        query=request.args.get("q",None)
        if not query: #si no se ha buscado nada se manda al inicio
            flash("write_something")
            return redirect(url_for("index.home"))
        else: #sino se reemplazan los espacios que venian antes con un + en el query string y se extraen los filtros
            query = query.replace("+"," ").replace("/"," ")
            filters=filters2url(request.args)
            url_with_get_params=True

    query = query.replace("_"," ") if query is not None else None #para que funcionen la busqueda cuando vienen varias palabras
    dict_filters, has_changed = url2filters(filters) #procesar los parametros
    if url_with_get_params or has_changed: #redirecciona si viene una url con get o si url2filters ha cambiado los parametros y no se mandan filtros si es un bot
        return redirect(url_for(".search", query=query.replace(" ","_"), filters=filters2url(dict_filters) if not search_bot else None, file_id=file_id),301 if search_bot else 302)

    static_download=download_file(file_id,file_name if file_name is not None else query) #obtener el download si se puede
    if "error" in static_download and static_download["error"][0]==301: #si es una redireccion de un id antiguo directamente se devuelve
        return static_download["html"]

    prepare_args(query, dict_filters)
    #añadir todos los sources a FILTERS
    filters_with_all_src=deepcopy(FILTERS)
    filters_with_all_src["src"].update(g.sources_names)
    #filtros encima de resultados de busqueda
    top_filters=OrderedDict([("type",[]),("src",[]),("size",[])])
    for name,values in dict_filters.iteritems():
        if name!="q":
            for value in values:
                if name=="type":
                    top_filters[name].append(_(value))
                elif name=="src" and value in filters_with_all_src["src"]:
                    top_filters[name].append(_(filters_with_all_src["src"][value]))
                elif name=="size":
                    top_filters[name]=[int(values[0]),int(values[1]),filters_with_all_src["size"][1],filters_with_all_src["size"][2]]

    if query is None: #en la pagina de download se intentan obtener palabras para buscar
        query=download_search(static_download["file_data"] if static_download and static_download["file_data"] else None,file_name, "foofind")
        if query:
            query.replace(":","")
    else:
        #titulo y descripción de la página para la busqueda
        g.title = query+(" - "+", ".join(top_filters["type"]) if top_filters["type"] else "")+" - "+g.title
        g.page_description = _("results_for").capitalize() + " " + query
        g.keywords.add(query)

    sure = False
    total_found=0
    if file_id is not None and "error" in static_download:  #si es un error y no hay nada para buscar se muestra en pantalla completa
        flash(static_download["error"][1])
        abort(static_download["error"][0])
    elif not full_browser: #busqueda estatica para browsers "incompletos"
        results=search_files(query,dict_filters,50,50,static_download, wait_time=1000)
        # cambia la busqueda canonica
        canonical_query = results["canonical_query"]
        if search_bot:
            searchd.log_bot_event(search_bot, (results["total_found"]>0 or results["sure"]))

        static_results=results["files"]
        total_found=results["total_found"]
        sure = True
    elif query is not None and static_download["file_data"] is not None: #si es busqueda con download se pone el primero el id que venga
        static_results=[render_template('files/file.html',file=static_download["file_data"],scroll_start=True)]
    else:
        static_results=[]


    #calcular el numero de sources que estan activados
    sources_count={"streaming":0,"download":0,"p2p":0}
    if "src" in g.args:
        active_sources=g.args["src"]
        if "streaming" in active_sources:
            sources_count["streaming"]=len(g.sources_streaming)
        else:
            if "other-streamings" in active_sources:
                sources_count["streaming"]=len(g.sources_streaming)-8

            for value in g.sources_streaming[:8]:
                if value in active_sources:
                    sources_count["streaming"]=sources_count["streaming"]+1

        if "download" in active_sources:
            sources_count["download"]=len(g.sources_download)
        else:
            if "other-downloads" in active_sources:
                sources_count["download"]=len(g.sources_download)-8

            for value in g.sources_download[:8]:
                if value in active_sources:
                    sources_count["download"]=sources_count["download"]+1

        if "p2p" in active_sources:
            sources_count["p2p"]=len(g.sources_p2p)
        else:
            for value in g.sources_p2p:
                if value in active_sources:
                    sources_count["p2p"]=sources_count["p2p"]+1

    alternate=[]
    f=static_download['file_data']
    if f: #si es download se añaden los otros nombres de archivos como url alternativas
        canonical=url_for('.download',lang="en",file_id=f['file']['id'],file_name=f['view']['fn'])
        for name_id,name in f['file']['fn'].items():
            file_name=extension_filename(name['n'],name['x'])
            if f['view']['fnid'] and "torrent:name" not in f["file"]["md"] and file_name!=f['view']['fn']:
                alternate.append(url_for('.download',lang='en',file_id=f['file']['id'],file_name=file_name))

    else:
        canonical=url_for('.search',lang='en',query=canonical_query)

    return render_template('files/search.html',
        search_info=render_template('files/results_number.html',sure=sure,results={"total_found":total_found,"time":0},search=query),
        sources_count=sources_count,
        top_filters=top_filters,
        static_results=static_results,
        static_download=static_download,
        full_browser=full_browser,
        query=query,
        share_url=url_for(".search", query=query.replace(" ","_"), filters=filters2url(dict_filters),_external=True),
        alternate=alternate,
        canonical=canonical if canonical!=request.path else None
    )

@files.route('/<lang>/searcha',methods=['POST'])
@csrf.exempt
def searcha():
    '''
    Responde las peticiones de busqueda por ajax
    '''
    data=request.form.get("filters",None) #puede venir query/filtros:... o solo query
    query,filters=(None,None) if not data else data.split("/",1) if "/" in data else (data,None)

    if not query:
        form_info = request.form
        logging.error("Invalid data for AJAX search request.")
        return jsonify({})

     #para que funcionen la busqueda cuando vienen varias palabras
    query=query.replace("_"," ")

    last_items = None
    try:
        last_items = b64decode(str(request.form["last_items"]) if "last_items" in request.form else "", "-_")
        if last_items:
            last_items = unpack("%dh"%(len(last_items)/2), last_items)
    except BaseException as e:
        logging.error("Error parsing last_items information from request.")

    dict_filters, has_changed = url2filters(filters)
    prepare_args(query, dict_filters)
    return jsonify(search_files(query, dict_filters, min_results=request.args.get("min_results",0), last_items=last_items or []))

from time import time
def search_files(query,filters,min_results=0,max_results=10,download=None,last_items=[],wait_time=500):
    '''
    Realiza una búsqueda de archivos
    '''
    if not last_items and min_results==0:
        min_results=5

    # obtener los resultados
    profiler_data={}
    profiler.checkpoint(profiler_data,opening=["sphinx"])
    s = searchd.search({"type":"text", "text":query}, filters=filters, wait_time=wait_time)
    ids = list(s.get_results(last_items, min_results, max_results))
    profiler.checkpoint(profiler_data,opening=["mongo"], closing=["sphinx"])
    ntts = {int(ntt["_id"]):ntt for ntt in entitiesdb.get_entities(list(s.entities))} if s.entities else {}

    '''
    # trae entidades relacionadas con las relacionadas
    if ntts:
        rel_ids = list(set(eid for ntt in ntts.itervalues() for eids in ntt["r"].itervalues() if "r" in ntt for eid in eids))
        rel_keys = [dict(kv) for kv in set(tuple(key.items()) for ntt in ntts.itervalues() for key in ntt["k"] if "episode" not in key and "season" not in key)]
        ntts.update({int(ntt["_id"]):ntt for ntt in entitiesdb.get_entities(rel_ids, rel_keys, (False, [u"episode"]))}) '''

    stats = s.get_stats()
    result = {"time": max(stats["t"].itervalues()) if stats["t"] else 0, "total_found": stats["cs"]}

    # elimina el id del download de la lista de resultados
    if download and "file_data" in download and download["file_data"]:
        download_id = mid2hex(download["file_data"]["file"]["_id"])
        ids = list(aid for aid in ids if aid[0]!=download_id)
    else:
        download_id = None

    files_dict={str(f["_id"]):secure_fill_data(f,text=query,ntts=ntts) for f in get_files(ids,s)}
    profiler.checkpoint(profiler_data,closing=["mongo"])

    # añade download a los resultados
    if download_id:
        files_dict[download_id] = download["file_data"]
        ids.insert(0,(download_id, -1, -1, -1))

    # ordena resultados y añade informacion de la busqueda
    files = []
    for search_result in ids:
        fid = search_result[0]
        if fid in files_dict and files_dict[fid]:
            afile = files_dict[fid]
            afile["search"] = search_result
            files.append(afile)

    # completa la descripcion de la pagina
    if files:
        names = set(". "+f["view"]["nfn"].capitalize() for f in files)
        page_description = g.page_description + "".join(names)

        # largo minimo
        if len(page_description)<50:
            descriptions = set(seoize_text(u(f["view"]["md"]["description"]), " ",True).capitalize() for f in files if "description" in f["view"]["md"])
            page_description += " " +". ".join(descriptions)

        # largo maximo
        if len(page_description)>250:
            page_description = page_description[:250]
            if " " in page_description:
                page_description = page_description[:page_description.rindex(" ")]

        # punto final
        if page_description[-1]!=".":
            page_description+="."
        g.page_description = page_description

    profiler.checkpoint(profiler_data,opening=["visited"])
    save_visited(files)
    profiler.checkpoint(profiler_data,closing=["visited"])
    profiler.save_data(profiler_data)
    #añadir todos los sources a FILTERS
    filters_with_all_src=deepcopy(FILTERS)
    filters_with_all_src["src"].update(g.sources_names)

    return {
        "files_ids":[f["file"]["id"] for f in files],
        "files":[render_template('files/file.html',file=f) for f in files],
        "result_number":render_template('files/results_number.html',results=result,search=query,sure=stats["s"]),
        "total_found":result["total_found"],
        "page":sum(stats["li"]),
        "last_items":b64encode(pack("%dh"%len(stats["li"]), *stats["li"]), "-_"),
        "sure":stats["s"],
        "wait":stats["w"],
        "canonical_query": stats["ct"]
    }

def download_search(file_data, file_text, fallback):
    '''
    Intenta buscar una cadena a buscar cuando viene download
    '''
    search_texts=[]
    if file_data:
        mds = file_data['view']['md']
        for key in ['artist', 'series', 'album', 'title']:
            if key in mds and isinstance(mds[key], basestring) and len(mds[key])>1:
                search_texts.append((u(mds[key]), False))

        search_texts.append((file_data["view"]["fn"], True))
        if file_data['view']["tags"]:
            fallback = "(%s)"%file_data['view']["tags"][0]
        elif file_data['view']['file_type']:
            fallback = "(%s)"%file_data['view']['file_type']

    if file_text:
        search_texts.append((file_text, True))

    best_candidate = None
    best_points = 10000
    for search_text, is_filename in search_texts:
        phrases = split_phrase(search_text, is_filename)

        for phrase in phrases:
            candidate = [part for part in phrase.split(" ") if part.strip()]

            candidate_points = abs(3-len(candidate))+len(candidate)/10.+sum(0.9 for word in candidate if len(word)<2)
            if candidate_points<best_points:
                best_points = candidate_points
                best_candidate = candidate

        if best_candidate and best_points<2:
            break

    if best_candidate:
        return " ".join(best_candidate[:5])

    return fallback

def download_file(file_id,file_name=None):
    '''
    Devuelve el archivo a descargar, votos, comentarios y archivos relacionados
    '''
    error=(None,"") #guarda el id y el texto de un error
    file_data=None
    if file_id is not None: #si viene un id se comprueba que sea correcto
        try: #intentar convertir el id que viene de la url a uno interno
            file_id=url2mid(file_id)
        except (bson.objectid.InvalidId, TypeError) as e:
            try: #comprueba si se trate de un ID antiguo
                possible_file_id = filesdb.get_newid(file_id)
                if possible_file_id is None:
                    logging.warn("Identificadores numericos antiguos sin resolver: %s."%e, extra={"fileid":file_id})
                    error=(404,"link_not_exist")
                else:
                    logging.warn("Identificadores numericos antiguos encontrados: %s."%e, extra={"fileid":file_id})
                    return {"html": redirect(url_for(".download", file_id=mid2url(possible_file_id), file_name=file_name), 301),"error":(301,"")}

            except filesdb.BogusMongoException as e:
                logging.exception(e)
                error=(503,"")

            file_id=None

        if file_id:
            try:
                file_data=get_file_metadata(file_id, file_name)
            except DatabaseError:
                error=(503,"")
            except FileNotExist:
                error=(404,"link_not_exist")
            except (FileRemoved, FileFoofindRemoved, FileNoSources):
                error=(410,"error_link_removed")
            except FileUnknownBlock:
                error=(404,"")

            if error[0] is None and not file_data: #si no ha habido errores ni hay datos, es porque existe y no se ha podido recuperar
                error=(503,"")

    if file_id is None or error[0] is not None:
        html=""
        if error[0] is not None:  #si hay algun error se devuelve renderizado
            message_msgid="error_%s_message" % error[0]
            message_msgstr=_(message_msgid)
            g.title="%s %s" % (error[0], message_msgstr if message_msgstr!=message_msgid else _("error_500_message"))
            html=render_template('error.html',error=error,full_screen=True)

        return {"html": html,"play":None,"file_data":file_data,"error":error}
    else:
        save_visited([file_data])
        title = u(file_data['view']['fn'])
        g.title = u"%s \"%s\" - %s" % (
            _(file_data['view']['action']).capitalize(),
            title[:100],
            g.title)
        g.page_description = u"%s %s"%(_(file_data['view']['action']).capitalize(), seoize_text(title," ",True))

        #si el usuario esta logueado se comprueba si ha votado el archivo para el idioma activo y si ha marcado el archivo como favorito
        vote=None
        favorite = False
        if current_user.is_authenticated():
            vote=usersdb.get_file_vote(file_id,current_user,g.lang)
            favorite=any(file_id==favorite["id"] for favorite in usersdb.get_fav_files(current_user))

        #formulario para enviar comentarios
        form = CommentForm(request.form)
        if request.method=='POST' and current_user.is_authenticated() and (current_user.type is None or current_user.type==0) and form.validate():
            usersdb.set_file_comment(file_id,current_user,g.lang,form.t.data)
            form.t.data=""
            flash("comment_published_succesfully")
            #actualizar el fichero con la suma de los comentarios por idioma
            filesdb.update_file({"_id":file_id,"cs":usersdb.get_file_comments_sum(file_id),"s":file_data["file"]["s"]},direct_connection=True)

        #si tiene comentarios se guarda el número del comentario, el usuario que lo ha escrito, el comentario en si y los votos que tiene
        comments=[]
        if "cs" in file_data["file"]:
            comments=[(i,usersdb.find_userid(comment["_id"].split("_")[0]),comment,comment_votes(file_id,comment)) for i,comment in enumerate(usersdb.get_file_comments(file_id,g.lang),1)]

        return {
            "html":render_template('files/download.html',file=file_data,vote={"k":0} if vote is None else vote,favorite=favorite,form=form,comments=comments),
            "play":file_data["view"]["play"] if "play" in file_data["view"] else "",
            "file_data":file_data,
        }

@files.route('/<lang>/download/<file_id>',methods=['GET','POST'])
@files.route('/<lang>/download/<file_id>/<path:file_name>',methods=['GET','POST'])
@files.route('/<lang>/download/<file_id>/<path:file_name>.html',methods=['GET','POST']) #siempre la ultima para el url_for
def download(file_id=None,file_name=None):
    '''
    Gestiona las URL de descarga de archivos
    '''
    return search(file_id=file_id,file_name=file_name)

@files.route('/<lang>/downloada',methods=['POST'])
@csrf.exempt
def downloada():
    '''
    Responde las peticiones de download por ajax
    '''
    file_id=request.form.get("id",None)
    file_name=request.form.get("name",None)
    static_download=download_file(file_id,file_name)
    return jsonify(html=static_download["html"],play=static_download["play"])

@files.route("/<lang>/lastfiles")
def last_files():
    '''
    Muestra los ultimos archivos indexados
    '''
    files=[]
    for f in (filesdb.get_last_files(200) or []):
        f=init_data(f)
        choose_filename(f)
        files.append(f)

    return render_template('files/last_files.html',files=files,date=datetime.utcnow())

@files.route("/<lang>/autocomplete")
@cache.cached_GET(
    unless=lambda:True, #TODO(felipe): BORRAR
    params={
        "t": lambda x: x.lower(),
        "term": lambda x: x.lower()
        })
def autocomplete():
    '''
    Devuelve la lista para el autocompletado de la búsqueda
    '''
    query = u(request.args.get("term", "")).lower()

    if not query: return "[]"

    ct = u(request.args.get("t","")).lower()

    tamingWeight = {"c":1, "lang":200}
    if ct:
        for cti in CONTENTS_CATEGORY[ct]:
            tamingWeight[TAMING_TYPES[cti]] = 200

    options = taming.tameText(query, tamingWeight, 5, 3, 0.2)
    if options is None:
        cache.cacheme = False
        return "[]"
    return json.dumps([result[2] for result in options])

@search.test
def test():
    s = u"".join(
        unichr(char)
        for char in xrange(sys.maxunicode + 1) # 0x10ffff + 1
        if unicodedata.category(unichr(char))[0] in ('LMNPSZ'))
    for t in ("","audio","video","image","document","software","archive"):
        for p in xrange(0, 10):
            q = "".join(random.sample(s, random.randint(10,50)))
            r = unit.client.get('/es/search', query_string={"type":t,"q":q})
            assert r.status_code == 200, u"Return code %d while searching %s in %s" % (r.status_code, repr(s), t)

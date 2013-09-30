# -*- coding: utf-8 -*-
"""
    Controladores de las páginas de búsqueda y de fichero.
"""
import urllib, json, unicodedata, random, sys, bson, time
from flask import request, render_template, g, current_app, jsonify, flash, redirect, url_for, abort, Markup
from flask.ext.login import login_required, current_user
from flask.ext.babelex import gettext as _, format_datetime
from datetime import datetime
from timelib import strtotime
from struct import pack, unpack
from collections import OrderedDict
from copy import deepcopy
from base64 import b64encode, b64decode
import newrelic.agent

from foofind.blueprints.files.fill_data import secure_fill_data, get_file_metadata, init_data, choose_filename
from foofind.blueprints.files.helpers import *
from foofind.services import *
from foofind.forms.files import SearchForm, CommentForm
from foofind.utils import url2mid, mid2bin, mid2hex, mid2url, bin2hex, u, canonical_url, logging, is_valid_url_fileid
from foofind.utils.content_types import *
from foofind.utils.splitter import split_phrase
from foofind.utils.pagination import Pagination
from foofind.utils.fooprint import Fooprint
from foofind.utils.seo import seoize_text

files = Fooprint('files', __name__, dup_on_startswith="/<lang>")

share=[
        ("send_by_email","email","mailto:?subject=%(title)s&amp;body=%(url)s"),
        (u"Facebook","facebookcom","http://www.facebook.com/share.php?u=%(url)s"),
        (u"Twitter","twittercom","http://twitter.com/home?status=%(url)s"),
        (u"Google+","plusgooglecom","https://plus.google.com/share?url=%(url)s"),
        (u"Myspace","myspacecom","http://www.myspace.com/Modules/PostTo/Page/?u=%(url)s"),
        (u"Delicious","deliciouscom","http://delicious.com/post?url=%(url)s"),
        (u"Digg","diggcom","http://digg.com/submit?phase2&amp;url=%(url)s&amp;title=%(title)s"),
        (u"Evernote","evernotecom","http://www.evernote.com/clip.action?url=%(url)s"),
        (u"Friendfeed","friendfeedcom","http://www.friendfeed.com/share?title=%(title)s&amp;url=%(url)s"),
        (u"Google","googlecom","http://www.google.com/bookmarks/mark?op=edit&amp;btmk=%(url)s"),
        (u"Newsvine","newsvinecom","http://www.newsvine.com/_tools/seed&amp;save?u=%(url)s"),
        (u"Reddit","redditcom","http://www.reddit.com/submit?url=%(url)s"),
        (u"Stumbleupon","stumbleuponcom","http://www.stumbleupon.com/submit?url=%(url)s"),
        (u"Technorati","technoraticom","http://technorati.com/faves?add=%(url)s"),
        (u"Menéame","meneamenet","http://meneame.net/submit.php?url=%(url)s&amp;title=%(title)s"),
        (u"Tuenti","tuenticom","http://www.tuenti.com/share?url=%(url)s"),
        (u"Pinterest","pinterestcom","http://pinterest.com/pin/create/button/?url=%(url)s"),
        (u"Netvibes","netvibescom","http://www.netvibes.com/share?title=%(title)s&amp;url=%(url)s"),
        (u"Linkedin","linkedincom","http://www.linkedin.com/shareArticle?mini=true&amp;url=%(url)s&amp;title=%(title)s"),
        (u"Posterous","posterouscom","http://posterous.com/share?linkto=%(url)s"),
        (u"Yahoo","yahoocom","http://bookmarks.yahoo.com/toolbar/savebm?u=%(url)s&amp;t=%(title)s"),
    ]

@files.context_processor
def file_var():
    if request.args.get("error",None)=="error":
        abort(404)

    return {"zone":"files","search_form":SearchForm(request.args),"share":share,"args":g.args,"active_types":g.active_types, "active_srcs":g.active_srcs}


@files.route('/<lang>/search_info/<query>')
@files.route('/<lang>/search_info/<query>/<path:filters>/')
def search_info(query, filters=None):
    query = query.replace("_"," ")
    dict_filters, has_changed = url2filters(filters)

    return jsonify(searchd.get_search_info(query, filters=filters))

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
    search_bot = g.search_bot
    full_browser = g.full_browser

    # si tiene download, cuenta como pagina de download
    if file_id:
        newrelic.agent.set_transaction_name("foofind.blueprints.files:download" + ("" if full_browser else "_static"))
    else:
        newrelic.agent.set_transaction_name("foofind.blueprints.files:search" + ("" if full_browser else "_static"))

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

    # obtiene parametros de busqueda de la url
    prepare_args(query, dict_filters)

    static_download=download_file(file_id,file_name if file_name is not None else query) #obtener el download si se puede
    if "error" in static_download and static_download["error"][0]==301: #si es una redireccion de un id antiguo directamente se devuelve
        return static_download["html"]

    #filtros encima de resultados de busqueda
    top_filters={"type":[], "src":[], "size":[]}
    available_space = 4
    if "type" in dict_filters:
        top_filters["type"] = types = [_(value) for value in dict_filters["type"]]
        available_space -= len(types)
    if "size" in dict_filters:
        values = dict_filters["size"]
        top_filters["size"]=sizes=[int(values[0]),int(values[1]),FILTERS["size"][1],FILTERS["size"][2]]
        available_space -= (1 if sizes[0]>0 else 0) + (1 if sizes[1]<50 else 0)

    if "src" in dict_filters:
        values = dict_filters["src"]
        sources_names = g.sources_names
        filters = []

        p2p_filters = []
        if "p2p" in values:
            p2p_filters = ["P2P"]
        else:
            p2p_filters = [sources_names[src] for src in g.sources_p2p if src in values]
        available_space -= len(p2p_filters)

        if "streaming" in values:
            filters.append("Streaming")
            available_space -= 1
        else:
            streaming = [sources_names[src] for src in g.visible_sources_streaming if src in values]
            streaming_len = len(streaming)
            if streaming_len>max(2,available_space):
                filters.append(_("some_streamings"))
                available_space -= 1
            elif streaming:
                available_space -= streaming_len
                filters.extend(streaming)
        if "download" in values:
            filters.append(_("direct_downloads"))
        else:
            downloads = [sources_names[src] for src in g.visible_sources_download if src in values]
            downloads_len = len(downloads)
            if downloads_len>max(2,available_space):
                filters.append(_("some_downloads"))
                available_space -= 1
            elif downloads:
                available_space -= downloads_len
                filters.extend(downloads)

        if p2p_filters:
            filters.extend(p2p_filters)

        top_filters["src"] = filters

    if query is None: # si no ha recibido query de la URL la coge de la que haya obtenido del download
        query = g.args.get("q", None)
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
        wait_time = 400 if dict_filters else 800 if file_id is None else 400
        async_time = (wait_time*2 if dict_filters else wait_time) + 200
        results=search_files(query, dict_filters, 50, 50, static_download, query_time=wait_time, extra_wait_time=200, async=async_time, max_extra_searches=1 if search_bot else 4)
        # cambia la busqueda canonica
        canonical_query = results["canonical_query"]
        if search_bot:
            # si la busqueda devuelve resultados pero mongo no los da, tambien cuenta como "not results".
            searchd.log_bot_event(search_bot, (results["total_found"]>0 or results["sure"]) and not (len(results["files"])==0 and results["total_found"]>0 ))

        static_results=results["files"], False
        total_found=results["total_found"]
        sure = True
    else:
        # empieza la busqueda en la primera peticion
        searchd.search(query, filters=dict_filters, start=True, group=True, no_group=False)

        if query is not None and static_download["file_data"] is not None: #si es busqueda con download se pone el primero el id que venga
            static_results=[static_download["file_data"]], True
        else:
            static_results=[], False

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
        search_info=render_template('files/results_number.html',sure=sure,total_found=total_found,search=query),
        sources_count=sources_count,
        top_filters=top_filters,
        files = static_results[0],
        scroll_start = static_results[1],
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

    search_results = search_files(query, dict_filters, min_results=request.args.get("min_results",0), last_items=last_items or [], extra_wait_time=300)
    search_results["files"] = render_template('files/file.html',files=search_results["files"])
    return jsonify(search_results)

def search_files(query,filters,min_results=0,max_results=30,download=None,last_items=[],query_time=None,extra_wait_time=500, async=False, max_extra_searches=4, non_group=False, order=None, weight_processor=None, tree_visitor=None):
    '''
    Realiza una búsqueda de archivos
    '''
    if not last_items and min_results==0:
        min_results=5

    # obtener los resultados
    profiler_data={}
    profiler.checkpoint(profiler_data,opening=["sphinx"])

    s = searchd.search(query, filters=filters, start=not bool(last_items), group=True, no_group=False, order=order)
    ids = [(bin2hex(fileid), server, sphinxid, weight, sg) for (fileid, server, sphinxid, weight, sg) in s.get_results((1.4, 0.1), last_items=last_items, min_results=min_results, max_results=max_results, extra_browse=0 if max_results>30 else None, weight_processor=weight_processor, tree_visitor=tree_visitor)]

    stats = s.get_stats()

    profiler.checkpoint(profiler_data,opening=["entities"], closing=["sphinx"])

    results_entities = list(set(int(aid[4])>>32 for aid in ids if int(aid[4])>>32))
    ntts = {int(ntt["_id"]):ntt for ntt in entitiesdb.get_entities(results_entities)} if results_entities else {}
    profiler.checkpoint(profiler_data, closing=["entities"])
    '''# trae entidades relacionadas
    if ntts:
        rel_ids = list(set(eid for ntt in ntts.itervalues() for eids in ntt["r"].itervalues() if "r" in ntt for eid in eids))
        ntts.update({int(ntt["_id"]):ntt for ntt in entitiesdb.get_entities(rel_ids, None, (False, [u"episode"]))})
    '''

    result = {"time": max(stats["t"].itervalues()) if stats["t"] else 0, "total_found": stats["cs"]}

    # elimina el id del download de la lista de resultados
    if download and "file_data" in download and download["file_data"]:
        download_id = mid2hex(download["file_data"]["file"]["_id"])
        ids = list(aid for aid in ids if aid[0]!=download_id)
    else:
        download_id = None

    profiler.checkpoint(profiler_data, opening=["mongo"])
    files_dict={str(f["_id"]):secure_fill_data(f,text=query,ntts=ntts) for f in get_files(ids,s)}
    profiler.checkpoint(profiler_data, closing=["mongo"])

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

    if len(ids)>=result["total_found"]:
        total_found = len(files)
    else:
        total_found = max(result["total_found"], len(files))

    # completa la descripcion de la pagina
    if files:
        # Descripcion inicial
        page_description = g.page_description + ". "

        # Descripcion de alguno de los primeros ficheros
        for f in files[:3]:
            if "description" in f["view"]["md"]:
                phrase = u(f["view"]["md"]["description"]).capitalize()
                page_description += phrase + " " if phrase[-1]=="." else ". "
                break

        # Busca frases para completar la descripcion hasta un minimo de 100 caracteres
        page_description_len = len(page_description)
        if page_description_len<100:
            phrases = []
            for f in files[1:]: # se salta el primer fichero, que podría ser el download actual
                phrase = f["view"]["nfn"].capitalize()

                if phrase not in phrases:
                    phrases.append(phrase)
                    page_description_len += len(phrase)

                if page_description_len>=100:
                    break
            page_description += ". ".join(phrases)

        # largo maximo
        if len(page_description)>230:
            page_description = page_description[:230]
            if " " in page_description:
                page_description = page_description[:page_description.rindex(" ")] + "..."

        # punto final
        if page_description[-1]!=".":
            page_description+="."
        g.page_description = page_description

    profiler.checkpoint(profiler_data,opening=["visited"])
    save_visited(files)
    profiler.checkpoint(profiler_data,closing=["visited"])

    profiler.save_data(profiler_data)

    return {
        "files_ids":[f["file"]["id"] for f in files],
        "files":files,
        "result_number":render_template('files/results_number.html',results=result,search=query,sure=stats["s"], total_found=total_found),
        "total_found":total_found,
        "page":sum(stats["li"]),
        "last_items":b64encode(pack("%dh"%len(stats["li"]), *stats["li"]), "-_"),
        "sure":stats["s"],
        "wait":stats["w"],
        "canonical_query": stats["ct"],
        "end": stats["end"]
    }

def download_search(file_data, file_text, fallback):
    '''
    Intenta buscar una cadena a buscar cuando viene download
    '''
    search_texts=[]
    if file_data:
        mds = file_data['file']['md']
        for key in ['audio:artist', 'audio:album', 'video:series', 'video:title', 'image:title', 'audio:title','application:name', 'application:title', 'book:title', 'torrent:title']:
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
    for main_position, (search_text, is_filename) in enumerate(search_texts):
        phrases = split_phrase(search_text, is_filename)

        for inner_position, phrase in enumerate(phrases):
            candidate = [part for part in phrase.split(" ") if part.strip()]

            count = sum(1 for word in candidate if len(word)>1)
            is_numeric = len(candidate)==1 and candidate[0].isdecimal()
            candidate_points = main_position+inner_position+(50 if count==0 else 5 if count==1 and is_numeric else 20 if count>15 else 0)

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
        if is_valid_url_fileid(file_id):
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

                except BaseException as e:
                    logging.exception(e)
                    error=(503,"")

                file_id=None
        else:
            abort(404)

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

        # en la pagina de download se intentan obtener palabras para buscar si no las hay
        if g.args.get("q", None) is None:
            query = download_search(file_data, file_name, "foofind")
            if query:
                g.args["q"] = query.replace(":","")

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
    prepare_args()

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

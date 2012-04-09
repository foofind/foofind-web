# -*- coding: utf-8 -*-
"""
    Controladores de las páginas de búsqueda y de fichero.
"""

from flask import Blueprint, request, render_template, g, current_app, jsonify, flash, redirect, url_for, abort
from flaskext.login import login_required, current_user
from flaskext.babel import gettext as _
from foofind.services import *
from foofind.services.search import search_files, search_related, get_ids, block_files
from foofind.forms.files import SearchForm, CommentForm
from foofind.utils import mid2url, url2mid, hex2mid, mid2hex, mid2bin, bin2hex, to_seconds, multipartition, u
from foofind.utils.splitter import split_file
from foofind.utils.taming import TamingClient
from foofind.utils.pagination import Pagination
from foofind.utils.async import async_generator

from urlparse import urlparse
from datetime import datetime
import urllib, logging, json, re, logging, unicodedata

files = Blueprint('files', __name__)

@files.context_processor
def file_var():
    return {"zone":"files","search_form":g.search_form,"args":request.args}

@files.before_request
def set_search_form():
    g.search_form = SearchForm(request.args)

def get_files(ids):
    '''
    Recibe lista de tuplas de tamaño 3 (como devueltas por get_ids de search)
    y devuelve los ficheros del mongo correspondiente que no estén bloqueados.
    Si se recibe un fichero bloqueado, lo omite y bloquea en el sphinx.

    @type ids: iterable de tuplas de tamaño 3 o mayor
    @param ids: lista de tuplas (mongoid, id servidor, id sphinx)

    @yield: cada uno de los resultados de los ids en los mongos

    '''
    already = False
    files = filesdb.get_files(ids, servers_known = True, bl = None)
    for r in files:
        if r["bl"] == 0 or r["bl"] is None:
            yield r
        elif not already:
            already = True
            id_list = {i[0]:i[2] for i in ids}
            block_files( sphinx_ids=[id_list[mid2bin(i["_id"])] for i in files if i["bl"] != 0 and not i["bl"] is None ])

def init_data(file_data):
    '''
    Inicializa el diccionario de datos del archivo
    '''
    file_id=mid2hex(file_data["_id"])
    file_data["id"]=mid2url(file_data['_id'])
    file_data['name']=file_data['src'][file_id]['url']
    return {"file":file_data,"view":{}}

def choose_filename(f,text=False):
    '''
    Elige el archivo correcto
    '''
    srcs = f['file']['src']
    fns = f['file']['fn']
    hist= ""
    chosen=""
    maxCount = 0
    hasText = 0
    for hexuri,src in srcs.items():
        if 'bl' in src and src['bl']!=0:
            continue

        srcfns = src['fn']
        for crc,srcfn in srcfns.items():
            fnn = fns[crc]['n']
            if len(fnn.strip())==0:
                continue

            thisHasText=0
            if 'c' in fns[crc]:
                fns[crc]['c']+=srcfn['m']
            else:
                fns[crc]['c']=srcfn['m']

            if text:
                if fnn.upper().find(text.upper())!=-1:
                    thisHasText = 2000
                else:
                    matches = 0
                    for word in [re.escape(w) for w in text.split(" ")]:
                        matches += len(re.findall(r"/((?:\b|_)%s(?:\b|_))/i"%word, fnn))

                    if matches>0:
                        thisHasText = 1000 + matches

            f['file']['fn'][crc]['tht'] = thisHasText
            better = fns[crc]['c']>maxCount
            if thisHasText > hasText or (better and thisHasText==hasText):
                hasText = thisHasText
                chosen = crc
                maxCount = fns[crc]['c']

    f['view']['url'] = mid2url(hex2mid(f['file']['_id']))
    if chosen!="":
        filename = fns[chosen]['n']
        ext = fns[chosen]['x']
    else:
        #uses filename from src
        srcurl = ""
        for crc,srcfn in srcfns.items():
            if src['url'].find("/")!=-1:
                srcurl = src['url']

        if srcurl=="":
            return

        srcurl = srcurl[srcurl.rfind("/")+1:]
        ext = srcurl[srcurl.rfind(".")+1:]
        filename = srcurl[0:srcurl.rfind(".")]

    if not ext in current_app.config["EXTENSIONS"].keys():
        filename += ext
        ext=""
        nfilename = filename
    else:
        #clean filename
        end = filename.upper().rfind("."+ext.upper())
        if end == -1:
            nfilename = filename
        else:
            nfilename = filename.strip()[0:end]

    f['view']['fn'] = filename
    f['view']['efn'] = filename.replace(" ", "%20")
    f['view']['fnx'] = ext

    #nombre del archivo con las palabras que coinciden con la busqueda resaltadas
    if not text:
        f['view']['fnh']=filename
    else:
        separators = u"".join({i for i in filename if not unicodedata.category(i)[0] in ("N","L")})
        separated_text = tuple(multipartition(text.lower(), separators))
        f['view']['fnh'] = u"".join(
            (u"<strong>%s</strong>" % filename_part)
            if filename_part.lower() in separated_text and not filename_part in separators
            else filename_part
            for filename_part in multipartition(filename, separators)
            )

    if nfilename.find(" ")==-1:
        nfilename = nfilename.replace(".", " ")

    nfilename = nfilename.replace("_", " ")
    f['view']['nfn'] = nfilename

def build_source_links(f, prevsrc=False):
    '''
    Construye los enlaces correctamente
    '''
    def get_domain(src):
        '''
        Devuelve el dominio de una URL
        '''
        url_parts=urlparse(src).netloc.split('.')
        i=len(url_parts)-1
        if len(url_parts[i])<=2 and len(url_parts[i-1])<=3:
            return url_parts[i-2]+'.'+url_parts[i-1]+'.'+url_parts[i]
        else:
            return url_parts[i-1]+'.'+url_parts[i];

    if not "fn" in f['view']:
        choose_filename(f)

    f['view']['action']='download'
    f['view']['sources']={}
    srcs=f['file']['src']
    max_weight=0
    for hexuri,src in srcs.items():
        if not src.get('bl',None) in (0, None):
            continue

        join=False
        count=0
        part=url=""
        source_data=filesdb.get_source_by_id(src["t"])
        #si es descarga directa
        if "w" in source_data["g"] or "f" in source_data["g"] or "s" in source_data["g"]:
            link_weight=1
            tip=source_data["d"]
            icon="web"
            url=src['url']
            if "f" in source_data["g"]:
                source=get_domain(src['url'])
            else:
                source=source_data["d"]
            #en caso de duda se prefiere streaming
            if "s" in source_data["g"]:
                f['view']['action']='watch'
                link_weight*=2

        elif source_data["d"]=="Gnutella":
            link_weight=0.2
            tip="Gnutella"
            source=icon="gnutella"
            part="xt:urn:sha1:"+src['url']
            join=True
            count=int(src['m'])
        elif source_data["d"]=="ed2k":
            link_weight=0.1
            tip="ED2K"
            source=icon="ed2k"
            url="ed2k://|file|"+f['view']['efn']+"|"+str(f['file']['z'] if "z" in f["file"] else 1)+"|"+src['url']+"|/"
            count=int(src['m'])
        elif source_data["d"]=="BitTorrent":
            link_weight=0.8
            icon="torrent"
            url=src['url']
            tip=source=get_domain(src['url'])
        elif source_data["d"]=="Tiger":
            link_weight=0
            tip="Gnutella"
            source=icon="gnutella"
            part="xt:urn:tiger="+src['url']
        elif source_data["d"]=="MD5":
            link_weight=0
            tip="Gnutella"
            source=icon="gnutella"
            part="xt:urn:md5="+src['url']
        elif source_data["d"]=="BitTorrentHash":
            link_weight=0.7 if 'torrent:tracker' in f['file']['md'] or 'torrent:trackers' in f['file']['md'] else 0.1
            tip="Torrent MagnetLink"
            source=icon="tmagnet"
            join=True
            count=int(src['m'])
            part="xt:urn:btih="+src['url']
            if 'torrent:tracker' in f['file']['md']:
                part += '&tr=' + urllib.quote_plus(f['file']['md']['torrent:tracker'])
            elif 'torrent:trackers' in f['file']['md']:
                trackers = f['file']['md']['torrent:trackers']
                if isinstance(trackers, basestring):
                    part += '&tr='.join(urllib.quote_plus(tr) for tr in trackers.split(" "))

        else:
            continue

        if not source in f['view']['sources']:
            f['view']['sources'][source]={}

        f['view']['sources'][source]['tip']=tip
        f['view']['sources'][source]['icon']=icon
        f['view']['sources'][source]['join']=join
        f['view']['sources'][source]['type']=source_data["g"]
        #para no machacar el numero si hay varios archivos del mismo source
        if not 'count' in f['view']['sources'][source] or count>0:
            f['view']['sources'][source]['count']=count

        if not "parts" in f['view']['sources'][source]:
            f['view']['sources'][source]['parts']=[]

        if not 'urls' in f['view']['sources'][source]:
            f['view']['sources'][source]['urls']=[]

        if part!="":
            f['view']['sources'][source]['parts'].append(part)

        if url!="":
            f['view']['sources'][source]['urls'].append(url)
            if source_data["d"]!="ed2k":
                f['view']['sources'][source]['count']+=1

        if link_weight>max_weight:
            max_weight = link_weight
            f['view']['source'] = source

    if icon!="web":
        for src,info in f['view']['sources'].items():
            if 'join' in info:
                size=""
                if 'z' in f['file']:
                    size = "&xl="+str(f['file']['z'])

                f['view']['sources'][src]['urls'].append("magnet:?dn="+f['view']['efn']+size+"&"+"&".join(info['parts']))
            elif not 'urls' in info:
                del(f['view']['sources'][src])

def choose_file_type(f, file_type=None):
    '''
    Elige el tipo de archivo
    '''
    if file_type is None:
        if "ct" in f["file"]:
            file_type = f["file"]["ct"]
        if file_type is None and "fnx" in f["view"] and f['view']['fnx'] in current_app.config["EXTENSIONS"]:
            file_type = current_app.config["EXTENSIONS"][f['view']['fnx']]
        if file_type is not None:
            file_type = current_app.config["CONTENTS"][file_type]

    if file_type is not None:
        f['view']['file_type'] = file_type.lower();

def get_images(f):
    '''
    Obtiene las imagenes para los archivos que las tienen
    '''
    if "i" in f["file"] and isinstance(f["file"]["i"],list):
        f["view"]["images_server"]=[]
        for image in f["file"]["i"]:
            server=filesdb.get_image_server(image)
            f["view"]["images_server"].append(str(int(server["_id"])))
            if not "first_image_server" in f["view"]:
                f["view"]["first_image_server"]=int(server["_id"])

        f["view"]["images_server"]="_".join(f["view"]["images_server"])

def format_metadata(f,details):
    '''
    Formatea los metadatos de los archivos
    '''
    def searchable(value,details):
        '''
        Añadie un enlace a la busqueda si es necesario
        '''
        if details:
            return "<a href='%s'>%s</a>" % (url_for(".search",q=value), value)
        else:
            return value

    view_md = f['view']['md'] = {}
    file_type = f['view']['file_type'] if 'file_type' in f['view'] else None
    if 'md' in f['file']:
        #si viene con el formato tipo:metadato se le quita el tipo
        file_md = {(meta.split(":")[-1] if ":" in meta else meta): value
            for meta, value in f['file']['md'].iteritems()}

        # Duración para vídeo e imágenes
        put_duration = False
        duration = [0, 0, 0] # h, m, s
        try:
            if "seconds" in file_md:
                put_duration = True
                duration[-1] = float(file_md["seconds"])
            if "minutes" in file_md:
                put_duration = True
                duration[-2] = float(file_md["minutes"])
            if "hours" in file_md:
                put_duration = True
                duration[-3] = float(file_md["hours"])
        except BaseException as e:
            logging.exception(e)
        if "duration" in file_md and not put_duration:
            # Si recibo duration y no la he recibido de otra forma
            try:
                duration[-1] = to_seconds(file_md["duration"])
                put_duration = True
            except BaseException as e:
                logging.exception(e)
        if put_duration:
            carry = 0
            for i in xrange(len(duration)-1,-1,-1):
                unit = duration[i] + carry
                duration[i] = unit%60
                carry = unit/60
            view_md["length"] = ":".join("%02d" % i for i in (duration if duration[-3] > 0 else duration[-2:]))

        # Tamaño para vídeos e imágenes
        if "width" in file_md and 'height' in file_md:
            try:
                width = (
                    int(file_md["width"].replace("pixels","").replace("px",""))
                    if isinstance(file_md["width"], basestring)
                    else int(file_md["width"]))
                height = (
                    int(file_md["height"].replace("pixels","").replace("px",""))
                    if isinstance(file_md["width"], basestring)
                    else int(file_md["height"]))
                view_md["size"] = "%dx%dpx" % (width, height)
            except BaseException as e:
                logging.exception(e)

        # Metadatos que no cambian
        try:
            view_md.update((meta, file_md[meta]) for meta in (
                ("folders","description","fileversion","os","files","pages","format")
                if details else ("files","pages","format")) if meta in file_md)
        except BaseException as e:
            logging.exception(e)

        # Metadatos multimedia
        try:
            if file_type in ("audio", "video", "image"):
                view_md.update((meta, file_md[meta]) for meta in
                    ("genre", "length", "track", "artist", "title", "author", "colors")
                    if meta in file_md)
        except BaseException as e:
            logging.exception(e)

        # Los que cambian o son especificos de un tipo
        try:
            if file_type == 'audio': #album, year, bitrate, seconds, track, genre, length
                if 'album' in file_md:
                    year = int(file_md["year"]) if "year" in file_md and str(file_md["year"]).isdigit() else 0
                    album = searchable(file_md["album"], details)
                    view_md["album"] = ("%s (%d)" % (album, year)) if 1900 <  year < 2100 else album
                if 'bitrate' in file_md:
                    bitrate = "%s kbps" % str(file_md["bitrate"]).replace("~","")
                    view_md["quality"] = ( # bitrate o bitrate - soundtype
                        ("%s - %s" % (bitrate, file_md["soundtype"]))
                        if details and "soundtype" in file_md else bitrate)
            elif file_type == 'archive': #title, name, unpackedsize, folders, files
                if "title" in file_md or "name" in file_md:
                    view_md["title"] = searchable(file_md["title" if "title" in file_md else "name"], details)
                if "unpackedsize" in file_md:
                    view_md["unpackedsize"] = file_md["unpackedsize"]
            elif file_type == 'document': #title, author, pages, format, version
                if details:
                    if "format" in file_md:
                        view_md["format"] = "%s%s" % (file_md["format"],
                            " %s" % file_md["formatversion"]
                            if "formatversion" in file_md else "")
                    version = []
                    if "version" in file_md: version.append(float(file_md["version"]))
                    if "revision" in file_md: version.append(float(file_md["revision"]))
                    if version: view_md["revision"] = " ".join(version)
            elif file_type == 'image': #title, artist, description, width, height, colors
                pass
            elif file_type == 'software': #title, version, fileversion, os
                if "title" in file_md:
                     view_md["title"] = "%s%s" % (
                        searchable(file_md["title"], details),
                        " %s" % file_md["version"] if "version" in file_md else "")
            elif file_type == 'video':
                if details:
                    quality = []
                    if 'framerate' in file_md: quality.append("%d fps" % float(file_md["framerate"]))
                    if 'codec' in file_md: quality.append(str(file_md["codec"]))
                    if quality: view_md["quality"] = " - ".join(quality)
        except BaseException as e:
            logging.exception(e)

def fill_data(file_data,details=False,text=False):
    '''
    Añade los datos necesarios para mostrar los archivos
    '''
    f=init_data(file_data)
    choose_filename(f,text)
    build_source_links(f)
    choose_file_type(f)
    get_images(f)
    format_metadata(f,details)
    return f

ROBOT_USER_AGENTS=("aol","ask","google","msn","yahoo")
def save_visited(files):
    '''
    Recibe una lista de resultados de fill_data y guarda las url que sean de web
    '''
    if request.user_agent.browser is not None and request.user_agent.browser not in ROBOT_USER_AGENTS:
        result=[]
        for file in files:
            result+=([{"_id": data['urls'][0]} for data in file['view']['sources'].itervalues() if data['icon'] == 'web'])

        if result!=[]:
            feedbackdb.visited_links(result)

@async_generator(1000)
def taming_search(config, query, ct, contextg):
    '''
    Obtiene los resultados que se muestran en el taming
    '''
    def generate_tags(res):
        '''
        Genera los tags
        '''
        tag = res[2][querylen+1:]
        return (tag[querylen+1:] if tag.startswith(query+" ") else tag, 100*max(min(1.25, res[0]/mean), 0.75))

    profiler.checkpoint(opening=["taming_tags"], contextg=contextg)
    tamingWeight = {"c":1, "lang":200}
    if ct in config["CONTENTS_CATEGORY"]:
        for cti in config["CONTENTS_CATEGORY"][ct]:
            tamingWeight[config["TAMING_TYPES"][cti]] = 200

    tc = TamingClient(config["SERVICE_TAMING_SERVERS"], config["SERVICE_TAMING_TIMEOUT"])
    if not tc.open_connection():
        return

    #tags relacionados
    querylen = len(query)
    try:
        tags = tc.tameText(query+" ", tamingWeight, 20, 4, 0.7, 0)
        if tags:
            mean = (tags[0][0] + tags[-1][0])/2
            tags = map(generate_tags, tags)
            tags.sort()
        else:
            tags = ()
    except Exception as e:
        logging.exception("Error getting search related tags.")
        tags = ()

    yield tags

    profiler.checkpoint(opening=["taming_dym"], closing=["taming_tags"], contextg=contextg)

    #quiso decir
    try:
        suggest = tc.tameText(query, tamingWeight, 1, 3, 0.8, 1, 0)
        didyoumean = None
        if suggest and suggest[0][2]!=query:
            didyoumean = suggest[0][2]
    except Exception as e:
        logging.exception("Error getting did you mean suggestion.")

    try:
        tc.close_connection()
    except:
        pass

    yield didyoumean
    profiler.checkpoint(closing=["taming_dym"], contextg=contextg)

@files.route('/<lang>/search')
def search():
    '''
    Realiza una búsqueda de archivo
    '''
    # TODO: seguridad en param
    #si no se ha buscado nada se manda al inicio
    query = request.args.get("q", None)
    if not query:
        flash("write_something")
        return redirect(url_for("index.home"))

    page = int(request.args.get("page", 1))
    g.title = query+" - "+g.title
    results = {"total_found":0,"total":0,"time":0}

    didyoumean = None
    tags = None
    if 0 < page < 101:
        #obtener los tags y el quiso decir
        taming = taming_search(current_app.config, query, request.args.get("type", None), contextg=g._get_current_object())

        #obtener los resultados y sacar la paginación
        profiler.checkpoint(opening=["sphinx"])
        results = search_files(query,request.args,page) or results
        ids = get_ids(results)
        profiler.checkpoint(opening=["mongo"], closing=["sphinx"])
        files_dict = {mid2hex(file_data["_id"]):fill_data(file_data, False, query) for file_data in get_files(ids)}
        profiler.checkpoint(opening=["visited"], closing=["mongo"])
        save_visited(files_dict.values())
        profiler.checkpoint(closing=["visited"])
        files=(files_dict[bin2hex(file_id[0])] for file_id in ids if bin2hex(file_id[0]) in files_dict)

        # recupera los resultados del taming
        try:
            tags = taming.next()
            didyoumean = taming.next()
        except:
            pass
    else:
        files = ()

    return render_template('files/search.html',
        results=results,
        search=request.args["q"].split(" "),
        files=files,
        pagination=Pagination(page, 10, min(results["total_found"], 1000)),
        didyoumean=didyoumean,
        tags=tags)

@files.route('/<lang>/download/<file_id>',methods=['GET','POST'])
@files.route('/<lang>/download/<file_id>/<path:file_name>.html',methods=['GET','POST'])
@files.route('/<lang>/download/<file_id>/<path:file_name>',methods=['GET','POST'])
def download(file_id,file_name=None):
    '''
    Muestra el archivo a descargar, votos, comentarios y archivos relacionados
    '''
    def choose_filename_related(file_data):
        '''
        Devuelve el nombre de fichero elegido
        '''
        f=init_data(file_data)
        choose_filename(f)
        return f

    def comment_votes(file_id,comment):
        '''
        Obtiene los votos de comentarios
        '''
        comment_votes={}
        if "vs" in comment:
            for i,comment_vote in enumerate(usersdb.get_file_comment_votes(file_id)):
                if not comment_vote["_id"] in comment_votes:
                    comment_votes[comment_vote["_id"][0:40]]=[0,0,0]

                if comment_vote["k"]>0:
                    comment_votes[comment_vote["_id"][0:40]][0]+=1
                else:
                    comment_votes[comment_vote["_id"][0:40]][1]+=1

                #si el usuario esta logueado y ha votado se guarda para mostrarlo activo
                if current_user.is_authenticated() and comment_vote["u"]==current_user.id:
                    comment_votes[comment_vote["_id"][0:40]][2]=comment_vote["k"]

        return comment_votes

    if file_name is not None:
        g.title = _("download").capitalize()+" "+file_name+" - "+g.title
    else:
        g.title =_("download").capitalize()

    #guardar los parametros desde donde se hizo la busqueda si procede
    args={}
    if request.referrer:
        querystring = urlparse(request.referrer).query
        if querystring:
            for params in querystring.split("&"):
                param=params.split("=")
                if len(param) == 2:
                    args[param[0]]=u(urllib.unquote_plus(param[1]))

    try:
        file_id=url2mid(file_id)
    except BaseException as e:
        logging.warn((e, file_id))
        abort(404)

    data = filesdb.get_file(file_id, bl = None)

    if data:
        if not data["bl"] in (0, None):
            if data["bl"] == 1: flash("link_not_exist", "error")
            elif data["bl"] == 3: flash("error_link_removed", "error")
            goback = True
            #block_files( mongo_ids=(data["_id"],) )
            abort(404)
    else:
        flash("link_not_exist", "error")
        abort(404)

    #obtener los datos
    file_data=fill_data(data, True, file_name)
    save_visited([file_data])

    #obtener los archivos relacionados
    related_files = search_related(split_file(file_data["file"])[0][0:50])
    bin_file_id=mid2bin(file_id)
    ids=sorted({fid for related in related_files for fid in get_ids(related) if fid[0]!=bin_file_id})[:5]
    files_related=[choose_filename_related(data) for data in get_files(ids)]

    #si el usuario esta logueado se comprueba si ha votado el archivo para el idioma activo
    vote=None
    if current_user.is_authenticated():
        vote=usersdb.get_file_vote(file_id,current_user,g.lang)

    if vote is None:
        vote={"k":0}

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
        comments=[(i,usersdb.find_userid(comment["_id"][0:24]),comment,comment_votes(file_id,comment)) for i,comment in enumerate(usersdb.get_file_comments(file_id,g.lang),1)]

    return render_template('files/download.html',file=file_data,args=args,vote=vote,files_related=files_related,comments=comments,form=form)

@files.route('/<lang>/vote/<t>/<int:server>/<file_id>/<int:vote>/<int:login>')
@files.route('/<lang>/vote/<t>/<int:server>/<file_id>/<comment_id>/<int:vote>/<int:login>')
@login_required
def vote(t,server,file_id,comment_id=None,vote=0,login=0):
    '''
    Gestiona las votaciones de archivos y comentarios
    '''
    g.title += " - Vote "
    #voto no permitido a administradores ni con valores extraños
    json={}
    if (current_user.type is None or current_user.type==0) and (vote==1 or vote==2):
        #si es el voto para archivos se guarda y actualiza el archivo
        if t=="file":
            json=usersdb.set_file_vote(url2mid(file_id),current_user,g.lang,vote)
            filesdb.update_file({"_id":url2mid(file_id),"vs":json,"s":server},direct_connection=True)
            json=json[g.lang]
        #si es para comentarios idem
        elif t=="comment":
            json=usersdb.set_file_comment_vote(comment_id,current_user,url2mid(file_id),vote)["1"]

    if login==0:
        return redirect(url_for(".download",file_id=file_id))
    else:
        return jsonify(json)

@files.route("/<lang>/lastfiles")
def last_files():
    '''
    Muestra los ultimos archivos indexados
    '''
    files=[]
    for f in filesdb.get_last_files():
        f=init_data(f)
        choose_filename(f)
        files.append(f)

    return render_template('files/last_files.html',files=files,date=datetime.utcnow())

@files.route("/<lang>/search/autocomplete")
# TODO(felipe): Descomentar esto, y cambiar la asignación a make_cache_key
#               cuando se hayan solucionado los bugs de flask-cache:
#                   https://github.com/thadeusb/flask-cache/pull/18
#                   https://github.com/thadeusb/flask-cache/pull/17
#
# @cache.cached(
#    timeout = 1800,
#    key_prefix = lambda: "view/%%s/%s:%s" % (
#        request.args.get("term",""),
#        request.args.get("ct","").lower())
#    )
@cache.cached()
def autocomplete():
    '''
    Devuelve la lista para el autocompletado de la búsqueda
    '''
    query = request.args["term"]
    ct = request.args.get("t",None)
    if ct: ct = ct.lower()

    tc = TamingClient(current_app.config["SERVICE_TAMING_SERVERS"], current_app.config["SERVICE_TAMING_TIMEOUT"])

    tamingWeight = {"c":1, "lang":200}
    if ct:
        for cti in current_app.config["CONTENTS_CATEGORY"][ct]:
            tamingWeight[current_app.config["TAMING_TYPES"][cti]] = 200

    options = tc.tameText(query, tamingWeight, 5, 3, 0.2)
    if options is None:
        results = []
    else:
        results = [result[2] for result in options]
    return json.dumps(results)

autocomplete.make_cache_key = lambda: "view/%%s/%s:%s" % (
        request.args.get("term","").replace(" ","_"),
        request.args.get("ct","").lower().replace(" ","_"))

# -*- coding: utf-8 -*-
"""
    Controladores de las páginas de búsqueda y de fichero.
"""
from flask import request, render_template, g, current_app, jsonify, flash, redirect, url_for, abort, Markup
from flask.ext.login import login_required, current_user
from flask.ext.babel import gettext as _
from flaskext.babel import format_datetime
from foofind.services import *
from foofind.forms.files import SearchForm, CommentForm
from foofind.utils import mid2url, url2mid, hex2mid, mid2hex, mid2bin, bin2hex, bin2url, url2bin, to_seconds, multipartition, u, slugify, canonical_url
from foofind.utils.splitter import split_phrase, SEPPER
from foofind.utils.pagination import Pagination
from foofind.utils.async import async_generator
from foofind.utils.fooprint import Fooprint
from foofind.utils.content_types import *
from foofind.utils.filepredictor import guess_doc_content_type
from foofind.datafixes import content_bugfixes
from collections import defaultdict
from urlparse import urlparse
from datetime import datetime
from timelib import strtotime
from struct import pack, unpack
import urllib, logging, json, unicodedata, random, sys

files = Fooprint('files', __name__, dup_on_startswith="/<lang>")

@files.before_request
def files_before_request():
    g.args={}
    g.extra_sources=[]

@files.context_processor
def file_var():
    if request.args.get("error",None)=="error":
        abort(404)

    return {"zone":"files","search_form":SearchForm(request.args),"args":g.args,"extra_sources":g.extra_sources}

def url2filters(urlfilters):
    has_changed = False
    if urlfilters:
        filters = dict(fil.split(":",1) for fil in urlfilters.split("/") if fil.find(":")!=-1)

        # borra parametros vacios o que no esten entre los filtros permitidos
        filters_keys= _filters.keys()
        for key in filters.keys():
            if key=="q" or key not in filters_keys or not filters[key]:
                del filters[key]
                has_changed = True

        if "src" in filters:
            if "," not in filters["src"]: #si puede ser un filtro antiguo
                srcs={"wf":"download","s":"streaming","e":"ed2k","t":"torrent","g":"gnutella","wftge":"","wfteg":"","wfge":"","swftge":"","ftg":""}
                if filters["src"] in srcs.iterkeys():
                    filters["src"]=srcs[filters["src"]]
                    has_changed=True
                else:
                    filters["src"]=[filters["src"]]
            else:
                try:
                    filters['src'] = filters["src"].split(",")
                except:
                    del filters['src']
                    has_changed = True

        if "type" in filters:
            try:
                type_filter = []
                for atype in filters["type"].split(","):
                    if atype in CONTENTS_CATEGORY:
                        type_filter.append(atype)
                    elif atype.lower() in CONTENTS_CATEGORY:
                        type_filter.append(atype.lower())
                        has_changed = True
                    else:
                        has_changed = True

                filters['type'] = type_filter
            except:
                del filters['type']
                has_changed = True

        if "size" in filters:
            try:
                sizes = filters["size"].split(",")
                if len(sizes)==1 and 0<int(sizes[0])<5: # corrige tamaños antiguos
                    filters["size"] = [["0", "20"], ["0", "23"], ["0", "27"], ["27", "50"]][int(sizes[0])-1]
                    has_changed = True
                elif len(sizes)!=2 or not (sizes[0].isdigit() and sizes[1].isdigit() and int(sizes[0])>=0 and int(sizes[1])<=50): # valores incorrectos
                    del filters["size"]
                    has_changed = True
                else:
                    filters["size"] = sizes
            except:
                del filters["size"]
                has_changed = True
    else:
        filters = {}

    return filters, has_changed

def filters2url(filters):
    keys=_filters.keys()
    return "/".join(key+":"+(",".join(value) if isinstance(value, list) else value) for key, value in filters.iteritems() if value and key in keys and key!="q") or None

def prepare_args(query, filters):
    args = filters.copy()
    args["q"] = query

    g.args=args

    #sources que se pueden elegir
    fetch_global_data()

    sources_streaming, sources_download, sources_p2p = searchd.get_sources_stats()

    g.sources_names = {}
    g.sources_streaming = []
    for src in sources_streaming:
        notld_src = src.split(".",1)[0].lower()
        g.sources_streaming.append(notld_src)
        g.sources_names[notld_src] = src
    g.sources_names["other-streamings"] = "other_streamings"

    g.sources_download = []
    for src in sources_download:
        notld_src = src.split(".",1)[0].lower()
        g.sources_download.append(notld_src)
        g.sources_names[notld_src] = src
    g.sources_names["other-downloads"] = "other_direct_downloads"

    g.sources_p2p = []
    for src in sources_p2p:
        notld_src = src.lower()
        g.sources_p2p.append(notld_src)
        g.sources_names[notld_src] = src

    g.extra_sources=g.sources_streaming+g.sources_download+g.sources_p2p

ROBOT_USER_AGENTS=("aol","ask","google","msn","yahoo")
def is_search_bot():
    '''
    Detecta si la peticion es de un robot de busqueda
    '''
    return request.user_agent.browser is not None and request.user_agent.browser in ROBOT_USER_AGENTS

FULL_BROWSERS_USER_AGENTS=("chrome", "firefox", "msie", "opera", "safari", "webkit")
def is_full_browser():
    '''
    Detecta si la peticion es de un robot de busqueda
    '''
    return request.user_agent.browser in FULL_BROWSERS_USER_AGENTS

def get_files(ids, block_on_sphinx=True):
    '''
    Recibe lista de tuplas de tamaño 3 o mayor (como las devueltas por search)
    y devuelve los ficheros del mongo correspondiente que no estén bloqueados.
    Si se recibe un fichero bloqueado, lo omite y bloquea en el sphinx.

    @type ids: iterable de tuplas de tamaño 3 o mayor
    @param ids: lista de tuplas (mongoid, id servidor, id sphinx)

    @yield: cada uno de los resultados de los ids en los mongos

    '''
    toblock = []
    for f in filesdb.get_files(ids, servers_known = True, bl = None):
        if f["bl"] == 0 or f["bl"] is None:
            yield f
        else:
            toblock.append((mid2hex(f["_id"]), f["s"]))

    # bloquea en sphinx los ficheros bloqueados
    if toblock and block_on_sphinx:
        cache.cacheme = False
        id_list = {i[0]:i[2] for i in ids}
        searchd.block_files([(id_list[mid],server) for mid, server in toblock])

def init_data(file_data):
    '''
    Inicializa el diccionario de datos del archivo
    '''
    file_data["id"]=mid2url(file_data['_id'])
    file_data['name']=file_data['src'][mid2hex(file_data["_id"])]['url']
    return {"file":file_data,"view":{}}

def extension_filename(filename,ext):
    '''
    Añade la extension al nombre de archivo
    '''
    filename = Markup(filename).striptags()[:512]
    if not ext in EXTENSIONS:
        nfilename = filename
    else: #nice filename
        end = filename.upper().rfind("."+ext.upper())
        nfilename = filename if end == -1 else filename.strip()[0:end]
    #si la extension no viene en el nombre se añade
    if ext and not filename.lower().endswith("."+ext.lower()):
        filename = filename+"."+ext

    return filename,nfilename

def choose_filename(f,text=False):
    '''
    Elige el archivo correcto
    '''
    text=slugify(text) if text else text
    srcs = f['file']['src']
    fns = f['file']['fn']
    chosen = None
    max_count = -1
    current_weight = -1
    if text in fns: # Si text es en realidad un ID de fn
        chosen = text
    else:
        for hexuri,src in srcs.items():
            if 'bl' in src and src['bl']!=0:
                continue

            for crc,srcfn in src['fn'].items():
                if crc not in fns: #para los sources que tienen nombre pero no estan en el archivo
                    continue

                #si no tiene nombre no se tiene en cuenta
                m = srcfn['m'] if len(fns[crc]['n'])>0 else 0
                if 'c' in fns[crc]:
                    fns[crc]['c']+=m
                else:
                    fns[crc]['c']=m

                text_weight = 0
                if text:
                    fn_parts = slugify(fns[crc]['n']).strip().split(" ")

                    if len(fn_parts)>0:
                        text_parts = text.split(" ")

                        # valora numero y orden coincidencias
                        last_pos = -1
                        max_length = length = 0
                        occurrences = [0]*len(text_parts)
                        for part in fn_parts:
                            pos = text_parts.index(part) if part in text_parts else -1
                            if pos != -1 and (last_pos==-1 or pos==last_pos+1):
                                length += 1
                            else:
                                if length > max_length: max_length = length
                                length = 0
                            if pos != -1:
                                occurrences[pos]=1
                            last_pos = pos
                        if length > max_length: max_length = length
                        text_weight = sum(occurrences)*100 + max_length

                f['file']['fn'][crc]['tht'] = text_weight
                better = fns[crc]['c']>max_count

                if text_weight > current_weight or (better and text_weight==current_weight):
                    current_weight = text_weight
                    chosen = crc
                    max_count = fns[crc]['c']

    f['view']['url'] = mid2url(hex2mid(f['file']['_id']))
    f['view']['fnid'] = chosen
    if chosen:
        filename = fns[chosen]['n'] if "torrent:name" not in f["file"]["md"] else f["file"]["md"]["torrent:name"]
        ext = fns[chosen]['x']
    else: #uses filename from src
        filename = ""
        for hexuri,src in srcs.items():
            if src['url'].find("/")!=-1:
                filename = src['url']

        if filename=="":
            return

        filename = filename[filename.rfind("/")+1:]
        ext = filename[filename.rfind(".")+1:]
        filename = filename[0:filename.rfind(".")]
        #TODO si no viene nombre de archivo buscar en los metadatos para formar uno (por ejemplo serie - titulo capitulo)

    filename,nfilename=extension_filename(filename,ext)
    f['view']['fn'] = filename.replace("?", "")
    f['view']['efn'] = filename.replace(" ", "%20")

    #poner bonito nombre del archivo
    if nfilename.find(" ")==-1:
        nfilename = nfilename.replace(".", " ")

    nfilename = nfilename.replace("_", " ")
    f['view']['nfn'] = nfilename

    # añade el nombre del fichero como palabra clave
    g.keywords.append(nfilename)

    #nombre del archivo escapado para generar las url de descarga
    f['view']['qfn'] = u(filename).encode("UTF-8")

    #nombre del archivo con las palabras que coinciden con la busqueda resaltadas
    if not text:# or not has_text:
        f['view']['fnh'] = filename #esto es solo para download que nunca tiene text
    else:
        f['view']['fnh'], f['view']['fnhs'] = highlight(text,filename,True)


    return current_weight>0 # indica si ha encontrado el texto buscado

def highlight(text,match,limit_length=False):
    '''
    Resalta la parte de una cadena que coincide con otra dada
    '''
    t=frozenset(text.lower().split(" "))
    result=u"".join("<strong>%s</strong>" % part if slugify(part) in t else part for part in multipartition(match,u"".join({i for i in match if i in SEPPER or i=="'"})))

    if limit_length: #si se quiere acortar el nombre del archivo para que aparezca al principio la primera palabra resaltada
        first_term = result.find("<strong>")
        first_part = None
        if first_term>15 and len(match)>25:
            first_part = result[:first_term]
            if first_part[-1]==" ": #hace que el ultimo espacio siempre se vea
                first_part = first_part[:-1]+u"&nbsp;"

        return (result,"<span>"+first_part+"</span>"+result[first_term:] if first_part else result)
    else:
        return result

def build_source_links(f):
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

    f['view']['action']='download'
    f['view']['sources']=defaultdict(dict)
    max_weight=0
    icon=""
    has_torrent=url_pattern=url_pattern_generated=False
    for hexuri,src in f['file']['src'].items():
        if not src.get('bl',None) in (0, None):
            continue

        join=False
        count=0
        part=url=""
        source_data=g.sources[src["t"]] if "t" in src and src["t"] in g.sources else None
        if source_data is None: #si no existe el origen del archivo
            logging.error("El fichero contiene un origen inexistente en la tabla \"sources\": %s" % src["t"], extra={"file":f})
            feedbackdb.notify_source_error(f['file']["_id"], f['file']["s"])
            continue
        elif "crbl" in source_data and int(source_data["crbl"])==1: #si el origen esta bloqueado
            continue
        elif "w" in source_data["g"] or "f" in source_data["g"] or "s" in source_data["g"]: #si es descarga directa o streaming
            link_weight=1
            tip=source_data["d"]
            icon="web"
            source=get_domain(src['url']) if "f" in source_data["g"] else source_data["d"]
            url=src['url']
            if "url_pattern" in source_data and not url.startswith(("https://","http://","ftp://")):
                url_pattern=True
            #en caso de duda se prefiere streaming
            if "s" in source_data["g"]:
                f['view']['action']='watch'
                link_weight*=2
        #torrenthash antes de torrent porque es un caso especifico
        elif source_data["d"]=="BitTorrentHash":
            link_weight=0.7 if 'torrent:tracker' in f['file']['md'] or 'torrent:trackers' in f['file']['md'] else 0.1
            tip="Torrent MagnetLink"
            source="tmagnet"
            icon="torrent"
            join=True
            count=int(src['m'])
            part="xt=urn:btih:"+src['url']
            if 'torrent:tracker' in f['file']['md']:
                part += unicode('&tr=' + urllib.quote_plus(u(f['file']['md']['torrent:tracker']).encode("UTF-8")), "UTF-8")
            elif 'torrent:trackers' in f['file']['md']:
                trackers = f['file']['md']['torrent:trackers']
                if isinstance(trackers, basestring):
                    part += unicode("".join('&tr='+urllib.quote_plus(tr) for tr in u(trackers).encode("UTF-8").split(" ")), "UTF-8")

        elif "t" in source_data["g"]:
            link_weight=0.8
            url=src['url']
            icon="torrent"
            tip=source=get_domain(src['url'])
            has_torrent = True
        elif source_data["d"]=="Gnutella":
            link_weight=0.2
            tip="Gnutella"
            source=icon="gnutella"
            part="xt:urn:sha1:"+src['url']
            join=True
            count=int(src['m'])
        elif source_data["d"]=="eD2k":
            link_weight=0.1
            tip="eD2k"
            source=icon="ed2k"
            url="ed2k://|file|"+f['view']['efn']+"|"+str(f['file']['z'] if "z" in f["file"] else 1)+"|"+src['url']+"|/"
            count=int(src['m'])
        elif source_data["d"]=="Tiger":
            link_weight=0
            tip="Gnutella"
            source=icon="gnutella"
            part="xt:urn:tiger="+src['url']
            join=True
        elif source_data["d"]=="MD5":
            link_weight=0
            tip="Gnutella"
            source=icon="gnutella"
            part="xt:urn:md5="+src['url']
            join=True
        else:
            continue

        f['view']['sources'][source].update(source_data)
        f['view']['sources'][source]['tip']=tip
        f['view']['sources'][source]['icon']=icon
        f['view']['sources'][source]['icons']=source_data.get("icons",False)
        f['view']['sources'][source]['join']=join
        f['view']['sources'][source]['source']="streaming" if "s" in source_data["g"] else "direct_download" if "w" in source_data["g"] else "P2P" if "p" in source_data["g"] else ""
        #para no machacar el numero si hay varios archivos del mismo source
        if not 'count' in f['view']['sources'][source] or count>0:
            f['view']['sources'][source]['count']=count

        if not "parts" in f['view']['sources'][source]:
            f['view']['sources'][source]['parts']=[]

        if not 'urls' in f['view']['sources'][source]:
            f['view']['sources'][source]['urls']=[]

        if part:
            f['view']['sources'][source]['parts'].append(part)

        if url:
            if url_pattern:
                f['view']['sources'][source]['urls']=[source_data["url_pattern"]%url]
                f['view']['source_id']=url
                url_pattern=False
                url_pattern_generated=True
            elif not url_pattern_generated:
                f['view']['sources'][source]['urls'].append(url)

            if source_data["d"]!="eD2k":
                f['view']['sources'][source]['count']+=1

        if link_weight>max_weight:
            max_weight = link_weight
            f['view']['source'] = source

    f['view']['list_magnet_source'] = 0 if has_torrent and "tmagnet" in f['view']['sources'] else 1

    if "source" not in f["view"]:
        raise FileNoSources

    if icon!="web":
        for src,info in f['view']['sources'].items():
            if info['join']:
                f['view']['sources'][src]['urls'].append("magnet:?dn="+f['view']['efn']+("&xl="+str(f['file']['z']) if 'z' in f['file'] else "")+"&"+"&".join(info['parts']))
            elif not 'urls' in info:
                del(f['view']['sources'][src])

def choose_file_type(f):
    '''
    Elige el tipo de archivo
    '''
    ct, file_tags, file_format = guess_doc_content_type(f["file"], g.sources)
    f['view']["ct"] = ct
    f['view']['file_type'] = CONTENTS[ct].lower()
    f['view']["tags"] = file_tags
    if file_format: f['view']['format'] = file_format

def get_images(f):
    '''
    Obtiene las imagenes para los archivos que las tienen
    '''
    if "i" in f["file"] and isinstance(f["file"]["i"],list):
        f["view"]["images_server"]=[]
        for image in f["file"]["i"]:
            server=g.image_servers[image]
            f["view"]["images_server"].append("%02d"%int(server["_id"]))
            if not "first_image_server" in f["view"]:
                f["view"]["first_image_server"]=server["ip"]

        f["view"]["images_server"]="_".join(f["view"]["images_server"])

def format_metadata(f,details,text):
    '''
    Formatea los metadatos de los archivos
    '''
    view_md = f['view']['md'] = {}
    file_type = f['view']['file_type'] if 'file_type' in f['view'] else None
    if 'md' in f['file']:
        #si viene con el formato tipo:metadato se le quita el tipo
        file_md = {(meta.split(":")[-1] if ":" in meta else meta): value for meta, value in f['file']['md'].iteritems()}

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
            logging.warn(e, extra=file_md)

        if not put_duration and "length" in file_md:
            # Si recibo length y no la he recibido duration de otra forma
            try:
                duration[-1] = to_seconds(file_md["length"])
                put_duration = True
            except BaseException as e:
                logging.error("Problema al parsear duración: 'length'", extra=file_md)

        if not put_duration and "duration" in file_md:
            # Si recibo duration y no la he recibido de otra forma
            try:
                duration[-1] = to_seconds(file_md["duration"])
                put_duration = True
            except BaseException as e:
                logging.error("Problema al parsear duración: 'duration'", extra=file_md)

        if put_duration:
            carry = 0
            for i in xrange(len(duration)-1,-1,-1):
                unit = long(duration[i]) + carry
                duration[i] = unit%60
                carry = unit/60

            view_md["length"] = "%d:%02d:%02d" % tuple(duration) if duration[-3] > 0 else "%02d:%02d" % tuple(duration[-2:])

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
                logging.warn(e)

        # Metadatos que no cambian
        try:
            view_md.update(
                (meta, file_md[meta]) for meta in
                (
                    (
                        "folders","description","fileversion","os","files","pages","format",
                        "seeds","leechs","composer","publisher","encoding"
                    ) if details else ("files","pages","format","seeds")
                ) if meta in file_md
            )
        except BaseException as e:
            logging.warn(e)

        #metadatos que tienen otros nombres
        try:
            #torrents -> filedir filesizes filepaths
            view_md.update(("tags", file_md[meta]) for meta in ("keywords", "tags", "tag") if meta in file_md)
            view_md.update(("comments", file_md[meta]) for meta in ("comments", "comment") if meta in file_md)
            view_md.update(("track", file_md[meta]) for meta in ("track", "track_number") if meta in file_md)
            view_md.update(("created_by", file_md[meta]) for meta in ("created_by", "encodedby","encoder") if meta in file_md)
            view_md.update(("language", file_md[meta]) for meta in ("language", "lang") if meta in file_md)
            view_md.update(("license", file_md[meta]) for meta in ("license", "licensetype") if meta in file_md)
            view_md.update(("date", file_md[meta]) for meta in ("published", "creationdate") if meta in file_md)
            view_md.update(("trackers", file_md[meta].split(" ")) for meta in ("trackers", "tracker") if meta in file_md and isinstance(file_md[meta], basestring))
            view_md.update(("hash", file_md[meta]) for meta in ("hash", "infohash") if meta in file_md)
            view_md.update(("visualizations", file_md[meta]) for meta in ("count", "viewCount") if meta in file_md)
            if "unpackedsize" in file_md:
                view_md["unpacked_size"]=file_md["unpackedsize"]

            if "privateflag" in file_md:
                view_md["private_file"]=file_md["privateflag"]
        except BaseException as e:
            logging.warn(e)

        # Metadatos multimedia
        try:
            #extraccion del codec de video y/o audio
            if "video_codec" in file_md: #si hay video_codec se concatena el audio_codec detras si es necesario
                view_md["codec"]=file_md["video_codec"]+" "+file_md["audio_codec"] if "audio_codec" in file_md else file_md["video_codec"]
            else: #sino se meten directamente
                view_md.update(("codec", file_md[meta]) for meta in ("audio_codec", "codec") if meta in file_md)

            if file_type in ("audio", "video", "image"):
                view_md.update((meta, file_md[meta]) for meta in ("genre", "track", "artist", "author", "colors") if meta in file_md)
        except BaseException as e:
            logging.warn(e)

        # No muestra titulo si es igual al nombre del fichero
        title = None
        if "name" in file_md:
            title = u(file_md["name"])
        elif "title" in file_md:
            title = u(file_md["title"])

        if title:
            show_title = True
            text_longer = title
            text_shorter = f["view"]["fn"]
            if len(text_shorter)>len(text_longer):
                text_longer, text_shorter = text_shorter, text_longer

            if text_longer.startswith(text_shorter):
                text_longer = text_longer[len(text_shorter):]
                if len(text_longer)==0 or (len(text_longer)>0 and text_longer.startswith(".") and text_longer[1:] in EXTENSIONS):
                    show_title = False

            if show_title:
                view_md["title"] = title

        # Los que cambian o son especificos de un tipo
        try:
            if "date" in view_md: #intentar obtener una fecha válida
                try:
                    view_md["date"]=format_datetime(datetime.fromtimestamp(strtotime(view_md["date"])))
                except:
                    del view_md["date"]

            if file_type == 'audio': #album, year, bitrate, seconds, track, genre, length
                if 'album' in file_md:
                    year = 0
                    if "year" in file_md:
                        md_year = u(file_md["year"]).strip().split()
                        for i in md_year:
                            if i.isdigit() and len(i) == 4:
                                year = int(i)
                                break
                    album = file_md["album"]
                    view_md["album"] = ("%s (%d)" % (album, year)) if 1900 <  year < 2100 else album
                if 'bitrate' in file_md: # bitrate o bitrate - soundtype o bitrate - soundtype - channels
                    soundtype=" - %s" % file_md["soundtype"] if details and "soundtype" in file_md else ""
                    channels=" (%g %s)" % (round(float(file_md["channels"]),1),_("channels")) if details and "channels" in file_md else ""
                    view_md["quality"] = "%g kbps %s%s" % (float(u(file_md["bitrate"]).replace("~","")),soundtype,channels)

            elif file_type == 'document': #title, author, pages, format, version
                if details:
                    if "format" in file_md:
                        view_md["format"] = "%s%s" % (file_md["format"]," %s" % file_md["formatversion"] if "formatversion" in file_md else "")
                    version = []
                    if "formatVersion" in file_md:
                        version.append(u(file_md["formatVersion"]))
                    elif "version" in file_md:
                        version.append(u(file_md["version"]))

                    if "revision" in file_md:
                        version.append(u(file_md["revision"]))

                    if version:
                        view_md["version"] = " ".join(version)
            elif file_type == 'image': #title, artist, description, width, height, colors
                pass
            elif file_type == 'software': #title, version, fileversion, os
                if "title" in view_md and "version" in file_md:
                     view_md["title"] += " %s" % file_md["version"]
            elif file_type == 'video':
                if details:
                    quality = []
                    try:
                        if 'framerate' in file_md:
                            quality.append("%d fps" % int(float(file_md["framerate"])))
                    except BaseException as e:
                        logging.warn(e)

                    if 'codec' in view_md: #si ya venia codec se muestra ahora en quality solamente
                        quality.append(u(view_md["codec"]))
                        del view_md["codec"]

                    if quality:
                        view_md["quality"] = " - ".join(quality)

                view_md.update((i, int(float(file_md[i]))) for i in ("episode", "season") if i in file_md and not isinstance(file_md[i],basestring))
                if "series" in file_md:
                    view_md["series"] = file_md["series"]

        except BaseException as e:
            logging.exception("Error obteniendo metadatos especificos del tipo de contenido.")

        view_mdh=f['view']['mdh']={}
        for metadata,value in view_md.items():

            # quita tags HTML de los valores
            if isinstance(value, list):
                value = "\n".join(Markup(aval).striptags() for aval in value)
            elif isinstance(value, basestring):
                value = Markup(value).striptags()

            # resaltar contenidos que coinciden con la busqueda
            if isinstance(value, basestring):
                if len(value)==0:
                    del view_md[metadata]
                    continue
                view_md[metadata]=value
                view_mdh[metadata]=highlight(text,value) if text else value
            elif isinstance(value, float): #no hay ningun metadato tipo float
                view_md[metadata]=view_mdh[metadata]=str(int(value))
            else:
                view_md[metadata]=view_mdh[metadata]=value
    # TODO: mostrar metadatos con palabras buscadas si no aparecen en lo mostrado

def embed_info(f):
    embed_width = 560
    embed_height = 315
    embed_code = None
    for src_id, src_data in f["file"]["src"].iteritems():
        source_id = src_data["t"]
        source_data = g.sources[source_id]
        if source_data.get("embed_active", False) and "embed" in source_data:
            try:
                embed_code = source_data["embed"]

                # comprueba si el content type se puede embeber
                embed_cts = source_data["embed_cts"] if "embed_cts" in source_data else DEFAULT_EMBED_CTS
                if not f["view"]["ct"] in embed_cts: continue

                embed_groups = ()
                # url directamente desde los sources
                if "source_id" in f["view"] and f["view"]["source_id"]:
                    embed_groups = {"id": f["view"]["source_id"]}
                elif "url_embed_regexp" in source_data and source_data["url_embed_regexp"]:
                    # comprueba si la url puede ser utilizada para embeber
                    embed_url = src_data["url"]
                    regexp = source_data["url_embed_regexp"]
                    embed_match = cache.regexp(regexp).match(embed_url)
                    if embed_match is None:
                        continue
                    embed_groups = embed_match.groupdict()

                if "%s" in embed_code and "id" in embed_groups: # Modo simple, %s intercambiado por el id
                    embed_code = embed_code % (
                        # Workaround para embeds con varios %s
                        # no se hace replace para permitir escapes ('\%s')
                        (embed_groups["id"],) * embed_code.count("%s")
                        )
                else:
                    # Modo completo, %(variable)s intercambiado por grupos con nombre
                    replace_dict = dict(f["file"]["md"])
                    replace_dict["width"] = embed_width
                    replace_dict["height"] = embed_height
                    replace_dict.update(embed_groups)
                    try:
                        embed_code = embed_code % replace_dict
                    except KeyError as e:
                        # No logeamos los errores por falta de metadatos 'special'
                        if all(i.startswith("special:") for i in e.args):
                            continue
                        raise e
            except BaseException as e:
                logging.exception(e)
                continue
            f["view"]["embed"] = embed_code
            f["view"]["play"]  = (source_data.get("embed_disabled", ""), source_data.get("embed_enabled", ""))
            break

def fetch_global_data():
    '''
    Carga en g los datos de origenes y servidores de imagen.
    '''
    if hasattr(g,"sources"):
        return

    g.sources = {int(s["_id"]):s for s in filesdb.get_sources(blocked=None)}
    g.image_servers = filesdb.get_image_servers()

def fill_data(file_data,details=False,text=False):
    '''
    Añade los datos necesarios para mostrar los archivos
    '''
    # se asegura que esten cargados los datos de origenes y servidor de imagen antes de empezar
    fetch_global_data()
    f=init_data(file_data)
    content_bugfixes(f["file"])
    # al elegir nombre de fichero, averigua si aparece el texto buscado
    search_text = text if choose_filename(f,text) else False
    build_source_links(f)
    choose_file_type(f)
    embed_info(f)
    get_images(f)
    # si hace falta, muestra metadatos extras con el texto buscado
    format_metadata(f,details,search_text)
    return f

def secure_fill_data(file_data,details=False,text=False):
    '''
    Maneja errores en fill_data
    '''
    try:
        return fill_data(file_data,details,text)
    except BaseException as e:
        logging.exception("Fill_data error on file %s: %s"%(str(file_data["_id"]),repr(e)))
        return None

class DatabaseError(Exception):
    pass

class FileNotExist(Exception):
    pass

class FileRemoved(Exception):
    pass

class FileFoofindRemoved(Exception):
    pass

class FileUnknownBlock(Exception):
    pass

class FileNoSources(Exception):
    pass

def get_file_metadata(file_id, file_name=None):
    '''
    Obtiene el fichero de base de datos y rellena sus metadatos.

    @type file_id: mongoid
    @param file_id: id de mongo del fichero

    @type file_name: basestring
    @param file_name: nombre del fichero

    @rtype dict
    @return Diccionario de datos del fichero con metadatos

    @raise DatabaseError: si falla la conexión con la base de datos
    @raise FileNotExist: si el fichero no existe o ha sido bloqueado
    @raise FileRemoved: si el fichero ha sido eliminado de su origen
    @raise FileFoofindRemoved: si el fichero ha sido bloqueado por foofind
    @raise FileUnknownBlock: si el fichero está bloqueado pero se desconoce el porqué
    @raise FileNoSources: si el fichero no tiene orígenes
    '''
    try:
        data = filesdb.get_file(file_id, bl = None)
    except filesdb.BogusMongoException as e:
        logging.exception(e)
        raise DatabaseError

    # intenta sacar el id del servidor de sphinx,
    # resuelve inconsistencias de los datos
    if not data:
        sid = searchd.get_id_server_from_search(file_id, file_name)
        if sid:
            try:
                data = filesdb.get_file(file_id, sid = sid, bl = None)
                feedbackdb.notify_indir(file_id, sid)
            except filesdb.BogusMongoException as e:
                logging.exception(e)
                raise DatabaseError

    if data:
        if "bl" in data and not data["bl"] in (0, None):
            if data["bl"] == 1: raise FileFoofindRemoved
            elif data["bl"] == 3: raise FileRemoved
            logging.warn(
                "File with an unknown 'bl' value found: %d" % data["bl"],
                extra=data)
            raise FileUnknownBlock
    else:
        raise FileNotExist

    #obtener los datos
    return fill_data(data, True, file_name)

def save_visited(files):
    '''
    Recibe una lista de resultados de fill_data y guarda las url que sean de web
    '''
    if not is_search_bot():
        result=[]
        for file in files:
            result+=([{"_id": data['urls'][0], "type":src} for src, data in file['view']['sources'].iteritems() if data['icon'] == 'web'])

        if result:
            feedbackdb.visited_links(result)

def taming_generate_tags(res, query, mean):
    '''
    Genera los tags
    '''
    querylen = len(query)
    tag = res[2][querylen+1:]
    return (tag[querylen+1:] if tag.startswith(query+" ") else tag, 100*max(min(1.25, res[0]/mean), 0.75))

def taming_tags(query, tamingWeight):
    try:
        tags = taming.tameText(
            text=query+" ",
            weights=tamingWeight,
            limit=20,
            maxdist=4,
            minsimil=0.7,
            dym=0
            )
        if tags:
            mean = (tags[0][0] + tags[-1][0])/2
            tags = map(lambda res: taming_generate_tags(res, query, mean), tags)
            tags.sort()
        else:
            tags = ()
    except BaseException as e:
        logging.exception("Error getting search related tags.")
        tags = ()
    return tags

def taming_dym(query, tamingWeight):
    try:
        suggest = taming.tameText(
            text=query,
            weights=tamingWeight,
            limit=1,
            maxdist=3,
            minsimil=0.8,
            dym=1,
            rel=0
            )
        didyoumean = None
        if suggest and suggest[0][2]!=query:
            didyoumean = suggest[0][2]
    except BaseException as e:
        logging.exception("Error getting did you mean suggestion.")
    return didyoumean


@async_generator(500)
def taming_search(query, ct):
    '''
    Obtiene los resultados que se muestran en el taming
    '''
    tamingWeight = {"c":1, "lang":200}
    if ct in CONTENTS_CATEGORY:
        for cti in CONTENTS_CATEGORY[ct]:
            tamingWeight[TAMING_TYPES[cti]] = 200

    yield taming_tags(query, tamingWeight)
    yield taming_dym(query, tamingWeight)

def download_search(file_data,file_name):
    '''
    Intenta buscar una cadena a buscar cuando viene download
    '''
    if file_data and file_data["file_data"]:
        mds = file_data["file_data"]['view']['md']
        for key in ['artist', 'series', 'album', 'title', 'genre']:
            if key in mds and isinstance(mds[key], basestring) and len(mds[key])>1:
                return u(mds[key])

        file_name = file_data["file_data"]["view"]["fn"]

    if file_name:
        phrases = split_phrase(file_name, True)
        best_candidate = []
        for phrase in phrases:
            candidate = [part for part in phrase.split(" ") if part.strip()]
            if len(candidate)<5:
                return " ".join(candidate)
            elif not best_candidate or len(candidate)<len(best_candidate):
                best_candidate = candidate

        if best_candidate:
            return " ".join(best_candidate[:5])
        else:
            return file_name
    else:
        return None

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
            query.replace("+"," ")
            filters=filters2url(request.args)
            url_with_get_params=True

    query = query.replace("_"," ") if query is not None else None #para que funcionen la busqueda cuando vienen varias palabras
    dict_filters, has_changed = url2filters(filters) #procesar los parametros
    if url_with_get_params or has_changed: #redirecciona si viene una url con get o si url2filters ha cambiado los parametros y no se mandan filtros si es un bot
        return redirect(url_for(".search", query=query.replace(" ","_"), filters=filters2url(dict_filters) if not search_bot else None, file_id=file_id),301 if search_bot else 302)

    static_download=download_file(file_id,file_name if file_name is not None else query) #obtener el download si se puede
    if "error" in static_download and static_download["error"][0]==301: #si es una redireccion de un id antiguo directamente se devuelve
        return static_download["html"]

    # en la pagina de download se intentan obtener palabras para buscar
    if query is None:
        query=download_search(static_download,file_name)
        if query:
            query.replace(":","")

    else: # titulo y descripción de la página para la busqueda
        filters_desc = " - "+", ".join(_(atype) for atype in dict_filters["type"]) if "type" in dict_filters else ""
        g.title = query+filters_desc+" - "+g.title
        g.page_description = _("results_for").capitalize() + " " + query
        g.keywords.append(query)

    prepare_args(query, dict_filters)
    total_found=0
    if file_id is not None and "error" in static_download:  #si es un error y no hay nada para buscar se muestra en pantalla completa
        flash(static_download["error"][1])
        abort(static_download["error"][0])
    elif not full_browser: #busqueda estatica para browsers "incompletos"
        results=search_files(query,dict_filters,100,100,static_download, wait_time=1500)

        # cambia la busqueda canonica
        canonical_query = results["canonical_query"]

        if search_bot and results["total_found"]==0 and not results["sure"]:
            logging.warn("No results returned to bot %s."%request.user_agent.browser)

        static_results=results["files"]
        total_found=results["total_found"]
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
            file_name=extension_filename(name['n'],name['x'])[0]
            if f['view']['fnid'] and "torrent:name" not in f["file"]["md"] and file_name!=f['view']['fn']:
                alternate.append(url_for('.download',lang='en',file_id=f['file']['id'],file_name=file_name))

    else:
        canonical=url_for('.search',lang='en',query=canonical_query)

    _filters["src"].update(g.sources_names)
    return render_template('files/search.html',
        search_info=render_template('files/results_number.html',results={"total_found":total_found,"time":0},search=request.args.get("q", None)),
        no_results=render_template('files/no_results.html',filters=_filters),
        sources_count=sources_count,
        static_results=static_results,
        static_download=static_download,
        full_browser=full_browser,
        query=query,
        alternate=alternate,
        canonical=canonical if canonical!=request.path else None
    )

_filters={
    "q":"",
    "type":["audio","video","image","document","software"],
    "src":{"download":'direct_downloads',"streaming":"Streaming","p2p":"P2P"},
    "size":['all_sizes','smaller_than','larger_than'],
    }
def search_files(query,filters,min_results=0,max_results=10,download=None,last_items=[], wait_time=500):
    '''
    Realiza una búsqueda de archivos
    '''
    if not last_items and min_results==0:
        min_results=5

    # obtener los resultados
    profiler.checkpoint(opening=["sphinx"])
    s = searchd.search({"type":"text", "text":query}, filters, wait_time)
    ids = list(s.get_results(last_items, min_results, max_results))
    stats = s.get_stats()
    result = {"time": max(stats["t"].itervalues()) if stats["t"] else 0, "total_found": stats["cs"]}
    profiler.checkpoint(opening=["mongo"], closing=["sphinx"])

    # elimina el id del download de la lista de resultados
    if download and "file_data" in download and download["file_data"]:
        download_id = mid2hex(download["file_data"]["file"]["_id"])
        ids = list(aid for aid in ids if aid[0]!=download_id)
    else:
        download_id = None

    files_dict={str(f["_id"]):secure_fill_data(f,text=query) for f in get_files(ids,True)}
    profiler.checkpoint(closing=["mongo"])

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
        g.page_description += "".join(", "+f["view"]["nfn"] for f in files[:5])

    profiler.checkpoint(opening=["visited"])
    save_visited(files)
    profiler.checkpoint(closing=["visited"])

    _filters["src"].update(g.sources_names)

    return {
        "files_ids":[f["file"]["id"] for f in files],
        "files":[render_template('files/file.html',file=f) for f in files],
        "no_results":render_template('files/no_results.html',filters=_filters),
        "result_number":render_template('files/results_number.html',results=result,search=query),
        "total_found":result["total_found"],
        "page":sum(stats["li"]),
        "last_items":bin2url(pack("%dh"%len(stats["li"]), *stats["li"])),
        "sure":stats["s"],
        "wait":stats["w"],
        "canonical_query": stats["ct"]
    }

@files.route('/<lang>/searcha',methods=['POST'])
def searcha():
    '''
    Responde las peticiones de busqueda por ajax
    '''
    data=request.form.get("filters",None) #puede venir query/filtros:... o solo query
    query,filters=data.split("/",1) if "/" in data else (data,None) if data else (None,None)
    query=query.replace("_"," ") if query is not None else query #para que funcionen la busqueda cuando vienen varias palabras
    last_items = url2bin(request.form.get("last_items",[]))
    if last_items:
        last_items = unpack("%dh"%(len(last_items)/2), last_items)

    dict_filters, has_changed = url2filters(filters)
    prepare_args(query, dict_filters)
    return jsonify(search_files(query, dict_filters, min_results=request.args.get("min_results",0), last_items=last_items))

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

def comment_votes(file_id,comment):
    '''
    Obtiene los votos de comentarios
    '''
    return {}

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

def download_file(file_id,file_name=None):
    '''
    Devuelve el archivo a descargar, votos, comentarios y archivos relacionados
    '''
    error=(None,"") #guarda el id y el texto de un error
    file_data=None
    if file_id is not None: #si viene un id se comprueba que sea correcto
        try: #intentar convertir el id que viene de la url a uno interno
            file_id=url2mid(file_id)
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

        except Exception as e:
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
        g.page_description = u"%s %s"%(_(file_data['view']['action']).capitalize(), title)

        #si el usuario esta logueado se comprueba si ha votado el archivo para el idioma activo y si ha marcado el archivo como favorito
        vote=None
        favorite = False
        if current_user.is_authenticated():
            vote=usersdb.get_file_vote(file_id,current_user,g.lang)
            file_mid=hex2mid(file_id)
            favorite=any(file_mid==favorite["id"] for favorite in usersdb.get_fav_files(current_user))

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


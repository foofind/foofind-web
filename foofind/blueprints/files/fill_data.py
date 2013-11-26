# -*- coding: utf-8 -*-
'''
    Toda la informacion de un fichero
'''
import urllib, re
from flask import g, Markup
from flask.ext.babelex import gettext as _
from urlparse import urlparse
from itertools import izip_longest, chain

from foofind.services import *
from foofind.blueprints.files.helpers import *
from foofind.utils import mid2url, mid2hex, hex2mid, to_seconds, u, logging
from foofind.utils.content_types import *
from foofind.utils.filepredictor import guess_doc_content_type
from foofind.datafixes import content_fixes
from foofind.utils.splitter import slugify
from foofind.utils.seo import seoize_text
from foofind.utils.html import clean_html

def init_data(file_data, ntts=[]):
    '''
    Inicializa el diccionario de datos del archivo
    '''
    file_data["id"]=mid2url(file_data['_id'])
    file_data['name']=file_data['src'].itervalues().next()['url']

    file_se = file_data["se"] if "se" in file_data else None
    ntt = ntts[int(float(file_se["_id"]))] if file_se and "_id" in file_se and file_se["_id"] in ntts else None
    if ntt:
        file_se["info"] = ntt

        file_se["rel"] = [ntts[relid] for relids in ntt["r"].itervalues() for relid in relids if relid in ntts] if "r" in ntt else []

    return {"file":file_data,"view":{}}

def choose_filename(f,text_cache=None):
    '''
    Elige el archivo correcto
    '''
    srcs = f['file']['src']
    fns = f['file']['fn']
    chosen = None
    max_count = -1
    current_weight = -1
    if text_cache and text_cache[0] in fns: # Si text es en realidad un ID de fn
        chosen = text_cache[0]
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
                if text_cache:
                    fn_parts = slugify(fns[crc]['n']).strip().split(" ")

                    if len(fn_parts)>0:
                        text_words =  slugify(text_cache[0]).split(" ")

                        # valora numero y orden coincidencias
                        last_pos = -1
                        max_length = length = 0
                        occurrences = [0]*len(text_words)
                        for part in fn_parts:
                            pos = text_words.index(part) if part in text_words else -1
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
        filename = fns[chosen]['n']
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

    filename = extension_filename(filename,ext)
    f['view']['fn'] = filename.replace("?", "")
    f['view']['qfn'] = qfn = u(filename).encode("UTF-8")  #nombre del archivo escapado para generar las url de descarga
    f['view']['pfn'] = urllib.quote(qfn).replace(" ", "%20")  # P2P filename

    nfilename = seoize_text(filename, " ",True, 0)
    f['view']['nfn'] = nfilename
    # añade el nombre del fichero como palabra clave
    g.keywords.update(set(keyword for keyword in nfilename.split(" ") if len(keyword)>1))

    #nombre del archivo con las palabras que coinciden con la busqueda resaltadas
    if text_cache:
        f['view']['fnh'], f['view']['fnhs'] = highlight(text_cache[2],filename,True)
    else:
        f['view']['fnh'] = filename #esto es solo para download que nunca tiene text

    return current_weight>0 # indica si ha encontrado el texto buscado

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
    f['view']['sources']={}
    max_weight=0
    icon=""

    # agrupación de origenes
    source_groups = {}

    file_sources = f['file']['src'].items()
    file_sources.sort(key=lambda x:x[1]["t"])
    for hexuri,src in file_sources:
        if not src.get('bl',None) in (0, None):
            continue

        url_pattern=downloader=join=False
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
            source_groups[icon] = tip
            source=get_domain(src['url']) if "f" in source_data["g"] else source_data["d"]
            url=src['url']
            if "url_pattern" in source_data and not url.startswith(("https://","http://","ftp://")):
                url_pattern=True
            #en caso de duda se prefiere streaming
            if "s" in source_data["g"]:
                f['view']['action']="listen" if f['view']['ct']==CONTENT_AUDIO else 'watch'
                link_weight*=2
        #torrenthash antes de torrent porque es un caso especifico
        elif source_data["d"]=="BitTorrentHash":
            downloader=True
            link_weight=0.7 if 'torrent:tracker' in f['file']['md'] or 'torrent:trackers' in f['file']['md'] else 0.1
            tip="Torrent MagnetLink"
            source="tmagnet"
            icon="torrent"
            if not icon in source_groups:
                source_groups[icon] = tip # magnet link tiene menos prioridad para el texto
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
            downloader=True
            link_weight=0.8
            url=src['url']
            if "url_pattern" in source_data and not url.startswith(("https://","http://","ftp://")):
                url_pattern=True
                tip=source=get_domain(source_data["url_pattern"]%url)
            else:
                tip=source=get_domain(src['url'])
            icon="torrent"
            source_groups[icon] = tip
        elif source_data["d"]=="Gnutella":
            link_weight=0.2
            tip="Gnutella"
            source=icon="gnutella"
            part="xt=urn:sha1:"+src['url']
            join=True
            count=int(src['m'])
            source_groups[icon] = tip
        elif source_data["d"]=="eD2k":
            downloader=True
            link_weight=0.1
            tip="eD2k"
            source=icon="ed2k"
            url="ed2k://|file|"+f['view']['pfn']+"|"+str(f['file']['z'] if "z" in f["file"] else 1)+"|"+src['url']+"|/"
            count=int(src['m'])
            source_groups[icon] = tip
        elif source_data["d"]=="Tiger":
            link_weight=0
            tip="Gnutella"
            source=icon="gnutella"
            part="xt=urn:tiger:"+src['url']
            join=True
        elif source_data["d"]=="MD5":
            link_weight=0
            tip="Gnutella"
            source=icon="gnutella"
            part="xt=urn:md5:"+src['url']
            source_groups[icon] = tip
            join=True
        else:
            continue

        if source in f['view']['sources']:
            view_source = f['view']['sources'][source]
        else:
            view_source = f['view']['sources'][source] = {}
        view_source.update(source_data)

        if 'downloader' in view_source:
            if downloader:
                view_source['downloader']=1
        else:
            view_source['downloader']=1 if downloader else 0

        view_source['tip']=tip
        view_source['icon']=icon
        view_source['icons']=source_data.get("icons",False)
        view_source['join']=join
        view_source['source']="streaming" if "s" in source_data["g"] else "direct_download" if "w" in source_data["g"] else "P2P" if "p" in source_data["g"] else ""
        #para no machacar el numero si hay varios archivos del mismo source
        if not 'count' in view_source or count>0:
            view_source['count']=count

        if not "parts" in view_source:
            view_source['parts']=[]

        if not 'urls' in view_source:
            view_source['urls']=[]

        if part:
            view_source['parts'].append(part)

        if url:
            if url_pattern:
                view_source['urls']=[source_data["url_pattern"]%url]
                f['view']['source_id']=url
                view_source["pattern_used"]=True
            elif not "pattern_used" in view_source:
                view_source['urls'].append(url)

            if source_data["d"]!="eD2k":
                view_source['count']+=1

        if link_weight>max_weight:
            max_weight = link_weight
            f['view']['source'] = source

    f['view']['source_groups'] = sorted(source_groups.items())

    if "source" not in f["view"]:
        raise FileNoSources

    if icon!="web":
        for src,info in f['view']['sources'].items():
            if info['join']:
                f['view']['sources'][src]['urls'].append("magnet:?"+"&".join(info['parts'])+"&dn="+f['view']['pfn']+("&xl="+str(f['file']['z']) if 'z' in f['file'] else ""))
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
    images = images_id = None
    if "i" in f["file"] and isinstance(f["file"]["i"],list):
        images = f["file"]["i"]
        images_id = f["file"]["_id"]
    elif "se" in f["file"] and "info" in f["file"]["se"]:
        for ntt in chain([f["file"]["se"]["info"]], f["file"]["se"]["rel"]):
            if "im" in ntt:
                images = ntt["im"]
                images_id = "e_%d_"%int(ntt["_id"])
                break

    if images:
        images_servers=[]
        for image in images:
            server=g.image_servers[image]
            images_servers.append("%02d"%int(server["_id"]))
            if not "first_image_server" in f["view"]:
                f["view"]["first_image_server"]=server["ip"]

        f["view"]["images_server"]="_".join(images_servers)
        f["view"]["images_id"] = images_id

def get_int(adict, key):
    if not key in adict:
        return None
    value = adict[key]
    if isinstance(value, (int,long)):
        return value
    elif isinstance(value, float):
        return int(value)
    elif isinstance(value, basestring):
        result = None
        for c in value:
            digit = ord(c)-48
            if 0<=digit<=9:
                if result:
                    result *= 10
                else:
                    result = 0
                result += digit
            else:
                break
        return result
    return None

def get_float(adict, key):
    if not key in adict:
        return None
    value = adict[key]
    if isinstance(value, float):
        return value
    elif isinstance(value, (int,long)):
        return float(value)
    elif isinstance(value, basestring):
        result = ""
        decimal = False
        for c in value:
            if c in "0123456789":
                result += c
            elif c in ".," and not decimal:
                result += "."
                decimal = True
            else:
                break

        if result:
            try:
                return float(result)
            except:
                pass
    return None


def format_metadata(f,text_cache, search_text_shown=False):
    '''
    Formatea los metadatos de los archivos
    '''
    text = text_cache[2] if text_cache else None
    view_md = f['view']['md'] = {}
    view_searches = f["view"]["searches"]={}
    file_type = f['view']['file_type'] if 'file_type' in f['view'] else None
    if 'md' in f['file']:
        #si viene con el formato tipo:metadato se le quita el tipo
        file_md = {(meta.split(":")[-1] if ":" in meta else meta): value for meta, value in f['file']['md'].iteritems()}

        # Duración para vídeo e imágenes
        seconds = get_float(file_md, "seconds")
        minutes = get_float(file_md, "minutes")
        hours = get_float(file_md, "hours")

        # Si no he recibido duracion de otra forma, pruebo con length y duration
        if seconds==minutes==hours==None:
            seconds = get_float(file_md, "length") or get_float(file_md, "duration")

        duration = [hours or 0, minutes or 0, seconds or 0] # h, m, s

        if any(duration):
            carry = 0
            for i in xrange(len(duration)-1,-1,-1):
                unit = long(duration[i]) + carry
                duration[i] = unit%60
                carry = unit/60

            view_md["length"] = "%d:%02d:%02d" % tuple(duration) if duration[0] > 0 else "%02d:%02d" % tuple(duration[1:])

        # Tamaño para vídeos e imágenes
        width = get_int(file_md, "width")
        height = get_int(file_md, "height")
        if width and height:
            view_md["size"] = "%dx%dpx" % (width, height)

        # Metadatos que no cambian
        try:
            view_md.update(
                (meta, file_md[meta]) for meta in
                (
                    "folders","description","fileversion","os","files","pages","format",
                    "seeds","leechs","composer","publisher","encoding","director","writer","starring","producer","released"
                ) if meta in file_md
            )
            view_searches.update(
                (meta, seoize_text(file_md[meta],"_",False)) for meta in
                (
                    "folders","os","composer","publisher","director","writer","starring","producer"
                ) if meta in file_md
            )
        except BaseException as e:
            logging.warn(e)

        # thumbnail
        if "thumbnail" in file_md:
            f["view"]["thumbnail"] = file_md["thumbnail"]

        #metadatos que tienen otros nombres
        try:
            view_md.update(("tags", file_md[meta]) for meta in ("keywords", "tags", "tag") if meta in file_md)
            if "tags" in view_md and isinstance(view_md["tags"], basestring):
                view_searches["tags"] = []
            view_md.update(("comments", file_md[meta]) for meta in ("comments", "comment") if meta in file_md)
            view_md.update(("track", file_md[meta]) for meta in ("track", "track_number") if meta in file_md)
            view_md.update(("created_by", file_md[meta]) for meta in ("created_by", "encodedby","encoder") if meta in file_md)
            view_md.update(("language", file_md[meta]) for meta in ("language", "lang") if meta in file_md)
            view_md.update(("date", file_md[meta]) for meta in ("published", "creationdate") if meta in file_md)
            view_md.update(("trackers", "\n".join(file_md[meta].split(" "))) for meta in ("trackers", "tracker") if meta in file_md and isinstance(file_md[meta], basestring))
            view_md.update(("hash", file_md[meta]) for meta in ("hash", "infohash") if meta in file_md)
            view_md.update(("visualizations", file_md[meta]) for meta in ("count", "viewCount") if meta in file_md)
            if "unpackedsize" in file_md:
                view_md["unpacked_size"]=file_md["unpackedsize"]

            if "privateflag" in file_md:
                view_md["private_file"]=file_md["privateflag"]
        except BaseException as e:
            logging.warn(e)

        #torrents -> filedir filesizes filepaths
        if "filepaths" in file_md:
            filepaths = {}
            for path, size in izip_longest(u(file_md["filepaths"]).split("///"), u(file_md.get("filesizes","")).split(" "), fillvalue=None):
                # no permite tamaños sin fichero
                if not path: break
                parts = path.strip("/").split("/")

                # crea subdirectorios
                relative_path = filepaths
                for part in parts[:-1]:
                    if "/"+part not in relative_path:
                        relative_path["/"+part] = {}
                    relative_path = relative_path["/"+part]

                # si ya existe el directorio no hace nada
                if "/"+parts[-1] in relative_path:
                    pass
                # si el ultimo nivel se repite es un directorio (fallo de contenido)
                elif parts[-1] in relative_path:
                    relative_path["/"+parts[-1]] = {}
                    del relative_path[parts[-1]]
                else:
                    relative_path[parts[-1]] = size

            if "filedir" in file_md:
                filepaths = {"/"+u(file_md["filedir"]).strip("/"):filepaths}

            if filepaths:
                view_md["filepaths"] = filepaths
                view_searches["filepaths"] = {}

        # Metadatos multimedia
        try:
            #extraccion del codec de video y/o audio
            if "video_codec" in file_md: #si hay video_codec se concatena el audio_codec detras si es necesario
                view_md["codec"]=file_md["video_codec"]+" "+file_md["audio_codec"] if "audio_codec" in file_md else file_md["video_codec"]
            else: #sino se meten directamente
                view_md.update(("codec", file_md[meta]) for meta in ("audio_codec", "codec") if meta in file_md)

            if file_type in ("audio", "video", "image"):
                view_md.update((meta, file_md[meta]) for meta in ("genre", "track", "artist", "author", "colors") if meta in file_md)
                view_searches.update((meta, seoize_text(file_md[meta], "_", False)) for meta in ("artist", "author") if meta in file_md)
        except BaseException as e:
            logging.warn(e)

        # No muestra titulo si es igual al nombre del fichero
        if "name" in file_md:
            title = u(file_md["name"])
        elif "title" in file_md:
            title = u(file_md["title"])
        else:
            title = f['view']['nfn']

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
                view_searches["title"] = seoize_text(title, "_", False)

        # Los que cambian o son especificos de un tipo
        try:
            if "date" in view_md: #intentar obtener una fecha válida
                try:
                    view_md["date"]=format_datetime(datetime.fromtimestamp(strtotime(view_md["date"])))
                except:
                    del view_md["date"]

            if file_type == 'audio': #album, year, bitrate, seconds, track, genre, length
                if 'album' in file_md:
                    album = u(file_md["album"])
                    year = get_int(file_md, "year")
                    if album:
                        view_md["album"] = album + (" (%d)"%year if year and 1900<year<2100 else "")
                        view_searches["album"] = seoize_text(album, "_", False)
                if 'bitrate' in file_md: # bitrate o bitrate - soundtype o bitrate - soundtype - channels
                    bitrate = get_int(file_md, "bitrate")
                    if bitrate:
                        soundtype=" - %s" % file_md["soundtype"] if "soundtype" in file_md else ""
                        channels = get_float(file_md, "channels")
                        channels=" (%g %s)" % (round(channels,1),_("channels")) if channels else ""
                        view_md["quality"] = "%g kbps %s%s" % (bitrate,soundtype,channels)

            elif file_type == 'document': #title, author, pages, format, version
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
                     view_searches["title"] += " %s" % seoize_text(file_md["version"], "_", False)
            elif file_type == 'video':
                quality = []

                framerate = get_int(file_md, "framerate")
                if framerate:
                    quality.append("%d fps" % framerate)

                if 'codec' in view_md: #si ya venia codec se muestra ahora en quality solamente
                    quality.append(u(view_md["codec"]))
                    del view_md["codec"]

                if quality:
                    view_md["quality"] = " - ".join(quality)

                if "series" in file_md:
                    series = u(file_md["series"])
                    if series:
                        safe_series = seoize_text(series, "_", False)
                        view_md["series"] = series
                        view_searches["series"]="%s_%s"%(safe_series,"(series)")

                        season = get_int(file_md, "season")
                        if season:
                            view_md["season"] = season
                            view_searches["season"]="%s_(s%d)"%(safe_series,season)

                            episode = get_int(file_md, "episode")
                            if episode:
                                view_md["episode"] = episode
                                view_searches["episode"]="%s_(s%de%d)"%(safe_series,season,episode)

        except BaseException as e:
            logging.exception("Error obteniendo metadatos especificos del tipo de contenido.")

        view_mdh=f['view']['mdh']={}
        for metadata,value in view_md.items():
            if isinstance(value, basestring):
                value = clean_html(value)
                if not value:
                    del view_md[metadata]
                    continue

                view_md[metadata]=value

                # resaltar contenidos que coinciden con la busqueda, para textos no muy largos
                if len(value)<500:
                    view_mdh[metadata]=highlight(text,value) if text and len(text)<100 else value
            elif isinstance(value, float): #no hay ningun metadato tipo float
                view_md[metadata]=str(int(value))
            else:
                view_md[metadata]=value
    # TODO: mostrar metadatos con palabras buscadas si no aparecen en lo mostrado

def embed_info(f):
    '''
        Añade la informacion del embed
    '''
    embed_width = 560
    embed_height = 315
    embed_code = None
    for src_id, src_data in f["file"]["src"].iteritems():
        source_id = src_data["t"]

        source_data = g.sources.get(source_id, None)
        if not (source_data and source_data.get("embed_active", False) and "embed" in source_data):
            continue

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

def fill_data(file_data, text=None, ntts={}):
    '''
    Añade los datos necesarios para mostrar los archivos
    '''
    if text:
        slug_text = slugify(text)
        text = (text, slug_text, frozenset(slug_text.split(" ")))

    # se asegura que esten cargados los datos de origenes y servidor de imagen antes de empezar
    fetch_global_data()
    f=init_data(file_data, ntts)
    content_fixes(f["file"])

    choose_file_type(f)
    # al elegir nombre de fichero, averigua si aparece el texto buscado
    search_text_shown = choose_filename(f,text)
    build_source_links(f)
    embed_info(f)
    get_images(f)
    # si hace falta, muestra metadatos extras con el texto buscado
    format_metadata(f,text, search_text_shown)
    return f

def secure_fill_data(file_data,text=None, ntts={}):
    '''
    Maneja errores en fill_data
    '''
    try:
        return fill_data(file_data,text,ntts)
    except BaseException as e:
        logging.exception("Fill_data error on file %s: %s"%(str(file_data["_id"]),repr(e)))
        return None

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
    except BaseException as e:
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
            except BaseException as e:
                logging.exception(e)
                raise DatabaseError

    if data:
        bl = data.get("bl",None)
        if bl and isinstance(bl, (str, unicode)) and bl.isdigit():
            bl = int(bl)
        if bl:
            if bl == 1: raise FileFoofindRemoved
            elif bl == 3: raise FileRemoved
            logging.warn(
                "File with an unknown 'bl' value found: %s" % repr(bl),
                    extra=data)
            raise FileUnknownBlock

        file_se = data["se"] if "se" in data else None

        file_ntt = entitiesdb.get_entity(file_se["_id"]) if file_se and "_id" in file_se else None
        ntts = {file_se["_id"]:file_ntt} if file_ntt else {}

        '''
        # trae entidades relacionadas
        if file_ntt and "r" in file_ntt:
            rel_ids = list(set(eid for eids in file_ntt["r"].itervalues() for eid in eids))
            ntts.update({int(ntt["_id"]):ntt for ntt in entitiesdb.get_entities(rel_ids, None, (False, [u"episode"]))})
        '''
    else:
        raise FileNotExist

    #obtener los datos
    return fill_data(data, file_name, ntts)

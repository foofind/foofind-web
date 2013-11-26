# -*- coding: utf-8 -*-
from flask import g, request, url_for
from flask.ext.babelex import gettext as _
from babel.numbers import get_decimal_symbol, get_group_symbol
from math import log, ceil
from datetime import datetime,timedelta
from foofind.utils.htmlcompress import HTMLCompress
from foofind.utils import u, fixurl, logging
from foofind.utils.seo import seoize_text
from foofind.blueprints.api import file_embed_link
from foofind.blueprints.files.helpers import FILTERS
from jinja2.utils import Markup
from jinja2 import escape
from urllib import quote_plus
from pprint import pformat
from markdown import markdown


def register_filters(app):
    '''
    Registra filtros de plantillas
    '''
    app.jinja_env.filters['numberformat'] = number_format_filter
    app.jinja_env.filters['numbersizeformat'] = number_size_format_filter
    app.jinja_env.filters['url_search'] = url_search_filter
    app.jinja_env.filters['format_timedelta'] = format_timedelta_filter
    app.jinja_env.filters['url_lang'] = url_lang_filter
    app.jinja_env.filters['urlencode'] = urlencode_filter
    app.jinja_env.filters['querystring_params'] = querystring_params_filter
    app.jinja_env.filters['file_embed_link'] = file_embed_link
    app.jinja_env.filters['numberfriendly'] = number_friendly_filter
    app.jinja_env.filters['pprint'] = pformat
    app.jinja_env.filters['numeric'] = numeric_filter
    app.jinja_env.filters['markdown'] = markdown_filter
    app.jinja_env.filters['emarkdown'] = escaped_markdown_filter
    app.jinja_env.filters['seoize'] = seoize_filter
    app.jinja_env.add_extension(HTMLCompress)

_attr_filter = "\"'" # Ampersand en primer lugar para no reescaparlo
def escaped_markdown_filter(text):
    return escape(markdown(text, output_format="html5"))

def markdown_filter(text):
    return Markup(markdown(text, output_format="html5"))

def numeric_filter(v):
    if isinstance(v, basestring) and v.isdigit():
        return int(v)
    return int(bool(v))

format_cache = {}
def number_size_format_filter(size, lang=None):
    '''
    Formatea un tamaño de fichero en el idioma actual
    '''
    if not size:
        return ""
    elif int(float(size))==0:
        return "0 B"

    if not lang:
        lang = g.lang

    if lang in format_cache:
        decimal_sep, group_sep = format_cache[lang]
    else:
        decimal_sep, group_sep = format_cache[lang] = (get_decimal_symbol(lang), get_group_symbol(lang))

    try:
        if float(size)<1000: # no aplica para los bytes
            return str(size)+" B"
        else:
            size = log(float(size),1024)
            number = 1024**(size-int(size))

            fix=0
            if number>=1000: #para que los tamaños entre 1000 y 1024 pasen a la unidad siguiente
                number/=1024
                fix=1

            # parte decimal
            dec_part = int((number-int(number))*100)
            dec_part = "" if dec_part==0 else decimal_sep+"0"+str(dec_part) if dec_part<10 else decimal_sep+str(dec_part)

            # genera salida
            return ''.join(
                reversed([c + group_sep if i != 0 and i % 3 == 0 else c for i, c in enumerate(reversed(str(int(number))))])
            ) + dec_part + (" KiB"," MiB"," GiB"," TiB")[int(size)-1+fix]
    except BaseException as e:
        logging.exception(e)
        return ""


def number_format_filter(number):
    '''
    Formatea un numero en el idioma actual
    '''
    try:
        return format_number(number, g.lang)
    except BaseException as e:
        logging.exception(e)
        return ""

def url_search_filter(new_params, args=None, delete_params=[]):
    '''
    Devuelve los parametros para generar una URL de busqueda
    '''

    # filtros actuales sin parametros eliminados
    filters = {key:value for key, value in args.iteritems() if key not in delete_params} if "all" not in delete_params else {"q":args["q"]} if "q" in args else {}

    # añade paramentros nuevos
    if "query" in new_params:
        filters["q"] = u(new_params["query"])

    if 'src' in new_params:
        active_srcs = g.active_srcs
        new_src = new_params["src"]

        ''' se genera el nuevo enlace teniendo en cuenta los origenes activos por tipo de origen
         - hacer click en un tipo de origen lo activan o desactivan completamente
         - hacer click en un origen lo activa o desactiva, teniendo en cuenta si el tipo de origen estaba activado
           o si toca activar todo el tipo
         - tiene en cuenta solo los origenes visibles en el filtro
        '''

        # streaming
        has_streaming, toggle_streaming = "streaming" in active_srcs, "streaming" == new_src
        streamings = [] if has_streaming and toggle_streaming else ["streaming"] if toggle_streaming else [value for value in g.visible_sources_streaming if (value==new_src)^(value in active_srcs)]
        if streamings==g.visible_sources_streaming: streamings = ["streaming"] # activa todo el tipo de origen

        # download
        has_download, toggle_download = "download" in active_srcs, "download" == new_src
        downloads = [] if has_download and toggle_download else ["download"] if toggle_download else [value for value in g.visible_sources_download if (value==new_src)^(value in active_srcs)]
        if downloads==g.visible_sources_download: downloads = ["download"] # activa todo el tipo de origen

        # p2p
        has_p2p, toggle_p2p = "p2p" in active_srcs, "p2p" == new_src
        p2ps = [] if has_p2p and toggle_p2p else ["p2p"] if toggle_p2p else [value for value in g.sources_p2p if (value==new_src)^(value in active_srcs)]
        if p2ps==g.sources_p2p: p2ps = ["p2p"] # activa todo el tipo de origen

        filters["src"] = streamings + downloads + p2ps
    if 'type' in new_params:
        filters["type"] = [value for value in FILTERS["type"] if (value==new_params["type"])^(value in filters["type"])] if "type" in filters else [new_params["type"]]
    if 'size' in new_params:
        filters["size"] = new_params["size"]

    # si estan todos los tipos activados en type o src es como no tener ninguno
    if "type" in filters and (not filters["type"] or all(atype in filters["type"] for atype in FILTERS["type"])):
        del filters["type"]
    if "src" in filters and (not filters["src"] or all(src in filters["src"] for src in FILTERS["src"].iterkeys())):
        del filters["src"]

    # separa query
    if "q" in filters:
        query = filters["q"].replace(" ","_") if filters["q"] else u""
        del filters["q"]
    else:
        query = u""

    # genera url de salida
    if filters:
        return g.search_url + quote_plus(query.encode('utf8')) + "/" + "/".join(param+":"+",".join(filters[param]) for param in ["type", "src", "size"] if param in filters)
    else:
        return g.search_url + quote_plus(query.encode('utf8'))

def querystring_params_filter(params):
    '''
    Genera una cadena querystring a partir de una lista de parámetros
    '''
    return "&".join("%s=%s"%(key,value) for key, value in params.iteritems() if value)

def format_timedelta_filter(date,granularity='second', threshold=.85):
    '''
    Devuelve la diferencia entre la fecha enviada y la actual
    '''
    TIMEDELTA_UNITS = (
        ('year',   3600 * 24 * 365),
        ('month',  3600 * 24 * 30),
        ('week',   3600 * 24 * 7),
        ('day',    3600 * 24),
        ('hour',   3600),
        ('minute', 60),
        ('second', 1)
    )
    delta=datetime.utcnow()-date
    if isinstance(delta, timedelta):
        seconds = int((delta.days * 86400) + delta.seconds)
    else:
        seconds = delta

    for unit, secs_per_unit in TIMEDELTA_UNITS:
        value = abs(seconds) / secs_per_unit
        if value >= threshold or unit == granularity:
            if unit == granularity and value > 0:
                value = max(1, value)
            value = int(round(value))
            rv = u'%s %s' % (value, _(unit))
            if value != 1:
                rv += u's'
            return rv

    return u''

def url_lang_filter(url, lang="en"):
    '''
    Devuelve la url con la parte del idioma indicada
    '''
    url = fixurl(url)
    if url.count("/") > 1:
        return "/%s/%s" % (lang, "/".join(url.split("/")[2:]))
    return "/%s" % lang

def urlencode_filter(s):
    return quote_plus(s.encode('utf8'))

def number_friendly_filter(number):
    number_pos = len(str(number))
    pos_round = (number_pos-1)/2
    return int(round(number/10.0**pos_round)*10**pos_round)

def seoize_filter(text, separator, is_url, max_length=None):
    return seoize_text(text, separator, is_url, max_length)




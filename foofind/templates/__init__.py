# -*- coding: utf-8 -*-
from flask import g, request, url_for
from flask.ext.babel import gettext as _
from babel.numbers import get_decimal_symbol, get_group_symbol
from math import log
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
    app.jinja_env.filters['search_params'] = search_params_filter
    app.jinja_env.filters['format_timedelta'] = format_timedelta_filter
    app.jinja_env.filters['url_lang'] = url_lang_filter
    app.jinja_env.filters['urlencode'] = urlencode_filter
    app.jinja_env.filters['querystring_params'] = querystring_params_filter
    app.jinja_env.filters['file_embed_link'] = file_embed_link
    app.jinja_env.filters['numberfriendly'] = number_friendly_filter
    app.jinja_env.filters['pprint'] = pformat
    app.jinja_env.filters['top_filters_src'] = top_filters_src
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
        size = log(float(size),1024)
        number = 1024**(size-int(size))
        fix=0
        if number>1000: #para que los tamaños entre 1000 y 1024 pasen a la unidad siguiente
            number/=1024
            fix=1

        # parte decimal
        dec_part = int((number-int(number))*100)
        dec_part = "" if dec_part==0 else decimal_sep+"0"+str(dec_part) if dec_part<10 else decimal_sep+str(dec_part)

        # genera salida
        return ''.join(
            reversed([c + group_sep if i != 0 and i % 3 == 0 else c for i, c in enumerate(reversed(str(int(number))))])
        ) + dec_part + (" B"," KiB"," MiB"," GiB"," TiB")[int(size)+fix]
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

def search_params_filter(new_params, delete_params=[], args=None, extra_sources=[]):
    '''
    Devuelve los parametros para generar una URL de busqueda
    '''
    if not args:
        args = request.args

    srcs=FILTERS["src"].copy().keys()+extra_sources
    types=FILTERS["type"]
    query = u(new_params["query"] if "query" in new_params else args["q"] if "q" in args else "").replace(" ","_")
    p={"query":query, "filters":{}}





    for param in ('src','type','size','page','alt'): #se recorren todos los parametros para guardarlos en orden
        if param in new_params: #añadir el parametro si es necesario
            if param=='src' or param=='type': #parametros concatenables
                if param in args: #recorre parametros en orden, añadiendo los de args y el nuevo valor se quita o se añade segun este en args
                    params = [value for value in (srcs if param=='src' else types) if (value==new_params[param])^(value in args[param])]
                    if params:
                        p["filters"][param] = params
                else:
                    p["filters"][param] = [new_params[param]]
            else: #parametros no concatenables
                p["filters"][param] = new_params[param]
        #mantener el parametro si ya estaba
        elif "all" not in delete_params and param not in delete_params and param in args:
            p["filters"][param] = args[param]

    if "type" in p["filters"] and p["filters"]["type"]==types: #si estan todos los tipos activados es como no tener ninguno
        del p["filters"]["type"]
    elif "src" in p["filters"] and p["filters"]["src"]==FILTERS["src"].keys(): #idem con los srcs
        del p["filters"]["src"]

    if p["filters"]: #unir los filtros
        p["filters"]="/".join(param+":"+(",".join(value) if param in ["type", "src", "size"] else value) for param, value in p["filters"].iteritems())
    else: #necesario para no devolver lista vacia
        del p["filters"]

    return p

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

def top_filters_src(values):
    '''
    Genera la lista de origenes a mostrar en la parte de arriba de la busqueda
    '''
    sources_names=dict((v,k) for k,v in g.sources_names.items()) #intercambiar clave y valor
    #obtener todos los sources
    all_sources=set()
    for value in values:
        if value in sources_names:
            all_sources.add(sources_names[value])
        elif value==_("other_streamings"):
            all_sources.add(_("other_streamings"))
        elif value==_("other_direct_downloads"):
            all_sources.add(_("other_direct_downloads"))
    #obtener los que hay de cada tipo
    streaming=list(all_sources&set(g.sources_streaming+[_("other_streamings")])) or (["Streaming"] if "Streaming" in values else [])
    download=list(all_sources&set(g.sources_download+[_("other_direct_downloads")])) or ([_("direct_downloads")] if _("direct_downloads") in values else [])
    p2p=list(all_sources&set(g.sources_p2p)) or (["P2P"] if "P2P" in values else [])
    #devolverlos concatenados
    return " - ".join(([_("some_streamings")] if len(streaming)>2 else streaming)+([_("some_downloads")] if len(download)>2 else download)+p2p)

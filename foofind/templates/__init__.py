# -*- coding: utf-8 -*-
from flask import g, request, url_for
from flask.ext.babel import gettext as _
from babel.numbers import format_decimal, get_decimal_symbol
from math import log
from datetime import datetime,timedelta
from foofind.utils.htmlcompress import HTMLCompress
from foofind.utils import u, fixurl
from foofind.blueprints.api import file_embed_link
from urllib import quote_plus
from pprint import pformat

# Registra filtros de plantillas
def register_filters(app):
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
    app.jinja_env.add_extension(HTMLCompress)

def number_size_format_filter(size):
    '''
    Formatea un tamaño de fichero en el idioma actual
    '''
    size=log(float(size),1024)
    number=round(1024**(size-int(size)), 2)
    fix=0
    if number>1000: #para que los tamaños entre 1000 y 1024 pasen a la unidad siguiente
        number/=1024
        fix=1

    int_part=format_decimal(number, locale=g.lang)
    dec_sep=get_decimal_symbol(locale=g.lang)
    if dec_sep in int_part:
        int_part, dec_part = int_part.split(dec_sep)
        if len(dec_part) > 2: # format_decimal sufre de problemas de precisión
            dec_part = str(round(float("0.%s" % dec_part), 2))[2:]

        return "%s%s%s %s" % (int_part, dec_sep, dec_part.ljust(2, "0"), ("B","KiB","MiB","GiB","TiB")[int(size)+fix])
    else:
        return "%s %s" % (int_part, ("B","KiB","MiB","GiB","TiB")[int(size)+fix])

def number_format_filter(number):
    '''
    Formatea un numero en el idioma actual
    '''
    return format_number(number, g.lang)

def search_params_filter(new_params, delete_params=[], args=None, extra_sources=[]):
    '''
    Devuelve los parametros para generar una URL de busqueda
    '''
    if not args:
        args = request.args

    srcs=['streaming','download','p2p']+extra_sources
    types=['audio','video','image','document','software']
    query = u(new_params["query"] if "query" in new_params else args["q"] if "q" in args else "").replace(" ","_")
    p={"query":query, "filters":{}}
    for param in ('src','type','size','page','alt'): #se recorren todos los parametros para guardarlos en orden

        #añadir el parametro si es necesario
        if param in new_params:
            if param=='src' or param=='type': # parametros concatenables
                if param in args:
                    # recorre parametros en orden, añadiendo los de args y el nuevo valor se quita o se añade segun este en args
                    params = [value for value in (srcs if param=='src' else types) if (value==new_params[param])^(value in args[param])]
                    if params:
                        p["filters"][param] = params
                else:
                    p["filters"][param] = [new_params[param]]

            else: # parametros no concatenables
                p["filters"][param] = new_params[param]

        # mantener el parametro si ya estaba
        elif "all" not in delete_params and param not in delete_params and param in args:
            p["filters"][param] = args[param]

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

def format_timedelta_filter(date,granularity='second', threshold=.85, locale=""):
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
    locale=g.lang
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

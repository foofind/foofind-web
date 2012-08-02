# -*- coding: utf-8 -*-
from flask import g, request
from flaskext.babel import gettext as _
from babel.numbers import format_decimal, get_decimal_symbol
from math import log
from datetime import datetime,timedelta
from foofind.utils.htmlcompress import HTMLCompress
from foofind.utils import u, fixurl

# Registra filtros de plantillas
def register_filters(app):
    app.jinja_env.filters['numberformat'] = number_format_filter
    app.jinja_env.filters['numbersizeformat'] = number_size_format_filter
    app.jinja_env.filters['search_params'] = search_params_filter
    app.jinja_env.filters['format_timedelta'] = format_timedelta_filter
    app.jinja_env.filters['url_lang'] = url_lang_filter
    app.jinja_env.add_extension(HTMLCompress)

def number_size_format_filter(size):
    '''
    Formatea un tamaño de fichero en el idioma actual
    '''
    size=log(float(size),1024)
    decsep = get_decimal_symbol(locale=g.lang)
    intpart = format_decimal(round(1024**(size-int(size)), 2), locale=g.lang)
    if decsep in intpart:
        intpart, decpart = intpart.split(decsep)
        if len(decpart) > 2: # format_decimal sufre de problemas de precisión
            decpart = str(round(float("0.%s" % decpart), 2))[2:]
        return "%s%s%s %s" % (intpart, decsep, decpart.ljust(2, "0"), ("B","KiB","MiB","GiB","TiB")[int(size)])
    return "%s%s00 %s" % (intpart, decsep, ("B","KiB","MiB","GiB","TiB")[int(size)])

def number_format_filter(number):
    '''
    Formatea un numero en el idioma actual
    '''
    return format_number(number, g.lang)

def search_params_filter(new_params, delete_params=[], args=None):
    '''
    Devuelve los parametros para generar una URL de busqueda
    '''
    if not args: args = request.args
    p={}
    for param in ['q','src','type','size','year','brate','page','alt']:
        if param in new_params:
            p[param]=new_params[param]
        elif "all" not in delete_params and param not in delete_params and param in args:
            p[param]=args[param]
        else:
            p[param]=None
    return p

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

def url_lang_filter(url, lang):
    '''
    Devuelve la url con la parte del idioma indicada
    '''
    url = fixurl(url)
    if url.count("/") > 1:
        return "/%s/%s" % (lang, "/".join(url.split("/")[2:]))
    return "/%s" % lang

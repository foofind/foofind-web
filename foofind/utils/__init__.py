# -*- coding: utf-8 -*-

import re
import os
import bson
import logging
import chardet
import pymongo
from base64 import b64encode, b64decode
from os.path import isfile, isdir
from urlparse import urlparse
from functools import wraps
from flask import make_response

def bin2mid(binary):
    '''
    Recibe una cadena binaria con el id y retorna un ObjectId
    '''
    return bson.objectid.ObjectId(binary.encode("hex"))

def mid2bin(objectid):
    '''
    Valida el objectid y lo devuelve como cadena binaria
    '''
    return str(bson.objectid.ObjectId(objectid)).decode("hex")

def bin2hex(binary):
    '''
    Recibe una cadena binaria con el id y retorna un hexadecimal
    '''
    return binary.encode("hex")

def hex2mid(hexuri):
    '''
    Convierte un identificador en formato hexadecimal a objeto
    identificador de MongoDB.
    '''
    return bson.objectid.ObjectId(hexuri)

def mid2hex(objectid):
    '''
    Valida el objectid y devuelve la representación hexadecimal
    '''
    return str(bson.objectid.ObjectId(objectid))

def mid2url(objectid):
    '''
    Valida el objectid y lo retorna en base64
    '''
    return b64encode(mid2bin(objectid), "!-")

def url2mid(b64id):
    '''
    Recibe un id de mongo en base64 y retorna ObjectId
    '''
    return bin2mid(b64decode(str(b64id), "!-"))

def fileurl2mid(url):
    '''
    Recibe una url de fichero de foofind y retorna ObjectId
    '''
    return url2mid(str(urlparse(url).path.split("/")[3]))

def lang_path(lang, ext="po"):
    '''
    Devuelve la ruta de la traducción del idioma pedido o None si no existe
    '''
    path = "foofind/translations/%s/LC_MESSAGES/messages.%s" % (lang, ext)
    return path if isfile(path) else None

def expanded_instance(cls, attrs, *args, **kwargs):
    '''
    Crea una clase heredando de la clase dada, con los atributos
    dados como diccionario, y retorna la instancia creada con los siguientes
    atributos.

    @type cls: object / type
    @param cls: objeto del que heredar

    @type attrs: dict-like
    @param attrs: Diccionario con las propiedades a añadir

    @param *args: argumentos que pasar al constructor
    @param *kwargs: argumentos con nombre que pasar al constructor

    @rtype: Objeto Expanded%(cls.__name__}s
    @return: Instancia
    '''
    return type("Expanded%s" % cls.__name__, (cls,), attrs)(*args, **kwargs)


def touch_path(path, pathsep=os.sep):
    '''
    Función que se asegura que una ruta de directorios existe, de lo
    contrario la crea.
    '''
    splited = path.split(pathsep)
    for i in xrange(2 if path.startswith("/") else 1, len(splited)+1):
        part = pathsep.join(splited[:i])
        if not isdir(part):
            os.mkdir(part)


re_sec = re.compile(r'\d+\s?(sec|secs|seconds)')
re_sec2 = re.compile(r'(seconds|secs|sec)')
re_min = re.compile(r'\d+\s?(min|mins|minutes)')
re_min2 = re.compile(r'(min|mins|minutes)')
re_hour = re.compile(r'\d+\s?(h|hour|hours|hr)')
re_hour2 = re.compile(r'(h|hour|hours|hr)')
def to_seconds(time_string):
    '''
    Convierte la duración de texto a segundos si es necesario
    por reset
    '''
    if not isinstance(time_string, basestring): return time_string
    if not ("sec" in time_string or "ms" in time_string):
        try:
            return int(time_string)
        except BaseException as e:
            logging.exception(e)
            return 0

    secs = 0
    match_obj = re_sec.search(time_string)

    if match_obj is not None:
        temp = match_obj.group()
        temp = re_sec2.sub('', temp)
        secs += int(temp)

    else:
        return secs

    match_obj = re_min.search(time_string)

    if match_obj is not None:
        temp = match_obj.group()
        temp = re_min2.sub('', temp)
        secs += (int(temp) * 60)

    match_obj = re_hour.search(time_string)

    if match_obj is not None:
        temp = match_obj.group()
        temp = re_hour2.sub('', temp)
        secs += (int(temp) * 3600)

    return secs

def multipartition(x, s):
    '''
    Recibe string y los divide por una lista de separadores incluyendo
    los separadores.

    @type x: basestring
    @param x: string a dividir

    @type s: iterable
    @param s: separadores

    @yield string
    '''
    if s:
        for p in ((x,) if isinstance(x, basestring) else x):
            while s[0] in p:
                p0, p1, p = p.partition(s[0])
                for m in multipartition((p0, p1), s[1:]):
                    yield m
            if p:
                for m in multipartition(p, s[1:]):
                    yield m
    elif isinstance(x, basestring):
        yield x
    else:
        for i in x:
            yield i

def generator_with_callback(iterator, callback):
    '''
    Recibe un iterador y ejecuta un callback
    '''
    for i in iterator:
        yield i
    callback()

def end_request(r, conn=None):
    if isinstance(r, pymongo.cursor.Cursor):
        return generator_with_callback(r, r.collection.database.connection.end_request)
    elif conn:
        conn.end_request()
        return r
    raise AttributeError("Cannot obtain connection from %s" % r)

def u(txt):
    ''' Parse any basestring (ascii str, encoded str, or unicode) to unicode '''
    if isinstance(txt,unicode):
        return txt
    else:
        try:
            return unicode(txt, chardet.detect(txt)["encoding"])
        except:
            pass
        return unicode("")

def fixurl(url):
    '''
    Si recibe una url interna con basura, intenta recuperarla.

    @return url válida en unicode
    '''
    # http://foofind.is/en/download/ZVbSk3bxVF9dgPBk/asdf%20style%20qwerty.MP4.html
    if url.startswith("http"):
        url = "/%s" % "/".join(url.split("/")[3:])
    elif not url.startswith("/"):
        return url # Uri no relativa

    # TODO: si algún día se necesita usar para urls externas
    # if not url.startswith("/"):
    #
    #    for i in ("http://foofind.", "https://foofind.", "http://www.foofind.", "https://www.foofind.", "http://localhost"):
    #        if url.startswith(i):
    #            # Si la url es nuestra, relativizamos
    #            url = "/%s" % "/".join(url.split("/")[i.count("/")+1:])
    #            break
    #    else:
    #        # Significa que la url no es de foofind, no la arreglamos
    #        return url

    if url.count("/") < 3:
        return url

    url_splitted = url.split("/")

    if url_splitted[2] == "download":
        # Caso para download
        for possible_valid_url in (url, url[:url.rfind("/")]):
            unicode_url = u(possible_valid_url)
            if unicode_url:
                return unicode_url
    return url


def nocache(fn):
    @wraps(fn)
    def decorated_view(*args, **kwargs):
        resp = make_response(fn(*args, **kwargs))
        resp.cache_control.no_cache = True
        resp.cache_control.no_store = True
        resp.cache_control.must_revalidate = True
        return resp
    return decorated_view

# -*- coding: utf-8 -*-

import re
import os
import bson
import logging
import chardet
import pymongo
import random
import urllib2
from unicodedata import normalize
from base64 import b64encode, b64decode
from os.path import isfile, isdir
from urlparse import urlparse
from functools import wraps
from content_types import *
from foofind.utils.splitter import SEPPER


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

def hex2bin(hexuri):
    '''
    Recibe una cadena binaria con el id y retorna un hexadecimal
    '''
    return hexuri.decode("hex")

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
    return url2mid(urllib2.unquote(urlparse(url).path.split("/")[3]))

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

def check_capped_collections(db, capped_dict):
    '''
    Crea las colecciones capadas pasadas por parámetro en una base de datos de
    mongodb si no existen ya.

    @type db: pymongo.database
    @param db: Base de datos de pymongo

    @type capped_dict: dict
    @param capped_dict: diccionario con nombres de colecciones capadas como
                        clave y su tamaño como valor.
    '''
    collnames = db.collection_names()
    for capped, size in capped_dict.iteritems():
        if not capped in collnames:
            db.create_collection(capped, capped = True, max = size, size = size)

TIME_UNITS = {
    "ms":0.001,
    "millisecs":0.001,
    "milliseconds":0.001,
    "":1,
    "s":1,
    "sec":1,
    "secs":1,
    "seconds":1,
    "m":60,
    "min":60,
    "mins":60,
    "minutes":60,
    "h":3600,
    "hour":3600,
    "hours":3600,
    "hr":3600,
    }
FLOATCHARS = "0123456789-+."
def to_seconds(time_string):
    '''
    Convierte la duración de texto a segundos si es necesario

    @type time_string: str
    @param time_string: cadena representando tiempo

    @rtype float
    @return tiempo en segundos (cero si no se ha podido parsear)
    '''
    if not isinstance(time_string, basestring): return time_string
    if ":" in time_string:
        op = time_string.split(":")
        if all(i.isdigit() for i in op):
            lop = len(op)
            if lop == 2:
                return int(op[0])*60+float(op[1])
            elif lop == 3:
                return int(op[0])*3600+int(op[1])*60+float(op[2])
        logging.warn("No se puede parsear time_string", extra=time_string)
        return 0
    secs = 0
    t = []
    op = []
    for i in time_string:
        if op: # Si tengo una unidad (parcial o completa)
            if i in FLOATCHARS:
                # Empieza otro dígito, asumo que la unidad está completa
                if t:
                    # Hay un número en la pila, uso la unidad para pasar el número a segundos
                    secs += float("".join(t)) * TIME_UNITS.get("".join(op).strip(), 0)
                # Vacío la pila de la unidad e inicio la de número con el dígito actual
                del op[:]
                t[:] = (i,)
            else:
                # Añado otro caracter de la unidad
                op.append(i)
        elif i in FLOATCHARS: # Si es un dígito
            t.append(i)
        else: # Si el carácter es de una unidad
            op.append(i)
    if t:
        # He terminado la cadena, y queda por sumar
        secs += float("".join(t)) * TIME_UNITS.get("".join(op).strip(), 0)
    return secs

def multipartition(x, s):
    '''
    Recibe string y los divide por una lista de separadores incluyendo
    los separadores en el resultado.

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
    elif isinstance(txt, basestring):
        try:
            return unicode(txt, chardet.detect(txt)["encoding"])
        except:
            return unicode("")
    return unicode(txt)

def fixurl(url):
    '''
    Si recibe una url interna con basura, intenta recuperarla.

    @return url válida en unicode
    '''
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
    from flask import make_response
    @wraps(fn)
    def decorated_view(*args, **kwargs):
        resp = make_response(fn(*args, **kwargs))
        resp.cache_control.no_cache = True
        resp.cache_control.no_store = True
        resp.cache_control.must_revalidate = True
        return resp
    return decorated_view

def uchr(x):
    '''
    chr para unicode
    '''
    if x < 256:
        return (u"\\x%x" % x).decode("unicode_escape")
    elif x < 65536:
        return (u"\\u%04x" % x).decode("unicode_escape")
    return ""

_punct_re = re.compile(r'[\t !"#$%&\'()*\-/<=>?@\[\\\]^_`{|},.:]+')
def slugify(text, delim=u' ', return_separators=False):
    '''
    Genera una cadena solo con caracteres ASCII
    '''
    result = []
    separators = []
    for word in _punct_re.split(u(text.lower())):
        word = normalize('NFKD', word).encode('ascii', 'ignore')
        if word:
            result.append(word)
        else:
            separators.append(word)

    if return_separators:
        return unicode(delim.join(separators))
    else:
        return unicode(delim.join(result))

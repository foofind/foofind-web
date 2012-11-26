# -*- coding: utf-8 -*-

import re
import os
import bson
import logging
import chardet
import pymongo
import random
import urllib2
import traceback
import threading
import time
import ctypes
import sys
import trace
from unicodedata import normalize
from base64 import b64encode, b64decode
from os.path import isfile, isdir, join
from urlparse import urlparse
from functools import wraps
from content_types import *
from foofind.utils.splitter import SEPPER
from collections import OrderedDict
from itertools import izip, ifilter

try:
    import  ctypes
except ImportError:
    ctypes = None

class VALUE_UNSET(object):
    '''
    Tipo para ser usado como valor por defecto (como por ejemplo en el caso de
    que None sea un valor válido en un parámetro). No instanciable.
    '''
    def __init__(self):
        raise RuntimeError("%s is an static class, cannot be instanced" % self.__class__.__name__)

class LimitedSizeDict(OrderedDict):
    def __init__(self, *args, **kwds):
        self.size_limit = kwds.pop("size_limit", None)
        OrderedDict.__init__(self, *args, **kwds)
        self._check_size_limit()

    def __setitem__(self, key, value):
        OrderedDict.__setitem__(self, key, value)
        self._check_size_limit()

    def _check_size_limit(self):
        if self.size_limit is not None and len(self) > self.size_limit:
            logging.warn(
                "Superado límite de tamaño: %d" % self.size_limit,
                extra={"trace":traceback.extract_tb()})
            while len(self) > self.size_limit:
                self.popitem(last=False)

class Parallel(object):
    '''
    Objeto paralelizador de tareas.

    Recibe un iterable de tareas.
    Cada tarea es un iterable de tres elementos:
        - Callable,
        - Iterable de parámetros anónimos o argumentos,
        - Dict-like de parámetros con nombre.

    Se ejecuta al incializarlo.
    '''
    '''
    # Test de kill desactivado
    >>> def a(t):
    ...     time.sleep(t)
    ...     return t
    >>> p = Parallel((
    ...     (a, (1,)),
    ...     (a, (4,))
    ...     ), killable=True)
    >>> p.join_and_terminate(2)
    True
    >>> time.sleep(1) # El terminate puede tardar un poco
    >>> p.exceptions.count(None) # Debe haber una excepción
    1
    >>> p.output
    [1, None]
    >>> time.sleep(2)
    >>> p.output # Comprobamos si el valor ha cambiado
    [1, None]
    '''
    @property
    def killable(self):
        return self._killable

    def __init__(self, tasks, killable=False):
        '''
        Ejecuta las tareas dadas en paralelo.

        @type tasks: iterable
        @arg tasks: iterable de iterables con tres elementos: callable, args, kwargs.
        '''
        self._killed = False
        self._killable = killable
        self._threads = [
            threading.Thread(target=self._wrapper, args=(n, args))
            for n, args in enumerate(tasks)
            ]
        self._thread_ids = [None for i in self._threads]
        self.exceptions = [None for i in self._threads]
        self.output = [None for i in self._threads]
        for thread in self._threads:
            thread.start()

    def _globaltrace(self, frame, why, arg):
        return self._localtrace if why == "call" else None

    def _localtrace(self, frame, why, arg):
        if self._killed:
            sys.settrace(None)
            raise SystemExit
        return self._localtrace

    def terminate(self):
        '''
        Finaliza los threads
        '''
        if self._killable:
            self._killed = True
            #'''
            # Deadlock en thread.__stop
            for tid, thread in izip(self._thread_ids, self._threads):
                res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
                    ctypes.c_long(tid),
                    ctypes.py_object(SystemExit))
                if res == 0 and thread.is_alive():
                    raise SystemError("PyThreadState_SetAsyncExc failed due wrong thread.ident")
                elif res > 1:
                    ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid), None)
                    raise SystemError("PyThreadState_SetAsyncExc failed")
            '''
            #'''
        else:
            raise RuntimeError, "Parallel.terminate method called on a non killable Parallel"

    def _wrapper(self, n, args):
        try:
            # Parseo de argumentos
            if callable(args):
                task = args
                uargs = ()
                kwargs = {}
                callback = None
            else:
                la = len(args)
                task = args[0]
                uargs = args[1] if la > 1 and not args[1] is None else ()
                kwargs = args[2] if la > 2 and not args[2] is None else {}
                callback = args[3] if la > 3 else None

            # Ejecución
            if self._killable: sys.settrace(self._globaltrace) # Traza para terminate
            self._thread_ids[n] = self._threads[n].ident
            self.output[n] = task(*uargs, **kwargs)
        except (BaseException, SystemExit) as e:
            self.exceptions[n] = e
        finally:
            if callable(callback): callback()

    def failed(self):
        '''
        Retorna True si ha habido alguna excepción en las tareas.
        '''
        return self.exceptions.count(None) < len(self.exceptions)

    def join(self, timeout=None):
        '''
        Espera a que las tareas terminen, admite un timeout.
        Para saber si una tarea ha terminado por timeout, comprobar el método
        `is_alive`.

        @type timeout: int, float or None
        @param timeout: Tiempo máximo a esperar,
        '''
        if timeout:
            t0 = time.time()
            for thread in self._threads:
                thread.join(timeout)
                t1 = time.time()
                timeout -= t1 - t0  # Tiempo de timeout restante
                if timeout <= 0:
                    break
                t0 = t1
        else:
            for thread in self._threads:
                thread.join()

    def join_and_terminate(self, timeout):
        '''
        Espera a que las tareas terminen, con un timeout, tas el cual mata los
        hilos.

        @type timeout: int, float or None
        @param timeout: Tiempo máximo a esperar

        @rtype bool
        @return True si se han tenido que cerrar los hilos a la fuerza tras el timeout.
        '''
        self.join(timeout)
        if self.is_alive():
            self.terminate()
            return True
        return False

    def is_alive(self):
        '''
        Devuelve True si se están ejecutando las tareas.

        @rtype bool
        @return True si hay alguna tarea ejecutándose, o False
        '''
        for thread in self._threads:
            if thread.is_alive():
                return True
        return False

def userid_parse(user_id):
    '''
    Función para soportar la convivencia temporal de los ids de usuario en
    ObjectId e int
    '''
    if isinstance(user_id, basestring):
        if len(user_id) == 24: # Mongoid
            return hex2mid(user_id)
    elif isinstance(user_id, bson.objectid.ObjectId):
        return user_id
    return int(user_id)

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
    return bin2url(mid2bin(objectid))

def bin2url(binary):
    '''
    Valida el objectid y lo retorna en base64
    '''
    return b64encode(binary, "!-")

def url2mid(b64id):
    '''
    Recibe un id de mongo en base64 y retorna ObjectId
    '''
    return bin2mid(url2bin(b64id))

def url2bin(b64id):
    '''
    Recibe un id de mongo en base64 y retorna ObjectId
    '''
    return b64decode(str(b64id), "!-")

def fileurl2mid(url):
    '''
    Recibe una url de fichero de foofind y retorna ObjectId
    '''
    idpart = urlparse(url).path.split("/download/")[1].split("/", 1)[0]
    return url2mid(urllib2.unquote(idpart))

def lang_path(lang, base_path, ext="po"):
    '''
    Devuelve la ruta de la traducción del idioma pedido o None si no existe
    '''
    path = join(base_path, "translations/%s/LC_MESSAGES/messages.%s" % (lang, ext))
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

def check_collection_indexes(db, indexes_dict):
    '''
    Comprueba que lass colecciones tienen los índices dados.

    @type db: pymong.database
    @param db: Base de datos de pymongo

    @type indexes_dict: dict
    @param indexes_dict: diccionario con nombres de collecciones como clave y
                         lista de diccionarios, como valor, con "key" como
                         lista de índices y el resto de campos como argumentos
                         por nombre.
    '''

    # BUG (upstream): ensure_index devuelve valor siempre, en contra de
    #                 lo dicho en la documentación.
    #                 Cerrado pero no corregido (no reproducible):
    #                  - https://jira.mongodb.org/browse/PYTHON-193
    #                 Workaround [1]: comprobar los índices con index_information

    for collection, indexes in indexes_dict.iteritems():
        # Workaround [1]
        collection_indexes = [indef["key"] for indef in db[collection].index_information().itervalues() if "key" in indef]
        for index_config_with_keys in indexes:
            # Workaround [1]
            if index_config_with_keys["key"] in collection_indexes: continue
            index_config = dict(index_config_with_keys)
            keys = index_config.pop("key")
            o = db[collection].ensure_index(keys, **index_config)
            if not o is None:
                logging.warn(
                    "Index %s created on collection %s" % (o, collection),
                    extra={
                        "collection": collection,
                        "config": index_config_with_keys
                        })



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
    extra = {"size":100000}
    for capped, data in capped_dict.iteritems():
        kwargs = extra.copy()
        if isinstance(data, dict): kwargs.update(data)
        elif isinstance(data, int):
            kwargs["max"] = data
            kwargs["size"] = data*512 # 512 bytes por documento
        if capped in collnames:
            options = db[capped].options()
            if not options.get("capped", False):
                logging.error(u"La colección (%s:%s).%s.%s no está capada."
                    % (db.connection.host, db.connection.port, db.name, capped)
                    )
            elif any(True for key, value in extra.iteritems() if key != "size" and options.get(key, VALUE_UNSET) != value ):
                # Comprobamos las diferencias en las opciones, obviando size
                # TODO(felipe): Analizar la viabilidad de rehacer automáticamente las capped collections que estén mal
                # db[capped].drop()
                # db.create_collection(capped, capped = True, **kwargs)

                logging.warn(
                    u"Diferencia encontrada en la configuración de capped collection en (%s:%s).%s.%s"
                        % (db.connection.host, db.connection.port, db.name, capped),
                    extra = {"in_app_config": kwargs, "current_config": options}
                    )
        else:
            db.create_collection(capped, capped = True, **kwargs)
        db.connection.end_request()

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
TIME_FLOATCHARS = "0123456789-+."
TIME_IGNORECHARS = ","
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
        if i in TIME_IGNORECHARS:
            pass
        elif op: # Si tengo una unidad (parcial o completa)
            if i in TIME_FLOATCHARS:
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
        elif i in TIME_FLOATCHARS: # Si es un dígito
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

    @type s: lista o str
    @param s: separadores

    @yield string

    >>> list(multipartition(".^this.is,a-spliteable^string.",".,-^"))
    ['', '.', '', '^', 'this', '.', 'is', ',', 'a', '-', 'spliteable', '^', 'string', '.', '']
    '''
    acum = []
    for char in x:
        if char in s:
            yield "".join(acum)
            del acum[:]
            yield char
        else:
            acum.append(char)
    yield "".join(acum)

def multisplit(x, s):
    '''
    Recibe string y los divide por una lista de separadores.

    @type x: basestring
    @param x: string a dividir

    @type s: lista o str
    @param s: separadores

    @yield string

    >>> list(multisplit(".^this.is,a-spliteable^string.",".,-^"))
    ['', '', 'this', 'is', 'a', 'spliteable', 'string', '']
    '''
    try:
        # Iterador con caracteres de x que son separadores
        gen = ifilter(s.__contains__, x)
        sc = gen.next()
        for sn in gen:
            x = x.replace(sn, sc)
        for p in x.split(sc):
            yield p
    except StopIteration:
        # Caso excepcional: ningún separador encontrado
        yield x

def generator_with_callback(iterator, callback):
    '''
    Recibe un iterador y ejecuta un callback
    '''
    for i in iterator:
        yield i
    callback()

def u(txt):
    ''' Parse any basestring (ascii str, encoded str, or unicode) to unicode '''
    if isinstance(txt, unicode):
        return txt
    elif isinstance(txt, basestring):
        try:
            return unicode(txt, chardet.detect(txt)["encoding"])
        except:
            return u""
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

def canonical_url(data):
    '''
    Genera la parte de la url de descarga canónica.

    @type data: dict
    @param data: Documento de fichero

    @rtype str
    @return Cadena lista para usar como url de descarga.
    '''

    fid = urllib2.quote(mid2url(data["_id"]))
    try:
        fname = "/%s.html" % urllib2.quote(
            ( name["n"] for crc, name in data["fn"].iteritems()
              if "n" in name and name["n"].strip()
              ).next().encode("utf8")[:512]
            )
    except StopIteration:
        fname = ""
    return "/en/download/%s%s" % (fid, fname)

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

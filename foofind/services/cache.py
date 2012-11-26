# -*- coding: utf-8 -*-

from flask.ext.cache import Cache as CacheBase
from flask import g, request
from functools import wraps
from hashlib import md5
from inspect import getsourcelines, currentframe
#from slimmer import html_slimmer
from foofind.utils import LimitedSizeDict

import logging, time, re

class ThrowFallback(Exception):
    '''
    Excepción para ser usada con Cache.fallback. No se registra la excepción.
    '''
    _enabled = type("ThrowFallbackEnabler", (object,), {})

class Cache(CacheBase):
    '''
    Objeto de caché heredado de Flask-cache con correcciones y mejoras.
    '''

    def __init__(self, *args, **kwargs):
        CacheBase.__init__(self, *args, **kwargs)
        self._local_cache = LimitedSizeDict(size_limit=10000)
        self._regexp_cache = LimitedSizeDict(size_limit=5000)

    def regexp(self, r):
        if not r in self._regexp_cache:
            self._regexp_cache[r] = re.compile(r)
        return self._regexp_cache[r]

    def local_get(self, key):
        '''
        Obtiene una clave del caché local para esta instancia de la aplicación.

        @type key: str
        @param key: clave de caché
        '''
        value, expiration = self._local_cache.get(key, (None, None))
        if expiration is None:
            if value is None and key is self._local_cache:
                del self._local_cache[key]
            return value
        elif expiration > time.time():
            return value
        del self._local_cache[key]
        return None

    def local_set(self, key, value, timeout=None):
        '''
        Asigna una clave del caché local para esta instancia de la aplicación.

        @type key: str
        @param key:
        '''
        self._local_cache[key] = (value, (time.time()+timeout) if timeout > 0 else None)

    def local_delete(self, key):
        '''
        Borra una clave del caché local para esta instancia de la aplicación.

        @type key: str
        @param key: clave de caché
        '''
        if key in self._local_cache:
            del self._local_cache[key]

    def local_delete_multi(self, *keys):
        '''
        Borra varias claves de caché local para esta instancia de la aplicación.

        @type key: str
        @param key: clave de caché
        '''
        for key in keys:
            if key in self._local_cache:
                del self._local_cache[key]

    '''
    @type cacheme: bool
    @ivar cacheme: si se establece a False, los decoradores no guardarán caché
                   en el contexto actual (se requiere contexto de Flask).
    '''
    @property
    def cacheme(self):
        if hasattr(g, "cache_cacheme"):
            return bool(g.cache_cacheme)
        return True

    @cacheme.setter
    def cacheme(self, v):
        try:
            g.cache_cacheme = bool(v)
        except RuntimeError as e:
            # Si no hay contexto no hace falta controlar el caché
            logging.debug("cache.cacheme accessed without context", extra={"e":e})

    @property
    def skip(self):
        if hasattr(g, "cache_skip"):
            return bool(g.cache_skip)
        return False

    @skip.setter
    def skip(self, v):
        g.cache_skip = v

    @classmethod
    def throw_fallback(cls):
        '''
        Función de conveniencia que lanza una excepción que hace saltar el
        fallback si registrar excepción.
        '''
        frame = currentframe()
        if not frame is None:
            # Comprobación: la pila no es accesible en todas las implementaciones de python
            while frame:
                if frame.f_locals.get("__", None) is ThrowFallback._enabled:
                    break
                frame = frame.f_back
            else:
                raise RuntimeError, "Cache.throw_fallback called outside any fallback scope"
        raise ThrowFallback

    def clear(self):
        '''Borrar todas las claves de caché.'''
        self.cache.clear()

    def cached(self, timeout=None, key_prefix='view%s', unless=None):
        '''
        Decorador para cachear vistas

        @type timeout: int o None
        @param timeout: segundos que debe de mantenerse el caché, si es None se
                        cacheará todo el tiempo que se pueda.

        @type key_prefix: str
        @param key_prefix: prefijo (formato) para la clave de memcache,
                           Se admite formato (con clave) de strings, el objeto
                           request será usado como objeto mapeado.
                           Si contiene %s, será reemplazado por request.path
                           (por retrocompatibilidad).
        @type unless: None o callable
        @param unless: Si es callable, y el resultado de la llamada es True,
                       no se cacheará.
        '''
        def decorator(f):
            uncached_fnc = self._get_uncached(f)

            @wraps(f)
            def decorated_function(*args, **kwargs):

                if callable(unless) and unless() is True:
                    return f(*args, **kwargs)

                cache_key = decorated_function.make_cache_key(*args, **kwargs)

                rv = self.get(cache_key) if not self.skip else None
                if rv is None:
                    rv = f(*args, **kwargs)
                    '''# Optimización: Si la respuesta es html, minificamos
                    if hasattr(rv, "mimetype") and response.mimetype == "text/html":
                        rv.data = html_slimmer(rv.data)
                    elif isinstance(rv, basestring) and rv.startswith("<!DOCTYPE html>"):
                        rv = html_slimmer(rv)
                    '''
                    if self.cacheme:
                        self.set(cache_key, rv, timeout=decorated_function.cache_timeout)
                return rv

            def make_cache_key(*args, **kwargs):
                if self._self_given(args, uncached_fnc):
                    args = args[1:]
                if callable(key_prefix):
                    cache_key = key_prefix()
                elif "%s" in key_prefix:
                    cache_key = key_prefix % request.path
                else:
                    cache_key = key_prefix % request.__dict__
                return cache_key.encode('utf-8')

            decorated_function.uncached = f
            decorated_function.cache_timeout = timeout
            decorated_function.make_cache_key = make_cache_key

            return decorated_function
        return decorator

    def cached_GET(self, timeout=None, params=None, unless=None):
        '''
        Cachea vista teniendo en cuenta los argumentos GET

        @type timeout: int
        @param timeout: segundos de vida de la clave

        @type params: iterable
        @param params: iterable con los parámetros GET que se tendrán en cuenta,
                      si es None, se tendrán todos en cuenta, y si es un
                      diccionario, los valores que sean "callable" serán ejecutados
                      con el valor del parámetro correspondiente, y su retorno
                      será el valor que se usará como clave de cache.

        @type unless: callable
        @param unless: será ejecutado sin parámetros y si su retorno es True
                      no se cacheará.
        '''
        if hasattr(params, "__contains__"):
            if hasattr(params, "__getitem__"):
                key_prefix = lambda: "view%s/%s" % (
                    request.path,
                    md5(repr({i:(params[i](request.args[i]) if callable(params[i]) else request.args[i])
                        for i in request.args if i in params})).hexdigest()
                    )
            else:
                key_prefix = lambda: "view%s/%s" % (
                    request.path,
                    md5(repr({i:request.args[i] for i in request.args if i in params})).hexdigest()
                    )
        else:
            key_prefix = lambda: "view%s/%s" % (
                request.path,
                md5(repr(dict(request.args))).hexdigest()
                )
        return self.cached(
            timeout,
            key_prefix=key_prefix,
            unless=unless
            )

    def _fnc_name(self, f):
        '''
        Genera un nombre único para la función dada
        '''
        try:
            lineno = getsourcelines(f)[1]
        except IOError:
            lineno = "unknown"
        return "%s:%s:%s" % (
            f.__module__ if hasattr(f, "__module__") else "__main__",
            lineno, f.__name__)

    def _self_given(self, args, uncached_fnc):
        '''
        Detecta si el primer elemento de args es "self" para uncached_fnc
        '''
        if args:
            namesake = getattr(args[0], uncached_fnc.__name__, None)
            return namesake and self._get_uncached(namesake) == uncached_fnc
        return False

    def _get_uncached(self, f):
        '''
        Retorna, recursivamente, la función original no decorada.
        '''
        if hasattr(f, "uncached"):
            return self._get_uncached(f.uncached)
        return f

    def local_memoize(self, timeout=None):
        '''
        Decorador para cachear en el hilo local. Se tendrán en cuenta los parámetros.

        @type timeout: int o None
        @param timeout: segundos que debe de mantenerse el caché, si es None se
                        cacheará todo el tiempo que se pueda.

        '''
        def memoize(f):
            funcname = self._fnc_name(f)
            uncached_fnc = self._get_uncached(f)

            @wraps(f)
            def decorated_function(*args, **kwargs):
                cache_key = decorated_function.make_cache_key(*args, **kwargs)
                rv = self.local_get(cache_key) if not self.skip else None
                if rv is None:
                    rv = f(*args, **kwargs)
                    if self.cacheme:
                        self.local_set(cache_key, rv,
                                       timeout=decorated_function.cache_timeout)
                return rv

            def make_cache_key(*args, **kwargs):
                if self._self_given(args, uncached_fnc):
                    args = args[1:]
                return "memoized/%s/%s" % (
                    funcname,
                    md5("%s:%s" % (repr(args), repr(kwargs))).hexdigest()
                    )

            def flush(*args, **kwargs):
                cache_key = decorated_function.make_cache_key(*args, **kwargs)
                self.local_delete(cache_key)

            decorated_function.flush = flush
            decorated_function.uncached = f
            decorated_function.cache_timeout = timeout
            decorated_function.make_cache_key = make_cache_key

            return decorated_function
        return memoize

    def memoize(self, timeout=None):
        '''
        Decorador para cachear funciones. Se tendrán en cuenta los parámetros.

        @type timeout: int o None
        @param timeout: segundos que debe de mantenerse el caché, si es None se
                        cacheará todo el tiempo que se pueda.

        '''
        def memoize(f):
            funcname = self._fnc_name(f)
            uncached_fnc = self._get_uncached(f)

            @wraps(f)
            def decorated_function(*args, **kwargs):
                cache_key = decorated_function.make_cache_key(*args, **kwargs)
                rv = self.get(cache_key) if not self.skip else None
                if rv is None:
                    rv = f(*args, **kwargs)
                    if self.cacheme:
                        self.set(cache_key, rv,
                                       timeout=decorated_function.cache_timeout)
                        self._memoized.append((funcname, cache_key))
                return rv

            def make_cache_key(*args, **kwargs):
                if self._self_given(args, uncached_fnc):
                    args = args[1:]
                return "memoized/%s/%s" % (
                    funcname,
                    md5("%s:%s" % (repr(args), repr(kwargs))).hexdigest()
                    )

            def flush(*args, **kwargs):
                cache_key = decorated_function.make_cache_key(*args, **kwargs)
                self.delete(cache_key)

            decorated_function.flush = flush
            decorated_function.uncached = f
            decorated_function.cache_timeout = timeout
            decorated_function.make_cache_key = make_cache_key

            return decorated_function
        return memoize

    def fallback(self, errors=(), timeout=0):
        '''
        Decorador para retornar caché en caso de error.

        @type errors: iterable o type
        @param errors: lista de tipos de errores que serán capturados para
                       retornar una respuesta cacheada, si está vacío, se
                       capturarán todos los errores.

        @type timeout: int o None
        @param timeout: segundos que debe de mantenerse el caché, si es cero
                        (por defecto) se cacheará todo el tiempo que se pueda,
                        si es None se cacheará según el default_timeout.

        '''
        def memoize(f):
            funcname = self._fnc_name(f)
            uncached_fnc = self._get_uncached(f)

            @wraps(f)
            def decorated_function(*args, **kwargs):
                __ = ThrowFallback._enabled
                cache_key = decorated_function.make_cache_key(*args, **kwargs)
                try:
                    rv = f(*args, **kwargs)
                    self.cache.set(cache_key, rv, timeout=decorated_function.cache_timeout)
                    return rv
                except ThrowFallback:
                    # Se ha ordenado usar el fallback
                    self.cacheme = False
                except BaseException as e:
                    if errors and not isinstance(e, errors):
                        # No se ha especificado capturar el error, propagamos
                        raise
                    self.cacheme = False
                    logging.exception(e)
                return self.cache.get(cache_key)

            def make_cache_key(*args, **kwargs):
                if self._self_given(args, uncached_fnc):
                    args = args[1:]
                return "fallback/%s/%s" % (
                    funcname,
                    md5("%s:%s" % (repr(args), repr(kwargs))).hexdigest()
                    )

            def flush(*args, **kwargs):
                cache_key = decorated_function.make_cache_key(*args, **kwargs)
                self.delete(cache_key)

            decorated_function.flush = flush
            decorated_function.uncached = f
            decorated_function.cache_timeout = timeout
            decorated_function.make_cache_key = make_cache_key

            return decorated_function
        return memoize

# -*- coding: utf-8 -*-

from flask import Blueprint as Blueprint, request, g, make_response
from collections import OrderedDict
from functools import wraps
from random import randint
from time import time
from foofind.utils import u, pyon, VALUE_UNSET
import newrelic.agent

from . import logging

#TODO(felipe): Comentar funcionalidad y ejemplo de uso

ALTERNATIVE_NOT_FOUND = type("AlternativeNotFound", (), {})
DEFAULT_CONFIG = type("UseDefaultConfig", (), {})

class RandomSelector(object):
    def __init__(self, **defaults):
        self._random_alternatives = []
        self._random_alternatives_len = 0
        self.config(defaults)

    _probability = None
    @property
    def probability(self):
        return self._probability

    @probability.setter
    def probability(self, x):
        self._random_alternatives = [k for k, v in x.iteritems() for i in xrange(0, v)]
        self._random_alternatives_lm1 = len(self._random_alternatives) - 1
        self._probability = x

    def config(self, config):
        self.probability = config.get("probability", None)
        return self

    def __call__(self, endpoint, cargs, ckwargs):
        return self._random_alternatives[randint(0, self._random_alternatives_lm1)]

class ParamSelector(object):
    _param_types = {
        "int": int,
        "float": float,
        "bool": lambda x: x.lower() in ("1","-1","true"),
        "str": str,
        "unicode": u,
        "chr": str,
        "unichr": u,
        "default": lambda x: x
        }
    _param_validators = {
        "int": lambda x: x.isdigit(),
        "float": lambda x: x.count() < 2 and x.replace(".","").isdigit(),
        "bool": lambda x: x.lower() in ("1","-1","true","false","0"),
        "chr": lambda x: len(x) == 1,
        "unichr": lambda x: len(x) == 1,
        "default": bool
        }

    def __init__(self, **defaults):
        self._random_alternatives = []
        self._random_alternatives_len = 0
        self.config(defaults)

    _param_parser = None
    _param_validator = None
    _param_type = None
    @property
    def param_type(self):
        return self._param_type

    @param_type.setter
    def param_type(self, x):
        if hasattr(x, "__name__"): x = x.__name__
        self._param_type = x
        self._param_parser = self._param_types.get(x, self._param_types["default"])
        self._param_validator = self._param_validators.get(x, self._param_validators["default"])

    def config(self, config):
        self.param_name = config.get("param_name", "alt")
        self.param_type = config.get("param_type", str)
        return self

    def __call__(self, endpoint, cargs, ckwargs):
        palt = request.args.get(self.param_name, None)
        if palt is not None and self._param_validator(palt):
            try:
                return self._param_parser(palt)
            except BaseException as e:
                logging.debug(e)
        return ALTERNATIVE_NOT_FOUND

class RememberSelector(object):
    def __init__(self, **defaults):
        self.config(defaults)

    def config(self, config):
        self.remember_id = config.get("remember_id", None)
        return self

    @property
    def cookie(self):
        if hasattr(g, "cookie_alternatives"):
            return g.cookie_alternatives
        elif "alternatives" in request.cookies:
            try:
                # Para evitar cookies problemáticas
                alternatives = pyon.loads(request.cookies["alternatives"].decode("hex"))
            except BaseException as e:
                alternatives = {}
        else:
            alternatives = {}
        g.cookie_alternatives = alternatives
        return alternatives

    def __call__(self, endpoint, cargs, ckwargs):
        cookie = endpoint if self.remember_id is None else self.remember_id
        return self.cookie.get(cookie, ALTERNATIVE_NOT_FOUND)

    def save(self, endpoint, alt):
        cookie = endpoint if self.remember_id is None else self.remember_id

        if self.cookie.get(cookie, ALTERNATIVE_NOT_FOUND) != alt:
            self.cookie[cookie] = alt
            Fooprint.set_cookie(
                "alternatives",
                pyon.dumps(self.cookie).encode("hex"),
                expires=time()+315576000
                )


class ManagedSelect(object):
    '''
    Gestión de alternativas, configurable, gestionado por ManagedSelectUpdater.
    La función de selección (atributo de sólo lectura select) devuelve un id
    aleatoriamente teniendo en cuenta las probabilidades relativas de cada id
    (definidas en la propiedad `probability`).

    Uso:
    >>> fooprint = Fooprint("test")
    >>> @fooprint.route("/test", aid=1)
    ... def test1():
    ...     return "test1"
    >>> @test1.alternative(2)
    ... def test2():
    ...     return "test2"
    >>> test1.select = ManagedSelect(
    ...     # Determinista, alternativa viene dada por parámetro "alt", pero si no viene se sacará de sesión
    ...     methods = "param, remember", # La alternativa se recordará para cuando no se especifique el parámetro 'alt'
    ...     param_name = "alt",   # El parámetro GET 'alt' elegirá alternativa
    ...     param_type = int      # El parámetro deberá ser un int
    ...     )
    >>> test1.select = ManagedSelect(
    ...     # Determinista, alternativa viene dada por parámetro "alt"
    ...     methods = "param",
    ...     param_name = "alt",   # El parámetro GET 'alt' elegirá alternativa
    ...     param_type = str      # El parámetro deberá ser un string
    ...     )
    >>> test1.select = ManagedSelect(
    ...     # Aleatorio, la alternativa se elije con probabilidad
    ...     methods = "remember, random", # intenta obtener de la sessión, y luego aleatorio
    ...     probability = {
    ...         1 : 2, # La alternativa 1 aparecerá 2 de cada 7 veces
    ...         2 : 5  # La alternativa 2 aparecerá 5 de cada 7 veces
    ...         })
    '''

    _method_classes = { k[:-8].lower(): v for k, v in globals().iteritems() if k.endswith("Selector") and isinstance(v, type) }

    @staticmethod
    def register(self, method_name, CustomSelector):
        assert isinstance(method_name, str)
        self._method_classes[method_name] =  CustomSelector

    def __init__(self, **defaults):
        self._methods = {}
        self._default_config = defaults
        self.config(defaults)

    _methods = None
    @property
    def methods(self):
        return self._methods.keys()

    _default_config = None
    @property
    def default_config(self):
        return self._default_config

    _current_config = None
    @property
    def current_config(self):
        return self._current_config

    def config(self, config):
        '''
        Establece la configuración del selector con los parámetros del
        diccionario dado.
        Los parámetros de configuración excluidos entre ejecuciones se
        reestablecerán a sus valores por defecto.

        @type config: dict-like object
        @param config: Diccionario de configuración.
            Claves:
                default:  ID de alternativa por defecto
                methods: iterable o string separado por comas con los modos
                         de selección de alternativas, cada modo reconoce sus
                         propias claves de configuración.

            Claves para method == "param":
                param_name: basestring, nombre del parámetro GET con la alternativa
                param_type: callable, recibe el valor del parámetro GET como
                            string o None (si no tiene valor), deberá
                            retornar el valor convertido en algún id de
                            alternativa válido (útil para ids en int), se
                            capturarán las excepciones heredadas de
                            BaseException por seguridad.

            Claves para method == "remember":
                remember_id: basestring, nombre de la clave interna para guardar la
                             alternativa si remember es True, si no se establece
                             se usará el nombre de endpoint.

            Claves para method == "random":
                probability: dict-like, las claves son los ids de alternativa,
                             los valores son la probabilidad relativa de
                             aparecer respecto al total. Tiene prevalencia
                             respecto a 'select'. Normalmente se recomienda
                             usar junto con 'remember'.

            Si se establece 'param' (nombre deparámetro GET) tendrá preferencia
            sobre el valor recordado si 'remember' es True, que a su vez tendrá
            preferencia sobre la función o valor asignado a 'select', que a su
            vez tendrá preferencia sobre el id asignado a 'default'.

            Si a un objeto Fooprint se le pasa un ID de alternativa no válido,
            escogerá la primera alternativa declarada.
        '''
        if config == DEFAULT_CONFIG: config = self._default_config
        self._current_config = config

        self.default = config.get("default", ALTERNATIVE_NOT_FOUND)

        method_config = config.get("methods", ())
        method_names = (
            tuple(i.strip() for i in method_config.split(","))
            if isinstance(method_config, basestring)
            else tuple(method_config)
            )
        self._methods = OrderedDict(((
                method,
                self._methods[method].config(config)
                if method in self._methods else
                self._method_classes[method](**config)
                )
            for method in method_names
            if method in self._method_classes
            ))

    def __call__(self, endpoint=None, cargs=None, ckwargs=None):
        if cargs is None: cargs = ()
        if ckwargs is None: ckwargs = {}

        for select in self._methods.itervalues():
            alt = select(endpoint, cargs, ckwargs)
            if alt != ALTERNATIVE_NOT_FOUND:
                if "remember" in self._methods:
                    self._methods["remember"].save(endpoint, alt)
                return alt
        return self.default

class DecoratedView(object):
    '''
    Clase para decorar los endpoints con alternativas, normalmente no hace
    falta instanciar esta clase manualmente, en vez de eso, usar la función
    decorador 'route' de Fooprint.

    La función decorada tendrá las siguientes propiedades
    alternative : función decorador, recibe por parámetro el id de
                  alternativa, y se usa para definir alternativas al
                  endpoint.
    selector    : decorador, se usa para definir la función que
                  devolverá el id de alternativa a elegir. La función
                  debe recibir tres parámetros: endpoint, args y
                  kwargs.

    '''
    select = None #: atributo con la función selectora o id de alternativa
    endpoint = "unknown" #: nombre del endpoint
    alternatives = None #:  OrderedDict de ids de alternativa con su función correspondiente

    def __init__(self, endpoint, aid, f):
        wraps(f)(self)
        self.select = ManagedSelect()
        self.endpoint = endpoint
        self.alternatives = OrderedDict(((aid,f),))

    def alternative(self, aid):
        def alternative_decorator(f):
            if not aid in self.alternatives:
                self.alternatives[aid] = f
            return wraps(f)(self)
        return alternative_decorator

    def selector(self, f):
        self.select = f
        return wraps(f)(self)

    def __call__(self, *args, **kwargs):
        option = self.select

        # Petición de id de alternativa
        if hasattr(option, "__call__"):
            option = option(self.endpoint, args, kwargs)

        # Ejecución del endpoint
        if option in self.alternatives:
            response = self.alternatives[option](*args, **kwargs)
        else:
            response = self.alternatives.values()[0](*args, **kwargs)

        # Escribe cookies
        if hasattr(g, "cookies_to_write"):
            # make_response ya detecta si se trata de una respuesta
            response = make_response(response)
            for key, (cargs, ckwargs) in g.cookies_to_write.iteritems():
                response.set_cookie(key, *cargs, **ckwargs)
        return response

class Fooprint(Blueprint):
    '''
    Clase Blueprint con gestión de alternativas (varias funciones para el mismo endpoint).

    Uso:
    >>> fooprint = Fooprint("test")
    >>> @fooprint.route("/test", aid=1)
    ... def test1():
    ...     return "test1"
    >>> @test1.alternative(2)
    ... def test2():
    ...     return "test2"
    >>> @test1.selector
    ... def test_selector(endpoint, cargs, ckwargs):
    ...     if request.args.get("alt","original") == "alternativo":
    ...         return 2
    ...     return 1

    '''

    @classmethod
    def set_cookie(cls, key, *args, **kwargs):
        if hasattr(g, "cookies_to_write"):
            g.cookies_to_write[key] = (args, kwargs)
        else:
            g.cookies_to_write = {key: (args, kwargs)}

    _dup_on_startswith = ()
    def __init__(self, *args, **kwargs):
        if "dup_on_startswith" in kwargs:
            ds = kwargs.pop("dup_on_startswith")
            if isinstance(ds, basestring) or not hasattr(ds, "__iter__"):
                self._dup_on_startswith = {ds}
            else:
                self._dup_on_startswith = set(ds)

        Blueprint.__init__(self, *args, **kwargs)
        self.decorators = {}

    def route(self, rule, **options):
        '''
        Decorador de rutas, similar al de Blueprint de Flask.

        @type rule: basestring
        @param rule: regla de ruta de Flask

        @param **options: parámetros extras tal cual los recibe Flask,
                          si se recibe "aid", será el ID de alternativa
                          de la alternativa por defecto (por defecto es None).

        '''

        rules = {rule}
        '''
        rules.update(
            "/" if rule == start else rule[len(start):]
            for start in self._dup_on_startswith
            if rule.startswith(start))
        '''

        def decorator(f):
            endpoint = options.pop("endpoint", f.__name__)
            aid = options.pop("aid", None)
            if endpoint in self.decorators:
                # Si el handler ya ha sido añadido y decorado
                decorated_view = self.decorators[endpoint]
                if not aid in decorated_view.alternatives:
                    # Si no ha sido registrado con este ID de alternativa
                    decorated_view.alternatives[aid] = f
            else:
                decorated_view = DecoratedView("%s.%s" % (self.name, endpoint), aid, f)
                self.decorators[endpoint] = decorated_view
            for rule in rules:
                self.add_url_rule(rule, endpoint, decorated_view, **options)
            return decorated_view
        return decorator

    def add_url_rule(self, rule, endpoint=None, view_func=None, **options):
        def record(app):
            fnc = view_func
            if endpoint in self.decorators and len(self.decorators[endpoint].alternatives) == 1:
                # Si sólo hay una alternativa, nos saltamos el decorated_view
                fnc = self.decorators[endpoint].alternatives.values()[0]
            fnc = newrelic.agent.transaction_name()(fnc)
            fnc._fooprint = self

            return app.add_url_rule(rule, endpoint, fnc, **options)
        self.record(record)



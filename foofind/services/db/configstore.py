# -*- coding: utf-8 -*-

import pymongo
import time
import memcache
import logging

from foofind.utils.fooprint import ManagedSelect, ParamSelector, DecoratedView, DEFAULT_CONFIG
from foofind.services.extensions import cache
from foofind.utils import end_request

class ConfigStore(object):
    '''
    Gestor de configuraciones en base de datos
    '''
    def __init__(self):
        self._alternatives_lt = 0
        self._alternatives_skip = set()

    def init_app(self, app):
        #remote_memcached_servers = app.config["REMOTE_MEMCACHED_SERVERS"]
        #self._remote_memcache = memcache.Client(remote_memcached_servers) if remote_memcached_servers else None
        self._local_memcache = cache.cache
        self._views = {
            endpoint: view_fnc
            for endpoint, view_fnc in app.view_functions.iteritems()
            if isinstance(view_fnc, DecoratedView) and isinstance(view_fnc.select, ManagedSelect)
            }
        self.max_pool_size = app.config["DATA_SOURCE_MAX_POOL_SIZE"]
        self.config_conn = pymongo.Connection(app.config["DATA_SOURCE_CONFIG"], slave_okay=True, max_pool_size=self.max_pool_size)

    def pull(self):
        '''
        Descarga la configuración de la base de datos y
        actualiza la configuración local.
        '''
        self.pull_alternatives()

    def pull_alternatives(self):
        '''
        Descarga la configuración de alternativas del servidor
        y la aplica.
        '''
        last = 0
        for alternative in self.config_conn.foofind.alternatives.find({
          "_id": { "$in": self._views.keys() },
          "lt" : { "$gt": self._alternatives_lt }
          }):
            endpoint = alternative["_id"]
            if endpoint in self._alternatives_skip:
                self._alternatives_skip.remove(endpoint)
            elif endpoint in self._views and "config" in alternative:
                if "probability" in alternative["config"]:
                    # Probability se guarda como lista por limitaciones de mongodb
                    alternative["config"]["probability"] = dict(alternative["config"]["probability"])
                self._views[endpoint].select.config(alternative["config"])
            if alternative.get("lt", 0) > last:
                last = alternative["lt"]

        self._alternatives_lt = max(self._alternatives_lt, last)

    def remove_alternative(self, altid):
        '''
        Borra la configuración de alternativa con id del servidor

        @type altid: str
        @param altid: identificador de alternativa
        '''
        self.config_conn.foofind.alternatives.remove({"_id":altid})

    def list_alternatives(self, skip=None, limit=None):
        '''
        Devuelve la lista de alternativas disponibles localmente o en la
        configuración del servidor. Soporta paginación

        @type skip: None o int
        @param skip: número de registros a ignorar (paginación)

        @type limit: None o int
        @param limit: número máximo de registros a retornar (paginación)

        @rtype list
        @return lista de ids de alternativas
        '''
        tr = []
        ids = []
        cur = self.config_conn.foofind.alternatives.find().sort("_id")
        if not skip is None: cur.skip(skip)
        if not limit is None: cur.limit(limit)
        for r in cur:
            a = r["config"]
            a["_id"] = r["_id"]
            ids.append(r["_id"])
            tr.append(a)
        self.config_conn.end_request()
        tr.extend({"_id": k, "methods": ", ".join(v.select.methods)} for k, v in self._views.iteritems() if k not in ids)
        tr.sort(key=lambda x:x["_id"])
        if skip:
            skip = min(len(self._views), skip)
            return tr[skip:skip+limit] if limit else tr[skip:]
        elif limit:
            return tr[:limit]
        return tr

    def count_alternatives(self):
        '''
        Devuelve el número de alternativas disponibles localmente o en la
        configuración del servidor.

        @rtype int
        @return número de alternativas
        '''
        tr = self.config_conn.foofind.alternatives.find({"_id":{"$nin":self._views.keys()}}).count(True)
        self.config_conn.end_request()
        return len(self._views)+tr

    def _normalize_config(self, config):
        if "param_type" in config and hasattr(config["param_type"], "__name__"):
            config["param_type"] = config["param_type"].__name__
        if "_id" in config:
            del config["_id"]

    def update_alternative_config(self, endpoint, new_config):
        '''
        Suben la configuración de alternativa al servidor y la aplican
        localmente.

        @type endpoint: str
        @param endpoint: identificación de endpoint

        @type new_config: dict
        @param new_config: nueva configuración de alternativa
        '''
        now = time.time()
        create = False
        config = self._get_alternative_config(endpoint)
        if not config:
            config = self._get_current_config(endpoint) or {}
            create = True
        config.update(new_config)
        self._normalize_config(config)
        if endpoint in self._views:
            self._views[endpoint].select.config(config.copy())
            self._alternatives_skip.add(endpoint)
            # Lo ignoro en el próximo pull_alternatives
        if "probability" in config:
            # Probability se guarda como lista por limitaciones de mongodb
            config["probability"] = config["probability"].items()
        if create:
            self.config_conn.foofind.alternatives.insert(
              { "_id": endpoint,
                "config": config,
                "lt": now })
        else:
            self.config_conn.foofind.alternatives.update(
              { "_id": endpoint },
              { "config": config,
                "lt": now })
        self.config_conn.end_request()

    def _get_alternative_config(self, endpoint):
        config = self.config_conn.foofind.alternatives.find_one({"_id":endpoint})
        self.config_conn.end_request()
        if config:
            if "probability" in config["config"]:
                # Probability se guarda como lista por limitaciones de mongodb
                config["config"]["probability"] = dict(config["config"]["probability"])
            return config["config"]
        return None

    def _get_current_config(self, endpoint):
        if endpoint in self._views:
            return self._views[endpoint].select.current_config.copy()
        return None

    def get_alternative_config(self, endpoint):
        '''
        Devuelve la configuración de alternativa para el endpoint dado.

        @type endpoint: str
        @param endpoint: identificador de endpoint

        @rtype dict
        @type diccionario con la configuración del endpoint
        '''
        tr = self._get_alternative_config(endpoint)
        if tr is None:
            tr = self._get_current_config(endpoint)
        if tr:
            self._normalize_config(tr)
            return tr
        return {}

    def list_alternatives_methods(self):
        '''
        Lista los métodos disponibles para la selección de alternativas tal
        cual se incluirían en el campo "methods" del diccionario de
        configuración de alternativas

        @rtype str
        @return identificador del método para la selección de alternativas.
        '''
        return ManagedSelect._method_classes.keys()

    def list_alternatives_endpoints(self, endpoint):
        '''
        Lista las alternativas disponibles para el endpoint dado.

        @type endpoint: str
        @param endpoint: identificador de endpoint

        @rtype list
        @return lista de handlers alternativos para el endpoint dado
        '''
        if endpoint in self._views:
            return self._views[endpoint].alternatives.keys()
        return []

    def list_alternatives_param_types(self):
        '''
        Lista los tipos de parámetro disponibles para el método de selección
        'param' (selección por parámetro GET). Es necesario porque valor de los parámetros GET para este método es validado y parseado dependiendo de
        el tipo dado.

        @rtype list
        @return lista de tipos de parámetros disponibles para el método de
                selección del aternativas por parámetro GET.
        '''
        return ParamSelector._param_types.keys()

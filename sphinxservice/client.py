# -*- coding: utf-8 -*-
from threading import Thread, Condition
from time import time, sleep
from .common import *
from collections import deque
import redis, logging, timeit

INFINITY            = float('inf')

PING_INTERVAL       = 10  # Segundos
PING_SWITCH_STEPS   =  6  # Cantidad de pings desfavorables al servidor actual para cambiar de servidor

ACTIVE_PART_TIMEOUT = 60
ACTIVE_PART_LIST_LEN = 3
ACTIVE_PART_INTERVAL = ACTIVE_PART_TIMEOUT/ACTIVE_PART_LIST_LEN
BROWSE_MAX_REQUESTS = 32

def safe_ping(conn):
    try:
        return timeit.timeit(conn.ping, number=1)
    except:
        return INFINITY

class Sphinx(Thread):
    EMPTY_STATS = {"cs":0, "s": False, "ct": None, "end": False, "total_sure": False, "li": [], "t":0, "w":500}

    def __init__(self, context, browser):
        Thread.__init__(self)

        self.daemon = True

        # inicializa variables
        self.version = WORKER_VERSION

        # Clase que recorre resultados
        self.context = context
        self.browser = browser

        # Constantes de conexion
        self.redis_conn = None
        self.redis_index = -1
        self.last_try = self.redis_confidence = 0

    def init_app(self, app):
        # configuracion
        self.requests = LimitedDict(app.config["SPHINX_CLIENT_REQUESTS_CACHE_SIZE"], app.config["SPHINX_CLIENT_REQUESTS_CACHE_TIMEOUT"])
        self.ct_weights = app.config["SEARCH_CONTENT_TYPE_WEIGHTS"]

        # conexiones a redis
        redis_servers = app.config["SPHINX_REDIS_SERVER"]
        self.redis_conns = [redis.StrictRedis(host=server[0], port=server[1], db=self.version) for server in redis_servers]
        self.redis_conns_ps = [redis.StrictRedis(host=server[0], port=server[1], db=self.version).pubsub() for server in redis_servers]

        self.update_redis_connections()

    def update_redis_connections(self, force=False):
        try:
            # evita ejecutar la funcion si no se ha cumplido el intervalo
            if not force and time()-self.last_try<PING_INTERVAL:
                return

            # calcula pings a conexiones
            self.last_try = time()
            self.redis_pings = [safe_ping(conn) for conn in self.redis_conns]
            ping, new_index = min((ping, index) for index, ping in enumerate(self.redis_pings))

            # evalua confianza de la conexion actual
            if self.redis_pings[self.redis_index]==INFINITY:    # conexión actual caída, se debe cambiar ya de conexion
                self.redis_confidence = 0
            elif new_index==self.redis_index:                   # mantiene confianza, esta conexion es la mejor
                self.redis_confidence = PING_SWITCH_STEPS
            elif new_index!=self.redis_index:                   # conexión alternativa mejor, disminuye la confianza
                self.redis_confidence -= 1

            # si ha perdido la confianza cambia de servidor
            if self.redis_confidence<=0:
                if self.redis_index == new_index: # si no hay alternativa, espera
                    sleep(PING_INTERVAL/2)
                else:
                    self.redis_index = new_index
                    self.redis_confidence = PING_SWITCH_STEPS
                    self.redis_conn = self.redis_conns[self.redis_index]
                    self.redis_conn_ps = self.redis_conns_ps[self.redis_index]
        except BaseException as e:
            logging.exception("Error updating redis connections.")

    def build_query(self, text, filters, limits, grouping, order):
        result = {"t":text, "f":filters, "l":limits, "g":(GROUPING_GROUP if grouping[0] else 0)|(GROUPING_NO_GROUP if grouping[1] else 0)}
        if order[0]:
            result["w"] = order[0]
        if order[1]:
            result["o"] = order[1]
        if order[2]:
            result["ok"] = order[2]
        return result

    def start_client(self, initial_parts):
        # almacena servicios iniciales
        now = time()
        self.last_parts_request = deque([0]*ACTIVE_PART_LIST_LEN, ACTIVE_PART_LIST_LEN)
        self.last_parts_request.append(now)
        self.active_parts = {int(part):now for part in initial_parts}

        # inicia el thread
        self.start()

    def update_blocked_sources(self, blocked_sources):
        try:
            self.redis_conn.set(CONTROL_KEY+"bs", format_data(blocked_sources))
            self.redis_conn.publish(CONTROL_CHANNEL, "bs")
        except redis.ConnectionError as e:
            logging.warning("Can't update_blocked sources on Redis.")
            self.update_redis_connections(True)

    def run(self):
        while True:
            try:
                # se suscribe al canal de resultados
                self.redis_conn_ps.subscribe(RESULTS_CHANNEL)

                for msg in self.redis_conn_ps.listen():
                    if msg["type"]!="message" or msg["data"]=="pn":
                        continue

                    # recibe notificaciones de resultados principales obtenidos y avisa a quienes esperan
                    request_id, server, info = parse_data(msg["data"])
                    server = ord(server)

                    # loguea que ha recibido información de esta parte
                    self._log_part_response(server)

                    # avisa de las novedades
                    exists, request = self._get_request_info(request_id)
                    with request[0]:
                        if info!=None:
                            request[2].append(info)
                        request[1].add(server)
                        request[0].notifyAll()

                    self.update_redis_connections()
            except redis.ConnectionError as e:
                logging.warning("Redis connection lost.")
                self.update_redis_connections(True)

    def _log_parts_request(self):
        '''
        Loguea que se han enviado peticiones a todas las partes.
        Quita de la lista de activas a las partes que no han respondido ultimamente.
        '''
        now = time()

        # ejecuta si ha pasado el intervalo de actuación
        if now-self.last_parts_request[-1]>ACTIVE_PART_INTERVAL:
            self.last_parts_request.append(now)

            # quita las partes cuya ultima respuesta sea anterior a los ultimos intervalos con peticiones
            threshold = self.last_parts_request[0]
            for part, last_response in self.active_parts.items():
                if last_response<threshold:
                    del self.active_parts[part]

    def _log_part_response(self, part):
        '''
        Loguea que ha recibido información de esta parte.
        '''
        self.active_parts[part]=self.last_parts_request[-1]

    def _get_request_info(self, request_id):
        '''
        Obtiene información de la peticion o la crea si no existe.
        '''
        # limpieza de cache antes de obtener o asignar peticiones
        self.requests.cleanup()

        if request_id in self.requests:
            return True, self.requests[request_id]
        else:
            new_request = self.requests[request_id] = (Condition(), set(), [])
            return False, new_request

    def get_id_server_from_search(self, bin_file_id, search_text, timeout):
        '''
        Obtiene servidor en el que se encuentra el fichero de los servicios de busqueda.
        '''
        # identificador unico de la peticion
        request_id = LOCATION_KEY+bin_file_id

        # mira si se ha calculado antes
        parts = self.redis_conn.hgetall(request_id)
        if parts:
            try:
                return (str(ord(server)) for server, has_it in parts.iteritems() if has_it=="H").next()
            except:
                # no hay ningun servidor que haya dicho que lo tiene
                if not "P" in parts.itervalues():
                    return None # no quedan servidores pendientes por mirar

        # envia la peticion a los procesos de busqueda si no estaba pedida
        exists, request = self._get_request_info(request_id)
        if not exists:
            self._log_parts_request()
            self.redis_conn.publish(EXECUTE_CHANNEL, format_data((request_id, search_text)))

        # espera resultados
        with request[0]:
            if not request[2]:
                request[0].wait(timeout/1000.)

        # devuelve el numero de servidor
        return str(ord(request[2][0])) if request[2] else None

    def get_search_info(self, query):
        '''
        Devuelve informacion de una busqueda.
        '''
        results = {"version":{}, "time":{}, "tries":{}, "warning":{}, "date":{}, "main":{}, "sg":{}}

        query_id = QUERY_KEY+hash_dict(query)
        search_info = self.redis_conn.hgetall(query_id)
        for key, value in search_info.iteritems():
            if key == INFO_KEY:
                results["canonical"] = parse_data(value)
            else:
                part = ord(key[1])

                if key[0]==VERSION_KEY:
                    results["version"][part] = value
                else:
                    parsed_value = parse_data(value)
                    if key[0]==PART_KEY:
                        results["date"][part] = parsed_value[0]
                        results["warning"][part] = parsed_value[1]
                        results["tries"][part] = parsed_value[2]
                        results["time"][part] = parsed_value[3]
                        results["main"][part] = repr(parsed_value[4])
                    else:
                        if part in results["sg"]:
                            sg_part = results["sg"][part]
                        else:
                            sg_part = results["sg"][part] = {}
                        sg_part[key[2:]] = repr(parsed_value)
        return results

    def get_group_count(self, query, mask):
        '''
        Devuelve numero de resultados para cada grupo generado por la funcion mask.
        NOTA: La búsqueda debe haberse realizado anteriormente para obtener resultados.
        '''
        query_id = QUERY_KEY+hash_dict(query)
        search_info = self.redis_conn.hgetall(query_id)
        results_count = {}
        for key, value in search_info.iteritems():
            if key[0]==PART_KEY:
                parsed_value = parse_data(value)
                for group, results in parsed_value[4].iteritems():
                    mask_id = mask(group)
                    if mask_id in results_count:
                        results_count[mask_id] += results[0]
                    else:
                        results_count[mask_id] = results[0]

                    if 0 in results_count:
                        results_count[0] += results[0]
                    else:
                        results_count[0] = results[0]

        return results_count

    def start_search(self, query, requests=None):
        # identificador unico de la peticion
        request_id = QUERY_KEY+hash_dict(query)
        if requests:
            must_execute = False
            pipe = self.redis_conn.pipeline()
            for server, subgroups in requests.iteritems():
                # evita pedir los mismos subgrupos varias veces seguidas
                subgroup_request_id = request_id+server+hash_dict(subgroups)
                if subgroup_request_id in self.requests:
                     continue
                must_execute = self.requests[subgroup_request_id] = True
                pipe = pipe.publish(EXECUTE_CHANNEL+chr(ord(server)), format_data((request_id, (query, subgroups))))
            if must_execute:
                pipe.execute()
        else:
            # crea entrada para esperar resultados
            exists, request = self._get_request_info(request_id)

            # envia la busqueda a los procesos de busqueda
            self._log_parts_request()
            self.redis_conn.publish(EXECUTE_CHANNEL, format_data((request_id, (query, None))))

    def get_results(self, query, timeouts, last_items, skip, min_results, max_results, hard_limit, extra_browse=None, weight_processor=None, tree_visitor=None):
        '''
        Obtiene los resultados de la busqueda en bruto
        '''
        # identificador unico de la busqueda
        request_id = QUERY_KEY+hash_dict(query)

        # espera que lleguen resultados
        if request_id in self.requests:
            request = self.requests[request_id]
            with request[0]:
                # espera si no ha recibido ninguna respuesta
                if len(request[1])==0:
                    request[0].wait(timeouts[0])

                # espera un poco mas si no ha recibido todas las respuestas
                if len(request[1])<len(self.active_parts):
                    request[0].wait(timeouts[1])

        # obtiene los datos del cache
        results = self.redis_conn.hgetall(request_id)

        if not results:
            return [], Sphinx.EMPTY_STATS

        # utiliza la clase que recorre resultados
        browser = self.browser(self.context, results, BROWSE_MAX_REQUESTS, self.ct_weights, weight_processor, tree_visitor)

        # añade una versión si hay cambios en la lista de versiones y obtiene la lista actualizada
        ignore, raw_versions = self.redis_conn.pipeline().zadd(request_id+VERSION_KEY, sum(browser.versions.itervalues()), format_data(sorted(browser.versions.iteritems()))).zrange(request_id+VERSION_KEY, 0, -1).execute()

        # prepara la lista de versiones para usarla
        all_versions = {}
        num_read_versions = len(raw_versions)
        for read_version, raw_version in enumerate(raw_versions):
            for server, write_version in dict(parse_data(raw_version)).iteritems():
                if not server in all_versions:
                    all_versions[server] = [-1]*num_read_versions
                all_versions[server][read_version] = write_version

        # manejo de versiones
        versions = [0]*(num_read_versions)
        last_versions = versions[:]
        last_items_len = len(last_items)
        last_versions[:last_items_len] = last_items
        new_versions = last_versions[:]

        # recorre resultados
        to_return = []
        returned = 0
        number = -1

        # recorrido extra de resultados para pedir mas
        extra_browsing = False
        if extra_browse==None:
            extra_browse = max_results

        # guarda number cuando empieza con el extra_browsing
        non_extra_number = None

        for number, (server, sg, weight, (fileid, sphinxid, file_write_version, rating, result_weight)) in enumerate(browser):

            # obtiene la version de escritura a partir de la de lectura
            version = 0
            for read_version, write_version in enumerate(all_versions[chr(int(server))]):
                 if file_write_version <= write_version: break
                 version = read_version

            # incrementa el navegador de la version del fichero
            versions[version]+=1

            # devuelve el resultado
            if not extra_browsing and versions[version]>last_versions[version]:
                if skip>0:
                    skip-=1
                else:
                    returned+=1
                    new_versions[version] = versions[version]
                    to_return.append((fileid, server, sphinxid, weight, sg))

            # decrementa el contador de recorrido extra
            if extra_browsing:
                extra_browse -= 1
            else:
                # empieza la navegacion
                extra_browsing = skip<=0 and (returned>min_results and bool(browser.requests)) or returned>=max_results

                # guarda number cuando empieza con el extra_browsing
                if extra_browsing:
                    non_extra_number = number

            # para si se ha alcanzado algun limite
            if not browser.fetch_more or (extra_browsing and extra_browse<=0) or number>hard_limit:
                break

        # si no ha hecho extra_browsing, non_extra_number es number
        if not extra_browsing:
            non_extra_number = number

        if not browser.sure: # vuelve a pedir buscar
            self.start_search(query)
        elif browser.requests: # pide mas resultados
            subgroup_query = query.copy()
            subgroup_query["l"] = (0, min(max(10, min_results),50), 10000, 2000000)
            self.start_search(subgroup_query, requests=browser.requests)

        # devuelve resultados e informacion de la busqueda
        return to_return, {"cs": browser.total, "s": browser.sure, "ct": parse_data(results[INFO_KEY]), "end": not browser.requests and (non_extra_number>=browser.total-1 or non_extra_number>hard_limit), "total_sure": browser.fetch_more==BROWSE_MAX_REQUESTS, "li": new_versions, "t":0, "w":500 if browser.requests else 100}

# -*- coding: utf-8 -*-
from foofind.services.extensions import cache
from foofind.utils import sphinxapi2, hex2bin, bin2hex, mid2hex, mid2url, hex2mid, u
from foofind.utils.event import EventManager
from foofind.utils.content_types import *
from foofind.utils.splitter import split_file, SEPPER
from threading import Thread, Lock
from Queue import Queue, Empty
from hashlib import md5
from time import time
from collections import defaultdict, deque
from copy import deepcopy
from math import sqrt, log, exp
from shlex import split
from heapq import heapify, heappop, heappush
from string import whitespace
from struct import Struct
from random import shuffle
import json
import functools
import itertools
import logging
import operator
import re

def get_group(sg):
    return str((long(sg)&0xF0000000L)>>28)

def get_group2(sg):
    return (long(sg)&0xFFFF000L)>>12

def get_ct(sg):
    return (int(sg)&0xF0000000L)>>28

def get_src(sg):
    return (int(sg)&0xFFFF000L)>>12

full_id_struct = Struct("III")
part_id_struct = Struct(">Q")

class StopWorker:
    pass

class SearchProxy:
    def __init__(self, config, filesdb):
        self.filesdb = filesdb
        self.config = config

        # inicializa variables
        self.tasks = {}
        self.client_pools = {}
        self.workers = {}

        # actualiza información de servidores
        self.update_servers()

        # worker para actualizaciones
        self.updates = Queue()
        self.updater_worker = Thread(target=self.updater)
        self.updater_worker.daemon = True
        self.updater_worker.start()

        # procesa respuestas de peticiones pendientes
        self.pendings_processor = EventManager()
        self.pendings_processor.start()
        self.pendings_processor.interval(config["SERVERS_REFRESH_INTERVAL"], self.update_servers)
        self.pendings_processor.interval(config["SERVICE_SPHINX_CLIENT_MAINTENANCE_INTERVAL"], self.sphinx_client_maintenance)

    def sphinx_client_maintenance(self):
        for server_id, cp in self.client_pools.iteritems():
            cp.maintenance()

    def search(self, allsearches):
        allsearches["_r"] = Queue()
        allsearches["_pt"] = set(allsearches["s"].iterkeys())
        # recorre las queries y las envia al servidor correspondiente
        for server, asearch in allsearches["s"].iteritems():
            if server in self.tasks:
                self.tasks[server].put((allsearches, asearch))

    def put_results(self, allsearches, asearch, server, results, messages):
        allsearches["_r"].put((server, asearch, results, messages))

    def browse_results(self, allsearches, timeout, pending_callback=None, pending_timeouts=None):
        start = time()
        while allsearches["_pt"]:
            try:
                wait = timeout*0.001 - (time()-start) # calcula cuanto hay que esperar
                result = allsearches["_r"].get(wait>0, wait)
                if result[0] in allsearches["_pt"]:
                    allsearches["_pt"].remove(result[0])
                    yield result
            except Empty: # Timeout
                break

        if allsearches["_pt"]:
            if pending_callback and pending_timeouts:
                self.async_process_results(allsearches, pending_callback, pending_timeouts)
            else:
                logging.warning("Timeout waiting for sphinx results from servers %s."%(str(allsearches["_pt"])), extra={"q":allsearches["s"].itervalues().next()["q"]})

    def async_process_results(self, allsearches, pending_callback, pending_timeouts):
        # si hay un retraso de más de 5 minutos en atender la próxima petición, no añade tareas pendientes
        if self.pendings_processor._timers and time()-self.pendings_processor._timers[0][0]>300:
            logging.warning("Too many pendings task for search.", extra={"queue_size":len(self.pendings_processor._timers), "now": time(), "next": self.pendings_processor._timers[0], "last": self.pendings_processor._timers[-5:]})
        else:
            self.pendings_processor.timeout(pending_timeouts[0]*0.001, pending_callback, hargs=[allsearches, pending_timeouts[1:]])

    def get_search_state(self, querykey, filters):
        filterskey = md5(json.dumps(filters)).hexdigest()

        # claves a obtener del cache
        query_cache_key = "search_t%s"%querykey
        filters_cache_key = "search_f%s_%s"%(querykey, filterskey)
        lock_cache_key = "search_l%s_%s"%(querykey, filterskey)

        # obtiene valores
        cache_data = cache.cache.get_many(query_cache_key, filters_cache_key, lock_cache_key)

        # extrae informacion de los valores obtenidos
        query_state = cache_data[0] or {}
        filters_state = cache_data[1] if query_state else {}
        locked_until = cache_data[2] or False

        return (query_state, filters_state, locked_until)

    def save_search_state(self, querykey, filters, query_state, filters_state, locking_time=None):

        # guarda valores en cache
        filterskey = md5(json.dumps(filters)).hexdigest()
        cache.cache.set_many({"search_t%s"%querykey: query_state, "search_f%s_%s"%(querykey, filterskey): filters_state},
                            timeout=0)

        # procesa bloqueo separadamente
        if locking_time==False: # borrar bloqueo
            cache.delete("search_l%s_%s"%(querykey, filterskey))
        elif locking_time: # establecer bloqueo
            self.lock_state(querykey, filters, locking_time)

    def lock_state(self, querykey, filters, locking_time):
        filterskey = md5(json.dumps(filters)).hexdigest()
        cache.set("search_l%s_%s"%(querykey, filterskey), time()+locking_time*0.001, timeout = locking_time*0.001)

    def update_sources(self):
        # obtiene los origenes
        self.sources = {int(s["_id"]): s for s in self.filesdb.get_sources(blocked=None)}

        # actualiza origenes bloqueados
        self.blocked_sources = [sid for sid, s in self.sources.iteritems() if "crbl" in s and int(s["crbl"])!=0]

        # calcula pesos para origenes no bloqueados
        sources_weights = {"w":1., "s":1., "t":0.3, "e":0.01, "g":0.005}
        self.sources_weights = {sid:max(v for k,v in sources_weights.iteritems() if k in s["g"]) for sid, s in self.sources.iteritems() if s not in self.blocked_sources}

        # listado de origenes ordenados por cantidad de ficheros
        sources = self.stats["src"]
        sources_relevance = sorted(((sources[sid], sid, s["g"]) for sid, s in self.sources.iteritems() if sid not in self.blocked_sources), reverse=True)
        self.sources_relevance_streaming = [self.sources[sid]["d"]
                                                for (count,sid,group) in sources_relevance if "s" in group]
        self.sources_relevance_download = [self.sources[sid]["d"]
                                                for (count,sid,group) in sources_relevance if ("w" in group or "f" in group)]
        self.sources_relevance_p2p = ["Torrent","eD2k","Gnutella"]

    def update_servers(self):
        # obtiene los servidores activos para busquedas
        self.servers = {int(server["_id"]):(str(server["sp"]), int(server["spp"])) for server in self.filesdb.get_servers() if "sp" in server and server["sp"]}
        self.servers_set = set(self.servers.iterkeys())

        # estadisticas de busqueda por servidor
        new_servers_stats = {server_id:self.filesdb.get_server_stats(server_id) for server_id in self.servers.iterkeys()}

        # estadisticas combinadas para todos los servidores
        new_stats = {"sg":defaultdict(int), # subgrupos
                      "rc":defaultdict(int), "ra":defaultdict(int), "rv":defaultdict(int), "rd":{}, "rM":defaultdict(int), # por rating
                      "src":defaultdict(int), "src_rc":defaultdict(int), "src_ra":defaultdict(int), "src_rv":defaultdict(int), "src_rd":defaultdict(int) # por rating y origen
                      }

        # evita multiples accesos a mismas claves globales
        subgroups = new_stats["sg"]
        rating_count = new_stats["rc"]
        rating_average = new_stats["ra"]
        rating_variance = new_stats["rv"]
        rating_maximum = new_stats["rM"]
        rating_deviation = new_stats["rd"]

        # recorre la información de cada servidor
        for server, server_stats in new_servers_stats.iteritems():

            # evita multiples accesos a mismas claves para el servidor
            server_rating_count = server_stats["rc"]
            server_rating_average = server_stats["ra"]
            server_rating_pow_average = server_stats["rpa"]
            server_rating_maximum = server_stats["rM"]

            # recorre los subgrupos del servidor
            for sg, count in server_stats["sg"].iteritems():
                subgroups[sg] += count
                if sg in server_rating_count:
                    rating_count[sg] += server_rating_count[sg]                                 # numero de entradas con rating
                    rating_average[sg] += server_rating_average[sg] * server_rating_count[sg]   # valor medio del rating (a falta de normalizar)
                    rating_variance[sg] += server_rating_pow_average[sg]                        # varianza del rating (a falta de normalizar)
                    rating_maximum[sg] = max(rating_maximum[sg], server_rating_maximum)         # maximo rating

        # actualiza valores dependientes de valores totales que no se pueden calcular en el bucle anterior
        for sg, count in rating_count.iteritems():
            rating_average[sg] /= count
            rating_variance[sg] /= count
            rating_deviation[sg] = sqrt(rating_variance[sg])

        # estadisticas por origenes
        sources = new_stats["src"]
        new_sources_rating_count = new_stats["src_rc"]
        new_sources_rating_average = new_stats["src_ra"]
        new_sources_rating_variance = new_stats["src_rv"]
        new_sources_rating_standard_deviation = new_stats["src_rd"]

        # recorre subgrupos agrupando por origenes
        for sg, count in subgroups.iteritems():
            src = get_src(sg)
            sources[src] += count
            if sg in rating_count:
                new_sources_rating_count[src] += rating_count[sg]
                new_sources_rating_average[src] += rating_average[sg]*rating_count[sg]
                new_sources_rating_variance[src] += rating_variance[sg]*rating_count[sg]

        # actualiza valores dependientes de valores totales que no se pueden calcular en el bucle anterior
        for src, count in new_sources_rating_count.iteritems():
            new_sources_rating_average[src] /= count
            new_sources_rating_variance[src] /= count
            new_sources_rating_standard_deviation[src] = sqrt(new_sources_rating_variance[src])

        self.servers_stats = new_servers_stats
        self.stats = new_stats
        self.sources_rating_count = new_sources_rating_count
        self.sources_rating_average = new_sources_rating_average
        self.sources_rating_variance = new_sources_rating_variance
        self.sources_rating_standard_deviation = new_sources_rating_standard_deviation
        self.sources_rating_count = new_sources_rating_count
        self.sources_rating_average = new_sources_rating_average
        self.sources_rating_variance = new_sources_rating_variance
        self.sources_rating_standard_deviation = new_sources_rating_standard_deviation

        # pesos para los origenes por tipo
        self.update_sources()

        # inicia workers de la busqueda nueva
        maintenance_interval = self.config["SERVICE_SPHINX_CLIENT_MAINTENANCE_INTERVAL"]
        client_min = self.config["SERVICE_SPHINX_CLIENT_MIN"]
        client_max = self.config["SERVICE_SPHINX_CLIENT_MAX"]
        client_step = self.config["SERVICE_SPHINX_CLIENT_STEP"]
        client_recycle = self.config["SERVICE_SPHINX_CLIENT_RECYCLE"]
        socket_timeout = self.config["SERVICE_SPHINX_SOCKET_TIMEOUT"]
        workers_per_server = self.config["SERVICE_SPHINX_WORKERS_PER_SERVER"]

        # quita servidores eliminados
        for server_id in self.tasks.keys():
            if not server_id in self.servers:
                workers = self.workers[server_id]
                for w in workers:
                    w.stop()
                self.client_pools[server_id].destroy()
                del self.client_pools[server_id]
                del self.workers[server_id]
                del self.tasks[server_id]

        # añade los servidores nuevos
        for server_id, address in self.servers.iteritems():
            if not server_id in self.tasks:
                self.tasks[server_id] = Queue()
                self.client_pools[server_id] = SphinxClientPool(address, maintenance_interval, client_min, client_max, client_step, client_recycle, socket_timeout)
                workers = self.workers[server_id] = [SphinxWorker(server_id, self.config, self.tasks[server_id], self.client_pools[server_id], self) for j in xrange(workers_per_server)]
                for w in workers: w.start()

    def updater(self):
        while True:
            try:
                client = None
                update = self.updates.get(True)

                servers = self.servers.keys() if update["server"]=="*" else [update["server"]]
                shuffle(servers)

                not_found_values = update["values"].copy()

                for server in servers:
                    client = self.client_pools[server].get_sphinx_client()

                    # no puede acceder al servidor de sphinx
                    if not client:
                        continue

                    index = update["index"]+str(server)

                    # convierte los ids de los ficheros en los ids de sphinx
                    server_values = {}
                    not_found_items = not_found_values.items()
                    for file_id, file_update in not_found_items:
                        # no modifica los ids de sphinx
                        if not isinstance(file_id, basestring):
                            server_values[file_id] = file_update
                            del not_found_values[file_id]
                            continue

                        # busca el registro con el id pedido
                        uri1, uri2, uri3 = full_id_struct.unpack(file_id)
                        client.SetFilter('uri1', [uri1])
                        client.SetFilter('uri2', [uri2])
                        client.SetFilter('uri3', [uri3])
                        client.SetIDRange(part_id_struct.unpack(file_id[:5]+"\x00\x00\x00")[0], part_id_struct.unpack(file_id[:5]+"\xFF\xFF\xFF")[0])
                        client.SetLimits(0,1,1,1)
                        results = client.Query("", index, "w_u"+bin2hex(file_id)[:6])

                        # comprueba resultados obtenidos
                        if results and "matches" in results and results["matches"]:
                            server_values[results["matches"][0]["id"]] = file_update
                            del not_found_values[file_id]

                    if server_values:
                        client.UpdateAttributes(index, update["attrs"], server_values)

                    self.client_pools[server].return_sphinx_client(client)
                    client = None

                    # termina antes si ya no quedan ids por mirar
                    if not not_found_values:
                        break

                # loguea si ha quedado algún registro sin actualizar
                if not_found_values:
                    logging.warn("Couldn't update some sphinx entries.", extra={"update":update, "not_found":not_found_values})

            except BaseException as e:
                logging.exception("Error updating sphinx: %s."%repr(e))
            finally:
                if client:
                    self.client_pools[server].return_sphinx_client(client)

    def block_files(self, ids=[], block=True):
        # valor a enviar
        value = [1 if block else 0]

        # separa los ids por servidor
        updates = defaultdict(dict)
        for file_id in ids:
            if isinstance(file_id, tuple):
                file_id, server = file_id
            else:
                server = "*"
            updates[server][file_id] = value

        # encola una consulta por servidor
        for server, values in updates.iteritems():
            self.updates.put({"server":server, "index":"idx_files", "attrs":["bl"], "values":values})

    def get_id_server_from_search(self, file_id, file_name):
        servers = self.servers.keys()
        shuffle(servers)

        if file_name:
            query = escape_string(file_name)
        else:
            query = ""

        for server in servers:
            try:
                client = None
                client = self.client_pools[server].get_sphinx_client()
                index = "idx_files"+str(server)

                # busca el registro con el id pedido
                bin_file_id = file_id.binary
                uri1, uri2, uri3 = full_id_struct.unpack(bin_file_id)
                client.SetFilter('uri1', [uri1])
                client.SetFilter('uri2', [uri2])
                client.SetFilter('uri3', [uri3])
                client.SetLimits(0,1,1,1)
                client.SetIDRange(part_id_struct.unpack(bin_file_id[:5]+"\x00\x00\x00")[0], part_id_struct.unpack(bin_file_id[:5]+"\xFF\xFF\xFF")[0])
                results = client.Query(query, index, "w_id"+mid2hex(file_id)[:6])

                # comprueba resultados obtenidos
                if results and "matches" in results and results["matches"]:
                    return server

            except BaseException as e:
                logging.exception("Error getting id from sphinx: %s."%repr(e))
            finally:
                if client:
                    self.client_pools[server].return_sphinx_client(client)
        return None

class SphinxClientPool():
    def __init__(self, server, maintenance_interval, min_free_clients, max_free_clients, create_client_step, recycle_uses, socket_timeout):
        self.server = server
        self.maintenance_interval = maintenance_interval
        self.min_free_clients = min_free_clients
        self.max_free_clients = max_free_clients
        self.create_client_step = create_client_step
        self.recycle_uses = recycle_uses
        self.socket_timeout = socket_timeout

        self.preclients = Queue()
        self.clients = deque()
        self.access = Lock()
        self.connection_failed = False

        self.clients_counter = 0

        self.maintenance()

    def get_sphinx_client(self):

        client = None
        self.access.acquire()

        # obtiene un cliente conectado, si hay
        if self.clients:
            client = self.clients.popleft()
        self.access.release()

        # si no hay cliente, lo crea
        if not client and not self.connection_failed:
            try:
                client = self.create_sphinx_client()
                client.Open()
                client.timeout = time()+self.socket_timeout
                client.uses = 0
            except:
                logging.exception("Ad-hoc connection to search server %s:%d failed."%self.server)
                self.connection_failed = True
                client = None

        if client:
            self.clients_counter+=1

        return client

    def return_sphinx_client(self, client, discard=False):
        client.uses+=1
        self.clients_counter-=1

        # si ha alcanzado el limite de usos, lo descarta
        if discard or client.uses>=self.recycle_uses:
            client.Close()
            return

        # reinicia el cliente para el próximo uso
        client.ResetFilters()
        client.ResetGroupBy()

        # actualiza el timeout del cliente y lo añade a la lista de clientes de nuevo
        client.timeout = time()+self.socket_timeout
        self.access.acquire()
        self.clients.append(client)
        self.access.release()

    def maintenance(self):
        # conecta clientes no conectados
        new_clients = []
        while not self.preclients.empty():
            client = self.preclients.get(False)
            try:
                client.Open()
                client.timeout = time()+self.socket_timeout
                new_clients.append(client)
                self.connection_failed = False
            except:
                logging.exception("Connection to search server %s:%d failed."%self.server)
                self.connection_failed = True
                with self.preclients.mutex: self.preclients.queue.clear()
                break

        add_clients = 0

        # va a tocar los clients
        self.access.acquire()

        # añade clientes conectados
        self.clients.extend(new_clients)

        now = time()
        # descarta conexiones caducadas
        while self.clients and self.clients[0].timeout<now:
            self.clients.popleft().Close()

        # no cuenta los clientes que caducaran en el siguiente intervalo de mantenimiento
        client_count = sum(1 for c in self.clients if c.timeout-now>self.maintenance_interval)

        # descarta conexiones que sobren (demasiadas)
        if client_count>self.max_free_clients:
            for i in xrange(client_count-self.max_free_clients):
                self.clients.popleft().Close()

        # calcula el numero de nuevas conexiones a crear
        elif client_count<self.min_free_clients:
            # solo crea una conexion si ha habido fallos
            add_clients = 1 if self.connection_failed else self.create_client_step

        # ya no necesita los clientes
        self.access.release()

        # añade los pre-clientes correspondientes
        for i in xrange(add_clients):
            self.preclients.put(self.create_sphinx_client())

    def create_sphinx_client(self):
        client = sphinxapi2.SphinxClient()
        client.SetConnectTimeout(self.socket_timeout)
        client.SetServer(self.server[0], self.server[1])
        client.uses = 0
        client.timeout = None
        return client

    def destroy(self):
        # va a tocar los clients
        self.access.acquire()

        # cierra conexiones
        for client in self.clients:
            client.Close()

        # ya no necesita los clientes
        self.access.release()


class SphinxWorker(Thread):
    def __init__(self, server, config, tasks, clients, proxy, *args, **kwargs):
        super(SphinxWorker, self).__init__(*args, **kwargs)
        self.server = server
        self.config = config
        self.tasks = tasks
        self.clients = clients
        self.proxy = proxy
        self.daemon = True
        self.stopped = False

    def stop(self):
        self.stopped = True
        self.tasks.put((StopWorker, None))

    def run(self):
        last_warn = 0
        while True:
            # loguea cuando la lista de tareas crece mucho
            if last_warn==0:
                if self.tasks.qsize()>10:
                    logging.warn("Too many searches in search queue.", extra={"thread_id":id(self), "size":self.tasks.qsize(), "server":self.server})
                    last_warn = 100
            else:
                last_warn-=1

            try:
                allsearches, asearch = self.tasks.get()
            except BaseException as e:
                logging.exception(e)
                continue

            # mensaje de parada
            if allsearches is StopWorker or self.stopped:
                break

            # pide un cliente de sphinx al pool de clientes
            client = self.clients.get_sphinx_client()

            # no puede atender peticiones sin cliente
            if not client:
                continue

            # inicializa variables
            results = None
            messages = (None, None)

            try:
                filters = asearch["f"] if "f" in asearch else {}
                query = asearch["q"]
                if "t" in query and query["t"]:
                    text = query["t"]
                else:
                    raise Exception("Empty query search received.")

                index = query["i"]+str(self.server)

                client.SetFieldWeights({"fn":100, "md":1})
                client.SetSortMode(sphinxapi2.SPH_SORT_EXTENDED, "rw DESC, r2 DESC, fs DESC, uri1 DESC" )
                client.SetMatchMode(sphinxapi2.SPH_MATCH_EXTENDED)

                if query["y"] == "related":
                    client.SetRankingMode(sphinxapi2.SPH_RANK_EXPR, "doc_word_count*max_lcs")
                    text = "%s (%s)" % (text, query["qrel"]) if text else query["qrel"]
                else:
                    client.SetRankingMode(sphinxapi2.SPH_RANK_EXPR, "sum((4*lcs+2.0/min_hit_pos)*user_weight)")
                    text = "@(fn,md) %s" % text

                # suma 10 a r, si r es 0, evita anular el peso de la coincidencia, si es -1, mantiene el peso positivo
                client.SetSelect("*,@weight*(r+10) as rw, min(if(z>0,z,100)) as zm, max(z) as zx")
                #logging.warn("busca en %d: %f" % (self.server, asearch["mt"]))
                client.SetMaxQueryTime(asearch["mt"])

                # filtra por rango de ids
                range_ids = query["ids"] if "ids" in query else None

                # traer resultados de uno o más grupos
                if "g" in asearch:
                    for group, first in asearch["g"].iteritems():
                        if range_ids: client.SetIDRange(range_ids[0], range_ids[1])
                        client.SetFilter('bl', [0])
                        client.SetFilter("g", [int(group)])
                        client.SetLimits(first, 10, 1000, 2000000)
                        if filters: self.apply_filters(client, filters)
                        client.AddQuery(text, index, "w_sg "+str(asearch["mt"])+" "+group)
                        client.ResetFilters()
                else:  # traer resumen principal de todos los grupos
                    if range_ids: client.SetIDRange(range_ids[0], range_ids[1])
                    client.SetFilter('bl', [0])
                    client.SetFilter("s", self.proxy.blocked_sources, True)
                    client.SetLimits(0, 300, 1000, 2000000)
                    client.SetGroupBy("g", sphinxapi2.SPH_GROUPBY_ATTR, "@count desc")

                    if asearch["st"]: # realiza la busqueda sin filtros
                        client.AddQuery(text, index, "w_st "+str(asearch["mt"]))
                    if asearch["sf"]: # realiza la busqueda con filtros
                        if filters: self.apply_filters(client, filters)
                        client.AddQuery(text, index, "w_sf "+str(asearch["mt"]))

                results = client.RunQueries()
                messages = (client.GetLastWarning(), client.GetLastError())
                if messages[1]:
                    results = None
                self.clients.return_sphinx_client(client, bool(results))
            except BaseException as e:
                results = None
                messages = (None, e.message)
                server = self.server
                logging.exception(e)
                self.clients.return_sphinx_client(client, True)
            finally:
                self.proxy.put_results(allsearches, asearch, self.server, results, messages)

    def apply_filters(self, client, filters):
        if "z" in filters:
            client.SetFilterFloatRange('z', float(filters["z"][0]), float(filters["z"][1]))
        if "e" in filters:
            client.SetFilterRange('e', filters["e"])
        if "ct" in filters:
            client.SetFilter('ct', filters["ct"])
        if "src" in filters:
            client.SetFilter('s', set(filters["src"]).difference(self.proxy.blocked_sources))

def search_subgroup_info():
    return {"c": defaultdict(int), "z":[0,100]}

def filters_group_info():
    return {"g2": defaultdict(filters_group2_info)}
def filters_group2_info():
    return {"sg": defaultdict(filters_subgroup_info)}
def filters_subgroup_info():
    return {"c":defaultdict(int), "lv":defaultdict(int), "h":[], "f":{}}

class Search(object):
    def __init__(self, proxy, query, filters, wait_time=500):
        self.access = Lock()
        self.has_changes = False
        self.proxy = proxy
        self.max_query_time = proxy.config["SERVICE_SPHINX_MAX_QUERY_TIME"]
        self.search_max_retries = proxy.config["SERVICE_SPHINX_SEARCH_MAX_RETRIES"]
        self.stats = None
        self.extra_wait_time = wait_time
        self.computable = True
        self.tags = []

        # normaliza texto de busqueda
        computable = True
        text = None
        if "text" in query:
            text = query["text"].strip().lower()
            text = " ".join(self.parse_query(text)).encode("utf-8")

        # analiza los tags


        # parsea filtros
        self.filters = {}
        if filters:
            if 'type' in filters:
                self.filters['ct'] = [sphinx_type for atype in filters["type"] for sphinx_type in CONTENTS_CATEGORY[atype]]

            if 'size' in filters:
                sizes = filters["size"]
                self.filters['z'] = [float(sizes[0]),float(sizes[1])]

            if "src" in filters:
                groups={"s":"streaming","w":"download","f":"download","p":"p2p","g":"gnutella","t":"torrent","e":"ed2k"}
                src = filters["src"]
                self.filters["src"] = [source_id for source_id, source in self.proxy.sources.iteritems()
                        if source["d"][:source["d"].rfind(".")] in src #esta el dominio en la URL
                        or any(groups[group] in src for group in source["g"]) #si viene el origen en vez del suborigen
                        or ("other-streamings" in src and source["d"] not in self.proxy.sources_relevance_streaming[:8] and "s" in source["g"]) #si viene other... y no esta en la lista de sources y el source tiene streaming
                        or ("other-downloads" in src and source["d"] not in self.proxy.sources_relevance_download[:8] and ("w" in source["g"] or "f" in source["g"])) #si viene other... y no esta en la lista de sources y el source tiene web
                ]

        # extrae informacion de la query recibida
        query_type = query["type"]
        if query_type=="related":
            self.query_key = u"R"+mid2hex(query["id"])
            self.query = {"y":query_type, "i": "idx_files", "id":query["id"]}
            if text:
                self.query["t"] = text
                self.query_key += md5(text).hexdigest()
        elif query_type=="list":
            self.query_key = u"L"+md5(query["user"]+"."+query["list"]).hexdigest()
            self.query = {"y":query_type, "i": "idx_lists", "ids": (query["user"]<<32, query["user"]<<32 | 0xFFFFFFFF)}
        elif query_type=="text":
            self.query_key = u"Q"+md5(text).hexdigest()
            self.query = {"y":query_type, "i": "idx_files", "t": text, "g":self.tags}

        self.query_state, self.filters_state, self.locked_until = proxy.get_search_state(self.query_key, self.filters)

        # para busquedas de relacionados
        if query_type=="related":
            if "qrel" in self.query_state:
                self.query["qrel"] = self.query_state["qrel"]
            else:
                file_data = query["file_data"] if "file_data" in query else self.proxy.filesdb.get_file(query["id"])
                self.query["qrel"] = " | ".join({escape_string(phrase) for phrase in split_file(file_data)})


    def parse_query(self, query):
        # inicializa variables
        acum = []
        yield_acum = False

        valid_acum = False          # indica que esta parte de la consulta tiene algún caracter letra o número
        not_mode = False            # indica que esta parte de la consulta está en modo negativo
        quote_mode = False          # indica que esta parte de la consulta va en entre comillas
        tag_mode = False            # indica que esta parte de la consulta es un tag
        any_not_not_part = False    # indica que alguna parte de la consulta no está en modo negativo

        # recorre caracteres (añade un espacio para considerar la ultima palabra)
        for ch in query+" ":
            if not acum and (ch=="-" or ch=="!"): # operador not
                not_mode = True
            elif ch=="(" and not tag_mode and not quote_mode:
                yield_acum = True
                tag_mode = True
            elif ch==")" and tag_mode:
                yield_acum = "\""
                tag_mode = False
            elif ch=="\"": # comillas
                if quote_mode:
                    yield_acum = "\"" # indica que incluya comillas en el resultados
                else:
                    yield_acum = True
                quote_mode = not quote_mode
            elif ch in whitespace: # separadores de palabras fuera de comillas
                if not quote_mode and not tag_mode:
                    yield_acum = True
                else:
                    acum.append(" ")
            else: # resto de caracters
                valid_acum = valid_acum or not (ch in SEPPER or ch in u"'ºª")
                acum.append(ch)

            # si toca devolver resultado, lo hace
            if yield_acum:
                if acum:
                    if yield_acum=="(":
                        acum_result = "".join(acum) if valid_acum else False
                        if acum_result:
                            any_not_not_part = any_not_not_part or not not_mode
                            self.tags.append((not not_mode, acum_result))
                    else:
                        acum_result = escape_string("".join(acum)) if valid_acum else False
                        if acum_result:
                            any_not_not_part = any_not_not_part or not not_mode
                            if yield_acum=="\"":
                                yield "%s\"%s\""%("-" if not_mode else "", acum_result)
                            else:
                                yield "%s%s"%("-" if not_mode else "", acum_result)
                    del acum[:]
                    valid_acum = False
                    not_mode = False
                yield_acum = False

        # si no se han devuelto partes no negativas, la consulta no es computable
        if not any_not_not_part:
            self.computable = False

    def _get_wait_time(self, multiplier, search_query=True, search_filters=True):
        return int(self.max_query_time*(1 + log(max(1, self.query_state["rt"] if search_query and self.query_state else 0,
                       self.filters_state["rt"] if search_filters and self.filters_state else 0))*multiplier))

    def search(self):
        # si la busqueda esta bloqueada o no es computable, sale sin hacer nada
        if self.locked_until or not self.computable:
            return self

        # comprueba a qué servidores debe pedirsele los datos iniciales de la busqueda
        now = time()

        can_retry_query = not self.query_state or self.query_state["rt"]<self.search_max_retries
        search_query = [i for i in self.proxy.servers.iterkeys()
                                if (can_retry_query and (not i in self.query_state["c"] or i in self.query_state["i"]))
                                    or self.query_state["d"]<self.proxy.servers_stats[i]["d1"]] \
                            if self.query_state else self.proxy.servers.keys()

        can_retry_filters = not self.filters_state or self.filters_state["rt"]<self.search_max_retries
        search_filters = [i for i in self.proxy.servers.iterkeys()
                                if (can_retry_filters and (not i in self.filters_state["c"] or i in self.filters_state["i"]))
                                    or self.filters_state["d"]<self.proxy.servers_stats[i]["d1"]] \
                            if self.filters_state else self.proxy.servers.keys()

        if search_query or search_filters:
            # tiempo de espera incremental logaritmicamente con los reintentos (elige el tiempo máximo de espera)
            wait_time = self._get_wait_time(3, search_query, search_filters)
            asearch = {"q":self.query, "f":self.filters, "mt":wait_time}

            # si no tiene estado para busqueda con filtros, lo crea
            if not self.filters_state:
                self.filters_state = {"cs":0, "v":0, "c": defaultdict(int), "t": defaultdict(int), "i":[], "r":[], "d": defaultdict(int), "g": defaultdict(filters_group_info), "rt": 0, "df":defaultdict(int)}

            # si no tiene estado para busqueda sin filtros, lo crea
            if not self.query_state:
                self.query_state = {"c": defaultdict(int), "t": defaultdict(int), "i":[], "d": defaultdict(int), "sg": defaultdict(search_subgroup_info), "rt": 0}

            if "qrel" in self.query:
                asearch["qrel"] = self.query_state["qrel"] = self.query["qrel"]

            # solo debe buscar con filtros si hay filtros o si no va a buscar la busqueda base
            must_search_filters = self.filters or not search_query

            # compone la búsqueda con las consultas correspondientes a cada servidor
            allsearches = {"s":
                            {server:dict(asearch.items() +  # busqueda base
                                        # buscar texto si toca en este servidor
                                        [("st",server in search_query),
                                        # buscar con filtros si se debe y toca en este servidor
                                        ("sf",must_search_filters and server in search_filters)
                                        ])
                            for server in set(search_query+(search_filters if must_search_filters else []))
                            }
                        }

            # realiza la busqueda y almacena los resultados
            self.proxy.search(allsearches)

            # espera el tiempo de sphinx mas medio segundo de extra
            # cuando hay dos busquedas en algun servidor (con y sin filtros) se espera el doble
            max_wait_time = (wait_time*2 if any((s["st"] and s["sf"]) for s in allsearches["s"].itervalues()) else wait_time)+self.extra_wait_time

            # si no llegan todos los resultados espera otro segundo más, por si hubiera habido problemas en la respuesta
            self.store_files(allsearches, max_wait_time, self.store_files_timeout, [1000])

        return self

    def update_stats(self):
        self.filters_state["cs"] = sum(self.filters_state["c"].itervalues()) - sum(self.filters_state["df"].itervalues())
        groups = self.filters_state["g"]

        # w=peso del grupo, l=ultimo elemento extraido, lw=ultimo peso
        for g, og in groups.iteritems():
            gts = 0
            for g2, og2 in og["g2"].iteritems():
                gts2 = 0
                for sg, osg in og2["sg"].iteritems():
                    ts = osg["cs"] = sum(osg["c"].itervalues())
                    gts2 += ts
                gts += gts2
                og2["cs"] = gts2
            og["cs"] = gts

    def save_state(self, locking_time):
        ''' Guarda en cache los datos de la búsqueda.
        Estos se dividen en dos partes:
         - la lista de ficheros que aplican para esta búsqueda de texto.
         - las listas de ficheros resultante para los filtros dados. '''
        self.filters_state["v"]+=1
        self.proxy.save_search_state(self.query_key, self.filters, self.query_state, self.filters_state, locking_time)

    def generate_stats(self):
        # generar estadisticas de la busqueda
        if not self.stats:
            if self.computable:
                self.stats = {k:v for k,v in self.filters_state.iteritems() if k in ("v", "t")}
                self.stats["cs"] = sum(count for server_id, count in self.filters_state["c"].iteritems() if server_id in self.proxy.servers) - sum(diff for server_id, diff in self.filters_state["df"].iteritems() if server_id in self.proxy.servers)


                self.stats["s"] = not (self.query_state["i"] or self.filters_state["i"] or
                                        self.proxy.servers_set.difference(set(self.query_state["c"])) or
                                        self.proxy.servers_set.difference(set(self.filters_state["c"])))
                self.stats["ct"] = self.query_state["ct"] if "ct" in self.query_state else None
            else:
                self.stats = {"cs":0, "v":0, "t":0, "s":True, "w":0, "li":[], "ct":None}

    def get_stats(self):
        self.generate_stats()
        return self.stats

    def get_modifiable_info(self):
        self.access.acquire(True)
        if self.has_changes: self.update_stats()

        groups = self.filters_state["g"]
        info = {}
        # w=peso del grupo, l=ultimo elemento extraido, lw=ultimo peso
        for g, og in groups.iteritems():
            g_weight = 0
            ig = info[g] = {"cs":og["cs"], "l":0, "g2":{}}

            for g2, og2 in og["g2"].iteritems():
                ig2 = ig["g2"][g2] = {"cs":og2["cs"], "l":0, "sg":{}}
                g2_weight = 0
                for sg, osg in og2["sg"].iteritems():
                    weight = -osg["h"][0][0]
                    if g==0: weight/4.0  # reduce peso al grupo desconocido
                    isg = ig2["sg"][sg] = {"cs":osg["cs"], "h":osg["h"][:], "f":osg["f"], "lw":weight, "l":0}
                    if weight>g2_weight: g2_weight = weight

                ig2["lw"] = ig2["w"] = g2_weight
                if g2_weight>g_weight: g_weight = g2_weight
            ig["lw"] = ig["w"] = g_weight

        self.access.release()
        return info

    def get_results(self, last_items=[], min_results=5, max_results=10, hard_limit=10000):

        # si todavia no hay informacion de filtros, sale sin devolver nada
        if not "g" in self.filters_state or not self.computable:
            raise StopIteration

        info = self.get_modifiable_info()
        groups = self.filters_state["g"]

        must_return = True
        stop_browsing = False

        # busquedas derivadas de la obtencion de resultados
        max_searches = 0 # numero maximo de busquedas en algun servidor
        searches = defaultdict(dict)

        returned = 0
        versions = [0]*self.filters_state["v"]
        last_versions = versions[:]
        last_items_len = len(last_items)
        last_versions[:last_items_len] = last_items
        new_versions = last_versions[:]

        for i in xrange(min(hard_limit,self.filters_state["cs"])):

            # si se han devuelto el minimo y no se puede más o se ha devuelto el maximo, para
            if returned>=max_results or stop_browsing and returned>=min_results: break

            # busca grupo del que sacar un resultado (por content_type)
            filtered_groups = [(og["lw"], og["l"], g, og) for g, og in info.iteritems() if og["lw"]>-1]
            if filtered_groups:
                w, l, g, og = max(filtered_groups)
            else:
                break

            # busca grupo del que sacar un resultado (por origen)
            filtered_groups2 = [(og2["lw"], og2["l"], g2, og2) for g2, og2 in og["g2"].iteritems() if og2["lw"]>-1]
            if filtered_groups2:
                w2, l2, g2, og2 = max(filtered_groups2)
            else:
                og["lw"]=-1
                if len(filtered_groups)==1:
                    break
                continue

            # busca subgrupo del que sacar un resultado
            filtered_subgroups = [(osg["lw"], osg["l"], sg, osg) for sg, osg in og2["sg"].iteritems() if osg["lw"]>-1]
            if filtered_subgroups:
                ws, ls, sg, osg = max(filtered_subgroups)
            else:
                og2["lw"]=-1
                if len(filtered_groups2)==1:
                    og["lw"] = -1
                    if len(filtered_groups)==1:
                        break
                continue

            # actualiza pesos y contador de resultados obtenidos de grupos y subgrupo
            l+=1
            l2+=1
            ls+=1
            og["l"] = l
            og2["l"] = l2
            osg["l"] = ls

            # obtiene resultado del subgrupo y su información asociada
            result_weight, result_id = heappop(osg["h"])
            server, sphinx_id, version, search_position = osg["f"][result_id]

            # actualiza peso del subgrupo
            if osg["h"]:
                osg["lw"] = -osg["h"][0][0]
            else:
                osg["lw"] = -1

            og2["lw"] = max(osg["lw"] for osg in og2["sg"].itervalues())
            if og2["lw"]>-1:
                og2["lw"]/=l2*2.

            og["lw"] = max(og2["lw"] for og2 in og["g2"].itervalues())
            if og["lw"]>-1:
                og["lw"]/=l*2.

            # incrementa el contador para esta version
            versions[version]+=1

            # guarda grupos de los que hay que pedir más
            group = groups[g]["g2"][g2]["sg"][sg]

            # si no existe el servidor, ignora el resultado
            if server not in self.proxy.servers:
                continue

            # devuelve el resultado
            if versions[version]>last_versions[version] and (must_return or returned<min_results):
                returned+=1
                new_versions[version] = versions[version]
                yield (result_id, server, sphinx_id, -result_weight)

            if group["c"][server] > search_position >= group["lv"][server]:
                # no realiza más de 4 busquedas por vez en un servidor para no tener que esperar mucho
                if not self.locked_until:
                    searches_len = len(searches[server])
                    if sg not in searches[server] and searches_len<4:
                        searches[server][sg]=group["lv"][server]
                        if searches_len >= max_searches:
                            max_searches = searches_len+1
                    elif searches_len==4:
                        stop_browsing = True

                # ya no debe devolver resultados si no son forzados
                must_return = False


        self.generate_stats()
        self.stats["li"] = new_versions

        if self.locked_until:
            self.stats["w"] = 500+self.locked_until-time()
        elif searches:
            wait_time = self._get_wait_time(0.5, False, True)
            allsearches = {"s":{server: {"st":False, "sf":False, "q":self.query, "f":self.filters, "mt":wait_time, "g":subgroups} for server, subgroups in searches.iteritems()}}
            self.proxy.search(allsearches)
            max_wait_time = wait_time * max_searches
            self.proxy.lock_state(self.query_key, self.filters, 500+max_wait_time)
            self.proxy.async_process_results(allsearches, self.store_files_timeout, [500+max_wait_time/4, 500+max_wait_time/2, 500+max_wait_time])
            self.stats["w"] = 1000+max_wait_time/4
        elif returned:
            self.stats["w"] = 0
        else:
            self.stats["w"] = self.max_query_time*2

    def store_files_timeout(self, allsearches, timeouts):
        self.has_changes = False
        new_query_state, new_filters_state, self.locked_until = self.proxy.get_search_state(self.query_key, self.filters)
        if new_filters_state and new_filters_state["v"]>self.filters_state["v"]:
            self.query_state, self.filters_state = new_query_state, new_filters_state

        self.store_files(allsearches, 0, self.store_files_timeout, timeouts)

    def store_files(self, allsearches, timeout, fallback=None, fallback_timeouts=None):
        groups = self.filters_state["g"]
        subgroups = self.query_state["sg"]

        # indica si se ha recorrido algun resultado de resumen de la busqueda
        any_query_main = any_filters_main = False

        for server, asearch, sphinx_results, messages in self.proxy.browse_results(allsearches, timeout, fallback, fallback_timeouts):
            self.access.acquire(True)
            # permite manejar igual resultados de querys simples o de multiquerys
            if not sphinx_results:
                logging.exception("Error in search thread:'%s'"%messages[1])
                self.access.release()
                continue
            elif "matches" in sphinx_results:
                sphinx_results = [sphinx_results]

            # comprueba si es una consulta de resumen
            if asearch["sf"] and asearch["st"]:
                main = [(True, False), (False, True)]
                any_query_main = any_filters_main = True
            elif asearch["sf"]:
                main = [(False, True)]
                any_filters_main = True
            elif asearch["st"]:
                main = [(True, True)]
                any_query_main = any_filters_main = True
            else:
                main = False

            # por defecto los valores son válidos
            valid = True

            # incorpora resultados al subgrupo que corresponda
            for result in sphinx_results:
                if not result:
                    logging.exception("No results received from server.")
                    continue

                elif result["error"]:
                    logging.exception("Search error (server %d): %s" % (server, result["error"]))
                    continue

                elif result["warning"]:
                    valid = False # los resultados se usan, pero se marcan como inválidos para próximas veces
                    if result["warning"]!="query time exceeded max_query_time": # no loguea el caso más comun
                        logging.exception("Warning on search (server %d): %s" % (server, result["warning"]))

                # almacena los ficheros cuando se recorre el resumen con filtros o más resultados
                must_store_files = not main or main[0][1]

                total = 0
                for index, r in enumerate(result["matches"]):

                    # calcula el subgrupo y el id del fichero
                    sg = str(r["attrs"]["g"])
                    fid = bin2hex(full_id_struct.pack(r["attrs"]["uri1"],r["attrs"]["uri2"],r["attrs"]["uri3"]))
                    g = get_group(sg)
                    g2 = get_group2(sg)
                    rating = r["attrs"]["r"]
                    weight = r["weight"]
                    if main:
                        count = r["attrs"]["@count"]
                        first = search_position = 1
                        total += count
                    else:
                        first = asearch["g"][sg]+1
                        search_position = first+index

                    subgroup = subgroups[sg]

                    # accede al grupo sólo si se va a almacenar el fichero (ak acceder se crea por ser un defaultdict)
                    if must_store_files:
                        group = groups[g]["g2"][g2]["sg"][sg]

                    # actualiza totales de grupos y subgrupos
                    if main:
                        if main[0][0]: # almacena en query_state
                            self.query_state["d"][server]=time()
                            if count>subgroup["c"][server]:
                                subgroup["c"][server] = count
                                subgroup["z"][0] = max(subgroup["z"][0], r["attrs"]["zm"])
                                subgroup["z"][1] = min(subgroup["z"][1], r["attrs"]["zx"])

                        if main[0][1]: # almacena en filters_state
                            # estadisticas
                            self.filters_state["d"][server]=time()
                            if count>group["c"][server]:
                                group["c"][server] = count

                    # almacena el fichero
                    if must_store_files:
                        if fid in group["f"]:
                            # ya se ha encontrado en otro servidor
                            if server != group["f"][fid][0]:
                                prev_server = group["f"][fid][0]
                                if server>prev_server:
                                    group["f"][fid][0] = server
                                    self.filters_state["df"][prev_server] += 1
                                    group["f"][fid][2] = self.filters_state["v"]
                                else:
                                    self.filters_state["df"][server] += 1

                            # ya se ha encontrado en este servidor antes, probablemente por fallo de sphinx
                            elif group["f"][fid][3]!=search_position:
                                group["f"][fid][3] = search_position
                        else:
                            std_dev = round(self.proxy.sources_rating_standard_deviation[g2], 3) if rating>=0 and g2 in self.proxy.sources_rating_standard_deviation and self.proxy.sources_rating_standard_deviation[g2] else 0
                            if std_dev>0:
                                val = (self.proxy.sources_rating_average[g2]-rating)/std_dev
                                val = max(-500, min(500, val))
                            else:
                                val = 0
                                rating = 0.5 if rating==-1 else 1.1 if rating==-2 else rating

                            normalized_weight = weight*self.proxy.sources_weights[g2]*(1./(1+exp(val)) if std_dev else rating)
                            heappush(group["h"], (-normalized_weight, fid))
                            group["f"][fid] = [server, r["id"], self.filters_state["v"], search_position]

                    # actualiza el último registro obtenido del resumen para el grupo en el servidor
                    if main and main[0][1]:
                        group["lv"][server] = max(1,group["lv"][server])

                # totales absolutos
                if main:
                    self.has_changes = True
                    if main[0][0]: # almacena en query_state
                        # actualiza el numero de ficheros en el servidor
                        if total > self.query_state["c"][server]:
                            self.query_state["c"][server] = total
                        self.query_state["t"][server] = result["time"]

                        # actualiza informacion sobre fiabilidad de los datos
                        if valid and server in self.query_state["i"]: self.query_state["i"].remove(server)
                        elif not valid and server not in self.query_state["i"]: self.query_state["i"].append(server)

                    if main[0][1]: # almacena en filters_state
                        # actualiza el numero de ficheros en el servidor
                        if total > self.filters_state["c"][server]:
                            self.filters_state["c"][server] = total
                        self.filters_state["t"][server] = result["time"]

                        # actualiza informacion sobre fiabilidad de los datos
                        if valid and server in self.filters_state["i"]: self.filters_state["i"].remove(server)
                        elif not valid and server not in self.filters_state["i"]: self.filters_state["i"].append(server)

                    main = main[1:]

                    if not "ct" in self.query_state and "words" in result:
                        self.query_state["ct"] = "_".join(word["word"] for word in result["words"])

                # ha recorrido más resultados
                elif result["matches"]:
                    self.has_changes = True
                    group["lv"][server] = max(search_position, group["lv"][server])
            self.access.release()

        if self.has_changes:
            # actualiza numero de reintentos en ambos estados
            if any_query_main: self.query_state["rt"]+=1
            if any_filters_main: self.filters_state["rt"]+=1
            self.access.acquire(True)
            self.update_stats()
            self.save_state(False if allsearches["_pt"]==0 else None if timeout==0 else fallback_timeouts[-1])
            self.has_changes = False
            self.access.release()

class Searchd:
    def __init__(self):
        pass

    def init_app(self, app, filesdb):
        self.proxy = SearchProxy(app.config, filesdb)

    def search(self, query, filters={}, wait_time=500):
        s = Search(self.proxy, query, filters, wait_time)
        return s.search()

    def get_search_info(self, query, filters={}):
        s = Search(self.proxy, query, filters)
        try:
            temp = s.get_modifiable_info()
        except:
            temp = False
        return {"query":s.query_state, "filters":s.filters_state, "temp":temp, "locked":s.locked_until}

    def block_files(self, ids=[], block=True):
        return self.proxy.block_files(ids, block)

    def get_id_server_from_search(self, file_id, file_name):
        return self.proxy.get_id_server_from_search(file_id, file_name)

    def get_sources_stats(self):
        return self.proxy.sources_relevance_streaming, self.proxy.sources_relevance_download, self.proxy.sources_relevance_p2p


_escaper = re.compile(r"([=|\-!@~&/\\\)\(\"\^\$\=])")
unespacer = re.compile(" +")
def escape_string(text):
    return "%s"%_escaper.sub(r"\\\1", unespacer.sub(" ",text)).strip()


# -*- coding: utf-8 -*-

from threading import Thread
from Queue import Queue, Empty
from collections import defaultdict
from hashlib import md5
from time import time
from random import shuffle
from math import sqrt, log
import json

from foofind.utils import mid2hex, hex2bin, logging
from foofind.utils.event import EventManager
from foofind.services.extensions import cache
from search import full_id_struct, part_id_struct, get_src, escape_string
from sphinx_pool import SphinxClientPool
from worker import SphinxWorker

class SearchProxy:
    def __init__(self, config, filesdb, entitiesdb, profiler):
        self.config = config
        self.filesdb = filesdb
        self.entitiesdb = entitiesdb
        self.profiler = profiler

        # inicializa variables
        self.tasks = {}
        self.client_pools = {}
        self.workers = {}
        self.last_profiling_info = {}

        # logs de informacion
        self.bot_events = defaultdict(int)
        self.timeouts = defaultdict(int)

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

        self.pendings_processor.interval(config["SERVICE_SEARCH_PROFILE_INTERVAL"], self.save_profile_info)
        self.pendings_processor.interval(config["SERVERS_REFRESH_INTERVAL"], self.update_servers)
        self.pendings_processor.interval(config["SERVICE_SEARCH_MAINTENANCE_INTERVAL"], self.maintenance)

    def save_profile_info(self):
        # actualizaciones y procesos de respuestas pendientes
        profiling_info = {"updates": self.updates.qsize(), "pendings":len(self.pendings_processor._timers)}

        # informacion de accesos de bots
        profiling_info.update(self.bot_events)
        self.bot_events = defaultdict(int)

        # informacion de timeouts esperando respuesta de servidores de sphinx
        profiling_info.update(self.timeouts)
        self.timeouts = defaultdict(int)

        # tamaño de las colas de tareas acumuladas para los workers
        for server_id, t in self.tasks.iteritems():
            profiling_info["sp_tasks"+server_id] = t.qsize()

        # información de pool de conexiones
        for server_id, cp in self.client_pools.iteritems():
            profiling_info["sp_conns"+server_id] = cp.clients_counter
            profiling_info["sp_freeconns"+server_id] = len(cp.clients)
            profiling_info["sp_preconns"+server_id] = cp.max_clients_counter
            profiling_info["sp_adhoc"+server_id] = cp.adhoc

        # evita repetir valores a cero
        for key, value in profiling_info.items():
            if value==0 and key in self.last_profiling_info and self.last_profiling_info[key]==0:
                del profiling_info[key]

        # guarda información
        self.profiler.save_data(profiling_info)

        # actualiza ultimos valores
        self.last_profiling_info.update(profiling_info)

    def maintenance(self):
        # información de pool de conexiones
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
                self.log_timeout(allsearches["_pt"])

    def async_process_results(self, allsearches, pending_callback, pending_timeouts):
        # si hay un retraso de más de 5 minutos en atender la próxima petición, no añade tareas pendientes
        if self.pendings_processor._timers and time()-self.pendings_processor._timers[0][0]>300:
            logging.warning("Too many pendings task for search.", extra={"queue_size":len(self.pendings_processor._timers), "now": time(), "next": self.pendings_processor._timers[0], "last": self.pendings_processor._timers[-5:]})
        else:
            self.pendings_processor.timeout(pending_timeouts[0]*0.001, pending_callback, hargs=[allsearches, pending_timeouts[1:]])

    def schedule_call(self, timeout, call, hargs):
        self.pendings_processor.timeout(timeout, call, hargs)

    def get_search_state(self, querykey, filters):
        filterskey = md5(json.dumps(filters)).hexdigest()

        # claves a obtener del cache
        query_cache_key = "search_t%s"%querykey
        filters_cache_key = "search_f%s_%s"%(querykey, filterskey)
        lock_cache_key = "search_l%s_%s"%(querykey, filterskey)
        block_files_cache_key = "search_b%s_%s"%(querykey, filterskey)

        # obtiene valores
        cache_data = cache.cache.get_many(query_cache_key, filters_cache_key, lock_cache_key, block_files_cache_key)

        # extrae informacion de los valores obtenidos
        query_state = cache_data[0] or {}
        filters_state = json.loads(cache_data[1]) if query_state and cache_data[1] else {}
        locked_until = cache_data[2] or False
        block_files_from_cache = [block_file.split(",") for block_file in cache_data[3].split(";")] if cache_data[3] else []

        return (query_state, filters_state, locked_until, block_files_from_cache)

    def get_lock_state(self, querykey, filters):
        filterskey = md5(json.dumps(filters)).hexdigest()
        return cache.get("search_l%s_%s"%(querykey, filterskey)) or False

    def get_blocked_files_state(self, querykey, filters):
        filterskey = md5(json.dumps(filters)).hexdigest()
        block_files_cache_key = "search_b%s_%s"%(querykey, filterskey)
        cache_data = cache.cache.get(block_files_cache_key)
        block_files_from_cache = [block_file.split(",") for block_file in cache_data.split(";")] if cache_data else []
        return block_files_from_cache

    def save_search_state(self, querykey, filters, query_state, filters_state, locking_time=None, blocked_ids=None):

        # guarda valores en cache
        filterskey = md5(json.dumps(filters)).hexdigest()

        if filters_state:
            filters_state = json.dumps(filters_state)

        # borra ids bloqueados
        if blocked_ids:
            block_files_cache_key = "search_b%s_%s"%(querykey, filterskey)
            cache.delete(block_files_cache_key) # TO-DO: podría borrar ids nuevos a bloquear que no han sido bloqueados

        cache.cache.set_many({"search_t%s"%querykey: query_state, "search_f%s_%s"%(querykey, filterskey): filters_state},
                            timeout=0)

        # procesa bloqueo separadamente
        if locking_time==False: # borrar bloqueo
            cache.delete("search_l%s_%s"%(querykey, filterskey))
            return False
        elif locking_time: # establecer bloqueo
            return self.set_lock_state(querykey, filters, locking_time)

        return None # este valor debe ignorarse, no se sabe la fecha de fin de bloqueo

    def set_lock_state(self, querykey, filters, locking_time):
        filterskey = md5(json.dumps(filters)).hexdigest()
        locked_until = time()+locking_time*0.001
        cache.set("search_l%s_%s"%(querykey, filterskey), locked_until, timeout = locking_time*0.001)
        return locked_until

    def update_sources(self):
        # obtiene los origenes
        self.sources = {int(s["_id"]): s for s in self.filesdb.get_sources(blocked=None)}

        # actualiza origenes bloqueados
        self.blocked_sources = [sid for sid, s in self.sources.iteritems() if "crbl" in s and int(s["crbl"])!=0]

        # calcula pesos para grupos de origenes
        groups_weights = {"e":0.01, "g":0.005} # por grupos

        # calcula pesos para origenes individuales por calidad
        self.sources_weights = {}
        speeds = {}
        max_kbps = 1000
        max_second_delay = 120.0
        for sid, s in self.sources.iteritems():
            if sid in self.blocked_sources:
                continue

            group_weight = max([v for k,v in groups_weights.iteritems() if k in s["g"]] or [1])

            if "quality" in s:
                quality = s["quality"]
            else:
                self.sources_weights[sid] = 0.8*group_weight
                continue

            if not (quality and "rating" in quality):
                self.sources_weights[sid] = 0.8*group_weight
                continue

            rating = quality["rating"]*.15+0.001 # evita valores 0 absolutos
            kbps = quality.get("kbps", None)
            # evita valores superiores a 1 mega
            if kbps and kbps>max_kbps:
                kbps = max_kbps

            second_delay = quality.get("second_delay", 0)
            # evita valores superiores a 2 minuto
            if second_delay and second_delay>max_second_delay:
                second_delay = max_second_delay

            speed = (1.5*log(kbps+1)/log(max_kbps) if kbps else 1)*(1.5-sqrt(second_delay/max_second_delay) if second_delay else 1)

            # problems
            login = quality.get("login", 0)*0.2
            password = quality.get("password", 0)*0.1
            no_downloadable = quality.get("no_downloadable", 0)*0.3
            geoblocking = quality.get("geoblocking", 0)*0.3
            problems = 1-(login+password+no_downloadable+geoblocking)

            self.sources_weights[sid] = group_weight*rating*problems*speed

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
        self.servers = {str(int(server["_id"])):(str(server["sp"]), int(server["spp"])) for server in self.filesdb.get_servers() if "sp" in server and server["sp"]}
        self.servers_set = set(self.servers.iterkeys())

        # estadisticas de busqueda por servidor
        new_servers_stats = {server_id:self.filesdb.get_server_stats(int(server_id)) for server_id in self.servers.iterkeys()}

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
        maintenance_interval = self.config["SERVICE_SEARCH_MAINTENANCE_INTERVAL"]
        client_min = self.config["SERVICE_SPHINX_CLIENT_MIN"]
        client_recycle = self.config["SERVICE_SPHINX_CLIENT_RECYCLE"]
        connect_timeout = self.config["SERVICE_SPHINX_CONNECT_TIMEOUT"]
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
                self.client_pools[server_id] = SphinxClientPool(address, maintenance_interval, client_min, client_recycle, connect_timeout)
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

                    index = update["index"]+server
                    all_filenames = update["filenames"]

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
                        bin_file_id = hex2bin(file_id)
                        uri1, uri2, uri3 = full_id_struct.unpack(bin_file_id)
                        client.SetFilter('uri1', [uri1])
                        client.SetFilter('uri2', [uri2])
                        client.SetFilter('uri3', [uri3])
                        client.SetIDRange(part_id_struct.unpack(bin_file_id[:5]+"\x00\x00\x00")[0], part_id_struct.unpack(bin_file_id[:5]+"\xFF\xFF\xFF")[0])
                        client.SetLimits(0,1,1,1)

                        results = client.Query(escape_string(all_filenames[file_id]) if file_id in all_filenames else "", index, "w_u"+str(file_id[:6]))

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
                logging.error("Error updating sphinx: %s."%repr(e))
            finally:
                if client:
                    self.client_pools[server].return_sphinx_client(client)

    def block_files(self, ids=[], block=True, query_key=None, filters=None):
        # valor a enviar
        value = [1 if block else 0]

        # separa los ids por servidor
        all_updates = defaultdict(dict)
        all_filenames = defaultdict(dict)
        for sphinx_id in ids:
            if isinstance(sphinx_id, tuple):
                if len(sphinx_id)==5:
                    sphinx_id, server, filename, file_id, sg = sphinx_id
                else:
                    sphinx_id, server, filename = sphinx_id
            else:
                server = "*"
            all_updates[server][sphinx_id] = value
            if filename:
                all_filenames[server][sphinx_id] = filename

        # encola una consulta por servidor
        for server, values in all_updates.iteritems():
            self.updates.put({"server":server, "index":"idx_files", "attrs":["bl"], "values":values, "filenames":all_filenames[server]})

        # encola en cache la eliminación de ficheros
        if block and query_key and filters:
            filterskey = md5(json.dumps(filters)).hexdigest()
            block_files_cache_key = ("search_b%s_%s"%(query_key, filterskey)).encode("utf-8")
            new_ids_to_block = ";".join("%s,%s,%s"%(server, file_id, sg) for sphinx_id, server, filename, file_id, sg in ids).encode("utf-8")

            # guarda en cache los ids a bloquear
            cache.append(block_files_cache_key, new_ids_to_block, create=True, separator=";", timeout=0)

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
                results = client.Query(query, index, "w_id"+str(mid2hex(file_id)[:6]))

                # comprueba resultados obtenidos
                if results and "matches" in results and results["matches"]:
                    return server

            except BaseException as e:
                logging.error("Error getting id from sphinx: %s."%repr(e))
            finally:
                if client:
                    self.client_pools[server].return_sphinx_client(client)
        return None

    def log_bot_event(self, bot, result):
        self.bot_events[("bot_" if result else "bot_no_") + bot] += 1

    def log_timeout(self, servers):
        for server in servers:
            self.timeouts["sp_timeout" + str(server)] += 1

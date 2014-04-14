# -*- coding: utf-8 -*-
from geventconnpool import ConnectionPool, retry
from gevent import signal, sleep, socket, monkey; monkey.patch_socket()
from gevent.pool import Pool
from time import time
from struct import Struct
from itertools import izip
from datetime import datetime
from signal import SIGINT
import argparse, sphinxapi, redis, re, logging

from raven import Client
from raven.handlers.logging import SentryHandler
from raven.conf import setup_logging

from common import *

# configuracion
DEFAULT_WORKERS = 15
LOCK_EXPIRATION = 10 # segundos
INDEX_NAME = "idx_files"
SPHINX_SOCKET_TIMEOUT = 120.
REDIS_TIMEOUT = 300.

DEFAULT_ORDER = "e DESC, ok DESC, r2 DESC, fs DESC, uri1 DESC"
DEFAULT_ORDER_KEY = "@weight*(r+10)" # suma 10 a r, si r es 0, evita anular el peso de la coincidencia, si es -1, mantiene el peso positivo
DEFAULT_GROUP_ORDER = "e ASC, @count desc"
DEFAULT_WEIGHT = "@weight"
DEFAULT_RANKING = "sum(0.01/min_hit_pos+4*lcs*user_weight)"
DEFAULT_FIELD_WEIGHTS = {"fn":100, "md":1, "fil":100, "ntt":200}
DEFAULT_MAX_QUERY_TIME = 500
MAX_MAX_QUERY_TIME = 5000
QUERY_TIME_STEP = 1000 # 1 segundo mas tiempo en cada peticion


FULL_ID_STRUCT = Struct("III")
PART_ID_STRUCT = Struct(">Q")

'''
    Información almacenada en cache para cada busqueda:
    * [query][part]ACTIVE
        Si existe, ya hay alguien buscando en esta parte
    * [query]
        - INFO = (canonical_query)
            Información genérica de la búsqueda.
        - VERSION[part]
            Numero de veces que se ha escrito en esta parte
        - PART[part] = (date, warning, tries, time, subgroups)
            Información de control de la búsqueda.
        - PART_SG[part][sg] = (current_count, result, result, ...)
            Información del subgrupo sus resultados (desde el 2do) e indicadores de version

    subgroups = {sg: (count, result), ...}
        Una entrada por cada subgrupo
    result = (fileid, sphinxid, version, weight)
        Los resultados incluyen id del fichero, en sphinx, la version y el peso del resultado
'''

# arregla problemas de codificación de la version de sphinx
def u(txt):
    ''' Parse any basestring (ascii str, encoded str, or unicode) to unicode '''
    if isinstance(txt, unicode):
        return txt
    elif isinstance(txt, basestring) and txt:
        try:
            c = chardet.detect(txt)
            return unicode(txt, c["encoding"], "ignore")
        except LookupError:
            if c=="EUC-TW":
                return unicode(txt, "gb2312", "ignore")
        except:
            pass
        return unicode(str(txt), "utf-8", "ignore")
    return unicode(txt)
SPHINX_WRONG_RANGE = re.compile("\xf0([\x80-\x8f])")
def fixer(char):
    return chr(ord(char.group(1))+96)
def fix_sphinx_result(word):
    return u(SPHINX_WRONG_RANGE.sub(fixer, word))

class SphinxError(Exception):
    pass

class RedisPool(ConnectionPool):
    def __init__(self, pool_size, server, version, redis_timeout):
        ConnectionPool.__init__(self, pool_size, keepalive=redis_timeout/2)
        self.server = server
        self.version = version

    def _new_connection(self):
        redisc = redis.StrictRedis(host=self.server[0], port=self.server[1], db=self.version)
        setattr(redisc, "used", True)
        return redisc

    def _keepalive(self, redisc):
        if redisc.used:
            redisc.used = False
        elif not redisc.ping():
            raise socket.error

class SphinxPool(ConnectionPool):
    def __init__(self, pool_size, server, max_max_query_time, socket_timeout):
        ConnectionPool.__init__(self, pool_size, keepalive=socket_timeout/2)
        self.server = server
        self.max_max_query_time = max_max_query_time
        self.socket_timeout = socket_timeout

    def _new_connection(self):
        sphinx = sphinxapi.SphinxClient()
        setattr(sphinx, "used", True)
        sphinx.SetServer(self.server[0], self.server[1])
        sphinx.SetConnectTimeout(self.socket_timeout)
        sphinx.Open()
        return sphinx

    def _keepalive(self, sphinx):
        if sphinx.used:
            sphinx.used = False
        else:
            status = sphinx.Status()
            print "["+datetime.now().isoformat(" ")+"]", repr(status[:4] if status else None)

class SphinxService:
    def __init__(self, redis_server, sphinx_server, part, workers):
        '''
        Inicializa el servidor, creando el pool de conexiones a Sphinx y las conexiones a Redis
        '''

        # configuraciones
        self.redis_server = redis_server
        self.sphinx_server = sphinx_server
        self.part = chr(part)

        self.version = WORKER_VERSION
        self.workers_pool_size = self.sphinx_pool_size = self.redis_pool_size = workers
        self.lock_expiration = LOCK_EXPIRATION
        self.index_name = INDEX_NAME+str(part)
        self.default_order = DEFAULT_ORDER
        self.default_order_key = DEFAULT_ORDER_KEY
        self.default_group_order = DEFAULT_GROUP_ORDER
        self.default_weight = DEFAULT_WEIGHT
        self.default_ranking = DEFAULT_RANKING
        self.default_field_weights = DEFAULT_FIELD_WEIGHTS
        self.default_max_query_time = DEFAULT_MAX_QUERY_TIME
        self.max_max_query_time = MAX_MAX_QUERY_TIME

        # pool de gevent
        self.gevent_pool = Pool(self.workers_pool_size)

        # pool conexiones sphinx
        self.sphinx_conns = SphinxPool(self.sphinx_pool_size, self.sphinx_server, self.max_max_query_time, SPHINX_SOCKET_TIMEOUT)

        # conexion a redis normal
        self.redis_conns = RedisPool(self.redis_pool_size, self.redis_server, self.version, REDIS_TIMEOUT)

        # inicializa variables de control
        self.last_reindex = -1.
        self.stop = False
        self.pubsub_used = True

    def update_last_reindex(self):
        ''' Averigua cuando se realizó la última reindexación de este servidor. '''
        with self.redis_conns.get() as redisc:
            previous = self.last_reindex
            self.last_reindex = float(redisc.get(CONTROL_KEY+"lr_%d"%ord(self.part)) or -1)
            redisc.used = True
            print "["+datetime.now().isoformat(" ")+"]", "Last reindex date updated: %.2f (%.2f)."%(self.last_reindex, previous)

    def update_blocked_sources(self):
        ''' Obtiene lista de origenes bloqueados. '''
        with self.redis_conns.get() as redisc:
            self.blocked_sources = parse_data(redisc.get(CONTROL_KEY+"bs") or "\x90")
            redisc.used = True
            print "["+datetime.now().isoformat(" ")+"]", "Blocked sources updated."

    def keepalive_pubsub(self, timeout):
        '''
        Mantiene viva la conexion pubsub si no llegan mensajes.
        '''
        while not self.stop:
            # espera un rato
            sleep(timeout)

            # comprueba que la conexion se haya utilizado o hace un ping
            if self.pubsub_used:
                self.pubsub_used = False
            else:
                with self.redis_conns.get() as redisc:
                    redisc.publish(RESULTS_CHANNEL, "pn")
                    redisc.publish(CONTROL_CHANNEL+self.part, "pn")
                    redisc.used = True

    def stop_server(self):
        print "["+datetime.now().isoformat(" ")+"]", "Stop command received."

        # deja de atender peticiones
        self.stop = True
        self.redis_pubsub.close()
        self.redis_pubsub.connection_pool.disconnect()

    def serve_forever(self):
        '''
        Recibe y procesa peticiones de busqueda.
        '''

        print "\n\n["+datetime.now().isoformat(" ")+"]", "Server started: %s, %d, %s, %d, %d"%(repr(self.redis_server), self.version, repr(self.sphinx_server), ord(self.part), self.workers_pool_size)

        # Inicializa intervalo de reintento en la conexion
        retry = 1

        while not self.stop:
            try:
                # actualiza variables globales
                self.update_last_reindex()
                self.update_blocked_sources()

                # conexion a redis para pubsub
                self.redis_pubsub = redis.StrictRedis(host=self.redis_server[0], port=self.redis_server[1], db=self.version).pubsub()
                self.redis_pubsub.subscribe(EXECUTE_CHANNEL)
                self.redis_pubsub.subscribe(EXECUTE_CHANNEL+self.part)
                self.redis_pubsub.subscribe(CONTROL_CHANNEL+self.part)

                # Reinicia intervalo de reintento en la conexion
                retry = 1

                # inicia el proceso de keepalive de la conexion pubsub
                self.gevent_pool.spawn(self.keepalive_pubsub, REDIS_TIMEOUT/5)

                # espera mensajes
                for msg in self.redis_pubsub.listen():
                    # marca que se ha usado la conexion
                    self.pubsub_used = True

                    # ignora los mensajes que no son mensajes
                    if msg["type"]!="message":
                        continue

                    # extrae informacion del mensaje
                    channel, part = msg["channel"][0], msg["channel"][1:]
                    data = msg["data"]

                    if channel==EXECUTE_CHANNEL:    # busqueda
                        # comprueba si es una busqueda general o es para este servidor
                        request_id, info = parse_data(data)

                        # procesa la peticion
                        if request_id[0]==QUERY_KEY:
                            self.gevent_pool.spawn(self.process_search_request, request_id, info)
                        elif request_id[0]==LOCATION_KEY:
                            self.gevent_pool.spawn(self.process_get_id_server_request, request_id, info)

                    elif channel==CONTROL_CHANNEL:  # control
                        if data == "lr":    # actualiza fecha de reindexado
                            self.gevent_pool.spawn(self.update_last_reindex)
                        elif data == "bs":  # actualiza lista de origenes bloqueados
                            self.gevent_pool.spawn(self.update_blocked_sources)
                        elif data == "pn":  # ping del keepalive
                            pass

                    elif channel==UPDATE_CHANNEL:  # actualizaciones
                        pass
            except redis.ConnectionError as e:
                if self.stop:
                    break
                else:
                    # Espera y elimina procesos pendientes
                    self.gevent_pool.join(timeout=2)
                    self.gevent_pool.kill(timeout=1)

                    print "["+datetime.now().isoformat(" ")+"]", "Server connection error %s:'%s'. Will reconnect in %d seconds." % (repr(e), e.message, retry)

                    # Espera tiempo de reintento e incrementa tiempo de reintento para la próxima vez (hasta 64 segundos)
                    sleep(retry)
                    if retry < 64: retry *= 2

            except BaseException as e:
                if self.stop:
                    break
                else:
                    print "["+datetime.now().isoformat(" ")+"]", "Server stopped with error %s:'%s'."%(repr(e), e.message)
                    logging.exception("Error on main loop on service %d."%ord(self.part))
                    return

        # espera los procesos que esten respondiendo
        self.gevent_pool.join(2)

        # si alguno no acabado en 2 segundos, lo mata
        self.gevent_pool.kill(timeout=1)

        print "["+datetime.now().isoformat(" ")+"]", "Server stopped normally."

    def process_get_id_server_request(self, request_id, info):
        try:
            # extrae parametros de la llamada
            bin_file_id = request_id[1:]
            query = info.decode("utf-8")

            # obtiene el cliente de redis
            with self.redis_conns.get() as redisc:

                # bloquea acceso si hace falta procesar esta peticion (nadie la esta haciendo o ha hecho ya)
                start_time = time()
                if redisc.hsetnx(request_id, self.part, "P"):
                    try:
                        block_time = time()
                        with self.sphinx_conns.get() as sphinx:
                            # busca el registro con el id pedido
                            uri1, uri2, uri3 = FULL_ID_STRUCT.unpack(bin_file_id)
                            sphinx.SetMaxQueryTime(MAX_MAX_QUERY_TIME)
                            sphinx.SetFilter('uri1', [uri1])
                            sphinx.SetFilter('uri2', [uri2])
                            sphinx.SetFilter('uri3', [uri3])
                            sphinx.SetLimits(0,1,1,1)
                            sphinx.SetIDRange(PART_ID_STRUCT.unpack(bin_file_id[:5]+"\x00\x00\x00")[0], PART_ID_STRUCT.unpack(bin_file_id[:5]+"\xFF\xFF\xFF")[0])
                            results = sphinx.Query(query, self.index_name, "d_id "+str(bin_file_id[:3].encode("hex")))
                            search_time = time()

                            # comprueba resultados obtenidos
                            has_it = results and "matches" in results and results["matches"]
                            if has_it:
                                redisc.pipeline().hset(request_id, self.part, "H").publish(RESULTS_CHANNEL, format_data((request_id, self.part, self.part))).execute()
                            else:
                                redisc.pipeline().hset(request_id, self.part, "N").publish(RESULTS_CHANNEL, format_data((request_id, self.part, None))).execute()
                            end_time = time()

                            print "["+datetime.fromtimestamp(start_time).isoformat(" ")+"]", self.gevent_pool.free_count(), ("*" if has_it else " ")+bin_file_id.encode("hex"), " %.2f (%.4f %.4f %.4f)"%(end_time-start_time, block_time-start_time, search_time-block_time, end_time-search_time), repr(query)


                    except BaseException as e:
                        redisc.hdel(request_id, self.part)
                        print "["+datetime.now().isoformat(" ")+"] ERROR", self.gevent_pool.free_count(), "process_get_id_server_request inner", repr(e), e.message
                        logging.exception("Error on searching for id %s on service %d."%(bin_file_id.encode("hex"), ord(self.part)))

                redisc.used = True
        except BaseException as e:
            print "["+datetime.now().isoformat(" ")+"] ERROR", "process_get_id_server_request outer", repr(e), e.message
            logging.exception("Error on process_get_id_server_request on service %d."%ord(self.part))

    def process_search_request(self, request_id, info):
        # extrae parametros de la llamada
        query = info[0]
        subgroups = info[1]

        try:
            # analiza la peticion para ver qué hay que buscar
            with self.redis_conns.get() as redisc:
                start_time = prep_time = search_time = time()

                query_key = QUERY_KEY+hash_dict(query)
                # genera informacion de la peticion
                search_info = {"query_key":query_key, "query":query, "subgroups":subgroups, "generate_info":False, "version":0, "tries":0}

                # intenta bloquear o ignora la peticion porque ya hay alguien trabajando en ellau
                lock = redisc.lock(query_key+self.part+ACTIVE_KEY, LOCK_EXPIRATION)
                if lock.acquire(False):
                    try:
                        must_search = self.prepare_search(redisc, search_info)
                        prep_time = search_time = time()

                        if must_search:
                            # realiza la busqueda
                            results = self.search(search_info)
                            search_time = time()

                            # almacena los resultados y avisa a quien ha hecho la peticion
                            self.store_results(redisc, search_info, results)
                    except BaseException as e:
                        print "["+datetime.now().isoformat(" ")+"] ERROR", self.gevent_pool.free_count(), "process_search_request inner", repr(e), e.message
                    finally:
                        lock.release()
                else:
                    must_search = None
                    prep_time = search_time = time()

                redisc.used = True

            # prepara info de la consulta para loguear
            end_time = time()
            query_sum = query["t"]
            if subgroups:
                subgroups_sum = sorted(subgroups.iteritems())
                query_sum += " %d/%d %s"%(len(search_info["subgroups"]), len(subgroups_sum), repr(subgroups_sum[:4]))

            # imprime información de la busqueda
            print "["+datetime.fromtimestamp(start_time).isoformat(" ")+"]", self.gevent_pool.free_count() ,"".join(name if flag else " " for name, flag in izip("BSEDW", (must_search==None, must_search, "early_response" in search_info, "delete_subgroups" in search_info, search_info["tries"]>0))), search_info["tries"], " %.2f (%.4f %.4f %.4f) "%(end_time-start_time, prep_time-start_time, search_time-prep_time, end_time-search_time), query_key.encode("hex")[-10:], query_sum
        except BaseException as e:
            print  "["+datetime.now().isoformat(" ")+"] ERROR", "process_search_request outer", repr(e), e.message
            logging.exception("Error on process_search_request on service %d."%ord(self.part))

    def prepare_search(self, redisc, search_info):
        '''
        Averigua si debe realizar esta busqueda.
        '''
        # por defecto no va a buscar, pero no avisa pronto
        early_response = must_search = False

        query_key = search_info["query_key"]
        subgroups = search_info["subgroups"]

        # decide que informacion necesita
        if subgroups:
            keys = [PART_KEY+self.part, VERSION_KEY+self.part]
            keys.extend(PART_SG_KEY+self.part+str(subgroup) for subgroup, start in subgroups.iteritems())
        else:
            keys = [PART_KEY+self.part, VERSION_KEY+self.part, INFO_KEY]

        # obtiene informacion de la busqueda del servidor
        search_cache = redisc.hmget(query_key, *keys)
        part_info, version, rest = search_cache[0], search_cache[1], search_cache[2:]

        # almacena la version actual
        search_info["version"] = int(version) if version else -1

        if part_info: # si esta parte ya se ha buscado, mira razones por que tenga que buscarse de nuevo o busca los subgrupos
            part_info = parse_data(part_info)

            # obtiene el numero de intentos necesitados para esta busqueda hasta ahora
            search_info["tries"] = part_info[2]

            # hay datos aunque puedan no ser validos, avisa que se pueden usar
            early_response = True

            # comprueba la fecha de la busqueda con respecto al ultimo indexado
            if part_info[0]<self.last_reindex:
                search_info["delete_subgroups"] = part_info[4].keys()
                must_search = True

            # comprueba warnings en respuesta (usualmente por falta de tiempo)
            elif part_info[1]:
                search_info["tries"] += 1
                must_search = True

            # busca en subgrupos solo si hay info valida de esta parte (must_search=False) y no hay info de algun subgrupo
            if subgroups:
                if must_search: # los datos principales son invalidos, no puede dar el subgrupo
                    must_search = False
                else:
                    # no piden los subgrupos que ya se tienen
                    new_subgroups = search_info["subgroups"] = {subgroup: (current_subgroup or [1]) for (subgroup, start), current_subgroup in izip(subgroups.iteritems(), (parse_data(asubgroup) if asubgroup else None for asubgroup in rest)) if not current_subgroup or current_subgroup[0]<=start}
                    must_search = bool(new_subgroups)
        else:
            # busca la info de esta parte, pero no un subgrupo
            if not subgroups:
                # genera información de la consulta si no la ha generado nadie aun
                if not rest[0]:
                    search_info["generate_info"] = True
                must_search = True

        # avisa, si hay datos disponibles aunque haya que buscar
        if not subgroups and early_response:
            search_info["early_response"] = True
            redisc.publish(RESULTS_CHANNEL, format_data((query_key, self.part, None)))

        # si no tiene que buscar, libera el bloqueo
        if not must_search:
            return False

        # debe buscar
        return True

    @retry
    def search(self, search_info):
        query = search_info["query"]
        subgroups = search_info["subgroups"]

        if not "t" in query:
            raise Exception("Empty query search received.")

        # parametros de busqueda
        text = query["t"]
        filters = query["f"] if "f" in query else {}
        order = query["o"] if "o" in query else self.default_order
        order_key = query["ok"] if "ok" in query else self.default_order_key
        group_order = query["go"] if "go" in query else self.default_group_order
        weight = query["w"] if "w" in query else self.default_weight
        range_ids = query["i"] if "i" in query else None
        field_weights = query["fw"] if "fw" in query else self.default_field_weights
        ranking = query["r"] if "r" in query else self.default_ranking

        # parametros que no varian la busqueda
        offset, limit, max_matches, cutoff = query["l"]
        grouping = query["g"] if not subgroups and "g" in query else (GROUPING_GROUP|GROUPING_NO_GROUP) # por defecto pide informacion sin y con agrupacion (solo para principal)?
        max_query_time = min(self.default_max_query_time+QUERY_TIME_STEP*search_info["tries"] if "tries" in search_info else query["mt"] if "mt" in query else self.default_max_query_time, self.max_max_query_time)

        # obtiene cliente de sphinx
        with self.sphinx_conns.get() as sphinx:
            sphinx.ResetFilters()
            sphinx.ResetGroupBy()

            # configura cliente
            sphinx.SetFieldWeights(field_weights)
            sphinx.SetSortMode(sphinxapi.SPH_SORT_EXTENDED, order)
            sphinx.SetMatchMode(sphinxapi.SPH_MATCH_EXTENDED)
            sphinx.SetRankingMode(sphinxapi.SPH_RANK_EXPR, ranking)
            sphinx.SetSelect("*, if(g>0xFFFFFFFF,1,0) as e, "+order_key+" as ok, "+weight+" as w")
            sphinx.SetMaxQueryTime(max_query_time)

            if range_ids:
                sphinx.SetIDRange(range_ids[0], range_ids[1])
            else:
                sphinx.SetIDRange(0, 0)

            # realiza la peticion
            if subgroups:
                for sg, current in subgroups.iteritems():
                    sphinx.SetFilter('bl', [0])
                    sphinx.SetFilter("g", [long(sg)])
                    sphinx.SetLimits(current[0], limit, max_matches, cutoff)
                    if filters: self._apply_filters(sphinx, filters)
                    sphinx.AddQuery(text, self.index_name, "d_s "+sg+" "+str(max_query_time))
                    sphinx.ResetFilters()
            else:  # traer resumen principal de todos los grupos
                sphinx.SetFilter('bl', [0])
                sphinx.SetFilter("s", self.blocked_sources, True)
                sphinx.SetLimits(offset, limit, max_matches, cutoff)

                if filters: self._apply_filters(sphinx, filters)

                if grouping&GROUPING_NO_GROUP:
                    sphinx.AddQuery(text, self.index_name, "d_ng "+str(max_query_time))

                if grouping&GROUPING_GROUP:
                    sphinx.SetGroupBy("g", sphinxapi.SPH_GROUPBY_ATTR, group_order)
                    sphinx.AddQuery(text, self.index_name, "d_m "+str(max_query_time))

            results = sphinx.RunQueries()
            error = sphinx.GetLastError()
            if error:
                raise SphinxError(error)

            sphinx.used = True

        return results

    def _apply_filters(self, sphinx, filters):
        if "z" in filters:
            sphinx.SetFilterFloatRange('z', float(filters["z"][0]), float(filters["z"][1]))
        if "e" in filters:
            sphinx.SetFilterRange('e', filters["e"])
        if "ct" in filters:
            sphinx.SetFilter('ct', filters["ct"])
        if "src" in filters:
            sphinx.SetFilter('s', set(filters["src"]).difference(self.blocked_sources))

    def store_results(self, redisc, search_info, results):
        # recorre resultados y los pone en el orden deseado
        subgroups = search_info["subgroups"]
        query = search_info["query"]
        query_key = search_info["query_key"]
        tries = search_info["tries"]

        # nueva version de los datos
        version = search_info["version"]+1

        save_info = {VERSION_KEY+self.part: version}
        now = time()

        if subgroups:
            ''' Va a guardar:
                - [part][sg] = con los resultados de los subgrupos de los que se han obtenido resultados '''
            for result, (sg, current) in izip(results, subgroups.iteritems()):
                current.extend((FULL_ID_STRUCT.pack(r["attrs"]["uri1"],r["attrs"]["uri2"],r["attrs"]["uri3"]), r["id"], version, r["attrs"]["r"], r["attrs"]["w"]) for r in result["matches"])
                current[0] = len(current) # el numero de resultados compensa el primer resultado
                if current[0]>1: # no guarda el subgrupo si no añade resultados
                    save_info[PART_SG_KEY+self.part+str(sg)] = format_data(current)
        else:
            # Tipo de agrupación
            grouping = query["g"]

            ''' Va a guardar:
                - INFO: si corresponde
                - [part]: con los resultados de la busqueda agrupada
                - [part][sg] = con los resultados de la busqueda no agrupada, para los subgrupos de los que se han obtenido resultados '''
            # Información de la busqueda agrupada
            if grouping&GROUPING_GROUP:
                result = results[-1] # es el ultimo resultado, puede ser el 0 o el 1 segun se haya pedido la busqueda sin agrupar
                save_info[PART_KEY+self.part] = format_data((now, bool(result["warning"]), tries, result["time"],
                                        {r["attrs"]["g"]:(r["attrs"]["@count"], (FULL_ID_STRUCT.pack(r["attrs"]["uri1"],r["attrs"]["uri2"],r["attrs"]["uri3"]), r["id"], version, r["attrs"]["r"], r["attrs"]["w"]))
                                            for r in result["matches"]}))

            # Almacena información de la búsqueda sin agrupar, si se ha pedido
            if grouping&GROUPING_NO_GROUP:
                result = results[0]

                # Agrupa resultados por subgrupos
                subgroups_extra = {}
                for r in result["matches"]:
                    sg = r["attrs"]["g"]
                    if sg in subgroups_extra:
                        subgroups_extra[sg].append((FULL_ID_STRUCT.pack(r["attrs"]["uri1"],r["attrs"]["uri2"],r["attrs"]["uri3"]), r["id"], version, r["attrs"]["r"], r["attrs"]["w"]))
                    else:
                        subgroups_extra[sg] = [] # no incluye el primer resultado, que ya está en el resumen

                # Genera listas a guardar
                for sg, files in subgroups_extra.iteritems():
                    if not files: continue # no crea grupos sin ficheros extra
                    files.insert(0,len(files)+1)
                    if files[0]>1: # no guarda el subgrupo si no añade resultados
                        save_info[PART_SG_KEY+self.part+str(sg)] = format_data(files)

            # genera información principal si hace falta
            if search_info["generate_info"]:
                save_info[INFO_KEY] = format_data([fix_sphinx_result(word["word"]).encode("utf-8") for word in results[0]["words"]])

        # almacena datos en redis
        if "delete_subgroups" in search_info:
            redisc.pipeline().hdel(query_key, search_info["delete_subgroups"]).hmset(query_key, save_info).execute()
        else:
            redisc.hmset(query_key, save_info)

        # avisa que estan disponibles los resultados principales
        if not subgroups:
            redisc.publish(RESULTS_CHANNEL, format_data((query_key, self.part, None)))

from os import environ
environ["FOOFIND_NOAPP"] = "1"

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sphinx search service.')
    parser.add_argument('host', type=str, help='Sphinx server ip address')
    parser.add_argument('port', type=int, help='Sphinx server port')
    parser.add_argument('part', type=int, help='Server number.')
    parser.add_argument('--workers', type=int, help='Number of microthread workers.', default=DEFAULT_WORKERS)
    parser.add_argument('--redis', type=int, default=0, help='Redis server index.')

    params = parser.parse_args()

    config = __import__("production").settings.__dict__
    redis_server = config["SPHINX_REDIS_SERVER"][params.redis]

    setup_logging(SentryHandler(Client(config["SENTRY_SPHINX_SERVICE_DNS"])))

    server = SphinxService(redis_server, (params.host, params.port), params.part, params.workers)

    # captura sigint
    def stop_server():
        server.stop_server()
    signal(SIGINT, stop_server)

    server.serve_forever()

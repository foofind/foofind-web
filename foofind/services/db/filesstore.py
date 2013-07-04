# -*- coding: utf-8 -*-
import pymongo, time, traceback, bson
from collections import defaultdict, OrderedDict
from threading import Lock, Event
from itertools import permutations
from datetime import datetime
from multiprocessing.pool import ThreadPool, TimeoutError

import foofind.services
from foofind.utils import hex2mid, u, Parallel, logging
from foofind.utils.async import MultiAsync
from foofind.services.extensions import cache

profiler = None
class MongoTimeout(Exception):
    '''
    Fallo de conexión de mongo
    '''
    pass

class FilesStore(object):
    '''
    Clase para acceder a los datos de los ficheros.
    '''
    def __init__(self):
        '''
        Inicialización de la clase.
        '''
        self.max_pool_size = 0
        self.server_conn = None
        self.servers_conn = {}
        self.current_server = -1

    def init_app(self, app):
        '''
        Inicializa la clase con la configuración de la aplicación.

        @param app: Aplicación de Flask.
        '''
        self.max_pool_size = app.config["DATA_SOURCE_MAX_POOL_SIZE"]
        self.get_files_timeout = app.config["GET_FILES_TIMEOUT"]
        self.max_autoreconnects = app.config["MAX_AUTORECONNECTIONS"]
        self.secondary_acceptable_latency_ms = app.config["SECONDARY_ACCEPTABLE_LATENCY_MS"]

        self.thread_pool_size = app.config["GET_FILES_POOL_SIZE"]
        self.thread_pool = None

        self.server_conn = pymongo.MongoReplicaSetClient(hosts_or_uri=app.config["DATA_SOURCE_SERVER"], replicaSet=app.config["DATA_SOURCE_SERVER_RS"], max_pool_size=self.max_pool_size, socketTimeoutMS=self.get_files_timeout*1000, read_preference=pymongo.read_preferences.ReadPreference.SECONDARY_PREFERRED)

        global profiler
        profiler = foofind.services.profiler

    def load_servers_conn(self):
        '''
        Configura las conexiones a las bases de datos indicadas en la tabla server de la bd principal.
        Puede ser llamada para actualizar las conexiones si se actualiza la tabla server.
        '''
        for server in self.server_conn.foofind.server.find():
            server_id = int(server["_id"])
            sid = str(server_id)
            if not sid in self.servers_conn:
                self.servers_conn[sid] = pymongo.MongoReplicaSetClient(hosts_or_uri="%s:%d,%s:%d"%(server["ip"], int(server["p"]), server["rip"], int(server["rp"])), replicaSet=server["rs"], max_pool_size=self.max_pool_size, socketTimeoutMS=self.get_files_timeout*1000, read_preference=pymongo.read_preferences.ReadPreference.SECONDARY_PREFERRED, secondary_acceptable_latency_ms=self.secondary_acceptable_latency_ms)
            if self.current_server < server_id:
                self.current_server = server_id


    def _get_server_files(self, params):
        '''
        Usado por el MultiAsync en get_files

        Recupera los datos de los ficheros con los ids dados del
        servidor mongo indicado por sid y los devuelve a través
        del objeto async.

        @param async: objeto async
        @type sid: str
        @param sid: id de servidor de archivos
        @type ids: list
        @param ids: lista de ids a obener
        '''
        sid, ids, bl = params
        data = tuple(
            self.servers_conn[sid].foofind.foo.find(
                {"_id": {"$in": ids}}
                if bl is None else
                {"_id": {"$in": ids},"bl":bl}))
        for doc in data:
            doc["s"] = sid
        return data

    def get_files(self, ids, servers_known = False, bl = 0):
        '''
        Devuelve los datos de los ficheros correspondientes a los ids
        dados en formato hexadecimal.

        @param ids: Lista de identificadores de los ficheros a recuperar. Si server_known es False, es una lista de cadenas. Si server_known es True, es una lista de tuplas, que incluyen el identificador del fichero y el número de servidor.
        @param servers_known: Indica si la lista de identificadores incluye el servidor donde se encuentra el fichero.


        @type bl: int o None
        @param bl: valor de bl para buscar, None para no restringir

        @rtype generator
        @return Generador con los documentos de ficheros
        '''

        if not ids: return ()

        sids = defaultdict(list)
        # si conoce los servidores en los que están los ficheros,
        # se analiza ids como un iterable (id, servidor, ...)
        # y evitamos buscar los servidores
        if servers_known:
            for x in ids:
                sids[x[1]].append(hex2mid(x[0]))
        else:
            # averigua en qué servidor está cada fichero
            nindir = self.server_conn.foofind.indir.find({"_id": {"$in": [hex2mid(fid) for fid in ids]}, "s": {"$exists": 1}})
            for ind in nindir:
                indserver = str(int(ind["s"])) # Bug en indir: 's' como float
                if indserver in self.servers_conn:
                    if "t" in ind: # si apunta a otro id, lo busca en vez del id dado
                        sids[indserver].append(ind["t"])
                    else:
                        sids[indserver].append(ind["_id"])
            self.server_conn.end_request()

        lsids = len(sids)
        if lsids == 0:
            # Si no hay servidores, no hay ficheros
            return ()
        elif lsids == 1:
            k, v = sids.iteritems().next()
            return self._get_server_files((k, v, bl))
        else:
            # crea el pool de hilos si no existe
            if not self.thread_pool:
                self.thread_pool = ThreadPool(processes=self.thread_pool_size)

            # obtiene la información de los ficheros de cada servidor
            results = []
            chunks = self.thread_pool.imap_unordered(self._get_server_files, ((k, v, bl) for k, v in sids.iteritems()))
            end = time.time()+self.get_files_timeout
            try:
                for i in xrange(len(sids)):
                    now = time.time()
                    if now>end:
                        break
                    for r in chunks.next(end-now):
                        results.append(r)
            except TimeoutError:
                pass
            except BaseException as e:
                logging.error("Error on get_files.")
            return results

    def get_file(self, fid, sid=None, bl=0):
        '''
        Obtiene un fichero del servidor

        @type fid: mongo id
        @param fid: id de fichero

        @type sid: str
        @param sid: id del servidor

        @type bl: int o None
        @param bl: valor de bl para buscar, None para no restringir

        @rtype mongodb document
        @return Documento del fichero
        '''
        if sid is None:
            # averigua en qué servidor está el fichero
            ind = self.server_conn.foofind.indir.find_one({"_id":fid})
            # Verificación para evitar basura de indir
            if ind is None or not "s" in ind or not ind["s"]:
                return None
            sid = str(int(ind["s"]))
            if not sid in self.servers_conn:
                return None
            if "t" in ind:
                fid = ind["t"]
            self.server_conn.end_request()

        data = self.servers_conn[sid].foofind.foo.find_one(
            {"_id":fid} if bl is None else
            {"_id":fid,"bl":bl})
        if data:
            data["s"] = sid
        return data

    def get_newid(self, oldid):
        '''
        Traduce un ID antiguo (secuencial) al nuevo formato.

        @type oldid: digit-str o int
        @param oldid: Id antiguo, en string de números o entero.

        @rtype None o ObjectID
        @return None si no es válido o no encontrado, o id de mongo.
        '''
        if isinstance(oldid, bson.objectid.ObjectId):
            return None
        elif isinstance(oldid, basestring):
            if not oldid.isdigit():
                return None

        doc = self.servers_conn["1"].foofind.foo.find_one({"i":int(oldid)})
        if doc:
            return doc["_id"]
        return None

    def update_file(self, data, remove=None, direct_connection=False, update_sphinx=True):
        '''
        Actualiza los datos del fichero dado.

        @type data: dict
        @param data: Diccionario con los datos del fichero a guardar. Se debe incluir '_id', y es recomendable incluir 's' por motivos de rendimiento.
        @type remove: iterable
        @param remove: Lista de campos a quitar del registro.
        @type direct_connection: bool
        @param direct_connection: Especifica si se crea una conexión directa, ineficiente e independiente al foo primario.
        @type update_sphinx: bool
        @param update_sphinx: si se especifica bl, de este parámetro depende el si se conecta al sphinx para actualizarlo
        '''
        update = {"$set":data.copy()}
        if remove is not None:
            update["$unset"] = {}
            for rem in remove:
                if rem in update["$set"]:
                    del update["$set"][rem]
                update["$unset"][rem] = 1

        fid = hex2mid(data["_id"])
        _indir = self.server_conn.foofind.indir.find_one({"_id": fid})
        if _indir and "t" in _indir:
            fid = hex2mid(_indir['t'])

        if "s" in data:
            server = str(data["s"])
        else:
            try:
                server = str(self.get_file(fid, bl=None)["s"])
            except (TypeError, KeyError) as e:
                logging.error("Se ha intentado actualizar un fichero que no se encuentra en la base de datos", extra=data)
                raise

        if update_sphinx and "bl" in data:
            try:
                file_name = i["fn"].itervalues().next()["n"]
            except:
                file_name = None
            block_files(mongo_ids=((i["_id"],file_name),), block=data["bl"])

        for i in ("_id", "s"):
            if i in update["$set"]:
                del update["$set"][i]

        # TODO: update cache
        self.servers_conn[server].foofind.foo.update({"_id":fid}, update)

    def count_files(self):
        '''
        Cuenta los ficheros totales indexados
        '''
        count = self.server_conn.foofind.server.group(None, None, {"c":0}, "function(o,p) { p.c += o.c; }")
        result = count[0]['c'] if count else 0
        self.server_conn.end_request()
        return result

    def _get_last_files_from_foo(self, n, offset=0):
        '''
        Obtiene los últimos ficheros del último mongo
        '''
        data = tuple( self.servers_conn[str(self.current_server)].foofind.foo.find({"bl":0})
            .sort([("$natural",-1)])
            .skip(offset)
            .limit(n) )
        return data

    def _get_last_files_indir_offset(self):
        '''
        Obtiene el timestamp de la última sincronización de indir
        '''
        data = self.server_conn.local.sources.find_one({"source":"main"}) # Workaround de bug de pymongo
        self.server_conn.end_request()
        if data and "syncedTo" in data:
            return data["syncedTo"].time
        return 0

    def _get_last_files_foo_offset(self):
        '''
        Obtiene el timestamp de la última sincronización del último mongo
        '''
        data = self.servers_conn[str(self.current_server)].local.sources.find_one({"source":"main"}) # Workaround de bug de pymongo
        if data and "syncedTo" in data:
            return data["syncedTo"].time
        return 0

    # Desfase de 8 ficheros por cada segundo, hasta un max de 200
    _last_files_per_second = 8
    _last_files_max_offset = 250
    _last_files_initial_skip = 500
    @cache.memoize(timeout=2)
    @cache.fallback(MongoTimeout)
    def get_last_files(self, n=25):
        '''
        Obtiene los últimos n ficheros indexados.
        '''
        p = Parallel((
            (self._get_last_files_indir_offset, (), {}),
            (self._get_last_files_foo_offset, (), {}),
            (self._get_last_files_from_foo, (n+self._last_files_max_offset, self._last_files_initial_skip), {}),
            ))
        p.join(1)
        # Parallel.join_and_terminate(1) evitaría los end_request: hay que
        # dejar que los hilos de parallel terminen
        if p.is_alive():
            raise MongoTimeout, "Timeout in get_last_files"
        elif p.failed():
            logging.error(
                "Some error found in Parallel tasks on get_last_files",
                extra={
                    "output": p.output,
                    "exceptions": p.exceptions
                    })
            cache.throw_fallback()

        inoff, fooff, lf = p.output
        fooff -= self._last_files_initial_skip/self._last_files_per_second
        if inoff < fooff: # Si indir está más desactualizado que el foo
            offset = (fooff-inoff)*self._last_files_per_second
            if offset > self._last_files_max_offset:
                logging.error(
                    "Indir and last foo's replicas are too desynchronized, get_last_files will fallback",
                    extra={
                        "file_offset":offset,
                        "file_offset_maximum":self._last_files_max_offset,
                        "replication_time_indir":inoff,
                        "replication_time_foo":fooff,
                        "replication_time_offset":fooff-inoff,
                        "replication_time_maximum_offset (calculated)": self._last_files_max_offset/float(self._last_files_per_second)
                        })
                cache.throw_fallback()
            return lf[offset:offset+n]
        return lf[:n]

    def remove_source_by_id(self, sid):
        '''
        Borra un origen con su id

        @type sid: int o float
        @param sid: identificador del origen
        '''
        self.request_conn.foofind.source.remove({"_id":sid})
        self.request_conn.end_request()

    @cache.memoize(timeout=60*60)
    def get_source_by_id(self, source):
        '''
        Obtiene un origen a través del id

        @type source: int o float
        @param source: identificador del source

        @rtype dict o None
        @return origen o None

        '''
        data = self.server_conn.foofind.source.find_one({"_id":source})
        self.server_conn.end_request()
        return data

    def update_source(self, data, remove=None):
        '''
        Actualiza los datos del origen dado.

        @type data: dict
        @param data: Diccionario con los datos del fichero a guardar. Se debe incluir '_id'.
        @type remove: iterable
        @param remove: Lista de campos a quitar del registro.
        '''

        update = {"$set":data.copy()}
        if remove is not None:
            update["$unset"] = {}
            for rem in remove:
                if rem in update["$set"]:
                    del update["$set"][rem]
                update["$unset"][rem] = 1

        oid = int(float(data["_id"])) #hex2mid(data["_id"])
        del update["$set"]["_id"]

        self.server_conn.foofind.source.update({"_id":oid}, update)
        self.server_conn.end_request()

    def create_source(self, data):
        '''
        Crea el origen dado.

        @type data: dict
        @param data: Diccionario con los datos del fichero a guardar. Se debe incluir '_id'.
        '''
        self.server_conn.foofind.source.insert(data)
        self.server_conn.end_request()

    @cache.memoize(timeout=60*60)
    def get_sources(self, skip=None, limit=None, blocked=False, group=None, must_contain_all=False, embed_active=None):
        '''
        Obtiene los orígenes como generador

        @type skip: int
        @param skip: número de elementos omitidos al inicio, None por defecto.

        @type limit: int
        @param limit: número de elementos máximos que obtener, None por defecto para todos.

        @type blocked: bool o None
        @param blocked: retornar elementos ya procesados, None para obtenerlos todos, False por defecto.

        @type group: basestring, iterable o None
        @param group: basestring para un grupo, si es None, para todos

        @type must_contain_all: bool
        @param must_contain_all: False para encontrar todos los que contengan alguno de los grupos de group, True para encontrar los que contengan todos los groups de group.

        @rtype: MongoDB cursor
        @return: cursor con resultados
        '''
        query = {}
        if blocked == True: query["crbl"] = 1
        elif blocked == False: query["$or"] = {"crbl": { "$exists" : False } }, {"crbl":0}
        if not group is None:
            if isinstance(group, basestring): query["g"] = group
            elif must_contain_all: query["g"] = {"$all": group}
            else: query["g"] = {"$in": group}
        if not embed_active is None:
            query["embed_active"] = int(embed_active)
        sources = self.server_conn.foofind.source.find(query).sort("d")
        if not skip is None: sources.skip(skip)
        if not limit is None: sources.limit(limit)
        data = tuple(sources)
        self.server_conn.end_request()
        return data

    def count_sources(self, blocked=False, group=None, must_contain_all=False, limit=None, embed_active=None):
        '''
        Obtiene el número de orígenes

        @type blocked: bool o None
        @param blocked: retornar elementos ya procesados, None para obtenerlos todos, False por defecto.

        @type group: basestring, iterable o None
        @param group: basestring para un grupo, si es None, para todos

        @type must_contain_all: bool
        @param must_contain_all: False para encontrar todos los que contengan alguno de los grupos de group, True para encontrar los que contengan todos los groups de group.

        @rtype integer
        @return Número de orígenes
        '''
        query = {} if limit is None else {"limit":limit}
        if blocked == True: query["crbl"] = 1
        elif blocked == False: query["$or"] = {"crbl": { "$exists" : False } }, {"crbl":0}
        if not group is None:
            if isinstance(group, basestring): query["g"] = group
            elif must_contain_all: query["g"] = {"$all": group}
            else: query["g"] = {"$in": group}
        if not embed_active is None:
            query["embed_active"] = int(embed_active)
        count = self.server_conn.foofind.source.find(query).count(True)
        self.server_conn.end_request()
        return count

    @cache.memoize(timeout=60*60)
    def get_sources_groups(self):
        '''
        Obtiene los grupos de los orígenes

        @return set de grupos de orígenes
        '''
        data = set( j
            for i in self.server_conn.foofind.source.find() if "g" in i
            for j in i["g"] )
        self.server_conn.end_request()
        return data

    @cache.memoize(timeout=60*60)
    def get_image_servers(self):
        '''
        Obtiene el servidor que contiene una imagen
        '''
        data = {s["_id"]:s for s in self.server_conn.foofind.serverImage.find()}
        self.server_conn.end_request()
        return data

    @cache.memoize(timeout=60*60)
    def get_image_server(self, server):
        '''
        Obtiene el servidor que contiene una imagen
        '''
        data = self.server_conn.foofind.serverImage.find_one({"_id":server})
        self.server_conn.end_request()
        return data

    @cache.memoize(timeout=60*60)
    def get_server_stats(self, server):
        '''
        Obtiene las estadisticas del servidor
        '''
        data = self.server_conn.foofind.search_stats.find_one({"_id":int(server)})
        self.server_conn.end_request()
        return data

    @cache.memoize(timeout=60*60)
    def get_servers(self):
        '''
        Obtiene informacion de los servidores de datos
        '''
        data = tuple(self.server_conn.foofind.server.find())
        self.server_conn.end_request()
        return data

    @cache.memoize(timeout=60*60)
    def get_server(self, sid):
        data = self.server_conn.foofind.server.find_one({"_id":{"$in":[int(sid), float(sid)]}})
        self.server_conn.end_request()
        return data

    def update_server(self, data, remove=None):
        '''
        Actualiza la información de un servidor

        @type data: dict
        @param data:

        @type remove: dict
        @param remove:
        '''
        update = {"$set": data.copy()}
        if remove is not None:
            update["$unset"] = {}
            for rem in remove:
                if rem in update["$set"]:
                    del update["$set"][rem]
                update["$unset"][rem] = 1

        oid = float(data["_id"])
        del update["$set"]["_id"]

        self.server_conn.foofind.server.update({"_id":{"$in":[oid, int(oid)]}}, update)
        self.server_conn.end_request()

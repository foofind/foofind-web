# -*- coding: utf-8 -*-
import pymongo, logging, time, traceback
from collections import defaultdict, OrderedDict
from foofind.utils import hex2mid, u, Parallel
from foofind.utils.async import MultiAsync
from foofind.services.extensions import cache
from threading import Lock, Event
from itertools import permutations
from datetime import datetime

import foofind.services
profiler = None

class BogusMongoException(Exception):
    '''
    Fallo de conexión de mongo
    '''
    pass

class MongoTimeout(Exception):
    '''
    Fallo de conexión de mongo
    '''
    pass

class BongoContext(object):
    '''
    Contexto seguro para peticiones a mongo de ficheros.
    Maneja excepciones de pymongo.connection.AutoReconect y
    pymongo.connection.ConnectionFailure e invalida el servidor para su
    posterior reconexión.
    '''

    _cache_uri = None
    _cache_conn = None

    @property
    def uri(self):
        if self._cache_uri is None:
            self._cache_uri, self._cache_conn = self.bongo.urimaster if self._usemaster else self.bongo.uriconn
        return self._cache_uri

    @property
    def conn(self):
        if self._cache_conn is None:
            self._cache_uri, self._cache_conn = self.bongo.urimaster if self._usemaster else self.bongo.uriconn
        return self._cache_conn

    @property
    def is_master(self):
        return self._usemaster or self.uri == self.bongo.urimaster[0]

    def __init__(self, bongo, usemaster=False):
        self._usemaster = usemaster
        self.bongo = bongo

    def __enter__(self):
        return self

    def __exit__(self, t, value, ex_traceback):
        if not self._cache_conn is None:
            # Si se ha utilizado la conexión, intentamos cerrarla
            try:
                self.conn.end_request()
            except BaseException as e:
                # TODO(felipe): Si no es útil, rebajar prioridad a warning
                logging.error("Error al cerrar la conexión.", extra={"error":e})
        if t:
            extra = locals()
            extra["traceback"] = traceback.format_tb(ex_traceback)
            extra["uri"] = self.uri
            extra["bongo"] = repr(self.bongo)
            try:
                cache.cacheme = False
            except (RuntimeError, BaseException) as e:
                logging.exception("cache.cacheme no accesible")

            if t is pymongo.connection.AutoReconnect:
                logging.warn("Autoreconnection throwed by MongoDB server data: %s." % self.uri, extra=extra)
                self.bongo.autoreconnection(self.uri)
            elif t is pymongo.connection.ConnectionFailure:
                logging.error("Error accessing to MongoDB server data: %s." % self.uri, extra=extra)
                self.bongo.invalidate(self.uri)
            else:
                logging.error("Error: %s" % value, extra=extra)

            return True


class Bongo(object):
    '''
    Clase para gestionar conexiones a Mongos que funcionan rematadamente mal.
    '''
    def __init__(self, server, pool_size, network_timeout, max_autoreconnects):
        self.info = server
        self.connections = OrderedDict((
            ("mongodb://%(rip)s:%(rp)d" % server, None),
            ("mongodb://%(ip)s:%(p)d" % server, None)
            ))
        self.connections_access = {uri:Event() for uri in self.connections.iterkeys()}
        self._pool_size = pool_size
        self._network_timeout = network_timeout
        self._numfails = {}
        self._lock = Lock()
        self._max_autoreconections = max_autoreconnects
        self.reconnect(False)

    def invalidate(self, uri):
        '''
        Deja de usar el mongo y lo marca para su reconexión.

        @type uri: str
        @param uri: dirección del mongo
        '''
        if self.connections_access[uri].is_set():
            self.connections_access[uri].clear()
            logging.warn("Invalidated connection: %s." % uri, extra={"conn":self.connections[uri]})
            with self._lock:
                if self.connections[uri]:
                    self.connections[uri].disconnect()
                    self.connections[uri] = None

    def autoreconnection(self, uri):
        '''
        Avisa del autoreconect, y si se superan las reconexiones, desconecta

        @type uri: str
        @param uri: dirección del mongo
        '''
        if self._numfails[uri] > self._max_autoreconections:
            self.invalidate(uri)
        self._numfails[uri] += 1

    def reconnect(self, again=True):
        '''
        Vuelve a crear conexiones con los mongos que hayan fallado.
        Debería ser llamado cada cierto tiempo.
        '''
        for uri, conn in self.connections.iteritems():
            self._numfails[uri] = 0
            if not self.connections_access[uri].is_set():
                try:
                    self.connections[uri] = pymongo.Connection(
                        uri,
                        max_pool_size = self._pool_size,
                        network_timeout = self._network_timeout
                        )
                    self.connections_access[uri].set()
                    logging.warn("Connection has been %sstablished with %s" % ("re" if again else "e", uri))
                except BaseException as e:
                    logging.warn("Unable to %sconnect with %s" % ("re" if again else "", uri), extra={"error":e})

    @property
    def uriconn(self):
        for uri, conn in self.connections.iteritems():
            if self.connections_access[uri].is_set():
                return uri, conn
        cache.cacheme = False
        raise BogusMongoException("%s has no available connections" % self.__str__())

    @property
    def urimaster(self):
        return self.connections.items()[-1]

    @property
    def conn(self):
        '''
        Devuelve la conexión al mongo
        '''
        return self.uriconn[1]

    @property
    def master(self):
        '''
        Devuelve conexión con el master
        '''
        return self.connections.values()[-1]

    @property
    def context(self):
        '''
        Devuelve contexto
        '''
        return BongoContext(self)

    @property
    def contextmaster(self):
        '''
        Devuelve contexto únicamente con el master
        '''
        return BongoContext(self, True)

    def __str__(self):
        return "<Bongo %s>" %  " ".join(self.connections.iterkeys())

    def __repr__(self):
        return self.__str__()

class FilesStore(object):
    '''
    Clase para acceder a los datos de los ficheros.
    '''
    BogusMongoException = BogusMongoException

    def __init__(self):
        '''
        Inicialización de la clase.
        '''
        self.max_pool_size = 0
        self.server_conn = None
        self.servers_conn = {}
        self.current_server = None

    def init_app(self, app):
        '''
        Inicializa la clase con la configuración de la aplicación.

        @param app: Aplicación de Flask.
        '''
        self.max_pool_size = app.config["DATA_SOURCE_MAX_POOL_SIZE"]
        self.get_files_timeout = app.config["GET_FILES_TIMEOUT"]
        self.max_autoreconnects = app.config["MAX_AUTORECONNECTIONS"]

        self.server_conn = pymongo.Connection(app.config["DATA_SOURCE_SERVER"], slave_okay=True, max_pool_size=self.max_pool_size)

        global profiler
        profiler = foofind.services.profiler

    def load_servers_conn(self):
        '''
        Configura las conexiones a las bases de datos indicadas en la tabla server de la bd principal.
        Puede ser llamada para actualizar las conexiones si se actualiza la tabla server.
        '''
        for bongo in self.servers_conn.itervalues():
            bongo.reconnect()
        for server in self.server_conn.foofind.server.find().sort([("lt",-1)]):
            sid = server["_id"]
            if not sid in self.servers_conn:
                self.servers_conn[sid] = Bongo(server, self.max_pool_size, self.get_files_timeout, self.max_autoreconnects)
        self.server_conn.end_request()
        self.current_server = max(
            self.servers_conn.itervalues(),
            key=lambda x: x.info["lt"] if "lt" in x.info else datetime.min)


    def _get_server_files(self, async, sid, ids, bl):
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
        with self.servers_conn[sid].context as context:
            profiler_key = "mongo%d%s"%(sid, "m" if context.is_master else "s")
            profiler.checkpoint(opening=[profiler_key])
            data = tuple(
                context.conn.foofind.foo.find(
                    {"_id": {"$in": ids}}
                    if bl is None else
                    {"_id": {"$in": ids},"bl":bl}))
            context.conn.end_request()
            for doc in data:
                doc["s"] = sid
            profiler.checkpoint(closing=[profiler_key])
            async.return_value(data)

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

        files_count = len(ids)
        if files_count == 0: return ()

        sids = defaultdict(list)
        # si conoce los servidores en los que están los ficheros,
        # se analiza ids como un iterable (id, servidor, ...)
        # y evitamos buscar los servidores
        if servers_known:
            for x in ids:
                sids[int(x[1])].append(hex2mid(x[0]))
        else:
            # averigua en qué servidor está cada fichero
            nindir = self.server_conn.foofind.indir.find({"_id": {"$in": [hex2mid(fid) for fid in ids]}, "s": {"$exists": 1}})
            for ind in nindir:
                indserver = int(ind["s"]) # Bug en indir: 's' como float
                if indserver in self.servers_conn:
                    if "t" in ind: # si apunta a otro id, lo busca en vez del id dado
                        sids[indserver].append(ind["t"])
                    else:
                        sids[indserver].append(ind["_id"])
            self.server_conn.end_request()

        if len(sids) == 0:
            # Si no hay servidores, no hay ficheros
            return ()

        # obtiene la información de los ficheros de cada servidor
        return MultiAsync(
            self._get_server_files,
            [(k, v, bl) for k, v in sids.iteritems()],
            files_count
            ).get_values(self.get_files_timeout)

    def get_file(self, fid, sid=None, bl=0):
        '''
        Obtiene un fichero del servidor

        @type fid: str
        @param fid: id de fichero en hexadecimal

        @type sid: str
        @param sid: id del servidor

        @type bl: int o None
        @param bl: valor de bl para buscar, None para no restringir

        @rtype mongodb document
        @return Documento del fichero
        '''
        mfid = hex2mid(fid)
        if sid is None:
            # averigua en qué servidor está el fichero
            ind = self.server_conn.foofind.indir.find_one({"_id":mfid})
            # Verificación para evitar basura de indir
            if ind is None or not "s" in ind:
                return None
            sid = int(ind["s"])
            if not sid in self.servers_conn:
                return None
            if "t" in ind:
                mfid = ind["t"]
            self.server_conn.end_request()

        with self.servers_conn[sid].context as context:
            profiler_key = "mongo%d%s"%(sid, "m" if context.is_master else "s")
            profiler.checkpoint(opening=[profiler_key])
            data = context.conn.foofind.foo.find_one(
                {"_id":mfid} if bl is None else
                {"_id":mfid,"bl":bl})
            context.conn.end_request()
            if data: data["s"] = sid
            profiler.checkpoint(closing=[profiler_key])
            return data

    def get_newid(self, oldid):
        '''
        Traduce un ID antiguo (secuencial) al nuevo formato.

        @type oldid: digit-str o int
        @param oldid: Id antiguo, en string de números o entero.

        @rtype None o ObjectID
        @return None si no es válido o no encontrado, o id de mongo.
        '''
        if isinstance(oldid, basestring):
            if not oldid.isdigit():
                return None
        with self.servers_conn[1.0].context as context:
            doc = context.conn.foofind.foo.find_one({"i":int(oldid)})
            context.conn.end_request()
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

        if "s" in data:
            server = data["s"]
        else:
            try:
                server = self.get_file(fid, bl=None)["s"]
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

        if direct_connection:
            # TODO: update cache
            with self.servers_conn[server].contextmaster as context:
                context.conn.foofind.foo.update({"_id":fid}, update)
                context.conn.end_request()
        else:
            #TODO(felipe): implementar usando el EventManager
            raise NotImplemented("No se ha implementado una forma eficiente de actualizar un foo")

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
        with self.current_server.context as context:
            data = tuple( context.conn.foofind.foo.find({"bl":0})
                .sort([("$natural",-1)])
                .skip(offset)
                .limit(n) )
            context.conn.end_request()
            return data
        return ()

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
        with self.current_server.context as context:
            data = context.conn.local.sources.find_one({"source":"main"}) # Workaround de bug de pymongo
            context.conn.end_request()
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

    @cache.memoize()
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

    @cache.memoize()
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

    @cache.memoize()
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


    @cache.memoize()
    def get_image_servers(self):
        '''
        Obtiene el servidor que contiene una imagen
        '''
        data = {s["_id"]:s for s in self.server_conn.foofind.serverImage.find()}
        self.server_conn.end_request()
        return data

    @cache.memoize()
    def get_image_server(self, server):
        '''
        Obtiene el servidor que contiene una imagen
        '''
        data = self.server_conn.foofind.serverImage.find_one({"_id":server})
        self.server_conn.end_request()
        return data

    @cache.memoize()
    def get_server_stats(self, server):
        '''
        Obtiene las estadisticas del servidor
        '''
        data = self.server_conn.foofind.search_stats.find_one({"_id":server})
        self.server_conn.end_request()
        return data

    @cache.memoize()
    def get_servers(self):
        '''
        Obtiene informacion de los servidores de datos
        '''
        data = tuple(self.server_conn.foofind.server.find())
        self.server_conn.end_request()
        return data

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

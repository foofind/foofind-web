# -*- coding: utf-8 -*-
import pymongo, logging
from collections import defaultdict
from foofind.utils import hex2mid
from foofind.utils.async import MultiAsync
from foofind.services.extensions import cache

from itertools import permutations

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
        self.current_server = None

    def init_app(self, app):
        '''
        Inicializa la clase con la configuración de la aplicación.

        @param app: Aplicación de Flask.
        '''
        self.max_pool_size = app.config["DATA_SOURCE_MAX_POOL_SIZE"]
        self.get_files_timeout = app.config["GET_FILES_TIMEOUT"]

        # Inicia conexiones
        self.server_conn = pymongo.Connection(app.config["DATA_SOURCE_SERVER"], slave_okay=True, max_pool_size=self.max_pool_size)
        self.load_servers_conn()

    def load_servers_conn(self):
        '''
        Configura las conexiones a las bases de datos indicadas en la tabla server de la bd principal.
        Puede ser llamada para actualizar las conexiones si se actualiza la tabla server.
        '''
        # recorre la tabla server
        for server in self.server_conn.foofind.server.find().sort([("lt",-1)]):
            sid = server["_id"]
            if self.current_server is None:
                self.current_server = sid

            connect = True
            if sid in self.servers_conn:
                # si el servidor ya existía y no ha cambiado su ubicación, no se conecta
                if all([server[k]==self.servers_conn[sid]["info"][k] for k in ["ip", "p", "rip", "rp"]]):
                    connect = False
            else:
                self.servers_conn[sid] = {"info":server}

            # define la conexión
            if connect:
                for murl in ("mongodb://%(rip)s:%(rp)d,%(ip)s:%(p)d","mongodb://%(rip)s:%(rp)d","mongodb://%(ip)s:%(p)d"):
                    try:
                        self.servers_conn[sid]["conn"] = conn = pymongo.Connection(
                            murl % server,
                            read_preference=pymongo.ReadPreference.SECONDARY,
                            max_pool_size=self.max_pool_size)
                        conn.end_request()
                        break
                    except BaseException as e:
                        logging.exception(e)

        # avisa que ya no necesita la conexión
        self.server_conn.end_request()

    def connect_foo(self, fooid, pool_size=None):
        '''
        Crea una conexión directa a MongoDB foo

        @rtype pymongo.Connection
        @return Conexión a MongoDB dada
        '''
        server = self.servers_conn[fooid]["info"]
        return pymongo.Connection("mongodb://%s:%d" % (server["ip"], server["p"]), slave_okay=True, max_pool_size=self.max_pool_size if pool_size is None else pool_size)

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

        if len(ids) == 0: return (i for i in ())

        sids = defaultdict(list)
        # si conoce los servidores en los que están los ficheros,
        # se analiza ids como un iterable (id, servidor, ...)
        # y evitamos buscar los servidores
        if servers_known:
            for x in ids:
                sids[x[1]].append(hex2mid(x[0]))
        else:
            # averigua en qué servidor está cada fichero
            nindir = self.server_conn.foofind.indir.find({"_id": {"$in": [hex2mid(fid) for fid in ids]}})
            for ind in nindir:
                if "t" in ind: # si apunta a otro id, lo busca en vez del id dado
                    sids[ind["s"]].append(ind["t"])
                else:
                    sids[ind["s"]].append(ind["_id"])
            self.server_conn.end_request()

        if len(sids) == 0: # Si no hay servidores, no hay ficheros
            return (i for i in ())
        elif len(sids) == 1: # Si todos los ficheros pertenecen al mismo servidor, evita MultiAsync
            sid = sids.keys()[0]
            if not "conn" in self.servers_conn[sid]: return ()
            conn = self.servers_conn[sid]["conn"]
            tr = conn.foofind.foo.find(
                {"_id":{"$in": sids[sid]}} if bl is None else
                {"_id":{"$in": sids[sid]},"bl":bl})
            conn.end_request()
            return (i for i in tr)

        # función que recupera ficheros
        def get_server_files(async, sid, ids):
            '''
                Recupera los datos de los ficheros con los ids dados del
                servidor mongo indicado por sid y los devuelve a través
                del objeto async.
            '''
            if not "conn" in self.servers_conn[sid]: return
            conn = self.servers_conn[sid]["conn"]
            async.return_value(conn.foofind.foo.find(
                {"_id": {"$in": ids}}
                if bl is None else
                {"_id": {"$in": ids},"bl":bl}))
            conn.end_request()

        # obtiene la información de los ficheros de cada servidor
        return MultiAsync(get_server_files, sids.items()).get_values(self.get_files_timeout)

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
            if ind is None: return None
            if "t" in ind: mfid = ind["t"]
            sid = ind["s"]
            self.server_conn.end_request()

        if not "conn" in self.servers_conn[sid]: return None
        conn = self.servers_conn[sid]["conn"]
        tr = conn.foofind.foo.find_one(
            {"_id":mfid} if bl is None else
            {"_id":mfid,"bl":bl})
        conn.end_request()
        return tr

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
            update["$unset"]=dict()
            for rem in remove:
                del update["$set"][rem]
                update["$unset"][rem]=1

        fid = hex2mid(data["_id"])

        if "s" in data:
            server = data["s"]
        else:
            server = self.get_file(fid, bl=None)["s"]

        if update_sphinx and "bl" in data:
            block_files(mongo_ids=(i["_id"],), block=data["bl"])

        for i in ("_id", "s"):
            if i in update["$set"]:
                del update["$set"][i]

        if direct_connection:
            # TODO: update cache
            foocon = self.connect_foo(server,1)
            foocon.foofind.foo.update({"_id":fid}, update)
            foocon.end_request()
        else:
            raise NotImplemented("No se ha implementado una forma eficiente de actualizar un foo")

    def count_files(self):
        '''
        Cuenta los ficheros totales indexados
        '''
        count = self.server_conn.foofind.server.group(None, None, {"c":0}, "function(o,p) { p.c += o.c; }")
        return count[0]['c'] if count else 0

    def get_last_files(self):
        '''
        Cuenta los ficheros totales indexados
        '''
        if not "conn" in self.servers_conn[self.current_server]: return ()
        conn = self.servers_conn[self.current_server]["conn"]
        tr = conn.foofind.foo.find({"bl":0}).sort([("$natural",-1)]).limit(25)
        conn.end_request()
        return (i for i in tr)

    @cache.memoize()
    def get_source_by_id(self,source):
        '''
        Obtiene un origen a través del id

        @type source: int o float
        @param source: identificador del source

        @rtype dict o None
        @return origen o None

        '''
        return self.server_conn.foofind.source.find_one({"_id":source})

    _sourceParse = { # Parseo de datos para base de datos
        "crbl":lambda x:int(float(x)),
        "g":lambda x: [i.strip() for i in x.split(",") if i.strip()],
        "*":unicode
        }
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
            update["$unset"]=dict()
            for rem in remove:
                del update["$set"][rem]
                update["$unset"][rem]=1

        oid = int(float(data["_id"])) #hex2mid(data["_id"])
        del update["$set"]["_id"]

        parser = self._sourceParse

        update["$set"].update(
            (key, parser[key](value) if key in parser else parser["*"](value))
            for key, value in update["$set"].iteritems())

        self.server_conn.foofind.source.update({"_id":oid}, update)

    @cache.memoize()
    def get_sources(self, skip=None, limit=None, blocked=False, group=None, must_contain_all=False):
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
        sources = self.server_conn.foofind.source.find(query).sort("d")
        if not skip is None: sources.skip(skip)
        if not limit is None: sources.limit(limit)
        return list(sources)

    #@cache.memoize()
    def count_sources(self, blocked=False, group=None, must_contain_all=False, limit=None):
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
        return self.server_conn.foofind.source.find(query).count(True)

    @cache.memoize()
    def get_sources_groups(self):
        '''
        Obtiene los grupos de los orígenes

        @return set de grupos de orígenes
        '''
        return set(j for i in self.server_conn.foofind.source.find() for j in i["g"])

    @cache.memoize()
    def get_image_server(self,server):
        '''
        Obtiene el servidor que contiene una imagen
        '''
        return self.server_conn.foofind.serverImage.find_one({"_id":server})

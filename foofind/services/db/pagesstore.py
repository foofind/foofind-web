# -*- coding: utf-8 -*-
import pymongo
from hashlib import sha256
from datetime import datetime

from foofind.utils import hex2mid

class PagesStore(object):
    '''
    Clase para acceder a los datos de la sección de pages
    '''
    def __init__(self):
        '''
        Inicialización de la clase.
        '''
        self.pages_conn = None

    def init_app(self, app):
        '''
        Apply pages database access configuration.

        @param app: Flask application.
        '''
        if app.config["DATA_SOURCE_PAGES"]:
            if "DATA_SOURCE_PAGES_RS" in app.config:
                self.pages_conn = pymongo.MongoReplicaSetClient(app.config["DATA_SOURCE_PAGES"],
                                                                max_pool_size=app.config["DATA_SOURCE_MAX_POOL_SIZE"],
                                                                replicaSet = app.config["DATA_SOURCE_PAGES_RS"],
                                                                read_preference = pymongo.read_preferences.ReadPreference.SECONDARY_PREFERRED,
                                                                tag_sets = app.config.get("DATA_SOURCE_PAGES_RS_TAG_SETS",[{}]),
                                                                secondary_acceptable_latency_ms = app.config.get("SECONDARY_ACCEPTABLE_LATENCY_MS", 15))
            else:
                self.pages_conn = pymongo.MongoClient(app.config["DATA_SOURCE_PAGES"], max_pool_size=app.config["DATA_SOURCE_MAX_POOL_SIZE"], slave_okay=True)

    def share_connections(self, pages_conn=None):
        '''
        Allows to share data source connections with other modules.
        '''
        if pages_conn:
            self.pages_conn = pages_conn

    def create_complaint(self,data):
        '''
        Guarda los enlaces reportados
        '''
        d = {key: data[key] for key in ('name', 'surname', 'company', 'email','phonenumber','linkreported','urlreported','reason','message')}
        d.update((("ip", sha256(data["ip"]).hexdigest()),("created",datetime.utcnow()),("processed",False)))
        self.pages_conn.users.complaint.insert(d)
        self.pages_conn.end_request()

    def get_complaints(self, skip=None, limit=None, processed=False):
        '''
        Obtiene los enlaces reportados como generador

        @type skip: int
        @param skip: número de elementos omitidos al inicio, None por defecto.

        @type limit: int
        @param limit: número de elementos máximos que obtener, None por defecto para todos.

        @type processed: bool o None
        @param processed: retornar elementos ya procesados, None para obtenerlos todos, False por defecto.

        @rtype: MongoDB cursor
        @return: cursor con resultados
        '''
        complaints = self.pages_conn.users.complaint.find(None if processed is None else {"processed":processed}).sort("created",-1)
        if not skip is None: complaints.skip(skip)
        if not limit is None: complaints.limit(limit)
        for doc in complaints:
            yield doc
        self.pages_conn.end_request()

    def get_complaint(self, hexid):
        '''
        Obtiene la información de un enlace reportado

        @type hexid: str
        @param hexid: cadena id de MongoDB en hexadecimal

        @rtype: MongoDB document or None
        @return: resultado
        '''
        data = self.pages_conn.users.complaint.find_one({"_id":hex2mid(hexid)})
        self.pages_conn.end_request()
        return data

    def update_complaint(self, data, remove=None):
        '''
        Actualiza una queja.

        @type data: dict
        @param data: datos
        '''
        update = {"$set":data.copy()}
        if remove is not None:
            update["$unset"]=dict()
            for rem in remove:
                del update["$set"][rem]
                update["$unset"][rem]=1

        del update["$set"]["_id"]
        self.pages_conn.users.complaint.update({"_id":hex2mid(data["_id"])}, update)
        self.pages_conn.end_request()

    def count_complaints(self,  processed=False, limit=0):
        '''
        Obtiene el número de enlaces reportados

        @type processed: bool or None
        @param processed: Contabilizar las peticiones procesadas, sin procesar o ambas, según sea True, False o None.
        @rtype integer
        @return Número de enlaces reportados
        '''
        count = self.pages_conn.users.complaint.find(
            None if processed is None else {"processed":processed},
            limit=limit
            ).count(True)
        self.pages_conn.end_request()
        return count

    def create_translation(self, data):
        '''
        Guarda una traducción

        @type data: dict
        @param data: 'ip' y  campos de traducción.
        '''
        d = data.copy()
        d.update((("ip",sha256(data["ip"]).hexdigest()),("created", datetime.utcnow()),("processed",False)))
        self.pages_conn.users.translation.insert(d)
        self.pages_conn.end_request()

    def count_translations(self, processed=False, limit=0):
        '''
        Obtiene el número de traducciones reportadas

        @type processed: bool or None
        @param processed: Contabilizar las traducciones procesadas, sin procesar o ambas, según sea True, False o None.
        @rtype integer
        @return Número de traducciones
        '''
        count = self.pages_conn.users.translation.find(
            None if processed is None else {"processed":processed},
            limit=limit
            ).count(True)
        self.pages_conn.end_request()
        return count

    def get_translations(self, skip=None, limit=None, processed=False):
        '''
        Retorna las traducciones

        @type skip: int
        @param skip: número de elementos omitidos al inicio, None por defecto.

        @type limit: int
        @param limit: número de elementos máximos que obtener, None por defecto para todos.

        @type processed: bool
        @param processed: retornar elementos ya procesados, None para obtenerlos todos, False por defecto.

        @rtype: MongoDB cursor
        @return: cursor con resultados
        '''
        translations = self.pages_conn.users.translation.find(None if processed is None else {"processed":processed}).sort("created",-1)
        if not skip is None: translations.skip(skip)
        if not limit is None: translations.limit(limit)
        for document in translations:
            yield document
        self.pages_conn.end_request()

    def get_translation(self, hexid):
        '''
        Obtiene la información de una traducción

        @type hexid: str
        @param hexid: cadena id de MongoDB en hexadecimal

        @rtype: MongoDB document or None
        @return: resultado
        '''
        data = self.pages_conn.users.translation.find_one({"_id":hex2mid(hexid)})
        self.pages_conn.end_request()
        return data

    def update_translation(self, data, remove=None):
        '''
        Actualiza una traducción.

        @type data: dict
        @param data: datos
        '''
        update = {"$set":data.copy()}
        if remove is not None:
            update["$unset"]=dict()
            for rem in remove:
                del update["$set"][rem]
                update["$unset"][rem]=1

        del update["$set"]["_id"]
        self.pages_conn.users.translation.update({"_id":hex2mid(data["_id"])}, update)
        self.pages_conn.end_request()


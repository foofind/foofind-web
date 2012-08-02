# -*- coding: utf-8 -*-
import pymongo
from foofind.utils import hex2mid, end_request
from hashlib import sha256
from datetime import datetime

class PagesStore(object):
    '''
    Clase para acceder a los datos de la sección de pages
    '''
    def __init__(self):
        '''
        Inicialización de la clase.
        '''
        self.max_pool_size = 0
        self.pages_conn = None

    def init_app(self, app):
        '''
        Inicializa la clase con la configuración de la aplicación.
        '''
        self.max_pool_size = app.config["DATA_SOURCE_MAX_POOL_SIZE"]

        # Inicia conexiones
        self.pages_conn = pymongo.Connection(app.config["DATA_SOURCE_USER"], slave_okay=True, max_pool_size=self.max_pool_size)

    def create_complaint(self,data):
        '''
        Guarda los enlaces reportados
        '''
        d = {key: data[key] for key in ('name', 'surname', 'company', 'email','phonenumber','linkreported','urlreported','reason','message')}
        d.update((("ip", sha256(data["ip"]).hexdigest()),("created",datetime.utcnow()),("processed",False)))
        self.pages_conn.foofind.complaint.insert(d)
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
        complaints = self.pages_conn.foofind.complaint.find(None if processed is None else {"processed":processed}).sort("created",-1)
        if not skip is None: complaints.skip(skip)
        if not limit is None: complaints.limit(limit)
        return end_request(complaints)

    def get_complaint(self, hexid):
        '''
        Obtiene la información de un enlace reportado

        @type hexid: str
        @param hexid: cadena id de MongoDB en hexadecimal

        @rtype: MongoDB document or None
        @return: resultado
        '''
        return end_request(self.pages_conn.foofind.complaint.find_one({"_id":hex2mid(hexid)}), self.pages_conn)

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
        self.pages_conn.foofind.complaint.update({"_id":hex2mid(data["_id"])}, update)
        self.pages_conn.end_request()

    def count_complaints(self,  processed=False, limit=0):
        '''
        Obtiene el número de enlaces reportados

        @type processed: bool or None
        @param processed: Contabilizar las peticiones procesadas, sin procesar o ambas, según sea True, False o None.
        @rtype integer
        @return Número de enlaces reportados
        '''
        return end_request(self.pages_conn.foofind.complaint.find(
            None if processed is None else {"processed":processed},
            limit=limit
            ).count(True), self.pages_conn)

    def create_translation(self, data):
        '''
        Guarda una traducción

        @type data: dict
        @param data: 'ip' y  campos de traducción.
        '''
        d = data.copy()
        d.update((("ip",sha256(data["ip"]).hexdigest()),("created", datetime.utcnow()),("processed",False)))
        self.pages_conn.foofind.translation.insert(d)
        self.pages_conn.end_request()

    def count_translations(self, processed=False, limit=0):
        '''
        Obtiene el número de traducciones reportadas

        @type processed: bool or None
        @param processed: Contabilizar las traducciones procesadas, sin procesar o ambas, según sea True, False o None.
        @rtype integer
        @return Número de traducciones
        '''
        return end_request(self.pages_conn.foofind.translation.find(
            None if processed is None else {"processed":processed},
            limit=limit
            ).count(True), self.pages_conn)

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
        translations = self.pages_conn.foofind.translation.find(None if processed is None else {"processed":processed}).sort("created",-1)
        if not skip is None: translations.skip(skip)
        if not limit is None: translations.limit(limit)
        return end_request(translations)

    def get_translation(self, hexid):
        '''
        Obtiene la información de una traducción

        @type hexid: str
        @param hexid: cadena id de MongoDB en hexadecimal

        @rtype: MongoDB document or None
        @return: resultado
        '''
        return end_request(
            self.pages_conn.foofind.translation.find_one({"_id":hex2mid(hexid)}),
            self.pages_conn)

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
        self.pages_conn.foofind.translation.update({"_id":hex2mid(data["_id"])}, update)
        self.pages_conn.end_request()

    def get_alternative_config(self, altid):
        '''


        '''
        return None

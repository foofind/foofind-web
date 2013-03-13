# -*- coding: utf-8 -*-
import pymongo
from foofind.services.extensions import cache
from foofind.utils import logging

class EntitiesStore(object):
    '''
    Clase para acceder a los datos de las entidades.
    '''
    def __init__(self):
        '''
        Inicialización de la clase.
        '''
        self.max_pool_size = 0
        self.entities_conn = None
        self.enabled = False

    def init_app(self, app):
        '''
        Inicializa la clase con la configuración de la aplicación.

        @param app: Aplicación de Flask.
        '''
        self.max_pool_size = app.config["DATA_SOURCE_MAX_POOL_SIZE"]
        self.entities_url = app.config["DATA_SOURCE_ENTITIES"]
        self.connect()

    def connect(self):
        # no intenta conectar si ya está conectado
        if self.enabled:
            return

        # Inicia conexiones
        try:
            self.entities_conn = pymongo.Connection(self.entities_url, slave_okay=True, max_pool_size=self.max_pool_size)
            self.enabled = True
        except BaseException as e:
            logging.warn("Can't connect to entities database. Entities disabled.")

    @cache.memoize(timeout=60*60)
    def get_entity(self, entity_id):
        '''
        Obtiene la información de una entidad por identificador

        @type entity_id: long
        @param entity_id: id de la entidad

        @rtype: MongoDB document or None
        @return: resultado
        '''
        if self.enabled:
            try:
                data = self.entities_conn.ontology.ontology.find_one({"_id":entity_id})
                self.entities_conn.end_request()
                return data
            except BaseException as e:
                logging.warn("Can't access to entities database. Entities disabled.")
            self.enabled = False
        return {}

    @cache.memoize(timeout=60*60)
    def get_entities(self, entities_ids=None, entities_keys=None, schemas=None):
        '''
        Obtiene la información de una entidad por identificador

        @type entity_id: long
        @param entity_id: id de la entidad

        @rtype: MongoDB document or None
        @return: resultado
        '''
        if self.enabled:
            try:
                query = {}
                if schemas:
                    if len(schemas[1])==1:
                        query["s"] = schemas[1][0] if schemas[0] else {"$ne":schemas[1][0]}
                    else:
                        query["s"] = {("$in" if schemas[0] else "$nin"):schemas[1]}
                if entities_ids and entities_keys:
                    query["$or"] = [{"_id":{"$in":entities_ids}}, {"k":{"$in":entities_keys}}]
                elif entities_ids:
                    query["_id"] = {"$in":entities_ids}
                elif entities_keys:
                    query["k"] = {"$in":entities_keys}

                data = tuple(self.entities_conn.ontology.ontology.find(query))
                self.entities_conn.end_request()
                return data
            except BaseException as e:
                logging.warn("Can't access to entities database. Entities disabled.")
            self.enabled = False
        return ()

    @cache.memoize(timeout=60*60)
    def find_entities(self, keys, exact = False):
        '''
        Obtiene la información de una o varias entidades por claves

        @type keys: array de diccionarios
        @param keys: claves a buscar

        @type exact: boolean
        @param exact: indica si la clave debe ser exacta o puede ser un subconjunto

        @rtype: MongoDB documents or None
        @return: resultado
        '''
        if self.enabled:
            try:
                data = tuple(self.entities_conn.ontology.ontology.find({"k":{"$all":keys} if exact else keys}))
                self.entities_conn.end_request()
                return data
            except BaseException as e:
                logging.warn("Can't access to entities database. Entities disabled.")
            self.enabled = False
        return ()

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
        self.entities_conn = None
        self.enabled = False

    def init_app(self, app):
        '''
        Apply entities database access configuration.

        @param app: Flask application.
        '''
        if app.config["DATA_SOURCE_ENTITIES"]:
            try:
                if "DATA_SOURCE_ENTITIES_RS" in app.config:
                    self.entities_conn = pymongo.MongoReplicaSetClient(app.config["DATA_SOURCE_ENTITIES"],
                                                                    max_pool_size=app.config["DATA_SOURCE_MAX_POOL_SIZE"],
                                                                    replicaSet = app.config["DATA_SOURCE_ENTITIES_RS"],
                                                                    read_preference = pymongo.read_preferences.ReadPreference.SECONDARY_PREFERRED,
                                                                    tag_sets = app.config.get("DATA_SOURCE_ENTITIES_RS_TAG_SETS",[{}]),
                                                                    secondary_acceptable_latency_ms = app.config.get("SECONDARY_ACCEPTABLE_LATENCY_MS", 15))
                else:
                    self.entities_conn = pymongo.MongoClient(app.config["DATA_SOURCE_ENTITIES"], max_pool_size=app.config["DATA_SOURCE_MAX_POOL_SIZE"], slave_okay=True)
                self.enabled = True
            except BaseException as e:
                logging.warn("Can't connect to entities database. Entities disabled.")

    def share_connections(self, entities_conn=None):
        '''
        Allows to share data source connections with other modules.
        '''
        if entities_conn:
            self.entities_conn = entities_conn
            self.enabled = True

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

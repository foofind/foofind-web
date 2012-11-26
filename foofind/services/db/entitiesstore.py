# -*- coding: utf-8 -*-
import pymongo
from foofind.services.extensions import cache

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

    def init_app(self, app):
        '''
        Inicializa la clase con la configuración de la aplicación.

        @param app: Aplicación de Flask.
        '''
        self.max_pool_size = app.config["DATA_SOURCE_MAX_POOL_SIZE"]

        # Inicia conexiones
        self.entities_conn = pymongo.Connection(app.config["DATA_SOURCE_ENTITIES"], slave_okay=True, max_pool_size=self.max_pool_size)

    @cache.memoize()
    def get_entity(self, entity_id):
        '''
        Obtiene la información de una entidad por identificador

        @type entity_id: long
        @param entity_id: id de la entidad

        @rtype: MongoDB document or None
        @return: resultado
        '''
        data = self.entities_conn.ontology.ontology.find_one({"_id":entity_id})
        self.entities_conn.end_request()
        return data

    @cache.memoize()
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
        data = tuple(self.entities_conn.ontology.ontology.find({"k":{"$all":keys} if exact else keys}))
        self.entities_conn.end_request()
        return data

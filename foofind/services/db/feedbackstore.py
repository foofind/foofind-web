# -*- coding: utf-8 -*-
import pymongo
from foofind.utils import hex2mid
from hashlib import sha256
from datetime import datetime

class FeedbackStore(object):
    '''
    Clase para acceder a los datos de la sección de pages
    '''
    def __init__(self):
        '''
        Inicialización de la clase.
        '''
        self.max_pool_size = 0
        self.feedback_conn = None

    def init_app(self, app):
        '''
        Inicializa la clase con la configuración de la aplicación.
        '''
        self.max_pool_size = app.config["DATA_SOURCE_MAX_POOL_SIZE"]

        # Inicia conexiones
        self.feedback_conn = pymongo.Connection(app.config["DATA_SOURCE_FEEDBACK"], slave_okay=True, max_pool_size=self.max_pool_size)

    def create_links(self,data):
        '''
        Guarda los enlaces enviados
        '''
        self.feedback_conn.foofind.links.save({"links":data["links"],"ip":sha256(data["ip"]).hexdigest(),"created": datetime.utcnow()})

    def visited_links(self,links):
        '''
        Guarda los enlaces visitados en la búsqueda
        '''
        self.feedback_conn.foofind.visited_links.insert(links)

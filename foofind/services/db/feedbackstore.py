# -*- coding: utf-8 -*-
import pymongo
from foofind.utils import hex2mid, end_request, check_capped_collections
from hashlib import sha256
from datetime import datetime
from time import time

class FeedbackStore(object):
    '''
    Clase para acceder a los datos de la sección de pages
    '''
    _capped = {
        "visited_links":100000,
        "notify_indir":100000,
        }
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

        # Crea las colecciones capadas si no existen
        check_capped_collections(self.feedback_conn.foofind, self._capped)

    def create_links(self,data):
        '''
        Guarda los enlaces enviados
        '''
        self.feedback_conn.foofind.links.save({"links":data["links"],"ip":sha256(data["ip"]).hexdigest(),"created": datetime.utcnow()})
        self.feedback_conn.end_request()

    def notify_indir(self, mongoid, server=None):
        '''
        Guarda un id de fichero en la tabla de indir
        '''
        self.feedback_conn.foofind.notify_indir.save({"_id":mongoid,"s":server})

    def visited_links(self,links):
        '''
        Guarda los enlaces visitados en la búsqueda
        '''
        self.feedback_conn.foofind.visited_links.insert(links)
        self.feedback_conn.end_request()

    def save_profile_info(self, info):
        info["_date"] = time()
        self.feedback_conn.foofind.profiler.insert(info)

    def get_profile_info(self, start):
        return end_request(self.feedback_conn.foofind.profiler.find({"_date":{"$gt":start}}))

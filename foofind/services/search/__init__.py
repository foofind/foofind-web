# -*- coding: utf-8 -*-
from proxy import SearchProxy
from search import Search, escape_string
from results_browser import ResultsBrowser
from sphinxservice import Sphinx
from foofind.utils import mid2bin, logging
from foofind.utils.splitter import slugify

class Searchd:
    '''
    Demonio de busquedas para la aplicacion Flask.
    '''
    def __init__(self):
        self.sphinx = Sphinx(self, ResultsBrowser)
        self.service = True

    def init_app(self, app, filesdb, entitiesdb, profiler):
        try:
            self.sphinx.init_app(app)
            self.proxy = SearchProxy(app.config, filesdb, entitiesdb, profiler, self.sphinx)
            self.sphinx.start_client(self.proxy.servers.keys())
        except BaseException as e:
            logging.exception("Error on search deamon initialization.")

    def search(self, text, filters={}, start=True, group=True, no_group=False, limits=None, order=None, dynamic_tags=None):
        return Search(self.proxy, text, filters, start, group, no_group, limits, order, dynamic_tags)

    def get_search_info(self, text, filters={}, group=True, no_group=False, limits=None, order=None):
        return Search(self.proxy, text, filters, False, group, no_group, limits, order).get_search_info()

    def block_files(self, ids=[], block=True):
        return None

    def get_id_server_from_search(self, file_id, file_name, timeout=1000):
        return self.sphinx.get_id_server_from_search(mid2bin(file_id), escape_string(" ".join(slugify(file_name).split(" ")[:4])) if file_name else "", timeout)

    def get_sources_stats(self):
        return self.proxy.sources_relevance_streaming, self.proxy.sources_relevance_download, self.proxy.sources_relevance_p2p

    def log_bot_event(self, bot, result):
        self.proxy.log_bot_event(bot, result)

    def get_redis_connection(self):
        return self.sphinx.redis_conn

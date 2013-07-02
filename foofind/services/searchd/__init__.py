# -*- coding: utf-8 -*-
from proxy import SearchProxy
from search import Search
from worker import REQUEST_MODE_PER_GROUPS, REQUEST_MODE_PER_SERVER

class Searchd:
    '''
    Demonio de busquedas para la aplicacion Flask.
    '''
    def __init__(self):
        self.service = False

    def init_app(self, app, filesdb, entitiesdb, profiler):
        self.proxy = SearchProxy(app.config, filesdb, entitiesdb, profiler)

    def search(self, query, filters={}, order=None, request_mode=REQUEST_MODE_PER_GROUPS, query_time=None, extra_wait_time=500, async=False, just_usable=False):
        s = Search(self.proxy, query, filters, order, request_mode, query_time, extra_wait_time)
        return s.search(async, just_usable)

    def get_search_info(self, query, filters={}, order=None, request_mode=REQUEST_MODE_PER_GROUPS):
        s = Search(self.proxy, query, filters, order, request_mode)
        try:
            temp = s.get_modifiable_info()
        except:
            temp = False
        return {"query":s.query_state, "filters":s.filters_state, "temp":temp, "locked":s.locked_until}

    def block_files(self, ids=[], block=True):
        return self.proxy.block_files(ids, block)

    def get_id_server_from_search(self, file_id, file_name):
        return self.proxy.get_id_server_from_search(file_id, file_name)

    def get_sources_stats(self):
        return self.proxy.sources_relevance_streaming, self.proxy.sources_relevance_download, self.proxy.sources_relevance_p2p

    def log_bot_event(self, bot, result):
        self.proxy.log_bot_event(bot, result)


# -*- coding: utf-8 -*-
from flask import g
from time import time

class Profiler:
    def init_app(self, app, store):
        self.store = store
        
        @app.before_request
        def init_profiler():
            g._profiler_info = {}
            
        @app.teardown_request
        def end_profiler(resp):
            if len(g._profiler_info)>0:
                self.store.save_profile_info(g._profiler_info)
        
    def checkpoint(self, opening=(), closing=()):
        t = time()        

        for i in opening:
            g._profiler_info[i] = -t

        for i in closing:
            g._profiler_info[i] = t+g._profiler_info[i]


    def get_data(self,start):
        results={}
        last_date = start
        for log in self.store.get_profile_info(start):
            for key,value in log.iteritems():
                if value<0 or key[0]=='_': continue
                if key in results:
                    curr = results[key]
                    results[key]={"count":curr["count"]+1, "max":max(value, curr["max"]), "min":min(value,curr["min"]), "mean":value+curr["mean"]}
                else:
                    results[key]={"count":1, "max":value, "min":value, "mean":value}
            last_date = max(log["_date"], last_date)
        
        for key in results.iterkeys():
            results[key]["mean"]/=results[key]["count"]
        return results, last_date

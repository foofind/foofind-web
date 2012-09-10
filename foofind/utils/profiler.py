# -*- coding: utf-8 -*-
from flask import g, has_request_context
from time import time
from itertools import islice

class Profiler:
    def init_app(self, app, store):
        self.store = store
        
        @app.before_request
        def init_profiler():
            g.profiler_info = {}
            
        @app.teardown_request
        def end_profiler(resp):
            if len(g.profiler_info)>0:
                self.store.save_profile_info(g.profiler_info)
        
    def checkpoint(self, opening=(), closing=()):
        # ignora llamadas a checkpoint fuera del contexto
        if not has_request_context():
            return
            
        t = time()
        for i in opening:
            g.profiler_info[i] = -t

        for i in closing:
            g.profiler_info[i] += t

    def get_data(self,start):
        results={}
        last_date = start
        for log in self.store.get_profile_info(start):
            for key,value in log.iteritems():
                if key[0]=='_': continue
                
                if key in results:
                    current = results[key]
                else:
                    current = {"timeout":0, "count":0}

                if value>0:
                    if current["count"]:
                        current["max"] = max(value, current["max"])
                        current["min"] = min(value, current["min"])
                        current["mean"] = value+current["mean"]
                    else:
                        current["max"] = value
                        current["min"] = value
                        current["mean"] = value
                    current["count"]+=1
                else:
                    current["timeout"]+=1
                results[key] = current
            last_date = max(log["_date"], last_date)
        
        for key in results.iterkeys():
            if "mean" in results[key]: results[key]["mean"]/=results[key]["count"]
            
        return results, last_date

# -*- coding: utf-8 -*-
from time import time
from itertools import islice

class Profiler:
    def init_app(self, app, store):
        self.store = store

    def checkpoint(self, data, opening=(), closing=()):
        t = time()
        for i in opening:
            data[i] = -t

        for i in closing:
            data[i] += t

    def save_data(self, data):
        try:
            self.store.save_profile_info(data)
        except:
            pass

    def get_data(self, start):
        results={}
        last_date = start
        for log in self.store.get_profile_info(start):
            for key,value in log.iteritems():
                if key[0]=='_': continue

                if key in results:
                    current = results[key]
                else:
                    current = {"count":0}

                if value>0:
                    if current["count"]:
                        current["max"] = max(value, current["max"])
                        current["min"] = min(value, current["min"])
                        current["sum"] = value+current["sum"]
                    else:
                        current["max"] = value
                        current["min"] = value
                        current["sum"] = value
                    current["count"]+=1
                results[key] = current
            last_date = max(log["_date"], last_date)

        for key in results.iterkeys():
            if "sum" in results[key]:
                results[key]["mean"] = results[key]["sum"]/results[key]["count"]

        return results, last_date

# -*- coding: utf-8 -*-
from foofind.services.extensions import cache
from foofind.utils import sphinxapi2, hex2bin, bin2hex
from foofind.utils.content_types import *
from threading import Thread
from Queue import Queue, Empty
from hashlib import md5
from time import time
from collections import defaultdict
from copy import deepcopy
from math import sqrt
from shlex import split
import json
import heapq
import functools
import itertools
import logging
import struct
import operator
import re


def get_group(sg):
    return str((long(sg)&0xF0000000L)>>28)
    
def get_group2(sg):
    return (long(sg)&0xFFFF000L)>>12
    
def get_ct(sg):
    return (int(sg)&0xF0000000L)>>28

def get_src(sg):
    return (int(sg)&0xFFFF000L)>>12
    
class SearchProxy:
    def __init__(self, config, filesdb):
        self.servers = [(server["_id"], str(server["sp"]), int(server["spp"])) for server in filesdb.get_servers() if "sp" in server]
        stats = {server[0]:filesdb.get_server_stats(server[0]) for server in self.servers}
        self.stats = deepcopy(stats)
        self.stats["*"] = {}
        
        sgs = {sg for server in stats for sg in stats[server]["sg"].iterkeys()}
        subgroups = self.stats["*"]["sg"] = {sg:sum(stats[server]["sg"].get(sg, 0) for server in stats) for sg in sgs}
        self.stats["*"]["rc"] = {sg:sum(stats[server]["rc"].get(sg, 0) for server in stats) for sg in sgs}
        self.stats["*"]["ra"] = {sg:sum(stats[server]["ra"].get(sg, 0)*stats[server]["rc"].get(sg, 0)/(self.stats["*"]["rc"][sg] or 1) for server in stats) for sg in sgs}
        self.stats["*"]["rv"] = {sg:sum(stats[server]["rpa"].get(sg, 0)/(self.stats["*"]["rc"][sg] or 1) for server in stats) for sg in sgs}
        self.stats["*"]["rd"] = {sg:sqrt(self.stats["*"]["rv"][sg]) for sg in sgs}
        self.stats["*"]["rM"] = {sg:max(stats[server]["rM"].get(sg, None) for server in stats) for sg in sgs}
        
        self.stats["*"]["zc"] = {sg:sum(stats[server]["zc"].get(sg, 0) for server in stats) for sg in sgs}
        self.stats["*"]["z"] = {k:sum(stats[server]["z"].get(k, 0) for server in stats) for k in {z for server in stats for z in stats[server]["z"].iterkeys()}}
        self.stats["*"]["za"] = {sg:sum(stats[server]["za"].get(sg, 0)*stats[server]["zc"].get(sg, 0)/(self.stats["*"]["zc"][sg] or 1) for server in stats) for sg in sgs}
        self.stats["*"]["zv"] = {sg:sum(stats[server]["zpa"].get(sg, 0)/(self.stats["*"]["rc"][sg] or 1) for server in stats) for sg in sgs}
        self.stats["*"]["zd"] = {sg:sqrt(self.stats["*"]["zv"][sg]) for sg in sgs}
        
        self.stats["*"]["lc"] = {sg:sum(stats[server]["lc"].get(sg, 0) for server in stats) for sg in sgs}
        self.stats["*"]["l"] = {k:sum(stats[server]["l"].get(k, 0) for server in stats) for k in {l for server in stats for l in stats[server]["l"].iterkeys()}}
        self.stats["*"]["la"] = {sg:sum(stats[server]["la"].get(sg, 0)*stats[server]["la"].get(sg, 0)/(self.stats["*"]["lc"][sg] or 1) for server in stats) for sg in sgs}
        self.stats["*"]["lv"] = {sg:sum(stats[server]["lpa"].get(sg, 0)/(self.stats["*"]["rc"][sg] or 1) for server in stats) for sg in sgs}
        self.stats["*"]["ld"] = {sg:sqrt(self.stats["*"]["lv"][sg]) for sg in sgs}
         
        groups = self.stats["*"]["g"] = defaultdict(int)
        self.sources = self.stats["*"]["src"] = defaultdict(int)
        self.sources_rating_rc = self.stats["*"]["src_rc"] = defaultdict(int)
        self.sources_rating_ra = self.stats["*"]["src_ra"] = defaultdict(float)
        self.sources_rating_rv = self.stats["*"]["src_rv"] = defaultdict(float)
        self.sources_rating_rd = self.stats["*"]["src_rd"] = defaultdict(float)
        
        for sg, sgc in subgroups.iteritems():
            groups[get_group(sg)] += sgc
            src = get_src(sg)
            self.sources[src] += sgc
            self.sources_rating_rc[src] += self.stats["*"]["rc"][sg]
            self.sources_rating_ra[src] += self.stats["*"]["ra"][sg]*self.stats["*"]["rc"][sg]
            self.sources_rating_rv[src] += self.stats["*"]["rv"][sg]*self.stats["*"]["rc"][sg]

        for src, srcrc in self.sources_rating_rc.iteritems():
            if srcrc==0: continue
            self.sources_rating_ra[src] /= srcrc
            self.sources_rating_rv[src] /= srcrc
            self.sources_rating_rd[src] = sqrt(self.sources_rating_rv[src])

        self.sources_weights = {"w":1, "s":1, "t":0.2, "e":0.08, "g":0.08}
        self.sources_weights = {int(s["_id"]):v for k,v in self.sources_weights.iteritems() for s in filesdb.get_sources(group=k)}
         
        if config["NEW_SEARCH_ACTIVE"]:
            self.nservers = len(self.servers)
            self.maxserverid = int(max(self.servers)[0])
            self.tasks = {s[0]:Queue() for s in self.servers}
            self.pendings = Queue()
            self.workers = [SphinxWorker(s,config,self.tasks[s[0]],self) for s in self.servers for j in xrange(config["SERVICE_SPHINX_WORKERS_PER_SERVER"])]
            for w in self.workers: w.start()

            pp = Thread(target=self.process_pendings)
            pp.daemon = True
            pp.start()
            
    def search(self, asearch):
        asearch["_r"] = Queue()
        counter = 0
        # recorre las queries y las envia al servidor correspondiente
        for server, query in asearch["q"].iteritems():
            print repr(server), repr(query)
            if server=="*": # query para todos los servidores
                for queue in self.tasks.itervalues():
                    queue.put((asearch, query))
                    counter += 1
            else:
                if server in self.tasks:
                    self.tasks[server].put((asearch, query))
                    counter += 1
        asearch["_pt"] = counter # tareas pendientes

    def put_results(self, asearch, query, server, results, messages):
        asearch["_r"].put((server, query, results, messages))
        
    def browse_results(self, asearch, timeout=5, pending_callback=None):
        start = time()
        for i in xrange(asearch["_pt"]):
            try:
                wait = timeout - (time()-start) # calcula cuanto hay que esperar
                if 0<wait<=timeout:  # comprueba que quede tiempo
                    yield asearch["_r"].get(True, wait)
                else:
                    raise Empty
            except Empty: # Timeout
                logging.warning("Timeout waiting sphinx results.")
                if pending_callback:
                    asearch["_pt"] -= i
                    asearch["_pc"] = pending_callback
                    self.pendings.put(asearch)
                return
           
    def process_pendings(self):
        while True:
            try:
                asearch = self.pendings.get()
                asearch["_pc"](asearch)
            except BaseException as e:
                logging.exception(e)

    def get_search_state(self, text, filters):
        textkey = md5(text.encode("utf8")).hexdigest()
        text_state = cache.get("search_t%s"%textkey) or {}
        filters_state = (cache.get("search_f%s_%s"%(textkey,md5(json.dumps(filters)).hexdigest())) or {}) if text_state else {}
        return (text_state, filters_state)
        
    def save_search_state(self, text, filters, text_state, filters_state):
        textkey = md5(text.encode("utf8")).hexdigest()
        cache.set("search_t%s"%textkey, text_state, timeout=60*60)
        cache.set("search_f%s_%s"%(textkey, md5(json.dumps(filters)).hexdigest()), filters_state, timeout=60*60)
        
class SphinxWorker(Thread):
    def __init__(self, server, config, tasks, proxy, *args, **kwargs):
        super(SphinxWorker, self).__init__(*args, **kwargs)
        self.server = server
        self.index = "idx_files%d"%server[0]
        self.config = config
        self.tasks = tasks
        self.proxy = proxy
        self.daemon = True
        self.client = None
        self.create_client()
        
    def create_client(self):
        if self.client:
            self.client.Close()
            self.client = None

        self.client = sphinxapi2.SphinxClient()
        self.client.SetConnectTimeout(100.0)
        self.client.SetServer(self.server[1], self.server[2])
        self.client.SetMatchMode(sphinxapi2.SPH_MATCH_EXTENDED2)
        self.client.SetRankingMode(sphinxapi2.SPH_RANK_EXPR, "sum((4*lcs+2.0/min_hit_pos)*user_weight)")
        self.client.SetFieldWeights({"fn":100, "md":1})
        self.client.SetMaxQueryTime(self.config["SERVICE_SPHINX_MAX_QUERY_TIME"])
        self.client.SetSortMode( sphinxapi2.SPH_SORT_EXTENDED, "rw DESC, r2 DESC" )
        
    def run(self):
        while True:
            try:
                asearch, query = self.tasks.get()
            except BaseException as e:
                logging.exception(e)
                continue

            try:
                text = query["t"]
                filters = query["f"] if "f" in query else {}
                self.client.SetSelect("*,@weight*r as rw, min(if(z>0,z,100)) as zm, max(z) as zx")

                # traer resultados de uno o más grupos
                if "g" in query: 
                    for group, first in query["g"].iteritems():
                        self.client.ResetFilters()
                        self.client.SetFilter('bl', [0])
                        self.client.SetFilter("g", [int(group)])
                        self.client.SetLimits(first, 20, 1000, 2000000)
                        if filters: self.apply_filters(filters)
                        self.client.AddQuery(text, self.index)
                else:  # traer resumen principal de todos los grupos
                    self.client.ResetFilters()
                    self.client.SetFilter('bl', [0])
                    self.client.SetLimits(0, 200, 1000, 2000000)
                    self.client.SetGroupBy("g", sphinxapi2.SPH_GROUPBY_ATTR, "@count desc")

                    if query["st"]: # realiza la busqueda sin filtros
                        self.client.AddQuery(text, self.index)
                        
                    if query["sf"]: # realiza la busqueda con filtros
                        if filters: self.apply_filters(filters)
                        self.client.AddQuery(text, self.index)

                results = self.client.RunQueries()
                self.proxy.put_results(asearch, query, self.server[0], results, (self.client.GetLastWarning(), self.client.GetLastError()))
            except BaseException as e:
                self.proxy.put_results(asearch, query, self.server[0], None, (None, e.message))
                logging.exception(e)
                
    def apply_filters(self, filters):
        if "z" in filters:
            self.client.SetFilterRange('z', filters["z"][0], filters["z"][1])
        if "e" in filters:
            self.client.SetFilterRange('e', filters["e"])
        if "ct" in filters:
            self.client.SetFilter('ct', filters["ct"])
        if "src" in filters:
            self.client.SetFilter('s', filters["src"])

def text_subgroup_info():
    return {"c": defaultdict(int), "z":[0,100], "f":{}}

def filters_group_info():
    return {"w":0, "lw":0, "l":0, "g2": defaultdict(filters_group2_info)}
def filters_group2_info():
    return {"w":0, "lw":0, "l":0, "sg": defaultdict(filters_subgroup_info)}
def filters_subgroup_info():
    return {"c":defaultdict(int), "lv":defaultdict(int), "h":[], "w":0, "lw":0, "l":0}
    
class Search(object):
    def __init__(self, proxy, text, filters, lang):
        self.proxy = proxy
        self.text = text
        self.filters = filters or {}
        self.lang = lang
        self.text_state, self.filters_state = proxy.get_search_state(text, self.filters)
        
    def search(self):
        search_text = [i for i in xrange(1,self.proxy.maxserverid+1) if not i in self.text_state["c"] or i in self.text_state["i"]] if self.text_state else range(1,self.proxy.maxserverid+1)
        search_filters = [i for i in xrange(1,self.proxy.maxserverid+1) if not i in self.filters_state["c"] or i in self.text_state["i"]] if self.filters_state else range(1,self.proxy.maxserverid+1)
        
        if search_text or search_filters:
            query = {"t":self.text, "f":self.filters, "l":self.lang}

            # si no tiene estado para busqueda con filtros, lo crea
            if not self.filters_state:
                self.filters_state = {"c": defaultdict(int), "t": defaultdict(int), "i":[], "r":[], "g": defaultdict(filters_group_info)}

                # si ya tiene información de búsqueda sin filtros, utiliza los ficheros que cumplen los filtros
                if self.text_state:
                    groups = self.filters_state["g"]
                    for sg, osg in self.text_state["sg"].iteritems():
                        if self.satisfies_filters(sg):
                            g = get_group(sg)
                            g2 = get_group2(sg)
                            h = groups[g]["g2"][g2]["sg"][sg]["h"]
                            h.extend((-f[0], fid) for fid, f in osg["f"].iteritems() if self.satisfies_filters(sg, f[2]))
                            heapq.heapify(h)
                           
            # si no tiene estado para busqueda sin filtros, lo crea
            if not self.text_state:
                self.text_state = {"c": defaultdict(int), "t": defaultdict(int), "i":[], "sg": defaultdict(text_subgroup_info)}

            # compone la búsqueda con las consultas correspondientes a cada servidor
            asearch = {"q":{server:dict(query.items()+[("st",search_text=="*" or server in search_text), ("sf",self.filters and (search_filters=="*" or server in search_filters))]) for server in {i for i in search_text+(search_filters if self.filters else [])}}}

            # realiza la busqueda y almacena los resultados
            self.proxy.search(asearch)
            self.store_files(asearch, 1.5, self.store_files_timeout)

            # calcula estadisticas de grupos y subgrupos y guarda el estado para próximas busquedas
            self.update_stats()
            self.save_state()
                
        return self

    def update_stats(self):
        self.filters_state["cs"] = sum(self.filters_state["c"].itervalues()) if "c" in self.filters_state else 0
        groups = self.filters_state["g"]

        # w=peso del grupo, l=ultimo elemento extraido, lw=ultimo peso
        for g, og in groups.iteritems():
            gts = 0
            for g2, og2 in og["g2"].iteritems():
                gts2 = 0
                for sg, osg in og2["sg"].iteritems():
                    ts = osg["cs"] = sum(osg["c"])
                    gts2 += ts
                gts += gts2
                og2["cs"] = gts2
            og["cs"] = gts
            
    def save_state(self):
        ''' Guarda en cache los datos de la búsqueda.
        Estos se dividen en dos partes:
         - la lista de ficheros que aplican para esta búsqueda de texto.
         - las listas de ficheros resultante para los filtros dados. '''
        self.proxy.save_search_state(self.text, self.filters, self.text_state, self.filters_state)
            
    def get_stats(self):
        return {k:v for k,v in self.filters_state.iteritems() if k in ("cs")}

    def get_modifiable_info(self):
        groups = self.filters_state["g"]

        info = {}
        # w=peso del grupo, l=ultimo elemento extraido, lw=ultimo peso
        for g, og in groups.iteritems():
            g_weight = 0
            ig = info[g] = {"cs":og["cs"], "l":0, "g2":{}}
            
            for g2, og2 in og["g2"].iteritems():
                ig2 = ig["g2"][g2] = {"cs":og2["cs"], "l":0, "sg":{}}
                g2_weight = 0
                for sg, osg in og2["sg"].iteritems():
                    weight = -osg["h"][0][0]
                    isg = ig2["sg"][sg] = {"cs":osg["cs"], "h":osg["h"][:], "lw":weight, "l":0}
                    if weight>g2_weight: g2_weight = weight
                ig2["lw"] = ig2["w"] = g2_weight
                if g2_weight>g_weight: g_weight = g2_weight
            ig["lw"] = ig["w"] = g_weight
        return info
        
    def get_results(self):
        
        subgroups = self.text_state["sg"]

        info = self.get_modifiable_info()

        # cuando lo saca de cache no es igual a cuando lo genera de nuevas
        for i in xrange(self.filters_state["cs"]):
            filtered_groups = [(og["lw"], og["l"], g, og) for g, og in info.iteritems() if og["lw"]>-1]
            if filtered_groups:
                w, l, g, og = max(filtered_groups)
            else:
                break

            filtered_groups2 = [(og2["lw"], og2["l"], g2, og2) for g2, og2 in og["g2"].iteritems() if og2["lw"]>-1]
            if filtered_groups2:
                w2, l2, g2, og2 = max(filtered_groups2)
            else:
                og["lw"]=-1
                if len(filtered_groups)==1:
                    break
                continue

            filtered_subgroups = [(osg["lw"], osg["l"], sg, osg) for sg, osg in og2["sg"].iteritems() if osg["lw"]>-1]
            if filtered_subgroups:
                ws, ls, sg, osg = max(filtered_subgroups)
            else:
                og2["lw"]=-1
                if len(filtered_groups2)==1:
                    og["lw"] = -1
                    if len(filtered_groups)==1:
                        break
                continue

            og["l"] = l+1
            og["lw"] = w/(2*l+1.0)
            og2["l"] = l2+1
            og2["lw"] = w2/(2*l+1.0)
            osg["l"] = ls+1

            fr = heapq.heappop(osg["h"])
            tr = subgroups[sg]["f"][fr[1]]
                
            if osg["h"]:
                osg["lw"] = -osg["h"][0][0]
            else:
                osg["lw"] = -1
                if len(filtered_subgroups)==1:
                    og2["lw"] = -1
                    if len(filtered_groups2)==1:
                        og["lw"] = -1
                        if len(filtered_groups)==1:
                            break
                            
            yield (fr[1], tr[1], tr[3])
        '''results = self.state["r"]
        groups = self.state["g"]
        subgroups = self.state["sg"]
        querys = defaultdict(dict)
        
        # mira si hay algun resultado que no se haya traido aún de una petición anterior
        for sgk, l in ((x[0], x[1]) for x in results if x[2]==None):
            subgroup = subgroups[sgk]
            lastres = subgroup["r"][-1]
            if not subgroup["h"]:
                # sólo guarda la minima posición necesaria para cada grupo
                if sgk not in querys[lastres[-3]]: querys[lastres[-3]][sgk]=l
            
        # calcula posiciones que no se han generado aún
        for i in xrange(len(results), start+maximum):
            # elige el grupo y subgrupo que tiene el fichero que va en esta posición
            gk = max(groups, key=lambda x: groups[x]["lw"]+groups[x]["l"]*1e-5 if groups[x]["c"]>groups[x]["l"] else 0)
            group = groups[gk]
            sgk = max(group["sg"], key=lambda x: subgroups[x]["lw"]+subgroups[x]["l"]*1e-5 if subgroups[x]["c"]>subgroups[x]["l"] else 0)
            subgroup = subgroups[sgk]

            # el último resultado obtenido del grupo indica que hay más en ese servidor?
            lastres = subgroup["r"][-1] if subgroup["r"] else None
            if (lastres and lastres[-1]) or not subgroup["h"]:
                # sólo guarda la minima posición necesaria para cada grupo
                if sgk not in querys[lastres[-3]]: querys[lastres[-3]][sgk]=subgroup["l"]+1
                result = None
            else:
                result = heapq.heappop(subgroup["h"])
                subgroup["r"].append(result)
            
            # guarda el numero de grupo y la posición dentro de este
            results.append([sgk, subgroup["l"]+1]+[result])

            # actualiza pesos del grupo y subgrupo elegido
            group["l"] += 1
            group["lw"] = group["w"]/(group["l"]*2+1.0)
            subgroup["l"] += 1
            subgroup["lw"] = subgroup["w"]/(subgroup["l"]*2+1.0)
            
        # pide grupos pendientes
        if querys:
            prev_search = None
            for s, qs in querys.iteritems():
                nsearch_obj = {"t":self.text, "s":s, "g":qs}
                self.proxy.search(nsearch_obj, prev_search)
                prev_search = nsearch_obj

            # mete resultados en los heaps de cada grupo
            for search in self.proxy.wait_results(nsearch_obj, 1, self.store_files_timeout):
                self.store_files(search)
            self.update_results()
            
        if self.cache: self.proxy.save_search_state(self)

        return [(r[1],r[-3],r[2]) for sgk, sgpos, r in results[start:maximum] if r]'''

    def store_files_timeout(self, asearch):
        self.store_files(asearch, 30, None)
        self.update_stats()
        self.save_state()

    def store_files(self, asearch, timeout, timeout_fallback):
        groups = self.filters_state["g"]
        subgroups = self.text_state["sg"]

        for server, query, sphinx_results, messages in self.proxy.browse_results(asearch, timeout, timeout_fallback):

            # permite manejar igual resultados de querys simples o de multiquerys
            if not sphinx_results:
                logging.error("Error in search thread:'%s'"%messages[1])
                continue
            elif "matches" in sphinx_results:
                sphinx_results = [sphinx_results]

            # comprueba si es una consulta de resumen
            if query["sf"]:
                main = [(True, False), (False, True)]
            elif query["st"]:
                main = [(True, True)]
            else:
                main = False

            # por defecto los valores son válidos
            valid = True

            # incorpora resultados al subgrupo que corresponda
            for result in sphinx_results:
                if not result:
                    logging.error("No ha llegado respuesta del servidor de búsquedas.")
                    continue
                    
                elif result["error"]:
                    logging.error("Error en búsqueda (servidor %d): %s" % (server, result["error"]))
                    continue
                    
                elif result["warning"]:
                    valid = False # los resultados se usan, pero se marcan como inválidos para próximas veces
                    logging.error("Alertas en búsqueda (servidor %d): %s" % (server, result["warning"]))

                total = 0
                for r in result["matches"]:
                    # calcula el subgrupo y el id del fichero
                    sg = str(r["attrs"]["g"])
                    fid = bin2hex(struct.pack('III',r["attrs"]["uri1"],r["attrs"]["uri2"],r["attrs"]["uri3"]))
                    g = get_group(sg)
                    g2 = get_group2(sg)
                    rating = r["attrs"]["r"]
                    weight = r["weight"]
                    count = r["attrs"]["@count"]
                    if not main: first = query["g"][g]
                    total += count
                    
                    # almacena fichero en grupos y subgrupos
                    if not fid in subgroups[sg]["f"]:
                        filtrable_info = {"z":r["attrs"]["z"], "e":r["attrs"]["e"]}
                        normalized_weight = self.proxy.sources_weights[g2]*((rating-self.proxy.sources_rating_ra[g2])/self.proxy.sources_rating_rd[g2] if g2 in self.proxy.sources_rating_rd and self.proxy.sources_rating_rd[g2] else rating)*weight
                        subgroups[sg]["f"][fid] = (normalized_weight, server, filtrable_info, r["id"])
                        
                        # si aplica para los filtros
                        if self.satisfies_filters(sg, filtrable_info):
                            heapq.heappush(groups[g]["g2"][g2]["sg"][sg]["h"], (-normalized_weight, fid))
                    
                    # actualiza totales de grupos y subgrupos
                    if main[0][0]: # almacena en text_state
                        subgroups[sg]["c"][server] = count
                        subgroups[sg]["z"][0] = max(subgroups[sg]["z"][0], r["attrs"]["zm"])
                        subgroups[sg]["z"][1] = min(subgroups[sg]["z"][1], r["attrs"]["zx"])
                        
                    if main[0][1]: # almacena en filters_state
                        groups[g]["g2"][g2]["sg"][sg]["c"][server] = count

                    # actualiza el último registro vaĺido obtenido para el grupo en el servidor
                    if valid:
                        if main:
                            groups[g]["g2"][g2]["sg"][sg]["lv"][server] = max(1,groups[g]["g2"][g2]["sg"][sg]["lv"][server])
                        else:
                            groups[g]["g2"][g2]["sg"][sg]["lv"][server] = max(first+count,groups[g]["g2"][g2]["sg"][sg]["lv"][server])

                # totales absolutos
                if main:
                    if main[0][0]: # almacena en text_state
                        self.text_state["c"][server] = total
                        if valid and server in self.text_state["i"]: self.text_state["i"].remove(server)
                        elif not valid and server not in self.text_state["i"]: self.text_state["i"].append(server)
                        self.text_state["t"][server] = result["time"]
                    if main[0][1]: # almacena en filters_state
                        self.filters_state["c"][server] = total
                        if valid and server in self.filters_state["i"]: self.filters_state["i"].remove(server)
                        elif not valid and server not in self.filters_state["i"]: self.filters_state["i"].append(server)
                        self.filters_state["t"][server] = result["time"]
                    main.pop()
        
    def satisfies_filters(self, subgroup, filtrable=None):
        if not self.filters: return True
        if "ct" in self.filters:
            if not get_ct(subgroup) in self.filters["ct"]: return False
        if "src" in self.filters:
            if not get_src(subgroup) in self.filters["src"]: return False
        if not filtrable: return True
        if "z" in self.filters:
            if not self.filters["z"][0]<=filtrable["z"]<=self.filters["z"][1]: return False
        if "e" in self.filters:
            if not filtrable["e"] in self.filters["e"]: return False
        return True

    '''
    def update_results(self):
        subgroups = self.state["sg"]
        results = self.state["r"]
        # coloca resultados donde corresponde
        for result in results:
            if not result[2] and subgroups[result[0]]["h"]:
                subgroup = subgroups[result[0]]
                result[2] = heapq.heappop(subgroup["h"])
                subgroup["r"].append(result[2])
    '''
    
def abs_threshold(val, t):
    if abs(val)<t: return 0
    return val
    
class Searchd:
    def __init__(self):
        pass

    def init_app(self, app, filesdb):
        self.proxy = SearchProxy(app.config, filesdb)

    def search(self, text, filters={}, lang=None):
        s = Search(self.proxy, text, filters, lang)
        return s.search()

    def get_search_info(self, text, filters={}):
        return self.proxy.get_search_state(text, filters)

_escaper = re.compile(r"([=\(\)|\-!@~\"&/\\\^\$\=])")
def escape_string(text):
    return "\"%s\""%_escaper.sub(r"\\\1", text) if " " in text else _escaper.sub(r"\\\1", text)
    
def parse_query(user_query, sources):

    # intenta hacer un split teniendo en cuenta comillas, si falla hace un split normal
    if(user_query):
        try:
            parts = split(user_query.lower())
        except:
            parts = user_query.lower().split(" ")
    else:
        return [{"q":"","s":[]}]
        
    parts = [(part[0],part[1:]) if len(part)>1 and part[0] in "!-" else ("",part) for part in parts]
    sources_found = {text:op for op, text in parts if text in sources}

    queries = [{"q":" ".join("|" if text in ["|", "or"] else op+escape_string(text) for op, text in parts), "s":list({sources[text] for text, op in sources_found.iteritems() if op and op in "!-"})}]

    positive_filters = {text for text, op in sources_found.iteritems() if op==""}

    if positive_filters:
        query = " ".join("|" if text in ["|", "or"] else op+escape_string(text) for op, text in parts if not text in positive_filters)
        if query:
            queries.append({"q":query, "s":[sources[source] for source in positive_filters]})
    return queries

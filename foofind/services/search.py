# -*- coding: utf-8 -*-
"""
    Servicio de búsqueda
"""
from flask import current_app
import json, struct, logging
from foofind.utils import hex2mid, mid2bin, mid2hex, mid2url, sphinxapi2
from foofind.utils.content_types import *
from foofind.utils.splitter import split_phrase
from foofind.services.extensions import cache
from foofind.services import filesdb, searchd, feedbackdb
from itertools import groupby
from datetime import datetime
from math import log

def get_sources_weights(sources):
    source_weights = {"w":1, "s":1, "t":0.2, "e":0.08, "g":0.08}
    sources_weights = {int(s["_id"]):v for k,v in source_weights.iteritems() for s in filesdb.get_sources(group=k) if int(s["_id"]) in sources}
    if 18 in sources: sources_weights[18] /= 1.8
    iclogs = {str(s): sources_weights.get(s,1.0)/(1.0+log(searchd.proxy.sources_rating_rc[s]+1)) for s in searchd.proxy.sources.iterkeys() if s in searchd.proxy.sources_rating_ra}
    avgs = {str(s): searchd.proxy.sources_rating_ra[s] for s in searchd.proxy.sources.iterkeys() if s in searchd.proxy.sources_rating_ra}
    devs = {str(s): ((searchd.proxy.sources_rating_rd[s] if s in searchd.proxy.sources_rating_rd else 1.0)-1.0) for s in searchd.proxy.sources_rating_rd.iterkeys()}
    avgs_vals = "+".join("IN(s,%s)*%f"%(",".join(v), k) for k,v in groupby(sorted(avgs,key=avgs.get), key=avgs.get) if not -1e-8<k<1e-8)
    devs_vals = "+".join("IN(s,%s)*%f"%(",".join(v), k) for k,v in groupby(sorted(devs,key=devs.get), key=devs.get) if not -1e-8<k<1e-8)
    iclog_vals = "+".join("IN(s,%s)*%f"%(",".join(v), k) for k,v in groupby(sorted(iclogs,key=iclogs.get), key=iclogs.get) if not -1e-8<k-1<1e-8)
    return "@weight*(%(iclog)s)*(0.4+(if(r>-1,r-%(avg)s,0))/(1.0+%(dev)s)) as wr" % {"iclog":iclog_vals, "avg":avgs_vals, "dev":devs_vals}

def block_files(sphinx_ids=(), mongo_ids=(), block=True):
    '''
    Recibe ids de sphinx u ObjectIDs de mongodb de ficheros y los bloquea en el
    sphinx (atributo bl a 1).
    '''
    sph = sphinxapi2.SphinxClient()
    sph.SetServer(current_app.config["SERVICE_SPHINX"], current_app.config["SERVICE_SPHINX_PORT"])
    sph.SetConnectTimeout(current_app.config["SERVICE_SPHINX_SOCKET_TIMEOUT"])
    sph.SetMatchMode(sphinxapi2.SPH_MATCH_EXTENDED2)
    sph.SetLimits(0, 1, 1, 1)
    sphinx_ids = list(sphinx_ids)
    if mongo_ids:
        # Si recibo ids de mongo, ejecuto una petición múltiple para encontrar
        # los ids de sphinx
        for i in xrange(0, len(mongo_ids), 32):
            # Proceso los ids de mongo en grupos de 32, que es el límite que
            # me permite sphinx


            for mongoid, file_name in mongo_ids[i:i+32]:
                uri1, uri2, uri3 = struct.unpack('III', mid2bin(mongoid))
                sph.ResetFilters()
                # TODO (felipe): Añadir setIDRange para los IDs de sphinx nuevos
                sph.SetFilter('uri1', [uri1])
                sph.SetFilter('uri2', [uri2])
                sph.SetFilter('uri3', [uri3])
                query = max((len(w),w) for w in " ".join(split_phrase(file_name, True)).split(" "))[1] if file_name else ""
                sph.AddQuery(query, "idx_files", "search.block_files %s" % mid2url(mongoid))
            results = sph.RunQueries()
            if results:
                for result in results:
                    if "matches" in result and result["matches"]:
                        sphinx_ids.append(result["matches"][0]["id"])
                    if "warning" in result and result["warning"]:
                        logging.warning(result["warning"])
            else:
                logging.error( sph.GetLastError() )
    sph.ResetFilters()
    tr = sph.UpdateAttributes("idx_files", ["bl"], {i:[1 if block else 0] for i in sphinx_ids})
    sph.Close()
    return tr == len(sphinx_ids) and tr == len(mongo_ids)

@cache.memoize(timeout=3600) # Mantener cache máximo 1 hora
def search_related(phrases):
    '''
    Busqueda de archivos relacionados
    '''
    if not phrases: return []

    sph = sphinxapi2.SphinxClient()
    sph.SetServer(current_app.config["SERVICE_SPHINX"], current_app.config["SERVICE_SPHINX_PORT"])
    sph.SetConnectTimeout(current_app.config["SERVICE_SPHINX_SOCKET_TIMEOUT"])
    sph.SetMatchMode(sphinxapi2.SPH_MATCH_EXTENDED2)
    sph.SetFieldWeights({"fn":100, "md":1})
    sph.SetRankingMode(sphinxapi2.SPH_RANK_EXPR, "sum((2.0*lcs/min_best_span_pos)*user_weight)")
    sph.SetSortMode(sphinxapi2.SPH_SORT_EXTENDED, "r DESC, r2 DESC, uri1 DESC")
    sph.SetMaxQueryTime(current_app.config["SERVICE_SPHINX_MAX_QUERY_TIME"])
    sph.SetLimits( 0, 6, 6, 10000)
    sph.SetFilter('bl', [0])

    sph.SetFilter("s", searchd.proxy.blocked_sources, True)

    if phrases[-1] in EXTENSIONS: phrases.pop()

    phrases.sort(key=len,reverse=True)
    for phrase in phrases[:5]:
        sph.AddQuery(sph.EscapeString(phrase), "idx_files", "search.search_related")

    try:
        querys = sph.RunQueries() or []
    except:
        querys = []

    lists = warn = error = None
    if querys:
        if "error" in querys:
            error = querys["error"]
            if "warning" in querys: warning = querys["warning"]
            querys = []
        else:
            lists = True
            warn = []
            error = []
            for query_res in querys:
                if query_res["warning"]: warn.append(query_res["warning"])
                if query_res["error"]: error.append(query_res["error"])
    else:
        warn = sph.GetLastWarning()
        error = sph.GetLastError()

    if warn:
        logging.warn("Warning on a Sphinx response", extra={"method": "search_related", "q":phrases[:5], "orig_msg":warn})
    if error:
        logging.error("Error on a Sphinx response", extra={"method": "search_related", "q":phrases[:5], "orig_msg":error})

    sph.Close()
    return querys

def serverid_from_sphinx_doc(docinfo):
    '''
    Solución temporal para la convivencia entre IDs deterministas (para
    búsqueda de ids por rango) e ids incrementales.
    '''
    r = docinfo["id"] >> 32
    if r > 8:
        # Workaround temporal a bug en los IDs de sphinx
        return (0xff0000 & docinfo["id"])>>16
    return r

def get_ids(results):
    '''
    Devuelve los ids que vienen troceados
    '''
    return [(struct.pack('III',
                docinfo["attrs"]["uri1"],
                docinfo["attrs"]["uri2"],
                docinfo["attrs"]["uri3"]),
            serverid_from_sphinx_doc(docinfo),
            docinfo["id"],
            docinfo["weight"],
            docinfo["attrs"]) for docinfo in results["matches"]
           ] if results and "matches" in results else []

def get_id_server_from_search(mongoid, file_name):
    uri1, uri2, uri3 = struct.unpack('III', mid2bin(mongoid))
    sph = sphinxapi2.SphinxClient()
    sph.SetMatchMode(sphinxapi2.SPH_MATCH_ALL)
    sph.SetServer(current_app.config["SERVICE_SPHINX"], current_app.config["SERVICE_SPHINX_PORT"])
    sph.SetConnectTimeout(current_app.config["SERVICE_SPHINX_SOCKET_TIMEOUT"])
    sph.SetMaxQueryTime(current_app.config["SERVICE_SPHINX_MAX_QUERY_TIME"])
    sph.SetLimits( 0, 1, 1, 1)
    sph.SetFilter('uri1', [uri1])
    sph.SetFilter('uri2', [uri2])
    sph.SetFilter('uri3', [uri3])

    if file_name:
        query = max((len(w),w) for w in " ".join(split_phrase(file_name, True)).split(" "))[1]
        query = sph.EscapeString(query)
    else:
        query = ""

    try:
        results = sph.Query(query, "idx_files", "web search.get_id_server_from_search %s" % mid2url(mongoid))
    except:
        results = []

    warn = error = ret = None
    if results and "total" in results and results["total"]==1:
        if results["warning"]: warn = results["warning"]
        if results["error"]: error = results["error"]
        ret = serverid_from_sphinx_doc(results["matches"][0])
    else:
        warn = sph.GetLastWarning()
        error = sph.GetLastError()
    if warn: logging.warn("Warning on a Sphinx response", extra={"method": "get_id_server_from_search", "id":mongoid, "orig_msg":warn})
    if error: logging.error("Error on a Sphinx response", extra={"method": "get_id_server_from_search", "id":mongoid, "orig_msg":error})
    sph.Close()

    feedbackdb.notify_indir(mongoid, ret)
    return ret

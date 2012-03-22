# -*- coding: utf-8 -*-
"""
    Servicio de búsqueda
"""
from flask import current_app
import json, struct, logging
from foofind.utils import sphinxapi, hex2mid, mid2bin, mid2hex
from foofind.services.extensions import cache
from foofind.services import filesdb
from datetime import datetime

@cache.memoize()
def search_files(query, filters, page=1):
    '''
    Busqueda simple de archivos con filtros
    '''
    sph = sphinxapi.SphinxClient()
    sph.SetServer(current_app.config["SERVICE_SPHINX"], current_app.config["SERVICE_SPHINX_PORT"])
    sph.SetMatchMode(sphinxapi.SPH_MATCH_EXTENDED2)
    sph.SetRankingMode(sphinxapi.SPH_RANK_SPH04)
    sph.SetFieldWeights({"fn1":100})
    sph.SetSelect("*, idiv(@weight,10000) as sw")
    sph.SetSortMode( sphinxapi.SPH_SORT_EXTENDED, "w DESC, sw DESC, ls DESC" )
    sph.SetMaxQueryTime(current_app.config["SERVICE_SPHINX_MAX_QUERY_TIME"])
    sph.SetLimits((page-1)*10, 10, 1000, 2000000)
    sph.ResetFilters()
    sph.SetFilter('bl', [0])

    #todos los filtros posibles de busqueda
    if 'type' in filters and len(filters["type"])>0 and filters["type"] in current_app.config["CONTENTS_CATEGORY"]:
        sph.SetFilter('ct', current_app.config["CONTENTS_CATEGORY"][filters["type"]])

    if 'src' in filters and len(filters["src"])>0:
        sph.SetFilter('t', [int(i["_id"]) for i in filesdb.get_sources(group=tuple(filters['src']))])
    else:
        sph.SetFilter('t', [int(i["_id"]) for i in filesdb.get_sources()])


    if 'size' in filters and len(filters["size"])>0:
        if filters['size']<4:
            sph.SetFilterRange('z', 1, 1048576*(10**(int(filters['size'])-1)))
        else:
            sph.SetFilterRange('z', 0, 104857600, True)

    if 'brate' in filters and len(filters["brate"])>0:
        sph.SetFilterRange('mab', 0, [127,191,255,319][int(filters['brate'])-1], True)

    if 'year' in filters and len(filters["year"])>0:
        sph.SetFilterRange('may', [0,60,70,80,90,100,datetime.utcnow().year-1][int(filters['year'])-1], [59,69,79,89,99,109,datetime.utcnow().year][int(filters['year'])-1])

    query = sph.Query(query, "idx_files")
    sph.Close()
    if query:
        if current_app.debug and query["warning"]: logging.warn(query["warning"])
        if query["error"]: logging.error(query["error"])
    return query

def block_files(sphinx_ids=(), mongo_ids=(), block=True):
    '''
    Recibe ids de sphinx u ObjectIDs de mongodb de ficheros y los bloquea en el
    sphinx (atributo bl a 1).
    '''
    sph = sphinxapi.SphinxClient()
    sph.SetServer(current_app.config["SERVICE_SPHINX"], current_app.config["SERVICE_SPHINX_PORT"])
    sph.SetMatchMode(sphinxapi.SPH_MATCH_FULLSCAN)
    sph.SetLimits(0, 1, 1, 1)
    sphinx_ids = list(sphinx_ids)
    if mongo_ids:
        # Si recibo ids de mongo, ejecuto una petición múltiple para encontrar
        # los ids de sphinx
        for mongoid in mongo_ids:
            uri1, uri2, uri3 = struct.unpack('III', mid2bin(mongoid))
            sph.ResetFilters()
            sph.SetFilter('uri1', [uri1])
            sph.SetFilter('uri2', [uri2])
            sph.SetFilter('uri3', [uri3])
            sph.AddQuery("", "idx_files", "Searching fileid %s" % mid2hex(mongoid))
        results = sph.RunQueries()
        if not results:
            logging.error( sph.GetLastError() )
            return False
        sphinx_ids.extend(r["matches"][0]["id"] for r in results)
    sph.ResetFilters()
    tr = sph.UpdateAttributes("idx_files", ["bl"], {i:1 if block else 0 for i in sphinx_ids})
    sph.Close()
    return tr == len(sphinx_ids) and tr == len(mongo_ids)

def search_related(phrases):
    '''
    Busqueda de archivos relacionados
    '''
    sph = sphinxapi.SphinxClient()
    sph.SetServer(current_app.config["SERVICE_SPHINX"], current_app.config["SERVICE_SPHINX_PORT"])
    sph.SetMatchMode(sphinxapi.SPH_MATCH_EXTENDED2)
    sph.SetRankingMode(sphinxapi.SPH_RANK_SPH04)
    sph.SetFieldWeights({"fn1":100})
    sph.SetSelect("*, idiv(@weight,10000) as sw")
    sph.SetSortMode(sphinxapi.SPH_SORT_EXTENDED, "w DESC, sw DESC, ls DESC")
    sph.SetMaxQueryTime(current_app.config["SERVICE_SPHINX_MAX_QUERY_TIME"])
    sph.SetLimits( 0, 6, 6, 10000)
    sph.SetFilter('bl', [0])
    sph.SetFilter('t', [int(i["_id"]) for i in filesdb.get_sources()])
    minlen = float("inf")
    for phrase in phrases:
        words = [word for word in phrase if len(word)>1]
        minlen = min(len(words),minlen)
        sph.AddQuery(" ".join(words), "idx_files")

    # añade busquedas más cortas
    if minlen>4:
        words = [word for word in phrases[0] if len(word)>1]
        sph.AddQuery(" ".join(words[0:3]), "idx_files")
        sph.AddQuery(" ".join(words[-3:]), "idx_files")

    query = sph.RunQueries() or []
    sph.Close()
    return query

def get_ids(results):
    '''
    Devuelve los ids que vienen troceados
    '''
    return [(struct.pack('III',
                docinfo["attrs"]["uri1"],
                docinfo["attrs"]["uri2"],
                docinfo["attrs"]["uri3"]),
            docinfo["id"]/0x100000000,
            docinfo["id"]) for docinfo in results["matches"]
           ] if "matches" in results else []


# -*- coding: utf-8 -*-

from collections import defaultdict
from math import sqrt, log

from foofind.utils import mid2hex, hex2bin, logging
from foofind.utils.event import EventManager
from .results_browser import get_src

class SearchProxy:
    def __init__(self, config, filesdb, entitiesdb, profiler, sphinx):
        self.config = config
        self.filesdb = filesdb
        self.entitiesdb = entitiesdb
        self.profiler = profiler
        self.sphinx = sphinx

        # logs de informacion
        self.bot_events = defaultdict(int)

        # actualiza información de servidores
        self.update_servers()

        self.maintenance = EventManager()
        self.maintenance.start()
        self.maintenance.interval(config["SERVICE_SEARCH_PROFILE_INTERVAL"], self.save_profile_info)
        self.maintenance.interval(config["SERVERS_REFRESH_INTERVAL"], self.update_servers)

    def save_profile_info(self):
        # informacion de accesos de bots
        profiling_info, self.bot_events = self.bot_events, defaultdict(int)

        # guarda información
        self.profiler.save_data(profiling_info)

    def update_sources(self):
        # obtiene los origenes
        self.sources = {int(s["_id"]): s for s in self.filesdb.get_sources(blocked=None)}

        # actualiza origenes bloqueados
        self.blocked_sources = [sid for sid, s in self.sources.iteritems() if "crbl" in s and int(s["crbl"])!=0]
        self.sphinx.update_blocked_sources(self.blocked_sources)

        # calcula pesos para origenes individuales por calidad
        self.sources_weights = _update_source_weights(self.sources, self.blocked_sources)

        # listado de origenes ordenados por cantidad de ficheros
        sources = self.stats["src"]
        sources_relevance = sorted(((sources[sid], sid, s["g"]) for sid, s in self.sources.iteritems() if sid not in self.blocked_sources), reverse=True)
        self.sources_relevance_streaming = [self.sources[sid]["d"]
                                                for (count,sid,group) in sources_relevance if "s" in group]
        self.sources_relevance_download = [self.sources[sid]["d"]
                                                for (count,sid,group) in sources_relevance if ("w" in group or "f" in group)]
        self.sources_relevance_p2p = ["Torrent","eD2k","Gnutella"]

    def update_servers(self):
        # obtiene los servidores activos para busquedas
        self.servers = {str(int(server["_id"])):(str(server["sp"]), int(server["spp"])) for server in self.filesdb.get_servers() if "sp" in server and server["sp"]}

        self.servers_set = set(self.servers.iterkeys())

        # estadisticas de busqueda por servidor
        new_servers_stats = {server_id:self.filesdb.get_server_stats(int(server_id)) for server_id in self.servers.iterkeys()}

        # estadisticas combinadas para todos los servidores
        new_stats = {"sg":defaultdict(int), # subgrupos
                      "rc":defaultdict(int), "ra":defaultdict(int), "rv":defaultdict(int), "rd":{}, "rM":defaultdict(int), # por rating
                      "src":defaultdict(int), "src_rc":defaultdict(int), "src_ra":defaultdict(int), "src_rv":defaultdict(int), "src_rd":defaultdict(int) # por rating y origen
                      }

        # evita multiples accesos a mismas claves globales
        subgroups = new_stats["sg"]
        rating_count = new_stats["rc"]
        rating_average = new_stats["ra"]
        rating_variance = new_stats["rv"]
        rating_maximum = new_stats["rM"]
        rating_deviation = new_stats["rd"]

        # recorre la información de cada servidor
        for server, server_stats in new_servers_stats.iteritems():

            if not "rc" in server_stats:
                continue

            # evita multiples accesos a mismas claves para el servidor
            server_rating_count = server_stats["rc"]
            server_rating_average = server_stats["ra"]
            server_rating_pow_average = server_stats["rpa"]
            server_rating_maximum = server_stats["rM"]

            # recorre los subgrupos del servidor
            for sg, count in server_stats["sg"].iteritems():
                subgroups[sg] += count
                if sg in server_rating_count:
                    rating_count[sg] += server_rating_count[sg]                                 # numero de entradas con rating
                    rating_average[sg] += server_rating_average[sg] * server_rating_count[sg]   # valor medio del rating (a falta de normalizar)
                    rating_variance[sg] += server_rating_pow_average[sg]                        # varianza del rating (a falta de normalizar)
                    rating_maximum[sg] = max(rating_maximum[sg], server_rating_maximum)         # maximo rating

        # actualiza valores dependientes de valores totales que no se pueden calcular en el bucle anterior
        for sg, count in rating_count.iteritems():
            rating_average[sg] /= count
            rating_variance[sg] /= count
            rating_deviation[sg] = sqrt(rating_variance[sg])

        # estadisticas por origenes
        sources = new_stats["src"]
        new_sources_rating_count = new_stats["src_rc"]
        new_sources_rating_average = new_stats["src_ra"]
        new_sources_rating_variance = new_stats["src_rv"]
        new_sources_rating_standard_deviation = new_stats["src_rd"]

        # recorre subgrupos agrupando por origenes
        for sg, count in subgroups.iteritems():
            src = get_src(sg)
            sources[src] += count
            if sg in rating_count:
                new_sources_rating_count[src] += rating_count[sg]
                new_sources_rating_average[src] += rating_average[sg]*rating_count[sg]
                new_sources_rating_variance[src] += rating_variance[sg]*rating_count[sg]

        # actualiza valores dependientes de valores totales que no se pueden calcular en el bucle anterior
        for src, count in new_sources_rating_count.iteritems():
            new_sources_rating_average[src] /= count
            new_sources_rating_variance[src] /= count
            new_sources_rating_standard_deviation[src] = sqrt(new_sources_rating_variance[src])

        self.servers_stats = new_servers_stats
        self.stats = new_stats
        self.sources_rating_count = new_sources_rating_count
        self.sources_rating_average = new_sources_rating_average
        self.sources_rating_variance = new_sources_rating_variance
        self.sources_rating_standard_deviation = new_sources_rating_standard_deviation
        self.sources_rating_count = new_sources_rating_count
        self.sources_rating_average = new_sources_rating_average
        self.sources_rating_variance = new_sources_rating_variance
        self.sources_rating_standard_deviation = new_sources_rating_standard_deviation

        # pesos para los origenes por tipo
        self.update_sources()

    def log_bot_event(self, bot, result):
        self.bot_events[("bot_" if result else "bot_no_") + bot] += 1

def _update_source_weights(sources, blocked_sources):
    results = {}

    # pesos para grupos de origenes
    groups_weights = {"e":0.01, "g":0.005, "t":2} # por grupos

    max_kbps = 1000
    max_second_delay = 120.0
    for sid, s in sources.iteritems():
        # ignora origenes bloqueados
        if sid in blocked_sources:
            continue

        group_weight = max([v for k,v in groups_weights.iteritems() if k in s["g"]] or [1])

        if "quality" in s:
            quality = s["quality"]
        else:
            results[sid] = 0.8*group_weight
            continue

        if not (quality and "rating" in quality):
            results[sid] = 0.8*group_weight
            continue

        rating = quality["rating"]*.15+0.001 # evita valores 0 absolutos
        kbps = quality.get("kbps", None)
        # evita valores superiores a 1 mega
        if kbps and kbps>max_kbps:
            kbps = max_kbps

        second_delay = quality.get("second_delay", 0)
        # evita valores superiores a 2 minuto
        if second_delay and second_delay>max_second_delay:
            second_delay = max_second_delay

        speed = (1.5*log(kbps+1)/log(max_kbps) if kbps else 1)*(1.5-sqrt(second_delay/max_second_delay) if second_delay else 1)

        # problems
        login = quality.get("login", 0)*0.2
        password = quality.get("password", 0)*0.1
        no_downloadable = quality.get("no_downloadable", 0)*0.3
        geoblocking = quality.get("geoblocking", 0)*0.3
        problems = 1-(login+password+no_downloadable+geoblocking)

        results[sid] = group_weight*rating*problems*speed
    return results

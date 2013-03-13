# -*- coding: utf-8 -*-
from threading import Thread
from foofind.utils import sphinxapi2, logging

REQUEST_MODE_PER_GROUPS = 0
REQUEST_MODE_PER_SERVER = 1

class StopWorker:
    pass

FIELD_NAMES = {"T": "@(fn,md) ", "F":"@fil ", "N": "@ntt "}

class SphinxWorker(Thread):
    def __init__(self, server, config, tasks, clients, proxy, *args, **kwargs):
        super(SphinxWorker, self).__init__(*args, **kwargs)
        self.server = server
        self.tasks = tasks
        self.clients = clients
        self.proxy = proxy
        self.daemon = True
        self.stopped = False
        self.disable_query_search = config["SERVICE_SPHINX_DISABLE_QUERY_SEARCH"] # desactiva busqueda sin filtros

    def stop(self):
        self.stopped = True
        self.tasks.put((StopWorker, None))

    def run(self):
        while True:
            try:
                allsearches, asearch = self.tasks.get()
            except BaseException as e:
                logging.error(e)
                continue

            # mensaje de parada
            if allsearches is StopWorker or self.stopped:
                break

            # pide un cliente de sphinx al pool de clientes
            client = self.clients.get_sphinx_client()

            # no puede atender peticiones sin cliente
            if not client:
                continue

            # inicializa variables
            results = None
            messages = (None, None)

            try:
                filters = asearch["f"] if "f" in asearch else {}
                sort = asearch["sr"] or "e DESC, rw DESC, r2 DESC, fs DESC, uri1 DESC"
                rating_formula = asearch["rf"] or "@weight*(r+10)"

                request_mode = asearch["rm"] if "rm" in asearch else REQUEST_MODE_PER_GROUPS
                offset, limit, max_matches, cutoff = asearch["l"]
                st, sf = asearch["st"], asearch["sf"]
                query = asearch["q"]
                query_parts = query["p"] if "p" in query and query["p"] else None

                if not query_parts:
                    raise Exception("Empty query search received.")

                index = query["i"]+self.server

                client.SetFieldWeights({"fn":100, "md":1, "fil":100, "ntt":200})
                client.SetSortMode(sphinxapi2.SPH_SORT_EXTENDED, sort)
                client.SetMatchMode(sphinxapi2.SPH_MATCH_EXTENDED)
                client.SetRankingMode(sphinxapi2.SPH_RANK_EXPR, "sum((4*lcs+2.0/min_hit_pos)*user_weight)")

                text_query_parts = []
                last_part_type = None
                for part_type, part_value in query_parts:
                    if part_type=="G":
                        text_query_parts.append("("+" | ".join(FIELD_NAMES[key]+value for key, value in part_value)+")")
                    elif last_part_type == part_type: # evita repetir nombres de campos
                        text_query_parts.append(part_value)
                    else:
                        text_query_parts.append(FIELD_NAMES[part_type]+part_value)
                    last_part_type = part_type
                text = " ".join(text_query_parts).encode("utf-8")

                # suma 10 a r, si r es 0, evita anular el peso de la coincidencia, si es -1, mantiene el peso positivo
                client.SetSelect("*, if(g>0xFFFFFFFF,1,0) as e, "+rating_formula+" as rw, min(if(z>0,z,100)) as zm, max(z) as zx")
                client.SetMaxQueryTime(asearch["mt"])

                # filtra por rango de ids
                range_ids = query["ids"] if "ids" in query else None

                # traer resultados de uno o más grupos
                if "g" in asearch:
                    for sg, first in asearch["g"].iteritems():
                        if range_ids:
                            client.SetIDRange(range_ids[0], range_ids[1])
                        else:
                            client.SetIDRange(0, 0)
                        client.SetFilter('bl', [0])
                        client.SetFilter("g", [long(sg)])
                        client.SetLimits(first, limit, max_matches, cutoff)
                        if filters: self.apply_filters(client, filters)
                        client.AddQuery(text, index, "w_sg "+str(asearch["mt"])+" "+sg)
                        client.ResetFilters()
                else:  # traer resumen principal de todos los grupos
                    if range_ids:
                        client.SetIDRange(range_ids[0], range_ids[1])
                    else:
                        client.SetIDRange(0, 0)
                    client.SetFilter('bl', [0])
                    client.SetFilter("s", self.proxy.blocked_sources, True)
                    client.SetLimits(offset, limit, max_matches, cutoff)
                    client.SetGroupBy("g", sphinxapi2.SPH_GROUPBY_ATTR, "e ASC, @count desc")

                    if not self.disable_query_search and st: # realiza la busqueda sin filtros
                        client.AddQuery(text, index, "w_st "+str(asearch["mt"]))

                    if sf or self.disable_query_search: # realiza la busqueda con filtros
                        if request_mode != REQUEST_MODE_PER_GROUPS:
                            client.ResetGroupBy()
                        if filters: self.apply_filters(client, filters)
                        client.AddQuery(text, index, "w_sf "+str(asearch["mt"]))

                results = client.RunQueries()
                messages = (client.GetLastWarning(), client.GetLastError())
                self.clients.return_sphinx_client(client, not bool(results))
            except BaseException as e:
                results = None
                messages = (None, e.message)
                server = self.server
                logging.error(e)
                self.clients.return_sphinx_client(client, True)
            finally:
                self.proxy.put_results(allsearches, asearch, self.server, results, messages)

    def apply_filters(self, client, filters):
        if "z" in filters:
            client.SetFilterFloatRange('z', float(filters["z"][0]), float(filters["z"][1]))
        if "e" in filters:
            client.SetFilterRange('e', filters["e"])
        if "ct" in filters:
            client.SetFilter('ct', filters["ct"])
        if "src" in filters:
            client.SetFilter('s', set(filters["src"]).difference(self.proxy.blocked_sources))

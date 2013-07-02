# -*- coding: utf-8 -*-
from sphinxservice import *
from math import exp

def get_ct(sg):
    return (int(sg)&0xF0000000L)>>28

def get_src(sg):
    return (int(sg)&0xFFFF000L)>>12

def DEFAULT_WEIGHT_PROCESSOR(w, ct, r, nr):
    return w*ct*nr

def DEFAULT_TREE_VISITOR(item):
    if item[0]=="_w" or item[0]=="_u":
        return None
    else:
        return item[1]["_w"]/(item[1]["_u"]+1.)

class ResultsBrowser:
    '''
        Recorre los resultados obtenidos de cache y los va obteniendo en orden.
        Almacena las peticiones pendientes al motor de busqueda.
    '''
    def __init__(self, context, results, max_requests, ct_weights, weight_processor=None, tree_visitor=None):
        self.context = context
        self.results = results
        self.fetch_more = self.max_requests = max_requests
        self.ct_weights = ct_weights
        self.weight_processor = weight_processor or DEFAULT_WEIGHT_PROCESSOR
        self.tree_visitor = tree_visitor or DEFAULT_TREE_VISITOR
        self.sure = True
        self.subgroups = {}
        self.visits = {}
        self.requests = {}
        self.versions = {}
        self._create_tree()

    def _create_tree(self):
        total = 0

        self.tree = tree = {}
        for key, part_info in self.results.iteritems():
            # almaceno versiones de las partes
            if key[0]==VERSION_KEY:
                self.versions[key[1]]=int(part_info)
                continue
            # solo me interesan claves de servidores
            elif key[0]!=PART_KEY:
                continue

            # obtiene informacion del servidor
            part_info = parse_data(part_info)
            if self.sure and part_info[1]:
                self.sure = False

            self.subgroups[key[1]] = subgroups = part_info[-1]

            # recorre subgrupos del servidor
            for isg, (count, result) in subgroups.iteritems():
                total += count
                ict, isrc = get_ct(isg), get_src(isg)
                sg, ct, src = str(isg), str(ict), str(isrc)
                nweight = self._normalize_weight(result[-2], result[-1], ict, isrc)

                # crea u obtiene rama por tipo de contenido
                if ct in tree:
                    group = tree[ct]
                else:
                    group = tree[ct] = {"_w":0, "_u":0}

                # crea u obtiene rama por origen
                if src in group:
                    group2 = group[src]
                else:
                    group2 = group[src] = {"_w":0, "_u":0}

                # crea u obtiene rama por subgrupo
                if sg in group2:
                    subgroup = group2[sg]
                else:
                    subgroup = group2[sg] = {"_w":0, "_u":0}

                # mantiene valores maximos
                if nweight>subgroup["_w"]:
                    subgroup["_w"] = nweight
                    if nweight>group2["_w"]:
                        group2["_w"] = nweight
                        if nweight>group["_w"]:
                            group["_w"] = nweight
        self.total = total

    def __iter__(self):
        return self

    def _normalize_weight(self, rating, weight, ict, isrc):
        std_dev = round(self.context.proxy.sources_rating_standard_deviation.get(isrc,0), 3) if rating>=0 else 0
        if std_dev>0:
            val = (rating-self.context.proxy.sources_rating_average.get(isrc,0)*1.5)/std_dev
            val = max(-500, min(500, val))
        else:
            val = 0
            rating = 0.5 if rating==-1 else 1.1 if rating==-2 else rating

        normalized_rating = self.context.proxy.sources_weights.get(isrc,0)*(1./(1+exp(-val)) if std_dev else rating)
        return self.weight_processor(weight, self.ct_weights[ict], rating, normalized_rating)

    def next(self):
        # no quedan resultados por devolver
        if not self.tree:
            raise StopIteration

        tree_visitor = self.tree_visitor

        # busca el subgrupo en el que buscar
        ct, group = max(self.tree.iteritems(), key=tree_visitor)
        src, group2 = max(group.iteritems(), key=tree_visitor)
        sg, subgroup = max(group2.iteritems(), key=tree_visitor)
        isg = int(sg)
        ict = int(ct)
        isrc = int(src)

        # busca el servidor con mas peso
        max_weight = None
        for server, subgroups in self.subgroups.iteritems():
            # ignora el servidor si no tiene el subgrupo
            if isg not in subgroups: continue

            # obtiene el numero de resultados y el primer resultado
            count, result = subgroups[isg]

            # obtiene la posicion en el grupo
            if sg+server in self.visits:
                position, results = self.visits[sg+server]
                this_result = results[position] # el primer elemento es el numero de elementos disponibles y reemplaza al que está en el resumen
                nweight = self._normalize_weight(this_result[-2], this_result[-1], ict, isrc)
            else:
                position = 0
                this_result = result
                nweight = self._normalize_weight(this_result[-2], this_result[-1], ict, isrc)

            # mira si es el maximo
            if nweight>max_weight:
                next_weight = max_weight
                max_weight = nweight
                max_server = server
                max_position = position
                max_result = this_result
                max_count = count
            # si no, mira si es el siguiente
            elif nweight>next_weight:
                next_weight = nweight

        # hay mas resultados disponibles en este subgrupo y servidor? hace falta pedirlos?
        delete_sg_server = need_request = False
        next_position = max_position+1

        # si se ha llegado al final del subgrupo en el servidor, se elimina el subgrupo
        if next_position==max_count:
            delete_sg_server = True
        elif max_position: # no es el primer resultado, ya hay info de visita
            if next_position>=self.visits[sg+max_server][1][0]:
                need_request = True
            else:
                self.visits[sg+max_server][0] = next_position
        else:
            if PART_SG_KEY+max_server+sg in self.results:
                self.visits[sg+max_server] = [next_position, parse_data(self.results[PART_SG_KEY+max_server+sg])]

                # si no hay resultados, debe pedir mas
                if next_position>=self.visits[sg+max_server][1][0]:
                    need_request = True
            else:
                need_request = True

        # pide mas resultados
        if need_request:
            if not max_server in self.requests:
                self.requests[max_server] = {}
            if len(self.requests[max_server])<self.max_requests:
                self.requests[max_server][sg] = next_position
                self.fetch_more-=1
            else:
                self.fetch_more = False
        elif not delete_sg_server:
            # compara el siguiente peso en otros servidores con el siguiente peso del servidor
            next_result = self.visits[sg+max_server][1][next_position]
            server_next_weight = self._normalize_weight(next_result[-2], next_result[-1], ict, isrc)
            if server_next_weight>next_weight:
                next_weight = server_next_weight

        # elimina el subgrupo para el servidor si ya no tiene mas resultados
        if delete_sg_server or need_request:
            if sg+max_server in self.visits: # si sólo hay un fichero en el subgrupo, esto no se ha creado
                del self.visits[sg+max_server]
            del self.subgroups[max_server][isg]

        # actualiza peso del subgrupo
        if next_weight==None: # no hay mas resultados para este subgrupo
            del group2[sg]
        else:
            subgroup["_u"]+=1
            subgroup["_w"]=next_weight

        # actualiza el subgrupo con mas peso en el grupo por origen
        new_weight=tree_visitor(max(group2.iteritems(), key=tree_visitor))
        if new_weight==None: # no hay mas resultados para este grupo
            del group[src]
        else:
            group2["_u"]+=1
            group2["_w"]=new_weight

        # actualiza el subgrupo con mas peso en el grupo por tipo de contenido
        new_weight=tree_visitor(max(group.iteritems(), key=tree_visitor))
        if new_weight==None: # no hay mas resultados para este grupo
            del self.tree[ct]
        else:
            group["_u"]+=1
            group["_w"]=new_weight

        return (str(ord(max_server)), sg, max_weight, max_result)

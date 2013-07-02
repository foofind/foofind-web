# -*- coding: utf-8 -*-

from struct import Struct
from threading import Lock, Event
from string import whitespace
from heapq import heappop, heappush
from math import sqrt, log, exp
from time import time
from hashlib import md5
from collections import defaultdict
import re, msgpack

from foofind.utils.splitter import split_file, SEPPER
from foofind.utils.content_types import *
from foofind.utils import bin2hex, logging, hex2url, u
from foofind.utils.filepredictor import ALL_TAGS, ALL_FORMATS

from .worker import REQUEST_MODE_PER_GROUPS, REQUEST_MODE_PER_SERVER

SEASON_EPISODE_SEARCHER = re.compile(r"^(?P<s>\d{1,2})x(?P<e>\d{1,2})$")
SEASON_EPISODE_SEARCHER2 = re.compile(r"^s(?P<s>\d{1,2})(\W*e(?P<e>\d{1,2}))?$")
BLEND_CHARS = frozenset(r"+&-@!$%?#")
NON_TEXT_CHARS = frozenset(SEPPER.union(set(u"'½²º³ª\u07e6\u12a2\u1233\u179c\u179fµ")).difference(BLEND_CHARS))
WORD_SEARCH_MIN_LEN = 2
BLOCKED_WORDS = frozenset(["www"])

BLOCKED_FILE_VERSION = -1

# arregla problemas de codificación de la version de sphinx
SPHINX_WRONG_RANGE = re.compile("\xf0([\x80-\x8f])")
def fixer(char):
    return chr(ord(char.group(1))+96)

def fix_sphinx_result(word):
    return u(SPHINX_WRONG_RANGE.sub(fixer, word))

class Search(object):
    def __init__(self, proxy, query, filters={}, order=None, request_mode=REQUEST_MODE_PER_GROUPS, query_time=None, extra_wait_time=500):
        self.request_mode = request_mode
        self.access = Lock()
        self.usable = Event()
        self.has_changes = False
        self.proxy = proxy
        self.query_time = query_time or proxy.config["SERVICE_SPHINX_MAX_QUERY_TIME"]
        self.extra_wait_time = extra_wait_time
        self.search_max_retries = proxy.config["SERVICE_SPHINX_SEARCH_MAX_RETRIES"]
        self.stats = None
        self.computable = True
        self.canonical_parts = []
        self.canonical_words = None
        self.seen_words = {}
        self.disable_query_search = proxy.config["SERVICE_SPHINX_DISABLE_QUERY_SEARCH"] # desactiva busqueda sin filtros

        # orden: rating para el peso, columnas para ordenar, funcion de ordenación y si se deben mezclar grupos en el orden
        self.order = order or (None, None, None, False)

        # normaliza texto de busqueda
        computable = True
        text = filter_tags = ntts = None
        options = []
        position = 0
        query_parts = []

        if "text" in query and query["text"]:
            text = query["text"].strip().lower()

            seen_filters = set()

            for word in BLOCKED_WORDS:
                self.seen_words[word] = -1

            for mode, not_mode, part_words in self.parse_query(text):
                # prefijo - para las busquedas negativas
                not_prefix = "-" if not_mode else ""
                words_count = len(part_words)
                first_word = part_words[0].lower()
                all_words = " ".join(part_words).lower()

                # si tiene parentesis o puede intentar intuir tags...
                if mode=="(" or (mode==True and words_count==1):

                    # modo intuicion?
                    guess_mode = mode==True and words_count==1

                    # mira si es un tipo de contenido
                    if not guess_mode and mode and words_count==1 and first_word in CONTENTS_CATEGORY.iterkeys():
                        current_filter = FILTER_PREFIX_CONTENT_TYPE.lower()+first_word
                        if current_filter in seen_filters: # no procesa dos veces el mismo filtro
                            mode = guess_mode # procesa como texto esta palabra
                        else:
                            query_parts.append(("F", not_prefix+current_filter))
                            self.canonical_parts.append(not_prefix+"("+first_word+")")

                            if current_filter not in self.seen_words:
                                self.seen_words[current_filter] = position
                                position+=1

                        seen_filters.add(current_filter)
                        mode = False

                    # mira si se trata de un tag
                    if mode and words_count==1:
                        if first_word in ALL_TAGS:
                            tag_word = first_word
                        else:
                            tag_word = first_word[:-1] if first_word.endswith("s") else first_word+"s"
                            if not tag_word in ALL_TAGS:
                                tag_word = None

                        if tag_word:
                            current_filter = FILTER_PREFIX_TAGS.lower()+tag_word
                            if current_filter in seen_filters: # no procesa dos veces el mismo filtro
                                mode = guess_mode # procesa como texto esta palabra
                            else:
                                if guess_mode:
                                    new_word = first_word not in self.seen_words
                                    position_word = position if new_word else self.seen_words[first_word]
                                    query_parts.append(("G", [("T",first_word), ("F",current_filter)]))
                                    self.canonical_parts.append("{%d}"%position_word)
                                    if new_word:
                                        self.seen_words[first_word] = position
                                        position+=1
                                else:
                                    query_parts.append(("F", not_prefix+current_filter))
                                    self.canonical_parts.append(not_prefix+"("+tag_word+")")

                                if current_filter not in self.seen_words:
                                    self.seen_words[current_filter] = position
                                    position+=1

                                seen_filters.add(current_filter)
                                mode = False

                    # mira si es un formato
                    if mode and (words_count<3 and guess_mode and first_word in ALL_FORMATS) or (not guess_mode and words_count==2 and first_word=="format" and part_words[1].lower() in ALL_FORMATS):
                        current_filter = FILTER_PREFIX_FORMAT.lower()+(first_word if guess_mode else part_words[1].lower())
                        if current_filter in seen_filters:
                            mode = guess_mode # procesa como texto esta palabra
                        else:
                            if guess_mode:
                                new_word = first_word not in self.seen_words
                                position_word = position if new_word else self.seen_words[first_word]
                                query_parts.append(("G", [("T",first_word), ("F",current_filter)]))
                                self.canonical_parts.append("{%d}"%position_word)
                                if new_word:
                                    self.seen_words[first_word] = position
                                    position+=1
                            else:
                                query_parts.append(("F", not_prefix+current_filter))
                                self.canonical_parts.append(not_prefix+"("+all_words+")")

                            if current_filter not in self.seen_words:
                                self.seen_words[current_filter] = position
                                position+=1
                            seen_filters.add(current_filter)
                            mode = False

                    # mira si es un código de temporada y episodio
                    if mode and words_count<3:
                        season = None
                        sea_epi = SEASON_EPISODE_SEARCHER.match(all_words) or SEASON_EPISODE_SEARCHER2.match(all_words)
                        if sea_epi:
                            sea_epi = sea_epi.groupdict()
                            if sea_epi["s"]:
                                season = "%02d"%int(sea_epi["s"])

                        # si al menos tiene temporada...
                        if season:
                            season_filter = FILTER_PREFIX_SEASON.lower()+season
                            if FILTER_PREFIX_EPISODE in seen_filters:
                                mode = guess_mode # procesa como texto esta palabra
                            else:
                                # si tiene episodio
                                episode = "%02d"%int(sea_epi["e"]) if sea_epi["e"] else None
                                episode_filter = FILTER_PREFIX_EPISODE.lower()+episode if episode else ""

                                if guess_mode:
                                    new_word = first_word not in self.seen_words
                                    position_word = position if new_word else self.seen_words[first_word]
                                    query_parts.append(("G",[("T",first_word), ("F","(%s %s)"%(season_filter, episode_filter) if episode else season_filter)]))
                                    self.canonical_parts.append("{%d}"%position_word)
                                    if new_word:
                                        self.seen_words[first_word] = position
                                        position+=1
                                else:
                                    query_parts.append(("F",not_prefix+season_filter))
                                    if episode:
                                        query_parts.append(("F", not_prefix+episode_filter))
                                    self.canonical_parts.append(not_prefix+"("+season_filter+episode_filter+")")

                                if season_filter not in self.seen_words:
                                    self.seen_words[season_filter] = position
                                    position+=1

                                if episode and episode_filter not in self.seen_words:
                                    self.seen_words[episode_filter] = position
                                    position+=1

                                seen_filters.add(FILTER_PREFIX_EPISODE)
                                mode = False

                    # las siguientes no aplican en modo guess

                    # mira si es un año
                    if not guess_mode and mode and words_count==1:
                        year = None
                        if first_word.isdecimal():
                            year = int(first_word)
                            year = year if 1900 < year < 2100 else None

                        if year:
                            current_filter = FILTER_PREFIX_YEAR.lower()+str(year)
                            if current_filter in seen_filters:
                                mode = guess_mode # procesa como texto esta palabra
                            else:
                                query_parts.append(("F",not_prefix+current_filter))
                                self.canonical_parts.append(not_prefix+"("+str(year)+")")

                                if current_filter not in self.seen_words:
                                    self.seen_words[current_filter] = position
                                    position+=1
                                seen_filters.add(current_filter)
                                mode = False

                    # mira entidades
                    if not guess_mode and mode and words_count==1:
                        if first_word[0]=="n" and first_word[1:].isdecimal():
                            ntt = int(first_word[1:])
                            query_parts.append(("N",not_prefix+str(ntt).rjust(4,"0")))
                            self.canonical_parts.append(not_prefix+"("+str(ntt)+")")
                            mode = False

                if mode:
                    if mode==True: # sin comillas ni parentesis
                        new_word = first_word not in self.seen_words
                        position_word = position if new_word else self.seen_words[first_word]

                        if position_word==-1: continue # ignora palabras bloquedas

                        valid_word = len(first_word)>=WORD_SEARCH_MIN_LEN # palabra muy corta
                        query_parts.append(("T",not_prefix+escape_string(first_word)))
                        self.canonical_parts.append(not_prefix+(first_word.replace("{", "{{").replace("}","}}") if not valid_word else "{%d}"%position_word))
                        if valid_word and new_word:
                            self.seen_words[first_word] = position
                            position+=1

                    else:
                        query_parts.append(("T", not_prefix+"\""+escape_string(all_words)+"\""))
                        mask = []
                        for word in part_words: # las palabra cortas no vuelven en los resultados de busqueda y hay que ponerlas a mano
                            word = word.lower()
                            new_word = word not in self.seen_words
                            valid_word = len(word)>=WORD_SEARCH_MIN_LEN # palabra muy corta
                            position_word = position if new_word else self.seen_words[word]

                            if position_word==-1: continue # ignora palabras bloquedas

                            if valid_word and new_word:
                                self.seen_words[word] = position
                                position+=1

                            # evita que las llaves de las palabras se confundan con placeholders
                            mask.append(word.replace("{", "{{").replace("}","}}") if not valid_word else "{%d}"%position_word)
                        self.canonical_parts.append(not_prefix+"\""+"_".join(mask)+"\"")
            self.canonical_words = position

        # parsea filtros
        self.filters = {}
        if filters:
            if 'type' in filters:
                self.filters['ct'] = [sphinx_type for atype in filters["type"] for sphinx_type in CONTENTS_CATEGORY[atype]]

            if 'size' in filters:
                sizes = filters["size"]
                self.filters['z'] = [float(sizes[0]),float(sizes[1])]

            if "src" in filters:
                groups={"s":"streaming","w":"download","f":"download","p":"p2p","g":"gnutella","t":"torrent","e":"ed2k"}
                src = filters["src"]
                self.filters["src"] = [source_id for source_id, source in self.proxy.sources.iteritems()
                        if source["d"][:source["d"].rfind(".")] in src #esta el dominio en la URL
                        or any(group in groups and groups[group] in src for group in source["g"]) #si viene el origen en vez del suborigen
                        or ("other-streamings" in src and source["d"] not in self.proxy.sources_relevance_streaming[:8] and "s" in source["g"]) #si viene other... y no esta en la lista de sources y el source tiene streaming
                        or ("other-downloads" in src and source["d"] not in self.proxy.sources_relevance_download[:8] and ("w" in source["g"] or "f" in source["g"])) #si viene other... y no esta en la lista de sources y el source tiene web
                ]

        # orden de los resultados
        order_repr = list(self.order)
        if order_repr[2]:
            order_repr[2] = order_repr[2].__module__+"."+order_repr[2].__name__

        # extrae informacion de la query recibida
        query_type = query["type"]
        if query_type=="list":
            self.query_key = u"L"+md5(query["user"]+"."+query["list"]).hexdigest()
            self.query = {"y":query_type, "i": "idx_lists", "ids": (query["user"]<<32, query["user"]<<32 | 0xFFFFFFFF)}
        elif query_type=="text":
            self.query_key = u"Q"+md5(msgpack.dumps(query_parts)+"".join("|"+str(part) if part else "|" for part in order_repr)).hexdigest()
            self.query = {"y":query_type, "i": "idx_files", "p": query_parts}

        self.query_state, self.filters_state, self.locked_until, self.blocked_ids = proxy.get_search_state(self.query_key, self.filters)

        self.entities = set(int(key)>>32 for key in self.filters_state["sg"].iterkeys() if int(key)>>32) if self.filters_state and "sg" in self.filters_state else set()

        # busqueda de la que se puede saber el numero de resultados por los hits de la palabra

        self.special_search = self.canonical_words==1 and query_parts[0][0]=="T" and not self.filters

    def parse_query(self, query):
        # inicializa variables
        acum = []                   # palabra actual
        all_acums = []              # lista de palabras a devolver
        yield_mode = False

        valid_acum = 0              # contador de caracteres validos, para evitar hacer consultas de una sola letra
        not_mode = False            # indica que esta parte de la consulta está en modo negativo
        quote_mode = False          # indica que esta parte de la consulta va en entre comillas
        tag_mode = False            # indica que esta parte de la consulta es un tag
        any_not_blend_char = False     # indica que alguna letra de la palabra no es un blend char
        any_not_not_part = False    # indica que alguna parte de la consulta no está en modo negativo

        # recorre caracteres (añade un espacio para considerar la ultima palabra)
        for ch in query.replace("\0","")+"\0":
            if not acum and (ch=="-" or ch=="!"): # operador not
                not_mode = True
            elif ch=="(" and not tag_mode and not quote_mode:
                yield_mode = True
                tag_mode = True
            elif ch==")" and tag_mode:
                yield_mode = "("
                tag_mode = False
            elif ch=="\"": # comillas
                if quote_mode:
                    yield_mode = "\"" # indica que incluya comillas en el resultados
                else:
                    yield_mode = True
                quote_mode = not quote_mode
            elif ch=="\0":
                yield_mode = "(" if tag_mode else "\"" if quote_mode else True
                tag_mode = quote_mode = False
            elif ch in NON_TEXT_CHARS: # separadores de palabras fuera de comillas
                # el menos no puede estar separado para negar
                if not acum and not_mode:
                    not_mode = False

                if not quote_mode and not tag_mode:
                    yield_mode = True
                else:
                    yield_mode = " "
            else: # resto de caracters
                valid_acum += 1
                any_not_blend_char = any_not_blend_char or ch not in BLEND_CHARS
                acum.append(ch)

            # si toca devolver resultado, lo hace
            if yield_mode:
                acum_result = "".join(acum) if acum and valid_acum else False

                # acumula palabras
                if acum_result:
                    all_acums.append(acum_result)
                    any_not_not_part = any_not_not_part or (any_not_blend_char and valid_acum>=WORD_SEARCH_MIN_LEN and not not_mode)
                    del acum[:]
                    valid_acum = 0
                    any_not_blend_char = False

                # devuelve palabras
                if yield_mode!=" " and all_acums:
                    yield (yield_mode, not_mode, all_acums)
                    all_acums = []
                    not_mode = False

                yield_mode = False

        # si no se han devuelto partes no negativas, la consulta no es computable
        if not any_not_not_part:
            self.computable = False

    def search(self, async=False, just_usable=False):
        # avisa que estas busqueda es usable
        if self.filters_state and self.filters_state["c"]:
            self.usable.set()

        # comprueba a qué servidores debe pedirsele los datos iniciales de la busqueda
        now = time()

        # si la busqueda esta bloqueada o no es computable, sale sin hacer nada
        if (self.locked_until and self.locked_until<now) or not self.computable:
            return self

        can_retry_query = not self.query_state or self.query_state["rt"]<self.search_max_retries
        search_query = [i for i in self.proxy.servers.iterkeys()
                                if (can_retry_query and (not i in self.query_state["c"] or i in self.query_state["i"]))
                                    or self.query_state["d"]<self.proxy.servers_stats[i]["d1"]] \
                            if self.query_state else self.proxy.servers.keys()

        # no busca por query
        if self.disable_query_search:
            search_query = []

        can_retry_filters = not self.filters_state or self.filters_state["rt"]<self.search_max_retries

        search_filters = [i for i in self.proxy.servers.iterkeys()
                                if (can_retry_filters and (not i in self.filters_state["c"] or i in self.filters_state["i"]))
                                    or self.filters_state["d"]<self.proxy.servers_stats[i]["d1"]] \
                            if self.filters_state else self.proxy.servers.keys()

        if search_query or search_filters:
            # tiempo de espera incremental logaritmicamente con los reintentos (elige el tiempo máximo de espera)
            wait_time = self.query_time

            asearch = {"q":self.query, "f":self.filters, "rm":self.request_mode, "mt":wait_time, "rf":self.order[0], "sr":self.order[1]}

            # si no tiene estado para busqueda con filtros, lo crea
            if not self.filters_state:
                self.filters_state = {"cs":0, "v":0, "c": {}, "t": {}, "i":[], "d": {}, "sg":{}, "rt": 0, "df":{}, "lv":{}}

            # si no tiene estado para busqueda sin filtros, lo crea
            if not self.query_state:
                self.query_state = {"c": {}, "t": {}, "i":[], "d": {}, "sg": {}, "rt": 0}

            # solo debe buscar con filtros si hay filtros o si no va a buscar la busqueda base
            must_search_filters = self.filters or not search_query

            # compone la búsqueda con las consultas correspondientes a cada servidor
            allsearches = {"s":
                            {server:dict(asearch.items() +  # busqueda base
                                        # buscar texto si toca en este servidor
                                        [("st",server in search_query),
                                        # buscar con filtros si se debe y toca en este servidor
                                        ("sf",must_search_filters and server in search_filters),
                                        # limite segun situacion
                                        ("l", (0, 500 if self.request_mode==REQUEST_MODE_PER_GROUPS else
                                                  5 if server in self.filters_state["i"] else
                                                100, 10000, 2000000))
                                        ])
                            for server in set(search_query+(search_filters if must_search_filters else []))
                            }
                        }

            # realiza la busqueda y almacena los resultados
            self.proxy.search(allsearches)

            # espera el tiempo de sphinx mas medio segundo de extra
            # cuando hay dos busquedas en algun servidor (con y sin filtros) se espera el doble
            num_waits = 2 if any((s["st"] and s["sf"]) for s in allsearches["s"].itervalues()) else 1

            # si la busqueda es asyncrona o sólo se requieren resultados usables, no bloquea
            if async or (just_usable and self.usable.is_set()):
                self.proxy.async_process_results(allsearches, self.store_files_timeout, [wait_time*num_waits+self.extra_wait_time])
            else:
                # si no llegan todos los resultados espera otro segundo más, por si hubiera habido problemas en la respuesta
                self.store_files(allsearches, wait_time, self.store_files_timeout, [wait_time]*(num_waits-1) + [self.extra_wait_time])

        return self

    def save_state(self, locking_time):
        ''' Guarda en cache los datos de la búsqueda.
        Estos se dividen en dos partes:
         - la lista de ficheros que aplican para esta búsqueda de texto.
         - las listas de ficheros resultante para los filtros dados. '''

        # si hay bloqueados, los actualiza y bloquea antes de guardar
        if self.blocked_ids:
            self.blocked_ids = self.proxy.get_blocked_files_state(self.query_key, self.filters)
            self.block_files_from_cache()

        self.filters_state["v"]+=1
        self.filters_state["cs"] = sum(self.filters_state["c"].itervalues()) - sum(dup+bl for dup, bl in self.filters_state["df"].itervalues())
        new_locked_until = self.proxy.save_search_state(self.query_key, self.filters, self.query_state, self.filters_state, locking_time, self.blocked_ids)
        if new_locked_until!=None:
            self.locked_until = new_locked_until

    def generate_stats(self, force_refresh=False):
        # generar estadisticas de la busqueda
        if self.computable:
            if force_refresh or not self.stats:
                self.stats = {k:v for k,v in self.filters_state.iteritems() if k in ("v", "t")}
                self.stats["cs"] = sum(count for server_id, count in self.filters_state["c"].iteritems() if server_id in self.proxy.servers) - sum(diff[0]+diff[1] for server_id, diff in self.filters_state["df"].iteritems() if server_id in self.proxy.servers)

                self.stats["rt"] = self.filters_state["rt"]
                self.stats["st"] = not (self.query_state["i"] or self.proxy.servers_set.difference(set(self.query_state["c"])))
                self.stats["sf"] = not (self.filters_state["i"] or self.proxy.servers_set.difference(set(self.filters_state["c"])))
                self.stats["s"] = self.stats["sf"] and (self.disable_query_search or self.stats["st"])
                self.stats["ct"] = self.query_state["ct"] if "ct" in self.query_state else None
        else:
            self.stats = {"cs":0, "rt":0, "v":0, "t":0, "s":True, "w":0, "li":[], "ct":None}

    def get_stats(self):
        self.generate_stats()
        return self.stats

    def get_modifiable_info(self):
        self.access.acquire(True)

        # quita ficheros bloqueados
        self.block_files_from_cache()

        subgroups = self.filters_state["sg"]

        info = {}
        # w=peso del grupo, l=ultimo elemento extraido, lw=ultimo peso
        for sg, osg in subgroups.iteritems():

            g = get_group(sg)
            g2 = get_group2(sg)

            weight = -osg["h"][0][0]
            cs = sum(osg["c"].itervalues())

            if g in info:
                ig = info[g]
                ig["cs"]+=cs
                if weight>ig["w"]: ig["w"]=weight
            else:
                ig = info[g] = {"cs":cs, "l":0, "g2":{}, "w":weight}

            if g2 in ig["g2"]:
                ig2 = ig["g2"][g2]
                ig2["cs"]+=cs
                if weight>ig2["w"]: ig2["w"]=weight
            else:
                ig2 = ig["g2"][g2] = {"w":weight, "cs":cs, "l":0, "sg":{}}

            isg = ig2["sg"][sg] = {"cs":cs, "h":osg["h"][:], "w":weight, "l":0}

        self.access.release()
        return info

    def get_results(self, last_items=[], skip=None, min_results=5, max_results=10, hard_limit=10000, max_extra_searches=4):

        # si todavia no hay informacion de filtros, sale sin devolver nada
        if not self.computable:
            raise StopIteration

        info = self.get_modifiable_info()
        subgroups = self.filters_state["sg"]

        must_return = True
        stop_browsing = False

        # busquedas derivadas de la obtencion de resultados
        max_searches = 0 # numero maximo de busquedas en algun servidor
        searches = defaultdict(dict) if self.request_mode==REQUEST_MODE_PER_GROUPS else defaultdict(int)

        returned = 0
        versions = [0]*self.filters_state["v"]
        last_versions = versions[:]
        last_items_len = len(last_items)
        last_versions[:last_items_len] = last_items
        new_versions = last_versions[:]

        src_count = defaultdict(int)

        if self.order[3]:
            ct_weights = {str(ct):1 for ct in (CONTENT_UNKNOWN, CONTENT_AUDIO, CONTENT_VIDEO, CONTENT_IMAGE, CONTENT_APPLICATION, CONTENT_DOCUMENT)}
        else:
            ct_weights = {str(ct):value for ct, value in [(CONTENT_UNKNOWN, 0.1), (CONTENT_AUDIO, 0.7), (CONTENT_VIDEO, 1.0),
                                                      (CONTENT_IMAGE, 0.5), (CONTENT_APPLICATION, 0.5), (CONTENT_DOCUMENT, 0.5)]}

        total_results = min(hard_limit,self.filters_state["cs"]+sum(dup+bl for dup, bl in self.filters_state["df"].itervalues()))
        i = 0

        for i in xrange(total_results):

            # si se han devuelto el minimo y no se puede más o se ha devuelto el maximo, para
            if returned>=max_results or stop_browsing and returned>=min_results: break

            # busca grupo del que sacar un resultado (por content_type)
            filtered_groups = [(og["w"]/(og["l"]+1)*ct_weights[g], og["l"], g, og) for g, og in info.iteritems() if og["w"]>-1]
            if filtered_groups:
                w, l, g, og = max(filtered_groups)
            else:
                break

            # busca grupo del que sacar un resultado (por origen)
            filtered_groups2 = [(og2["w"]/(src_count[g2]+1), og2["l"], g2, og2) for g2, og2 in og["g2"].iteritems() if og2["w"]>-1]
            if filtered_groups2:
                w2, l2, g2, og2 = max(filtered_groups2)
            else:
                og["w"]=-1
                if len(filtered_groups)==1:
                    break
                continue

            # busca subgrupo del que sacar un resultado
            filtered_subgroups = [(osg["w"], osg["l"], sg, osg) for sg, osg in og2["sg"].iteritems() if osg["w"]>-1]
            if filtered_subgroups:
                ws, ls, sg, osg = max(filtered_subgroups)
            else:
                og2["w"]=-1
                if len(filtered_groups2)==1:
                    og["w"] = -1
                    if len(filtered_groups)==1:
                        break
                continue

            # actualiza pesos y contador de resultados obtenidos de grupos y subgrupo
            if not self.order[3]:
                og["l"] = l+1
                og2["l"] = l2+1
                osg["l"] = ls+1
                src_count[g2]+=1

            # guarda grupos de los que hay que pedir más
            subgroup = subgroups[sg]

            # obtiene resultado del subgrupo y su información asociada
            result_weight, result_id = heappop(osg["h"])
            server, sphinx_id, version, search_position = subgroup["f"][result_id]

            # actualiza peso del subgrupo
            if osg["h"]:
                osg["w"] = -osg["h"][0][0]
            else:
                osg["w"] = -1

            og2["w"] = max(osg["w"] for osg in og2["sg"].itervalues())
            og["w"] = max(og2["w"] for og2 in og["g2"].itervalues())

            # incrementa el contador para esta version
            if version!=BLOCKED_FILE_VERSION:
                versions[version]+=1

            # si no existe el servidor, ignora el resultado
            if server not in self.proxy.servers:
                continue

            # devuelve el resultado
            if version!=BLOCKED_FILE_VERSION and versions[version]>last_versions[version] and (must_return or returned<min_results):
                if skip:
                    skip-=1
                else:
                    returned+=1
                    new_versions[version] = versions[version]
                    yield (result_id, server, sphinx_id, -result_weight, sg)

            if self.request_mode==REQUEST_MODE_PER_GROUPS:
                count_diff = subgroup["df"].get(server, [0,0])[1] if "df" in subgroup else 0
                if subgroup["c"][server]-count_diff > search_position >= subgroup["lv"][server]-count_diff:

                    # no realiza más de un numero de busquedas por vez en un servidor para no tener que esperar mucho
                    if not self.locked_until:
                        searches_len = len(searches[server])
                        if sg not in searches[server] and searches_len<max_extra_searches:
                            # al pedir mas ficheros tiene que tener en cuenta los ficheros movidos o bloqueados
                            last_visited_position = subgroup["lv"][server] - count_diff
                            if last_visited_position<0:
                                logging.warn("Trying to retrieve negative positions from search.")
                                last_visited_position = 0
                            searches[server][str(sg)] = last_visited_position
                            if searches_len >= max_searches:
                                max_searches = searches_len+1
                        elif searches_len==max_extra_searches:
                            stop_browsing = True

                    # ya no debe devolver resultados si no son forzados
                    must_return = False
            elif self.request_mode==REQUEST_MODE_PER_SERVER:
                count_diff = self.filters_state["df"].get(server, [0,0])[1] if "df" in self.filters_state else 0
                if self.filters_state["c"][server]-count_diff > search_position >= self.filters_state["lv"][server]-count_diff:
                    if not self.locked_until:
                        if server not in searches:
                            # al pedir mas ficheros tiene que tener en cuenta los ficheros movidos o bloqueados
                            last_visited_position = self.filters_state["lv"][server] - count_diff
                            if last_visited_position<0:
                                logging.warn("Trying to retrieve negative positions from search.")
                                last_visited_position = 0
                            searches[server] = last_visited_position
                            max_searches = 1
                    # ya no debe devolver resultados si no son forzados
                    must_return = False

        self.generate_stats()
        self.stats["end"] = i>=total_results-1
        self.stats["total_sure"] = must_return
        self.stats["li"] = new_versions

        if not self.locked_until:
            self.locked_until = self.proxy.get_lock_state(self.query_key, self.filters)

        if self.locked_until:
            self.stats["w"] = -1
        elif searches:
            wait_time = self.query_time

            if self.request_mode==REQUEST_MODE_PER_GROUPS:
                allsearches = {"s":{server: {"l":(0, 10, 1000, 2000000), "rm":self.request_mode, "st":False, "sf":False, "q":self.query, "f":self.filters, "mt":wait_time, "g":subgroups, "rf":self.order[0], "sr":self.order[1]} for server, subgroups in searches.iteritems()}}
            else:
                allsearches = {"s":{server: {"l":(position, 100, 1000, 2000000), "rm":self.request_mode, "st":False, "sf":True, "q":self.query, "f":self.filters, "mt":wait_time, "rf":self.order[0], "sr":self.order[1]} for server, position in searches.iteritems()}}
            self.proxy.search(allsearches)
            max_wait_time = wait_time * max_searches
            self.locked_until = self.proxy.set_lock_state(self.query_key, self.filters, 500+max_wait_time)
            self.proxy.async_process_results(allsearches, self.store_files_timeout, [max_wait_time/3]*3)
            self.stats["w"] = max_wait_time/3
        elif returned:
            self.stats["w"] = 0
        else:
            self.stats["w"] = self.query_time*2

    def store_files_timeout(self, allsearches, timeouts):
        self.has_changes = False
        new_query_state, new_filters_state, self.locked_until, self.blocked_ids = self.proxy.get_search_state(self.query_key, self.filters)
        if new_filters_state and new_filters_state["v"]>self.filters_state["v"]:
            self.query_state, self.filters_state = new_query_state, new_filters_state

        self.store_files(allsearches, 0, self.store_files_timeout, timeouts)

    def block_files_from_cache(self):

        # no hace nada si no hay ficheros bloqueados
        if not self.blocked_ids: return

        # borra de cache los ficheros eliminados
        filters_subgroups = self.filters_state["sg"]

        for server, file_id, sg in self.blocked_ids:
            if sg in filters_subgroups:
                osg = filters_subgroups[sg]
                if file_id in osg["f"]:
                    afile = osg["f"][file_id]

                    # si ya está bloqueado no lo bloquea de nuevo
                    if afile[2] == -1:
                        continue

                    self.has_changes = True
                    # fichero y subgrupo encontrados, bloquea en cache para que no vuelva a contarse el fichero
                    afile[2] = -1 # version -1 indica que el fichero está bloqueado

                    # añade df en resultados globales y en subgrupo
                    for where in (self.filters_state, osg):
                        if "df" not in where:
                            where["df"] = {server:[0,1]}
                        elif server not in where["df"]:
                            where["df"][server] = [0,1]
                        else:
                            where["df"][server][1] += 1

    def block_files(self, ids):
        return self.proxy.block_files(ids, True, self.query_key, self.filters)

    def store_files(self, allsearches, timeout, fallback=None, fallback_timeouts=None):
        filters_subgroups = self.filters_state["sg"]
        query_subgroups = self.query_state["sg"]

        # indica si se ha recorrido algun resultado de resumen de la busqueda
        any_query_main = any_filters_main = False

        for server, asearch, sphinx_results, messages in self.proxy.browse_results(allsearches, timeout, fallback, fallback_timeouts):
            self.access.acquire(True)

            # permite manejar igual resultados de querys simples o de multiquerys
            if not sphinx_results:
                logging.error("Error in search thread:'%s'"%messages[1])
                self.access.release()
                continue
            elif "matches" in sphinx_results:
                sphinx_results = [sphinx_results]

            # comprueba si es una consulta de resumen
            if asearch["sf"] and asearch["st"]:
                main = [(True, False), (False, True)] # dos consultas, una para sin filtros y otra con filtros
                any_query_main = any_filters_main = True
            elif asearch["sf"]: # solo consulta con filtros
                main = [(False, True)]
                any_filters_main = asearch["l"][0]==0 # solo es principal si empieza en cero
            elif asearch["st"]: # solo consulta sin filtros, puede aplicar para la consulta con filtros (si no hay filtro)
                main = [(True, not self.filters)]
                any_query_main = any_filters_main = True
            elif "g" in asearch:
                main = False
            else:
                main = [(True, True)]

            # incorpora resultados al subgrupo que corresponda
            for result in sphinx_results:

                # por defecto los valores son válidos
                valid = not (messages[0] or messages[1])

                if not result:
                    logging.error("No results received from server.")
                    continue

                elif result["error"]:
                    logging.error("Search error (server %s): %s" % (server, result["error"]))
                    continue

                elif result["warning"]:
                    valid = False # los resultados se usan, pero se marcan como inválidos para próximas veces
                    if result["warning"]!="query time exceeded max_query_time": # no loguea el caso más comun
                        logging.error("Warning on search (server %s): %s" % (server, result["warning"]))

                # almacena los ficheros cuando se recorre el resumen con filtros o más resultados
                must_store_files = (not main) or main[0][1]

                total = 0
                for index, r in enumerate(result["matches"]):

                    # calcula el subgrupo y el id del fichero
                    sg = str(r["attrs"]["g"])
                    isg = r["attrs"]["g"]
                    ntt = long(r["attrs"]["g"])>>32
                    fid = bin2hex(full_id_struct.pack(r["attrs"]["uri1"],r["attrs"]["uri2"],r["attrs"]["uri3"]))
                    src = get_src(sg)
                    rating = r["attrs"]["r"]
                    weight = r["weight"]
                    if main:
                        if self.request_mode == REQUEST_MODE_PER_GROUPS:
                            search_position = 1
                            count = r["attrs"]["@count"]
                            total += count
                        else:
                            search_position = asearch["l"][0]+index
                            count = 100000
                            total = result["total_found"]
                    else:
                        search_position = asearch["g"][sg]+1+index

                    if sg in query_subgroups:
                        query_subgroup = query_subgroups[sg]
                    else:
                        query_subgroup = query_subgroups[sg] = {"c": {}, "z":[0,100]}

                    # accede al grupo sólo si se van a almacenar ficheros
                    if must_store_files:
                        if sg in filters_subgroups:
                            filter_subgroup = filters_subgroups[sg]
                        else:
                            filter_subgroup = filters_subgroups[sg] = {"c":{}, "lv":{}, "h":[], "f":{}}
                            if isg>>32:
                                self.entities.add(isg>>32)
                        filter_subgroup_files = filter_subgroup["f"]

                        # si ha habido ficheros bloqueados, incrementa la posicion para mantener datos consistentes
                        if "df" in filter_subgroup:
                            search_position += filter_subgroup["df"].get(server, [0,0])[1]

                    # actualiza totales de grupos y subgrupos
                    if main:
                        if main[0][0]: # almacena en query_state
                            if server not in query_subgroup["c"] or count>query_subgroup["c"][server]:
                                query_subgroup["c"][server] = count
                                query_subgroup["z"][0] = max(query_subgroup["z"][0], r["attrs"]["zm"])
                                query_subgroup["z"][1] = min(query_subgroup["z"][1], r["attrs"]["zx"])

                        if main[0][1]: # almacena en filters_state
                            # estadisticas
                            if server not in filter_subgroup["c"] or count>filter_subgroup["c"][server]:
                                filter_subgroup["c"][server] = count

                    # almacena el fichero
                    if must_store_files:
                        if fid in filter_subgroup_files:
                            # ya se ha encontrado en otro servidor
                            if server != filter_subgroup_files[fid][0]:
                                prev_server = filter_subgroup_files[fid][0]
                                if int(server)>int(prev_server):
                                    filter_subgroup_files[fid][0] = server
                                    dup_server = prev_server
                                    filter_subgroup_files[fid][2] = self.filters_state["v"]
                                else:
                                    dup_server = server

                                if dup_server in self.filters_state["df"]:
                                    self.filters_state["df"][dup_server][0] += 1
                                else:
                                    self.filters_state["df"][dup_server] = [1,0]

                            # ya se ha encontrado en este servidor antes, probablemente por fallo de sphinx
                            elif filter_subgroup_files[fid][3]!=search_position:
                                filter_subgroup_files[fid][3] = search_position
                        else:
                            std_dev = round(self.proxy.sources_rating_standard_deviation[src], 3) if rating>=0 and src in self.proxy.sources_rating_standard_deviation and self.proxy.sources_rating_standard_deviation[src] else 0
                            if std_dev>0:
                                val = (rating-self.proxy.sources_rating_average[src]*1.5)/std_dev
                                val = max(-500, min(500, val))
                            else:
                                val = 0
                                rating = 0.5 if rating==-1 else 1.1 if rating==-2 else rating

                            normalized_rating = self.proxy.sources_weights[src]*(1./(1+exp(-val)) if std_dev else rating)

                            # para ordenar, se utiliza la función de orden
                            if self.order[2]:
                                normalized_weight = self.order[2](r, weight, rating, normalized_rating)
                            else:
                                normalized_weight = weight*normalized_rating
                            heappush(filter_subgroup["h"], (-normalized_weight, fid))
                            filter_subgroup_files[fid] = [server, r["id"], self.filters_state["v"], search_position]

                    # actualiza el último registro obtenido del resumen para el grupo en el servidor
                    if main and main[0][1]:
                        if self.request_mode == REQUEST_MODE_PER_GROUPS:
                            filter_subgroup["lv"][server] = max(1,filter_subgroup["lv"].get(server,0))
                        else:
                            self.filters_state["lv"][server] = max(search_position, self.filters_state["lv"].get(server,0))

                # totales absolutos
                if main:
                    self.has_changes = True
                    # recorre la informacion de palabras buscadas
                    if "words" in result and result["words"] and self.canonical_parts:
                        words = result["words"]

                        # busqueda canonica
                        word_list = [fix_sphinx_result(word["word"]) for word in words]
                        if not "ct" in self.query_state:

                            word_positions = {word:position for word,position in self.seen_words.iteritems() if position>=0} # evita palabras bloqueadas
                            new_word_list = [""]*len(word_positions)

                            for word in word_list:
                                if word in word_positions:
                                    new_word_list[word_positions[word]] = word
                                    del word_positions[word]

                            for word, position in word_positions.iteritems():
                                new_word_list[position] = min((3*abs(len(word)-len(aword))+sum(1 for w1,w2 in zip(word,aword) if w1!=w2),aword) for aword in word_list)[1]

                            word_list = new_word_list

                            self.query_state["ct"] = u"_".join(self.canonical_parts).format(*word_list)

                        # obtiene el total del numero de resultados con la palabra
                        if self.special_search:
                            word = word_list[0]
                            prefix = word[0].upper()

                            # No lo hace cuando la palabra es una palabra especial de filtro
                            if not (word.isdecimal() or (prefix==FILTER_PREFIX_SOURCE) or
                                    (prefix==FILTER_PREFIX_CONTENT_TYPE and word[1:] in CONTENTS_CATEGORY) or
                                    (prefix==FILTER_PREFIX_SOURCE_GROUP and word[1:] in ['torrent','download','streaming','p2p']) or
                                    (prefix==FILTER_PREFIX_TAGS and word[1:] in ALL_TAGS) or
                                    (prefix==FILTER_PREFIX_FORMAT and word[1:] in ALL_FORMATS) or
                                    (prefix==FILTER_PREFIX_YEAR and word[1:].isdecimal()) or
                                    (prefix==FILTER_PREFIX_SEASON and len(word)==3 and word[1:].isdecimal()) or
                                    (prefix==FILTER_PREFIX_EPISODE and len(word)==3 and word[1:].isdecimal())):

                                total_words = words[0]["docs"]
                                if total_words>total:
                                    total = total_words

                    if main[0][0]: # almacena en query_state

                        # actualiza el numero de ficheros en el servidor
                        if total >= self.query_state["c"].get(server,0):
                            self.query_state["c"][server] = total
                        self.query_state["t"][server] = result["time"]
                        self.query_state["d"][server]=time()

                        # actualiza informacion sobre fiabilidad de los datos
                        if valid and server in self.query_state["i"]:
                            self.query_state["i"].remove(server)
                        elif not valid and server not in self.query_state["i"]:
                            self.query_state["i"].append(server)

                    if main[0][1]: # almacena en filters_state
                        # actualiza el numero de ficheros en el servidor
                        if total >= self.filters_state["c"].get(server,0):
                            self.filters_state["c"][server] = total
                        self.filters_state["t"][server] = result["time"]
                        self.filters_state["d"][server] = time()


                        # actualiza informacion sobre fiabilidad de los datos
                        if valid and server in self.filters_state["i"]:
                            self.filters_state["i"].remove(server)
                        elif not valid and server not in self.filters_state["i"]:
                            self.filters_state["i"].append(server)

                    main = main[1:]

                # ha recorrido más resultados
                elif result["matches"]:
                    self.has_changes = True

                    if self.request_mode == REQUEST_MODE_PER_SERVER:
                        self.filters_state["lv"][server] = max(search_position, self.filters_state["lv"].get(server,0))
                    else:
                        filter_subgroup["lv"][server] = max(search_position,filter_subgroup["lv"].get(server,0))

            self.access.release()

        if self.has_changes:
            # actualiza numero de reintentos en ambos estados
            if any_query_main: self.query_state["rt"]+=1
            if any_filters_main: self.filters_state["rt"]+=1
            self.access.acquire(True)
            self.save_state(False if len(allsearches["_pt"])==0 else None if timeout==0 or not fallback_timeouts else sum(fallback_timeouts))
            self.has_changes = False
            self.access.release()

            # avisa que la busqueda ya es usable
            if not self.usable.is_set():
                self.usable.set()

_escaper = re.compile(r"([=|\-!@~&/\\\)\(\"\^\$\=])")
def escape_string(text):
    return "%s"%_escaper.sub(r"\\\1", text).strip()

def get_group(sg):
    return str((long(sg)&0xF0000000L)>>28)

def get_group2(sg):
    return str((long(sg)&0xFFFF000L)>>12)

def get_ct(sg):
    return (int(sg)&0xF0000000L)>>28

def get_src(sg):
    return (int(sg)&0xFFFF000L)>>12

full_id_struct = Struct("III")
part_id_struct = Struct(">Q")


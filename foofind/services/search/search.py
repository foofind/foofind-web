# -*- coding: utf-8 -*-
from sphinxservice import *
from collections import defaultdict
from itertools import groupby
import re, sys

from foofind.utils.splitter import split_file, SEPPER
from foofind.utils.content_types import *
from foofind.utils import bin2hex, logging, hex2url, u
from foofind.utils.filepredictor import ALL_TAGS, ALL_FORMATS

FIELD_NAMES = {"T": "@(fn,md) ", "F":"@fil ", "N": "@ntt "}

SEASON_EPISODE_SEARCHER = re.compile(r"^(?P<s>\d{1,2})x(?P<e>\d{1,2})$")
SEASON_EPISODE_SEARCHER2 = re.compile(r"^s(?P<s>\d{1,2})(\W*e(?P<e>\d{1,2}))?$")
BLEND_CHARS = frozenset(r"+&-@!$%?#")
NGRAM_CHARS = frozenset(unichr(i) for i in xrange(0x3000, 0x2FA1F)) if sys.maxunicode>2**16 else frozenset()

NON_TEXT_CHARS = frozenset(SEPPER.union(set(u"'½²º³ª\u07e6\u12a2\u1233\u179c\u179fµ")).difference(BLEND_CHARS))
WORD_SEARCH_MIN_LEN = 2
BLOCKED_WORDS = frozenset(["www"])

START_SEEN_WORDS = {word:-1 for word in BLOCKED_WORDS}

# arregla problemas de codificación de la version de sphinx
SPHINX_WRONG_RANGE = re.compile("\xf0([\x80-\x8f])")
def fixer(char):
    return chr(ord(char.group(1))+96)

def fix_sphinx_result(word):
    return u(SPHINX_WRONG_RANGE.sub(fixer, word))

_escaper = re.compile(r"([=|\-!@~&/\\\)\(\"\^\$\=])")
def escape_string(text):
    return "%s"%_escaper.sub(r"\\\1", text).strip()

def ngram_separator(x):
    return x if x in NGRAM_CHARS else False

class Search(object):
    def __init__(self, proxy, original_text, filters={}, start=True, group=True, no_group=False, limits=None, order=None, dynamic_tags=None):
        self.proxy = proxy
        self.stats = None
        self.computable = True
        self.canonical_parts = []
        self.canonical_words = None
        self.seen_words = START_SEEN_WORDS.copy()
        self.grouping = (group, no_group)
        self.limits = limits or (0, 500, 10000, 2000000)

        # orden: rating para el peso, columnas para ordenar, funcion de ordenación y si se deben mezclar grupos en el orden
        self.order = order or (None, None, None)

        # normaliza texto de busqueda
        computable = True
        text = filter_tags = ntts = None
        options = []
        position = 0
        query_parts = []
        seen_filters = set()

        text = original_text.strip().lower()

        for mode, not_mode, has_ngrams, part_words in self.parse_query(text):
            # prefijo - para las busquedas negativas
            not_prefix = "-" if not_mode else ""
            words_count = len(part_words)

            if has_ngrams:
                first_word = "".join(part_words[0]).lower()
                all_words = " ".join("".join(part) for part in part_words).lower()
                if mode==True: # desactiva el modo palabra simple
                    mode="N"
            else:
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
                        tags_prefix = FILTER_PREFIX_TAGS
                        if first_word in ALL_TAGS:
                            tag_word = first_word
                        else:
                            tag_word = first_word[:-1] if first_word.endswith("s") else first_word+"s"
                            if not tag_word in ALL_TAGS:
                                tag_word = None

                        # si no es un tag, mira si está en los tags dinámicos
                        if not guess_mode and not tag_word and dynamic_tags:
                            tags_prefix = FILTER_PREFIX_DYNAMIC_TAGS
                            if first_word in dynamic_tags:
                                tag_word = first_word
                            else:
                                tag_word = first_word[:-1] if first_word.endswith("s") else first_word+"s"
                                if not tag_word in dynamic_tags:
                                    tag_word = None

                        if tag_word:
                            current_filter = tags_prefix.lower()+tag_word
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

                    # mira si se trata de un grupo de origenes
                    if mode and words_count==1:
                        if first_word in SOURCE_GROUPS:
                            source_group = first_word
                        else:
                            source_group = first_word[:-1] if first_word.endswith("s") else first_word+"s"
                            if not source_group in SOURCE_GROUPS:
                                source_group = None

                        if source_group:
                            current_filter = FILTER_PREFIX_SOURCE_GROUP.lower()+source_group
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
                                    self.canonical_parts.append(not_prefix+"("+source_group+")")

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
                if mode==True: # sin comillas ni parentesis ni ngramas
                    new_word = first_word not in self.seen_words
                    position_word = position if new_word else self.seen_words[first_word]

                    if position_word==-1: continue # ignora palabras bloquedas

                    valid_word = len(first_word)>=WORD_SEARCH_MIN_LEN # palabra no muy corta
                    query_parts.append(("T",not_prefix+escape_string(first_word)))
                    self.canonical_parts.append(not_prefix+(first_word.replace("{", "{{").replace("}","}}") if not valid_word else "{%d}"%position_word))
                    if valid_word and new_word:
                        self.seen_words[first_word] = position
                        position+=1

                else:
                    mask = []
                    for word_candidate in part_words:
                        for word in (word_candidate if isinstance(word_candidate, list) else [word_candidate]):
                            word = word.lower()
                            new_word = word not in self.seen_words

                            # las palabra cortas no vuelven en los resultados de busqueda y hay que ponerlas a mano
                            valid_word = len(word)>=WORD_SEARCH_MIN_LEN or word in NGRAM_CHARS # palabra no muy corta o ngrama

                            position_word = position if new_word else self.seen_words[word]

                            if position_word==-1: continue # ignora palabras bloquedas

                            if valid_word and new_word:
                                self.seen_words[word] = position
                                position+=1

                            # evita que las llaves de las palabras se confundan con placeholders
                            mask.append(word.replace("{", "{{").replace("}","}}") if not valid_word else "{%d}"%position_word)

                    if mode=="N":
                        query_parts.append(("T", not_prefix+escape_string(all_words)))
                        self.canonical_parts.append(not_prefix+"_".join(mask))
                    else:
                        query_parts.append(("T", not_prefix+"\""+escape_string(all_words)+"\""))
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

        self.text = text
        self.query = self.proxy.sphinx.build_query(self.text, self.filters, self.limits, self.grouping, self.order)

        if start and self.computable:
            self.proxy.sphinx.start_search(self.query)

    def parse_query(self, query):
        # inicializa variables
        acum = []                   # palabra actual
        all_acums = []              # lista de palabras a devolver
        yield_mode = False

        valid_acum = 0              # contador de caracteres validos, para evitar hacer consultas de una sola letra
        not_mode = False            # indica que esta parte de la consulta está en modo negativo
        quote_mode = False          # indica que esta parte de la consulta va en entre comillas
        tag_mode = False            # indica que esta parte de la consulta es un tag
        any_not_blend_char = False  # indica que alguna letra de la palabra no es un blend char
        any_ngram = False           # indica que alguna letra de la palabra es un n-grama
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
                any_ngram = any_ngram or ch in NGRAM_CHARS
                acum.append(ch)

            # si toca devolver resultado, lo hace
            if yield_mode:
                acum_result = "".join(acum) if acum and valid_acum else False

                # acumula palabras
                if acum_result:
                    if any_ngram:
                        all_acums.append(["".join(i[1]) for i in groupby(acum_result, ngram_separator)])
                    else:
                        all_acums.append(acum_result)
                    any_not_not_part = any_not_not_part or (not not_mode and (any_ngram or
                                                                            (any_not_blend_char and valid_acum>=WORD_SEARCH_MIN_LEN)))
                    del acum[:]
                    valid_acum = 0
                    any_not_blend_char = False

                # devuelve palabras
                if yield_mode!=" " and all_acums:
                    yield (yield_mode, not_mode, any_ngram, all_acums)
                    all_acums = []
                    any_ngram = not_mode = False

                yield_mode = False

        # si no se han devuelto partes no negativas, la consulta no es computable
        if not any_not_not_part:
            self.computable = False

    def get_results(self, timeouts, last_items=[], skip=None, min_results=5, max_results=10, hard_limit=10000, extra_browse=None, weight_processor=None, tree_visitor=None, restart_if_skip=False):
        if self.computable:
            results, self.stats = self.proxy.sphinx.get_results(self.query, timeouts, last_items, skip, min_results, max_results, hard_limit, extra_browse, weight_processor, tree_visitor)

            # no search results available for this search, if has skipped and user wants to, start search with grouping information included
            if skip and restart_if_skip and self.stats == Sphinx.EMPTY_STATS:
                self.query = self.proxy.sphinx.build_query(self.text, self.filters, self.limits, (True, True), self.order)
                self.proxy.sphinx.start_search(self.query)
                results, self.stats = self.proxy.sphinx.get_results(self.query, timeouts, last_items, skip, min_results, max_results, hard_limit, extra_browse, weight_processor, tree_visitor)

            self._generate_canonical_query()
            return results
        else:
            self.stats = Sphinx.EMPTY_STATS
            self.stats["s"] = self.stats["end"] = self.stats["total_sure"] = True
            return []

    def get_group_count(self, mask):
        return self.proxy.sphinx.get_group_count(self.query, mask)

    def get_search_info(self):
        if self.computable:
            return self.proxy.sphinx.get_search_info(self.query)
        else:
            return {"computable":False}

    def _generate_canonical_query(self):

        # no tiene informacion para calcular la busqueda canonica, usa la busqueda original
        if not self.stats.get("ct", None):
            return

        # calcular consulta canonica
        word_list = [word.decode("utf-8") for word in self.stats["ct"]]
        word_positions = {word:position for word,position in self.seen_words.iteritems() if position>=0} # evita palabras bloqueadas
        new_word_list = [""]*len(word_positions)

        for word in word_list:
            if word in word_positions:
                new_word_list[word_positions[word]] = word
                del word_positions[word]

        for word, position in word_positions.iteritems():
            new_word_list[position] = min((3*abs(len(word)-len(aword))+sum(1 for w1,w2 in zip(word,aword) if w1!=w2),aword) for aword in word_list)[1]

        self.stats["ct"] = u"_".join(self.canonical_parts).format(*new_word_list)

    def get_stats(self):
        return self.stats

    def block_files(self, ids):
        return None


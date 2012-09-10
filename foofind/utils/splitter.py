#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from itertools import izip, groupby, chain
from collections import defaultdict, Counter
from operator import itemgetter
from foofind.utils.content_types import *

split = re.compile(r"(?:[^\w\']|\_)|(?:[^\_\W]|\')+", re.UNICODE)
propercaseparser = re.compile('((?=[A-Z0-9][a-z])|(?<=[a-z])(?=[A-Z0-9]))', re.UNICODE)

wsepsws = {" ":0.9, "_":0.6, ".":0.2, "-":0.1, "&":0.01, "(":-0.1, "[":-0.1, "{":-0.1, "}":-0.1, "]":-0.1, ")":-0.1}
mds = frozenset({'audio:artist','audio:composer','archive:folders', 'archive:files','video:keywords','audio:album', 'audio:title', 'document:title', 'torrent:name', 'image:title', 'video:title', 'torrent:filepaths', "video:series", "video:season", "video:episode", "video:lang", "video:subs", "video:director", "video:genre", "video:year", "video:cast", "video:quality", "book:title" })

sepre = re.compile(r"[^\w\']", re.UNICODE)
SEPPER = frozenset(unichr(i) for i in xrange(0x10000) if sepre.match(unichr(i)) or unichr(i)=='_')

def isallnumeric(l):
    return all(s.isdigit() for s in l)

def grouper(x): return x if x in SEPPER else False

empty_join = "".join

def split_phrase(phrase, filename=False):
    #split between words and separators
    parts = [i[0] or empty_join(i[1]) for i in groupby(phrase, grouper)]
    
    len_parts = len(parts)
    
    ct = None
    #ignore small phrases
    if filename and len_parts>2:
        #ignore extensions
        i = len_parts-1
        while i>0 and parts[i-1]=='.' and parts[i].lower() in EXTENSIONS:
            ct = EXTENSIONS[parts[i].lower()]
            del(parts[i-1:])
            len_parts-=2
            i-=2
            
    #ignore small phrases
    if len_parts<=3: return [propercaseparser.sub(" ", w).lstrip() for w in parts if len(w)>1 or w not in SEPPER], ct
    
    #identify separators
    seppos = tuple(w if len(w)==1 and w in SEPPER else False for w in parts)

    # valora los separadores de palabras y de frase
    comparer = izip(seppos[:-2], seppos[1:-1], seppos[2:])
    wseps = {comp1:(2 if not comp0 and not comp2 else -1 if comp0==comp2 else 1) + (wsepsws[comp1] if comp1 in wsepsws else 0) for comp0, comp1, comp2 in comparer}
    pseps = frozenset(comp1 for comp0, comp1, comp2 in comparer if comp0!=False and comp0==comp2)

    # obtiene el separador principal
    wsep = max(wseps,key=wseps.get) if wseps else " "

    ret = ([parts[0]] if not parts[0] in SEPPER else []) + [p1 if p1 not in wseps else
            " " if p1 == wsep or (p2==wsep and (p1 in [",", "&"] or (p1 == "." and not p0.isdigit()))) else
            ("" if len(p2)==3 else p1) if p1 in [".",","] and p0.isdigit() and p2.isdigit() else
            "|" for p0, p1, p2 in izip(parts[:-2], parts[1:-1], parts[2:])] + ([parts[-1]] if not parts[-1] in SEPPER else [])
    return [propercaseparser.sub(" ", ph).lstrip() for ph in empty_join(ret).split("|") if len(ph)>1 and not ph.isdigit()], ct

def split_file(f):
    res, cs = zip(*[split_phrase(fn["n"], True) for fn in f["fn"].itervalues()])
    cts = Counter(cs)
    res = list(chain(*res)) + list(chain(*[split_phrase(value)[0] for key,value in f["md"].iteritems() if key in mds and isinstance(value, basestring)]))
    sumcts = sum(cts.values())*1.0
    return res, {k:v/sumcts for k,v in cts.iteritems()}

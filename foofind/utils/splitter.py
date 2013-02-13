#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re, logging
from itertools import izip, groupby, chain
from collections import defaultdict, Counter
from operator import itemgetter
from foofind.utils.content_types import *
from unicodedata import normalize

split = re.compile(r"(?:[^\w\']|\_)|(?:[^\_\W]|\')+", re.UNICODE)
wsepsws = {" ":2, "_":1, "+":1, ".":1, "-":0.5}
psepsws = {"-":2, "~":1, "\"": 0.5}

sepre = re.compile(r"[^\w\']", re.UNICODE)
SEPPER = frozenset(unichr(i) for i in xrange(0x10000) if not (0xD7FF<i<0xE000) and sepre.match(unichr(i)) or unichr(i)=='_')

empty_join = "".join
space_join = " ".join

proper_case_options = {"u^": ("U",  False), "uL": ("U",  True),
                       "uD2":("U",  True),                        # mayus lleva a minus desde nada, minus o numero largo
                       "lU": ("L",  False), "lL": ("L",  False),  # minus lleva a minus desde mayus o minus
                       "d^": ("D1", False),"dL":  ("D1", True),   # num lleva a num1 desde nada o minus
                       "dD1":("D2", False),"dD2": ("D2", False),  # num lleva a num2 desde num1 o num2
                       "$L": ("$",  True),  "$D2":("$",  True),   # fin lleva a fin desde minus o num2
                      }

def proper_case(expr):
    ''' Separa palabras por mayusculas o numeros de más de 2 digitos. '''
    mode = "^"
    start = 0
    result = []
    # recorre caracteres
    for pos, char in enumerate(expr):
        char_info = "d" if char.isdigit() else "l" if char.islower() else "u" if char.isupper() else False
        # avanza si es caracter valido y existe la combinación
        if char_info and char_info+mode in proper_case_options:
            mode, phrase_break = proper_case_options[char_info+mode]
            if phrase_break: # si la combinación lo indica, añade palabra a resultados
                result.append(expr[start:pos])
                start = pos
        else:
            return expr
    # comprueba el final
    if not "$"+mode in proper_case_options:
        return expr

    result.append(expr[start:])
    return space_join(result)

def group_parts(phrase):
    last_char = None
    acum = []
    for char in phrase:
        if char in SEPPER:
            if acum:
                yield empty_join(acum)
                last_char = None
                acum = []
            if last_char!=char:
                last_char = char
                yield char
        else:
            acum.append(char)
    if acum:
        yield empty_join(acum)

def slugify(text):
    text = empty_join(" " if c in SEPPER else c for c in text.lower())
    try:
        text = empty_join(c for c in normalize('NFKD', unicode(text)) if c==" " or c not in SEPPER)
    except:
        logging.warn("Problem slugifing text.")
    return text

def split_phrase(phrase, filename=False):
    #split between words and separators
    parts = list(group_parts(phrase))
    len_parts = len(parts)

    #ignore small phrases
    if filename and len_parts>2:
        #ignore extensions
        i = len_parts-1
        while i>0 and parts[i-1]=='.' and parts[i].lower() in EXTENSIONS:
            del(parts[i-1:])
            len_parts-=2
            i-=2

    # split words with numbers
    parts = [proper_case(part) for part in parts]

    #ignore small phrases
    if len_parts<=3:
        return [empty_join(parts)]

    #identify separators
    seppos = tuple(w if len(w)==1 and w in SEPPER else False for w in parts)

    # valora los separadores de palabras y de frase

    # obtiene el separador principal
    wseps = defaultdict(int)
    comparer = zip(seppos[:-2], seppos[1:-1], seppos[2:])
    for comp0, comp1, comp2 in comparer:
        if comp1!=False and comp1 in wsepsws:
            wseps[comp1] += 0.2 if not comp0 and not comp2 else -0.1 if comp0==comp2 else 0.1
    for comp1, w in wseps.items():
        wseps[comp1] = w*wsepsws[comp1]

    wsep = max(wseps,key=wseps.get) if wseps else " "

    # obtiene el separador de frase principal
    pseps = defaultdict(int)
    for comp0, comp1, comp2 in comparer:
        if comp1!=False and comp1!=wsep and comp1 in psepsws:
            pseps[comp1] += 0.5 if comp0==comp1==wsep else 0.1 if comp0==wsep or comp2==wsep else 0
    for comp1, w in pseps.items():
        pseps[comp1] = w*psepsws[comp1]

    psep = max(pseps,key=pseps.get) if pseps else None

    ret = ([parts[0]] if not parts[0] in SEPPER else []) + [p1 if p1 not in SEPPER or (p0!=wsep and p1!=wsep and p2!=wsep) else
            " " if p1 == wsep or (p2==wsep and (p1 in [",", "&"] or (p1 == "." and not p0.isdigit()))) else
            ("" if len(p2)==3 else p1) if p1 in [".",","] and p0.isdigit() and p2.isdigit() else
            "|" for p0, p1, p2 in izip(parts[:-2], parts[1:-1], parts[2:])] + ([parts[-1]] if not parts[-1] in SEPPER else [])

    return [ph for ph in empty_join(ret).split("|") if len(ph)>1 and not ph.isdigit()]

def split_file(f):
    res = [split_phrase(fn["n"], True) for fn in f["fn"].itervalues()]
    res = list(chain(*res)) + list(chain(*[split_phrase(value) for key,value in f["md"].iteritems() if key in GOOD_MDS and isinstance(value, basestring)]))
    return res

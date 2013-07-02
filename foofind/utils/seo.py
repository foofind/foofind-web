#!/usr/bin/env python
# -*- coding: utf-8 -*-

import itertools
from . import u
from .splitter import SEPPER, EXTENSIONS
from unicodedata import normalize

NONSEPPER = frozenset(unichr(i) for i in xrange(0x10000) if unichr(i) not in SEPPER)

seoize_table = {c:normalize('NFKC', u"".join(c2 for c2 in normalize('NFKD', c) if c2 in NONSEPPER))
                    for c in NONSEPPER}
seoize_table = {ord(c):to for c, to in seoize_table.iteritems() if to and to!=c}

def seoize_text(x, separator="-", is_url=False, max_length=None, min_length=20):
    # normaliza la cadena y la pasa a minusculas

    ret = u(x).lower().translate(seoize_table)

    # quita extensiones del final
    if is_url:
        ext_pos = ret.rfind(".")
        while ext_pos>0 and ret[ext_pos+1:] in EXTENSIONS:
            ret = ret[:ext_pos]
            ext_pos = ret.rfind(".")

        if max_length==None:
            max_length = 50

    try:
        # Iterador con caracteres de ret que son separadores
        gen = (SEPPER.intersection(ret)).__iter__()
        sc = gen.next()
        for sn in gen:
            ret = ret.replace(sn, sc)

        if max_length:
            parts = []
            length = 0
            for part in ret.split(sc):
                if part:
                    this_length = len(part)
                    if length+this_length<max_length:
                        length+=this_length+1
                        parts.append(part)
                    else:
                        if length<min_length:
                            parts.append(part)
                        break
            return separator.join(parts)[:max_length]
        else:
            return separator.join(part for part in ret.split(sc) if part)

    except StopIteration as e:
        # Caso excepcional: ningún separador encontrado
        return ret[:max_length] if max_length else ret

def is_filename_seoized(filename):
    return not any(i in SEPPER for i in filename.replace("-",""))


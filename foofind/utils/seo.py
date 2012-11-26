#!/usr/bin/env python
# -*- coding: utf-8 -*-

import itertools

from .splitter import SEPPER

def seoize_filename(x):
    try:
        # Iterador con caracteres de x que son separadores
        gen = itertools.ifilter(SEPPER.__contains__, x)
        sc = gen.next()
        for sn in gen:
            x = x.replace(sn, sc)
        return "-".join(i for i in x.split(sc) if i).lower()
    except StopIteration:
        # Caso excepcional: ningún separador encontrado
        return x.lower()

def is_filename_seoized(filename):
    return not any(i in SEPPER for i in filename.replace("-",""))


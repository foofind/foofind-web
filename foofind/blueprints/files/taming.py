# -*- coding: utf-8 -*-
from foofind.utils.async import async_generator
from foofind.utils.content_types import *

'''
    Gestiona todo lo relacionado con el taming (suggest, didyoumean, etc)
'''
def taming_generate_tags(res, query, mean):
    '''
    Genera los tags
    '''
    querylen = len(query)
    tag = res[2][querylen+1:]
    return (tag[querylen+1:] if tag.startswith(query+" ") else tag, 100*max(min(1.25, res[0]/mean), 0.75))

def taming_tags(query, tamingWeight):
    try:
        tags = taming.tameText(
            text=query+" ",
            weights=tamingWeight,
            limit=20,
            maxdist=4,
            minsimil=0.7,
            dym=0
            )
        if tags:
            mean = (tags[0][0] + tags[-1][0])/2
            tags = map(lambda res: taming_generate_tags(res, query, mean), tags)
            tags.sort()
        else:
            tags = ()
    except BaseException as e:
        logging.exception("Error getting search related tags.")
        tags = ()
    return tags

def taming_dym(query, tamingWeight):
    try:
        suggest = taming.tameText(
            text=query,
            weights=tamingWeight,
            limit=1,
            maxdist=3,
            minsimil=0.8,
            dym=1,
            rel=0
            )
        didyoumean = None
        if suggest and suggest[0][2]!=query:
            didyoumean = suggest[0][2]
    except BaseException as e:
        logging.exception("Error getting did you mean suggestion.")
    return didyoumean

@async_generator(500)
def taming_search(query, ct):
    '''
    Obtiene los resultados que se muestran en el taming
    '''
    tamingWeight = {"c":1, "lang":200}
    if ct in CONTENTS_CATEGORY:
        for cti in CONTENTS_CATEGORY[ct]:
            tamingWeight[TAMING_TYPES[cti]] = 200

    yield taming_tags(query, tamingWeight)
    yield taming_dym(query, tamingWeight)

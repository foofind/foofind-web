# -*- coding: utf-8 -*-
'''
    Funciones utilizadas en traducciones
'''
import re, random
from foofind.translations.samples_values import samples
from flask.ext.babel import gettext as _
from foofind.utils import multipartition

_strarg = re.compile('(%\(([^\)]+)\)([s|d]))')
def fix_lang_values(entry, sample=False):
    '''
    Si la traduccion contiene campos de valores los sustituimos por ______[X] y ponemos un ejemplo de uso,
    además se eliminan los saltos de linea
    '''
    result=_strarg.finditer(entry.msgstr)
    subs=dict()
    # se cargan los ejemplos si es necesario
    if entry.msgid in samples:
        subs=samples[entry.msgid]

    # para cada valor encontrado se sustituye por _____[X]
    for i,item in enumerate(result):
        entry.msgstr=entry.msgstr.replace(item.group(1),"_____["+str(i+1)+"]")
        # para los ejemplos numericos se utiliza uno aleatorio
        if item.group(3)=="d":
            subs[item.group(2)]=random.randint(2,10)

    if sample:
        if subs!={}:
            return (entry.msgid,(entry.msgstr,_(entry.msgid,**subs)))
        else:
            return (entry.msgid,(entry.msgstr,False))

    # se sustituyen los saltos de linea html y se devuelve todo
    return (entry.msgid,entry.msgstr.replace("<br>","\n").replace("<br />","\n").replace("<br/>","\n") if "<br" in entry.msgstr else entry.msgstr)

def unfix_lang_values(entry, base_entry):
    '''
    Convierte msgstrs de traducción de formato ______[X] a %(n)m.
    '''
    original = dict(enumerate(i.group(0) for i in _strarg.finditer(base_entry)))

    lf = "_____["
    if not lf in entry:
        return None if original else entry # No se han usado los argumentos

    tr = []
    lpp = False
    for part in multipartition(entry, (lf,)):
        if part == lf:
            lpp = True
            continue
        if lpp:
            try:
                index = int(part[:part.find("]")])
            except ValueError:
                return None # Índice inválido
            try:
                tr.append(original.pop(index-1))
            except KeyError:
                return None # Índice repetido
            part = part[part.find("]")+1:]
        tr.append(part)
        lpp = False
    if original:
        return None # No se han usado todos los argumentos
    return "".join(tr)

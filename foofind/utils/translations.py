# -*- coding: utf-8 -*-
'''
    Funciones utilizadas en traducciones
'''
import re, random
from foofind.translations.samples_values import samples
from flask.ext.babel import gettext as _

_strarg = re.compile('(%\(([^\)]+)\)([s|d]))')
def fix_lang_values(entry, sample=False):
    '''
    Si la traduccion contiene campos de valores los sustituimos por _____[X] y ponemos un ejemplo de uso,
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
    Convierte msgstrs de traducción de formato _____[X] a %(n)m.
    
    >>> p = "This is a test of text %(first)s y %(second)s."
    >>> unfix_lang_values("Esto es una prueba de texto _____[1] y _____[2].", p)
    'Esto es una prueba de texto %(first)s y %(second)s.'
    >>> unfix_lang_values("Esto es una prueba de texto _____[1] y _____[1].", p)
    'Esto es una prueba de texto %(first)s y %(second)s.'
    >>> unfix_lang_values("Esto es una prueba de texto _____[1], _____[1] y _____[1].", p)
    'Esto es una prueba de texto _____[1], _____[1] y _____[1].'
    >>> unfix_lang_values("Esto es una prueba de texto _____[a].", p)
    'Esto es una prueba de texto _____[a].'
    '''
    original = dict(enumerate(i.group(0) for i in _strarg.finditer(base_entry)))
    
    lf = "_____["
    if not lf in entry:
        return None if original else entry # No se han usado los argumentos
    
    tr = []
    for part in entry.split(lf):
        if tr:
            try:
                index, text = part.split("]", 1)
                tr.extend((original.pop(int(index)-1), text))
            except ValueError: # No se encuentra ']' o índice no numérico
                tr.extend((lf, part))
            except KeyError: # Índice repetido, devolvemos sin procesar
                if len(original) == 1: # Sólo queda un posible valor
                    index = original.iterkeys().next()
                    tr.extend((original.pop(index), text))
                else:
                    return entry
        else:
            tr.append(part)
    return "".join(tr)

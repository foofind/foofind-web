#!/usr/bin/env python
# -*- coding: utf-8 -*-
from jinja2._markupsafe._constants import HTML_ENTITIES
import re

HTML_SEPS = re.compile("([<>&])")
def clean_html(text):
    # cadena vacia o sin separadores
    if "<" not in text and ">" not in text and "&" not in text:
        return text.strip()

    # mete en la cola las posiciones de los separadores que se encuentran
    parts = HTML_SEPS.split(text)
    if len(parts)==1:
        return parts[0]

    # inicializa valores
    in_tag = False
    results = []
    next_offset = 0

    # recorre partes y separadores
    part_count = len(parts)
    parts.reverse()
    while part_count>1:
        part_count-=2
        part = parts.pop()
        sep = parts.pop()
        next_part = parts[-1]

        # valores por defecto
        suffix = None
        add_text = not in_tag
        offset = next_offset

        # ampersand
        if sep=="&":
            if next_part[:3]=="lt;":
                vsep="<"
                next_offset=3
            elif next_part[:3]=="gt;":
                vsep=">"
                next_offset=3
            else:
                vsep="&"
                comma = next_part[:10].find(";")
                if comma==-1 or (next_part[:comma] not in HTML_ENTITIES and next_part[0]!="#"):
                    suffix = "&amp;"
                else:
                    suffix = "&"
        else:
            vsep = sep

        if vsep=="<": # comienzo tag
            in_tag = True
        elif vsep==">": # final tag
            in_tag = False

        # añade el texto a los resultados y el sufijo si toca
        if add_text:
            results.append(part[offset:])
            if suffix:
                results.append(suffix)

    # parte final, si hay
    if part_count:
        results.append(parts[0][next_offset:])

    # devuelve resultado final
    return "".join(results).strip()

if __name__=="__main__":
    from flask import Markup
    import timeit
    texts = ["", "short test", "<a href='http://foofind.is'>foofind</a>", "<br />", "<invalid", u"unicode\u0023",
                "&amp; &amp;", "& & &", "num &#34;", "my &quot; ", "& & &", "num &#34;", "my &quot; ", "&&&", "<&a>", "<&a>asd</a>&", "&&", "\n\n"]

    def t1():
        value = text
        multiline = "\n" in value
        if multiline:
            value = value.replace("|"," ||").replace("\n"," |n")
        if "&" in value or "<" in value:
            value = Markup(Markup(value).unescape()).striptags()
        # soporte multilinea
        if multiline:
            values = value.split(" |n")
            # strip de la lista
            start = 0
            end = len(values)-1
            while values and start<end and not values[start].strip(): start+=1
            while values and end>start and not values[end].strip(): end-=1
            if values and start<end:
                value = "\n".join(value.replace(" ||", "|") for value in values[start:end+1])
            else:
                value = ""
        return value

    def t2():
        value = text
        return clean_html(value)

    for text in texts:
        print
        print repr(text),
        t1r, t2r = t1(), t2()
        if t1r==t2r:
            print "OK"
        else:
            print "ERR"
            maxlen = max(len(t1r),len(t2r))
            t2r = t2r.ljust(maxlen,"*")
            t1r = t1r.ljust(maxlen,"*")
            for i in xrange(maxlen):
                if t1r[i]!=t2r[i]:
                    print "posicion ", i
                    start, end = i-20, i+20
                    if start<0: start = 0
                    print repr(t1r[start:end])
                    print repr(t2r[start:end])
                    break
        print timeit.timeit(t1, number=1000)
        print timeit.timeit(t2, number=1000)

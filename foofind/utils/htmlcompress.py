# -*- coding: utf-8 -*-

from jinja2.ext import Extension
from jinja2.lexer import Token
from jinja2 import TemplateSyntaxError

import re

class HTMLCompress(Extension):
    '''
    Compresión de HTML para jinja2.

    Jinja2 precompila las plantillas, de modo que el coste de minificar el HTML es nulo.
    '''
    intoken = re.compile(r'>\s+<', re.MULTILINE)
    pretoken = re.compile(r'[\n\t\s]+<', re.MULTILINE)

    def __init__(self, *args, **kwargs):
        Extension.__init__(self, *args, **kwargs)
        self.lastchar = None

    def filter_stream(self, stream):
        for token in stream:
            if token.type != 'data':
                yield token
                continue
            data = token.value
            #if self.lastchar == ">": data = self.pretoken.sub("<", data)
            #if data.strip(): self.lastchar = data.strip()[-1]
            yield Token(1, 'data', self.intoken.sub('><', data))

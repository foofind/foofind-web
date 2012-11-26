# The MIT License
#
# Copyright (c) 2008
# Shibzoukhov Zaur Moukhadinovich
# szport@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

NoneType = type(None)
from types import BuiltinFunctionType, BuiltinMethodType, FunctionType, MethodType

from sys import version_info
py_version = version_info[0]*10 + version_info[1]
if py_version >= 30:
    simpleTypes = (NoneType, int, str, bool, float, bytes)
else:
    simpleTypes = (NoneType, int, long, str, unicode, bool, float, bytes)

constants = ((), frozenset())

class Cacher(object):
    #
    def __init__(self):
        self.objects_cache = {}
        self.objects_info = {}

method_cache = {}
def cache_method(name):
    def func(m, name=name):
        method_cache[name] = m
        return m
    return func

def visit(self, o):
    if type(o) in simpleTypes:
        return

    if o in constants:
        return

    oId = id(o)
    if oId in self.objects_cache:
        info = self.objects_info[oId]
        if info == 0:
            self.objects_info[oId] = 1
    else:
        self.objects_cache[oId] = o
        self.objects_info[oId] = 0
        method = method_cache.get(o.__class__.__name__, visit_object)
        method(self, o)
#
@cache_method('list')
def visit_list(self, o):
    for item in o:
        visit(self, item)
#
@cache_method('set')
def visit_set(self, o):
    for item in o:
        visit(self, item)
#
@cache_method('frosenset')
def visit_frozenset(self, o):
    for item in o:
        visit(self, item)
#
@cache_method('tuple')
def visit_tuple(self, o):
    for item in o:
        visit(self, item)
#
@cache_method('object')
def visit_object(self, o):
    return
#
@cache_method('type')
def visit_type(self, o):
    metatype = o.__class__
    if metatype == type:
        return
    else:
        return
#
@cache_method('dict')
def visit_dict(self, o):
    for key,item in o.items():
        visit(self, key)
        visit(self, item)

@cache_method('property')
def visit_property(self, o):
    for f in (o.fget, o.fset, o.fdel, o.__doc__):
        if f is not None:
            visit(self, f)

@cache_method('function')
def visit_function(self, o):
    return
#
@cache_method('method')
def visit_method(self, o):
    return visit(self, o.__self__)
#
@cache_method('builtin_function_or_method')
def visit_builtin_function_or_method(self, o):
    return

@cache_method('object')
def visit_object(self, o):
    if isinstance(o, type):
        return visit_type(self, o)

    reduce = getattr(o, '__reduce__', None)
    if reduce:
        state = reduce()
        return with_reduce(self, state)
    else:
        newname = o.__class__.__name__

        newargs = None
        getnewargs = getattr(o, '__getnewargs__', None)
        if getnewargs:
            newargs = getnewargs()

        state = None
        getstate = getattr(o, '__getstate__', None)
        if getstate:
            state = getstate()
        else:
            state = getattr(o, '__dict__', None)
            if state is None:
                state = {}
                for name in o.__slots__:
                    value = getattr(o, name, null)
                    if value is not null:
                        state[name] = value
        return without_reduce(self, newargs, state)
#
def with_reduce(self, state):
    visit(self, state[0])
    n = len(state)
    if n > 1:
        if state[1]:
            for item in state[1]:
                visit(self, item)
        if n > 2:
            if state[2]:
                for k, v in state[2].items():
                    visit(self, v)
            if n > 3:
                if state[3]:
                    for v in state[3]:
                        visit(self, v)
                if n > 4:
                    if state[4]:
                        for k, v in state[4].items():
                            visit(self, k)
                            visit(self, v)
#
def without_reduce(self, args, state):
    if args:
        for item in args:
            visit(self, item)
    if state:
        visit(self, state)

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


try:
    from copyreg import dispatch_table
except:
    from copy_reg import dispatch_table

from . import dumpcache

import sys
import os

__all__ = ['dumps', 'currentScope']

null = object()

NoneType = type(None)
from types import BuiltinFunctionType, BuiltinMethodType, FunctionType, MethodType

from sys import version_info
py_version = version_info[0]*10 + version_info[1]
if py_version >= 30:
    simpleTypes = (NoneType, int, str, bool, float, bytes)
else:
    simpleTypes = (NoneType, int, long, str, unicode, bool, float, bytes)

def currentScope(given=None, level=1):
    frame = sys._getframe(level)
    scope = dict(frame.f_globals)
    scope.update(frame.f_locals)
    if given:
        scope.update(given)
    return scope

def _sortkey1(item):
    key, value = item
    return type(key).__name__, key, value

def _sortkey2(item):
    return type(item[0]).__name__

def _safe_sorted(items):
    try:
        return sorted(items)
    except TypeError:
        try:
            return sorted(items, key=_sortkey1)
        except TypeError:
            return sorted(items, key=_sortkey2)

# cache for visit functions
method_cache = {}

def cache_method(tp, repr_func=None):
    """Decorator for caching methods"""
    if repr_func is None:
        def func(m, type=tp):
            if isinstance(tp, list):
                for x in tp:
                    method_cache[x] = m
            else:
                method_cache[tp] = m
            return m
        return func
    else:
        method_cache[tp] = repr_func

for tp in simpleTypes:
    cache_method(tp, repr)

MODULE_NAMES = ('__cache__', '__main__', '__builtin__', 'builtins')

class DumpContext(object):

    def __init__(self, fast=False, classdef=False, given=None, prefix='_p__', sorted=False, pretty=False):
        self.fast = fast
        self.classdef = classdef
        self.assigns = []
        self.pretty = pretty
        #self.nl = pretty and os.linesep or ''
        self.nl = pretty and '\n' or ''
        self.reprs = {}
        self.typeNames = {}
        self.prefix = prefix
        self.sorted = sorted
        if given:
            self.given = dict((id(o),name) for name, o in given.items())
        else:
            self.given = {}
        self.n = 0

def dump_it(self, text, offset, start=None):
    if start is None:
        return self.nl + offset + text
    else:
        return start + text

def visit(self, o, offset, start=None):

    if isinstance(o, type):
        return visit_type(self, o, offset, start)

    method = method_cache.get(o.__class__, visit_object)

    if start is None:
        real_offset = self.nl + offset
    else:
        real_offset = start

    if method is repr:
        return real_offset + repr(o)

    if self.fast:
        return method(self, o, offset, start)
    else:
        oId = id(o)
        if oId in self.objects_cache:
            varName = self.reprs.get(oId, None)
            if varName is None:
                varName = self.given.get(oId, self.prefix + str(self.n))
                self.n += 1

                self.reprs[oId] = varName

                oRepr = method(self, o, '', start)

                self.assigns.append(varName + "=" + oRepr)
            return real_offset + varName
        else:
            return real_offset + method(self, o, offset, start)

#
@cache_method(property)
def visit_property(self, o, offset, start=None):
    items = [f for f in (o.fget, o.fset, o.fdel, o.__doc__) if f is not None]
    return 'property(' + dump_items(self, items, offset, '') + ')'
#
@cache_method(list)
def visit_list(self, o, offset, start=None):
    offset1 = self.pretty and offset + '  ' or ''
    n = len(o)
    if n == 0:
        return '[]'
    elif n == 1:
        return '[' + visit(self, o[0], offset1).lstrip() + ']'
    else:
        return '[' + dump_items(self, o, offset1) + dump_it(self, ']', offset)
#
@cache_method(tuple)
def visit_tuple(self, o, offset, start=None):
    offset1 = self.pretty and offset + ' ' or ''
    n = len(o)
    if n == 0:
        return '()'
    elif n == 1:
        return '(' + visit(self, o[0], offset1).lstrip() + ',)'
    else:
        return '(' + dump_items(self, o, offset1) + dump_it(self, ')', offset)
#
@cache_method(dict)
def visit_dict(self, o, offset, start=None):
    offset1 = self.pretty and offset + '  ' or ''
    n = len(o)
    if n == 0:
        return '{}'
    elif n == 1:
        key, value = o.popitem()
        return '{' + visit(self, key, offset1) + ':' + visit(self, value, offset1, '') + '}'
    else:
        return '{' + dump_mapping(self, o, offset1) + dump_it(self, '}', offset)
#
@cache_method(set)
def visit_set(self, o, offset, start=None):
    offset1 = self.pretty and offset + '  ' or ''
    n = len(o)
    if n == 0:
        return 'set()'
    elif n == 1:
        return '{' + visit(self, o.pop(), offset1).lstrip() + '}'
    else:
        return '{' + dump_items(self, o, offset1) + dump_it(self, '}', offset)
#
@cache_method(frozenset)
def visit_frozenset(self, o, offset, start=None):
    offset1 = self.pretty and offset + '  ' or ''
    n = len(o)
    if n == 0:
        return 'frozenset()'
    elif n == 1:
        return 'frozenset([' + visit(self, o.pop(), offset1).lstrip() + '])'
    else:
        return 'frozenset([' + dump_items(self, o, offset1) + dump_it(self, '])', offset)
#
def dump_items(self, items, offset, start=None):
    return ','.join(visit(self, item, offset, start) for item in items)
#
def dump_mapping(self, mapping, offset, start=None):
    if self.sorted:
        mapping = _safe_sorted(mapping)
    return ','.join(visit(self, k, offset) + ':' + visit(self, v, offset, '') for k, v in mapping.items())
#
def dump_kwitems(self, kwitems, offset, start=None):
    if start is None:
        real_offset = self.nl + offset
    else:
        real_offset = start
    if self.sorted:
        kwitems = _safe_sorted(kwitems)
    return ','.join(real_offset + k + '=' + visit(self, v, offset, '') for k, v in kwitems.items())
#
@cache_method(FunctionType)
def visit_function(self, o, offset, start=None):
    if o.__module__ in MODULE_NAMES:
        name = o.__name__
    else:
        name = o.__module__ + '.' + o.__name__
    return name
#
@cache_method(MethodType)
def visit_method(self, o, offset, start=None):
    return visit(self, o.__self__, offset, start) + "." + o.__func__.__name__
#
@cache_method([BuiltinFunctionType, BuiltinMethodType])
def visit_builtin_function_or_method(self, o, offset, start=None):
    if o.__module__ in MODULE_NAMES:
        name = o.__name__
    else:
        name = o.__module__ + '.' + o.__name__
    return name
#
@cache_method(type)
def visit_type(self, o, offset, start=None):
    if not self.classdef:
        if o.__module__ in MODULE_NAMES:
            name = o.__name__
        else:
            name = o.__module__ + '.' + o.__name__
        return name

    offset1 = self.pretty and offset + '  ' or ''

    try:
        metatype = o.__metaclass__
    except:
        metatype =  o.__class__

    if metatype == type:
        return o.__name__
    else:
        factory = metatype
        args = (o.__name__, o.__bases__)

        if factory.__module__ in MODULE_NAMES:
            name = factory.__name__
        else:
            if isinstance(factory, type):
                name = factory.__module__ + '.' + factory.__name__
            else:
                name = factory.__class__.__module__ + '.' + factory.__name__

        ret = name + '('
        if args:
            ret += dump_items(self, args, offset1) + ','

        kwargs = dict(o.__dict__)
        kwargs.pop('__dict__')
        kwargs.pop('__weakref__')
        if '__metaclass__' in kwargs:
            kwargs.pop('__metaclass__')

        if kwargs:
            if '__module__' in kwargs:
                kwargs.pop('__module__')
            if kwargs['__doc__'] is None:
                kwargs.pop('__doc__')
            ret += dump_kwitems(self, kwargs, offset1)
        if ret[-1] == ',':
            ret = ret[:-1]
        ret += ')'
        return ret
#
@cache_method(object)
def visit_object(self, o, offset, start=None):

    oId = id(o)
    reduce = dispatch_table.get(type(o))
    if reduce:
        rv = reduce(obj)

    reduce = getattr(o, '__reduce_ex__', None)
    if reduce:
        state = reduce(3)
        return with_reduce(self, o, state, offset, start)
    else:
        reduce = getattr(o, '__reduce__', None)
        if reduce:
            state = reduce()
            return with_reduce(self, o, state, offset, start)
        else:
            return without_reduce(self, o, offset, start)
#
def with_reduce(self, o, state, offset, start=None):
    name = visit(self, state[0], offset).lstrip()
    offset1 = self.pretty and offset + '  ' or ''
    if start is None:
        real_offset = self.nl + offset1
    else:
        real_offset = offset + start

    n = len(state)
    ret = ''
    if n > 0:
        ret += name + '('
    if n > 1:
        if state[1]:
            ret += dump_items(self, state[1], offset1) + ','
        if n > 2:
            if state[2]:
                ret += dump_kwitems(self, state[2], offset1) +','
            if n > 3:
                if state[3]:
                    offset2 = self.pretty and offset1 + '  ' or ''
                    ret += real_offset + '*[' + dump_items(self, state[3], offset2) + dump_it(self, '],', offset1)
                if n > 4:
                    if state[4]:
                        offset3 = self.pretty and offset1 + '   ' or ''
                        ret += real_offset + '**{' + dump_mapping(self, state[4], offset3) + dump_it(self, '}', offset1)
    if ret[-1] == ',':
        ret = ret[:-1]
    ret += dump_it(self, ')', offset)
    return ret
#
def without_reduce(self, o, offset=None):
    clsname = o.__class__.__name__

    newargs = None
    getnewargs = getattr(o, '__getnewargs__', None)
    if getnewargs:
        newargs = getnewargs()

    state = None
    if hasattr(o, '__getstate__'):
        state = o.__getstate__()
    else:
        state = getattr(o, '__dict__', None)
        if state is None:
            state = {}
            for name in o.__slots__:
                value = getattr(o, name, null)
                if value is not null:
                    state[name] = value

    offset1 = self.pretty and offset + '  ' or ''
    n = len(state)
    ret = ''
    if n > 0:
        ret += clsname + '('
    if n > 1:
        if state[1]:
            ret += dump_items(self, state[1], offset1) + ','
        if n > 2:
            if state[2]:
                ret += '*'+ visit(self, state[2], offset1).lstrip() + ','
    if ret[-1] == ',':
        ret = ret[:-1]
    ret += ')'
    return ret

def dumps(o, fast=False, classdef=False, pretty=False, sorted=True, given=None):
    if not fast:
        cacher = dumpcache.Cacher()
        dumpcache.visit(cacher, o)
        objects_info = dict((oId,n) for oId,n in cacher.objects_info.items() if n > 0)
        objects_cache = dict((oId,o) for oId,o in cacher.objects_cache.items() if oId in objects_info)

    _given = currentScope(given=given, level=2)

    context = DumpContext(fast=fast, classdef=classdef, given=_given, sorted=False, pretty=pretty)
    if not fast:
        context.objects_cache = objects_cache

    text = visit(context, o, '').lstrip()
    if context.assigns:
        assigns = "\n".join(context.assigns)
    else:
        assigns = ""
    return "\n".join(s for s in [assigns,text] if s)

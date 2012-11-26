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

import sys

def defaultResolver(use_modules = True, given={}):
    _dict = dict(sys._getframe(2).f_globals)
    _dict.update(sys._getframe(2).f_locals)
    if given:
        _dict.update(given)
    return dictResolver(_dict, use_modules)

def dictResolver(mapping, use_modules=True, given = {}):
        _dict = dict(mapping)
        _dict.update(given)
        if use_modules:
            def resolver(name):
                try:
                    return _dict[name]
                except: pass
                __import__(name, level=0)
                return sys.modules[name]
            return resolver
        else:
            def resolver(name):
                return _dict[name]
            return resolver

def customResolver(nameResolver, use_modules=True, given={}):
    if given:
        if use_modules:
            def resolver(name):
                try:
                    return given[name]
                except: pass
                try:
                    return nameResolver(name)
                except: pass
                __import__(name, level=0)
                return sys.modules[name]
            return resolver
        else:
            def resolver(name):
                try:
                    return given[name]
                except: pass
                try:
                    return nameResolver(name)
                except: pass
            return resolver
    else:
        if use_modules:
            def resolver(name):
                try:
                    return nameResolver(name)
                except: pass
                __import__(name, level=0)
                return sys.modules[name]
            return resolver
        else:
            return nameResolver

def safeNameResolver(name):
    return type(name, (_Element_,), {'__module__' : '__cache__'})

class _Element_(object):
    #
    def __init__(self, *args):
        self._args__ = args
        self._sequence__ = []
        self._mapping__ = {}
    #
    @property
    def tag(self):
        return self.__class__.__name__
    #
    @property
    def args(self):
        return self._args__
    #
    @property
    def sequence(self):
        return self._sequence__
    #
    @property
    def mapping(self):
        return self._mapping__
    #
    def append(self, item):
        self.sequence.append(item)
    #
    def __setitem__(self, key, item):
        self.mapping[key] = item
    #
    def __reduce__(self):
        _dict = dict(self.__dict__)
        args = _dict.pop('_args__', None)
        sequence = _dict.pop('_sequence__', None)
        mapping = _dict.pop('_mapping__', None)
        return safeNameResolver(self.__class__.__name__), args, _dict, sequence, mapping
    #
    def __setstate__(self, kwargs):
        self.__dict__.update(kwargs)

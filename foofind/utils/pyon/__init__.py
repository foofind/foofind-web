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

"""
`pyon` package implements dump/load facilities based on PyON
(http://code.google.com/p/pyon).

Python object notation (PyON) is designed for literal representation of objects
based on the syntax of the python language.

In PyON we have attempted to join the pickle protocol and current syntax of
the python language for reconstructable literal representation of python objects.

There some features of PyON:
  * `pyon` dosn't use exec or eval during object reconstruction anyway;
  * `pyon` allows a programmer to manage the process of resolving names in PyON string;
  * `pyon` supports raw and "pretty" string dumping mode.

Here is a rough definition of the PyON::

    <pyon> ::= <expr>
    <pyon> ::= <assign>  ... <assign> <expr>
    <assign> ::= <name> = <expr> <newline>
    <assign> ::= <name> [ <expr> ] = <expr> <newline>
    <expr> ::= <int>
    <expr> ::= <bool>
    <expr> ::= <string>
    <expr> ::= <float>
    <expr> ::= <list>
    <expr> ::= <tuple>
    <expr> ::= <dict>
    <expr> ::= <instance>
    <expr> ::= <name>
    <expr> ::= <name>.<name> ... .<name>
    <list> ::= [ <expr>, ..., <expr> ]
    <tuple> ::= ( <expr>, ..., <expr> )
    <dict> ::= { <expr> : <expr>, ..., <expr> : <expr> }
    <instance> ::= <name> ( <args>, <kw>, <starargs>, <starkw> )
    <args> ::=  <expr>, ..., <expr>
    <kw> := <name> = <expr>, ..., <name> = <expr>
    <starargs> ::= *<list>
    <starargs> ::= *<tuple>
    <starkw> ::= **<dict>
"""

from .load import loads
from .dump import dumps, currentScope

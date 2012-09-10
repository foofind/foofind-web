#!/usr/bin/env python
# -*- coding: utf-8 -*-


import foofind.utils
import flask.globals

from werkzeug.local import LocalStack, LocalProxy, get_ident

class ThreadingStack(LocalStack):
    '''
    Pila para acceder al contexto en hilos hijos.
    Para que el contexto sea accesible, debe llamarse a
    ThreadingStack.register_child con el id del padre (cuyo contexto heradará)
    como primer parámetro.
    '''
    _threads = foofind.utils.LimitedSizeDict(size_limit=5000)

    def __init__(self, old_stack=None):
        LocalStack.__init__(self)
        if not old_stack is None: self._local = old_stack._local
        self.__ident_func__ = self.get_parent_id

    @property
    def ctx(self):
        top = self.top
        if top is None:
            raise RuntimeError('Fallo de contexto de flask')
        return rop

    @property
    def top(self):
        try:
            return self._local.stack[-1]
        except (AttributeError, IndexError):
            return None

    @classmethod
    def get_parent_id(cls):
        '''
        Retorna el padre del id del proceso actual, tal cual recibe
        `register_child` por parámetro.
        '''
        current = get_ident()
        return cls._threads.get(current, current)

    @classmethod
    def register_child(cls, parent_id):
        '''
        Registra el proceso actual como hijo del padre con id dado, para
        acceder a su contexto.
        '''
        cls._threads[get_ident()] = parent_id

    @classmethod
    def unregister_child(cls):
        '''
        Borra el proceso actual del listado de hijos.
        '''
        del cls._threads[get_ident()]


requestctx = ThreadingStack(flask.globals._request_ctx_stack)
appctx = ThreadingStack(flask.globals._app_ctx_stack)

flask.globals._request_ctx_stack = requestctx
flask.globals._app_ctx_stack = appctx

current_app = LocalProxy(lambda: appctx.ctx.app)
request = LocalProxy(lambda: requestctx.ctx.request)
session = LocalProxy(lambda: requestctx.ctx.session)
g = LocalProxy(lambda: requestctx.ctx.g)



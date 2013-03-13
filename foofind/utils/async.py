# -*- coding: utf-8 -*-
"""
    Utilidad para ejecución ágil y sencilla de tareas en paralelo para obtención de datos.
"""

import greenlet
import threading
from Queue import Queue, Empty
import foofind.globals
from time import time
from . import logging

class EOFQueue(object):
    pass

class MultiAsync(object):
    '''
        Clase que implementa la ejecución de la misma tarea en paralelo
    '''
    def _target_wrapper(self, f, args):
        foofind.globals.ThreadingStack.register_child(self._parent_id)
        try:
            f(self, *args)
        except BaseException as e:
            logging.exception(e)
        foofind.globals.ThreadingStack.unregister_child()


    def __init__(self, target, elems, maxsize):
        '''
            Inicializa la clase con la función target a ejecutar y la lista
            de tuplas a pasar como parametro a la función
        '''
        self.maxsize = maxsize
        self.nelems = len(elems)
        self.values = Queue(maxsize)
        self._parent_id = foofind.globals.ThreadingStack.get_parent_id()
        for args in elems:
            threading.Thread(target=self._target_wrapper, args=(target, args)).start()

    def return_value(self, values):
        '''
            Obtiene la lista de resultados para la tarea con el identificador dado
        '''
        for item in values:
            self.values.put(item)
        self.values.put(EOFQueue)

    def get_values(self, timeout=None):
        '''
            Devuelve los valores generados a medida que se van obteniendo
        '''
        start = time()
        to = None
        rest = self.nelems
        try:
            while rest>0:
                if timeout:
                    to = timeout-time()+start
                ty = self.values.get(to>0, to)
                if ty is EOFQueue:
                    rest-=1
                else:
                    yield ty
        except Empty as e:
            pass
        except BaseException as e:
            logging.exception("Error retrieving values asynchronally.")

class AsyncThread(threading.Thread):
    '''
        Clase que permite la ejecución de una tarea en paralelo, acumulando
        en una cola los resultados de esta.
    '''
    def __init__(self, timeout, target, target_args, target_kwargs, *args, **kwargs):
        super(AsyncThread, self).__init__(*args, **kwargs)
        self.target = target
        self.args = target_args
        self.kwargs = target_kwargs
        self.results = Queue()
        self.timeout = timeout
        self._parent_id = foofind.globals.ThreadingStack.get_parent_id()
        self.start_time = time()

    def run(self):
        foofind.globals.ThreadingStack.register_child(self._parent_id)
        try:
            for ret in self.target(*self.args, **self.kwargs):
                self.results.put(ret)
            self.results.put(EOFQueue)
        except BaseException, e:
            logging.exception("Error while performing an asynchronous task.")
            self.results.put(EOFQueue)
        foofind.globals.ThreadingStack.unregister_child()

    def __iter__(self):
        return self

    def next(self):
        try:
            if self.timeout:
                to = self.timeout-time()+self.start_time
                if to<0: to = 0
            item = self.results.get(True, to)
            if item is EOFQueue:
                raise StopIteration()
            else:
                return item
        except Empty as e:
            logging.warning("Timeout waiting for async task response.")
            raise StopIteration()

def async_generator(timeout):
    def wrap(f):
        def inner(*args, **kwargs):
            at = AsyncThread(timeout, target=f, target_args=args, target_kwargs=kwargs)
            at.start()
            return at
        return inner
    return wrap

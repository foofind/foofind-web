# -*- coding: utf-8 -*-
"""
    Utilidad para ejecución ágil y sencilla de tareas en paralelo para obtención de datos.
"""

import greenlet
import threading
import Queue
import logging

'''
 ATENCIÓN: Este código debe ejecutarse antes que nada.

 Utilizar la funcion child_thread de async para que los nuevos hilos
compartan el contexto de flask del hilo padre.
'''
import werkzeug.local
def thread_ident():
    cur = greenlet.getcurrent()
    return cur.parent or cur
werkzeug.local.get_ident = thread_ident

def child_thread(f):
    thread_id = greenlet.getcurrent()
    def nf(*args, **kwargs):
        greenlet.getcurrent().parent = thread_id
        try:   
            f(*args, **kwargs)
        except Exception as e:
            logging.exception("Unhandled exception on child thread.")
    return nf


class EOFQueue(object):
    pass
    
class MultiAsync(object):
    '''
        Clase que implementa la ejecución de la misma tarea en paralelo
    '''
    def __init__(self, target, elems, maxsize):
        '''
            Inicializa la clase con la función target a ejecutar y la lista
            de tuplas a pasar como parametro a la función
        '''
        self.maxsize = maxsize
        self.nelems = len(elems)
        self.values = Queue.Queue(maxsize)
        for arg in elems: threading.Thread(target=child_thread(target), args=(self,)+arg).start()

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
        try:
            stops = 0
            for i in xrange(self.maxsize):
                ty = self.values.get(True, timeout)
                if ty is EOFQueue:
                    stops += 1
                    if stops >= self.nelems:
                        break
                else:
                    yield ty
        except Exception as e:
            logging.exception("Error retrieving values asynchronally.")

class AsyncThread(threading.Thread):
    '''
        Clase que permite la ejecución de una tarea en paralelo, acumulando
        en una cola los resultados de esta.
    '''
    def __init__(self, results, target, target_args, target_kwargs, *args, **kwargs):
        super(AsyncThread, self).__init__(*args, **kwargs)
        self.results = results
        self.target = target
        self.args = target_args
        self.kwargs = target_kwargs
        self.run = child_thread(self.run)
        
    def run(self):
        try:
            for ret in self.target(*self.args, **self.kwargs):
                self.results.put(ret)
            self.results.put(EOFQueue)
        except Exception, e:
            logging.exception("Error while performing an asynchronous task.")
            self.results.put(EOFQueue)

def async_generator(timeout):
    def wrap(f):
        def inner(*args, **kwargs):
            results = Queue.Queue()
            at = AsyncThread(results, target=f, target_args=args, target_kwargs=kwargs)
            at.start()
            try:
                while True:
                    item = results.get(timeout=timeout)
                    results.task_done()
                    if item is EOFQueue:
                        break
                    else:
                        yield item
            except Queue.Empty as e:
                logging.warning("Timeout waiting for async task response.")
        return inner
    return wrap

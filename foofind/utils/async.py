# -*- coding: utf-8 -*-
"""
    Utilidad para ejecución ágil y sencilla de tareas en paralelo para obtención de datos.
"""
import threading
import Queue
import functools

class MultiAsync(object):
    '''
        Clase que implementa la ejecución de la misma tarea en paralelo
    '''
    def __init__(self, target, elems):
        '''
            Inicializa la clase con la función target a ejecutar y la lista
            de tuplas a pasar como parametro a la función
        '''
        self.threads = len(elems)
        self.values = Queue.Queue(self.threads)
        for arg in elems: threading.Thread(target=target, args=(self,)+arg).start()

    def return_value(self, values):
        '''
            Obtiene la lista de resultados para la tarea con el identificador dado
        '''
        self.values.put(values)

    def get_values(self, timeout=None):
        '''
            Devuelve los valores generados a medida que se van obteniendo
        '''
        for i in xrange(self.threads):
            for values in self.values.get(timeout):
                yield values

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

    def run(self):
        for ret in self.target(*self.args, **self.kwargs):
            self.results.put(ret)
        self.results.join()

def async_generator(f):
    @functools.wraps(f)
    def inner(*args, **kwargs):
        results = Queue.Queue()
        at = AsyncThread(results, target=f, target_args=args, target_kwargs=kwargs)
        at.start()
        while at.is_alive:
            yield results.get()
            results.task_done()

    return inner

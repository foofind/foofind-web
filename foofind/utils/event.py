# -*- coding: utf-8 -*-

from heapq import heappush, heappop
import threading
import time
import sys
import uuid
import logging

try: import ctypes
except ImportError: ctypes = None

class EventManager(threading.Thread):
    '''
    Hilo gestor de eventos para ejecutar tareas programadas asíncronamente.
    '''
    _timers = None
    _run = False
    _abort = False
    _intervalids = None
    _wakeup = False
    _stress = 0
    _running_onetime_tasks = False
    def __init__(self, poll_time=0.1, relax_time=1, stress_limit=10):
        threading.Thread.__init__(self)
        self.daemon = True
        self._timers = []
        self._events = {}
        self._callbacks = {}
        self._lock = threading.Lock()
        self._ignore = []
        self.poll_time = poll_time
        self.relax_time = relax_time
        self.stress_limit = stress_limit

    def _put_timer_id(self, ntime, tid):
        # Tuplas por baja probabilidad de collisión de timers
        with self._lock:
            heappush(self._timers, (ntime, tid))
            self._wakeup = True

    def _put_timer(self, ntime, handler, hargs=None, hkwargs=None, interval=0):
        with self._lock:
            tid = uuid.uuid4().hex
            self._callbacks[tid] = (handler, hargs or (), hkwargs or {}, interval)
        self._put_timer_id(ntime, tid)

    def _put_once(self, ntime, handler, hargs=None, hkwargs=None, interval=0):
        with self._lock:
            heappush(self._timers, (ntime, (handler, hargs or (), hkwargs or {}, interval)))
            self._wakeup = True
            
    _thread_id = None
    @property
    def thread_id(self):
        if not self._thread_id: return self._thread_id
        for tid, tobj in threading._active.items():
            if tobj is self:
                self._thread_id = tid
                return tid
        raise threading.ThreadError()

    def once(self, handler, hargs=None, hkwargs=None):
        self._running_onetime_tasks = True
        return self._put_once(time.time(), handler, hargs, hkwargs, -1)

    def interval(self, seconds, handler, hargs=None, hkwargs=None):
        return self._put_timer(time.time()+seconds, handler, hargs, hkwargs, seconds)

    def timeout(self, seconds, handler, hargs=None, hkwargs=None):
        return self._put_once(time.time()+seconds, handler, hargs, hkwargs, 0)

    def clear_interval(self, tid):
        with self._lock:
            del self._callbacks[tid]

    def register(self, event, handler, hargs=None, hkwargs=None, repeat=True):
        if event in self._events:
            self._events.append((handler, hargs or (), hkwargs or {}, repeat))
        self._events = [(handler, hargs or (), hkwargs or {}, repeat)]

    def event(self, event):
        # TODO: implementación asíncrona si hace falta
        if event in self._events:
            for cb, cbargs, cbkwargs, repeat in self._events[event]:
                cb(*cbargs, **cbkwargs)
            self._events[event][:] = (i for i in self._events[event] if i[3])

    def stop(self):
        self._run = False

    def kill(self, exctype=SystemExit):
        if ctypes is None:
            self.stop()
        else:
            tid = self.thread_id
            res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
            if res != 1:
                if res != 0:
                    ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, 0)
                raise SystemError

    def _poll(self):
        if self._timers:
            self._stress = 0
            ctime = time.time()
            with self._lock:
                ntime = self._timers[0][0]
            wait = ntime - ctime
            if wait > 0:
                return None, wait
            with self._lock:
                return heappop(self._timers)[1], 0
        if self._stress < self.stress_limit:
            self._stress += 1
            return None, self.poll_time
        return None, self.relax_time

    def start(self):
        self._run = True
        threading.Thread.start(self)
        self.wait_for_unique_tasks()

    def wait_for_unique_tasks(self):
        while self._running_onetime_tasks:
            time.sleep(self.relax_time)

    def run(self):
        while self._run:
            tid, wait = self._poll()
            if tid:
                if isinstance(tid, str):
                    # Si hay timers, los recorro para ejecutar
                    with self._lock:
                        if not tid in self._callbacks:
                            continue
                        cb, cbargs, cbkwargs, interval = self._callbacks[tid]
                    self._running_onetime_tasks = False
                else:
                    cb, cbargs, cbkwargs, interval = tid
                    self._running_onetime_tasks = True
                    
                try:
                    cb(*cbargs, **cbkwargs)
                except BaseException as e:
                    logging.exception(e)
                except:
                    et, ev, etb = sys.exc_info()
                    logging.error(
                        "Excepción no capturada",
                        extra={
                            "type":et,
                            "value":ev,
                            "traceback":etb
                            })
                            
                if interval > 0:
                    self._put_timer_id(time.time()+interval, tid)
                    
            elif self._running_onetime_tasks:
                self._running_onetime_tasks = False
                
            if wait > 0.:
                time.sleep(min(wait, self.relax_time))
                # TODO(felipe) encontrar una forma más eficiente de hacer esto

# -*- coding: utf-8 -*-

import threading
import time
import sys
try: import ctypes
except ImportError: ctypes = None

class EventManager(threading.Thread):
    _timers = None
    _run = False
    _abort = False
    def __init__(self, poll_time=0.1, relax_time=1, stress_limit=10):
        threading.Thread.__init__(self)
        self.daemon = True
        self._timers = {}
        self._events = {}
        self._lock = threading.Lock()
        self._ignore = []
        self.poll_time = poll_time
        self.relax_time = relax_time
        self.stress_limit = stress_limit
        self.start()

    def __del__(self):
        self.kill()

    def _put_timer(self, ntime, handler, hargs=None, hkwargs=None, interval=0, ):
        with self._lock:
            if ntime in self._timers:
                # Probabilidad extremadamente baja
                self._timers[ntime] += ((handler, hargs or (), hkwargs or {}, interval),)
            else:
                self._timers[ntime] = ((handler, hargs or (), hkwargs or {}, interval),)

    def _pop_timer(self, ntime):
        with self._lock:
            return self._timers.pop(ntime)

    _thread_id = None
    @property
    def thread_id(self):
        if not self._thread_id: return self._thread_id
        for tid, tobj in threading._active.items():
            if tobj is self:
                self._thread_id = tid
                return tid
        raise threading.ThreadError()

    def interval(self, seconds, handler, hargs=None, hkwargs=None):
        self._put_timer(time.time()+seconds, handler, hargs, hkwargs, seconds)

    def timeout(self, seconds, handler, hargs=None, hkwargs=None):
        self._put_timer(time.time()+seconds, handler, hargs, hkwargs, 0)

    def clear_interval(self, fnc):
        self._ignore.append(fnc)

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
        self.join()

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

    def run(self):
        self._run = True
        stress = 0
        while self._run:
            if self._timers:
                stress = 0
                ctime = time.time()
                ntime = min(self._timers.iterkeys())
                while ctime > ntime:
                    for cb, cbargs, cbkwargs, interval in self._pop_timer(ntime):
                        if not cb in self._ignore:
                            cb(*cbargs, **cbkwargs)
                            if interval:
                                self._put_timer(ctime+interval, cb, cbargs, cbkwargs, interval)
                    if not self._timers:
                        # If we break, we do not pass through while-else
                        break
                    ctime = time.time()
                    ntime = min(self._timers.iterkeys())
                else:
                    time.sleep(ntime-ctime)
            elif stress < self.stress_limit:
                stress += 1
                time.sleep(self.poll_time)
            else:
                time.sleep(self.relax_time)


# -*- coding: utf-8 -*-

import socket
import collections
import time
import threading
import errno
import json
import math

from . import logging

class TamingSocket(object):
    control = "\1"
    wtime = 0.001
    bufsize = 1024

    def __init__(self, manager, address):
        self.timeout = manager.timeout
        self.rcni = int(math.ceil(self.timeout/self.wtime))

        self.address = address
        self.manager = manager
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect(address)

    def close(self):
        self.socket.close()

    def get(self, obj):
        data = json.dumps(obj).encode("utf-8")
        self.socket.settimeout(self.timeout)
        self.socket.send("%s\0" % data)
        tr = "".join(self._recv_until("\0")).decode("utf-8")

        try:
            if tr:
                return json.loads(tr)
        except BaseException as e:
            pass
        return ((), False)

    def _recv_until(self, char):
        for i in xrange(self.rcni):
            # Número de iteraciones con sleep para cumplir el timeout
            tr = self.socket.recv(self.bufsize)
            if char in tr:
                yield tr[:tr.find(char)]
                break
            yield tr
            time.sleep(self.wtime)
        else:
            yield ""

class TamingClient(object):
    busy = None
    sockets = None
    timeout = 1
    servers = ()
    INF = float("inf")

    def __init__(self):
        self._b0 = collections.defaultdict(int)
        self._b1 = collections.defaultdict(int)
        self.lock = threading.Lock()

    def init_app(self, app):
        self.servers = tuple(app.config["SERVICE_TAMING_SERVERS"])
        self.timeout = app.config["SERVICE_TAMING_TIMEOUT"]

    def get_socket(self):
        with self.lock:
            # Orden de prioridad para elegir address (usando time e inf)
            #   _b0 _b1
            #   < N  X   Puerto que lleve más tiempo sin usar.
            #   INF < N  Puerto que lleve más tiempo en uso.
            s = min(self.servers, key = self._b0.__getitem__ )
            if self._b0[s] < self.INF:
                self._b0[s] = self.INF
            else:
                s = min(self.servers, key = self._b1.__getitem__ )
            self._b1[s] = time.time()
        return TamingSocket(self, s)

    def discard_socket(self, k):
        a = k.address
        with self.lock:
            self._b0[a] = time.time()
        try:
            k.close()
        except BaseException as e:
            logging.exception("Error trying to close a discarded taming socket")

    def open_connect(self): pass

    def close_connection(self): pass

    def tameText(self, text, weights, limit, maxdist, minsimil, dym=1, rel=1):
        con = None
        result = None
        ok = False
        try:
            con = self.get_socket()
            result, ok = con.get({"t": text, "w":weights, "l":limit, "s":minsimil, "md":maxdist, "d":dym, "r":rel})
        except socket.timeout as e:
            logging.warn("Timeout when calling taming service.")
        except socket.error as e:
            logging.exception("Socket error talking to taming service.")
        except IOError as e:
            if e.errno == errno.EPIPE: logging.exception("Broken pipe talking to taming service.")
            else: logging.exception("IOError talking to taming service.")
        except BaseException as e:
            logging.exception("Exception talking to taming service.")
        if con:
            self.discard_socket(con)
        return result

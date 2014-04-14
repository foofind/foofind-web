# -*- coding: utf-8 -*-
from hashlib import md5
from msgpack import unpackb as parse_data, packb as format_data
from time import time
from collections import deque
from threading import Lock
import re

__all__ = ["WORKER_VERSION", "EXECUTE_CHANNEL","RESULTS_CHANNEL","CONTROL_CHANNEL", "UPDATE_CHANNEL",
           "GROUPING_GROUP", "GROUPING_NO_GROUP",
           "CONTROL_KEY", "LOCATION_KEY", "QUERY_KEY",
           "ACTIVE_KEY", "INFO_KEY", "LOCATION_KEY", "PART_KEY", "PART_SG_KEY", "VERSION_KEY",
           "hash_dict", "parse_data", "format_data", "LimitedDict"]


# constantes
WORKER_VERSION = 0 # solo puede ser 1 o 0
CONTROL_CHANNEL = "c" if WORKER_VERSION==0 else "C"
EXECUTE_CHANNEL = "e" if WORKER_VERSION==0 else "E"
RESULTS_CHANNEL = "r" if WORKER_VERSION==0 else "R"
UPDATE_CHANNEL = "u" if WORKER_VERSION==0 else "U"

GROUPING_GROUP = 0b01
GROUPING_NO_GROUP = 0b10

# Claves de cache
CONTROL_KEY = "c"
LOCATION_KEY = "l"
QUERY_KEY = "q"

# subclaves
ACTIVE_KEY = "a"
INFO_KEY = "i"
LOCATION_KEY = "l"
PART_KEY = "p"
PART_SG_KEY = "s"
VERSION_KEY = "v"

INVARIANT_QUERY_KEYS = set(["l","g","mt"])

def hash_dict(adict):
    data = "\x00".join(key+"\x01"+format_data(value) for key,value in sorted(adict.iteritems()) if key not in INVARIANT_QUERY_KEYS)
    return md5(data).digest()

class LimitedDict(dict):
    def __init__(self, max_size=None, timeout=None, cleanup_min_interval=0.1, *args, **kwds):
        dict.__init__(self, *args, **kwds)
        self.__max_size = max_size
        self.__timeout = timeout
        now = time()

        # control de datos
        self.access = Lock()
        self.__order = deque(((key,now) for key in dict.iterkeys(self)), self.__max_size)
        self.__full = self.__max_size and len(self.__order)==self.__max_size

        # control de limpieza
        self.__last_cleanup = now
        self.__cleanup_min_interval = cleanup_min_interval
        self.cleanup()

    def cleanup(self):
        # claves expiradas
        if self.__timeout and time()-self.__last_cleanup>self.__cleanup_min_interval:
            with self.access:
                self.__last_cleanup = now = time()
                expired_now = now-self.__timeout
                while self.__order and self.__order[0][1]<expired_now:
                    dict.__delitem__(self, self.__order[0][0])
                    self.__order.popleft()
                    self.__full = False

    def __setitem__(self, key, value):
        with self.access:
            if not key in self:
                if self.__full:
                    dict.__delitem__(self, self.__order[0][0])
                elif self.__max_size and len(self.__order)+1==self.__max_size:
                    self.__full = True
                self.__order.append((key, time()))
            dict.__setitem__(self, key, value)

    def __delitem__(self, key):
        raise NotImplemented("You can't delete from this dictionary.")

if __name__ == "__main__":
    from time import sleep

    a=LimitedDict(5, timeout=1)
    for i in xrange(10):
        a[i]="tam"
    a.cleanup()
    print "5 maximo", repr(a)
    for i in xrange(3):
        a["n"+str(i)]="caducan"
    sleep(0.2)
    for i in xrange(3):
        a["c"+str(i)]="no caducan"
    a.cleanup()
    print "0.2 segundo", repr(a)
    sleep(0.9)
    a.cleanup()
    print "0.9 segundo", repr(a)

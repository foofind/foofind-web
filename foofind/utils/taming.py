# -*- coding: utf-8 -*-

import socket
import logging
from threading import Thread
from flask import json

class TamingClient():

    def __init__(self, address, timeout):
        '''
            @param timeout: número de segundos máximos de espera. Puede
            ser un valor con decimales.
        '''
        self.address = address
        self.timeout = timeout
        self.connection = None
        
    def open_connection(self):
        if self.connection is None:
            try:
                self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.connection.connect(self.address)
                self.connection.settimeout(self.timeout)
            except Exception as e:
                logging.exception("Error connecting to taming service.")
                return False
        return True
        
    def close_connection(self):
        if not self.connection is None:
            try:
                self.connection.close()
            except Exception as e:
                logging.exception("Error closing taming service connection.")
                return False
            
    def tameText(self, text, weights, limit, maxdist, minsimil, dym=1, rel=1):
        # Si no estaba conectado, se conecta y se desconectará al final
        wasnt_connected = self.connection is None
        if wasnt_connected and not self.open_connection(): return None

        try:
            params = json.dumps({"t": text, "w":weights, "l":limit, "s":minsimil, "md":maxdist, "d":dym, "r":rel})
            paramslen = len(params)
            self.connection.send(chr(paramslen/256)+chr(paramslen%256)+params)
            lenrec = self.connection.recv(2)
            lenrec = ord(lenrec[0])*256+ord(lenrec[1])
            line = self.connection.recv(lenrec)
            result = json.loads(line[:-1])
        except Exception as e:
            logging.exception("Error talking to taming service.")
            result = None
        finally:
            if wasnt_connected: self.connection.close()
        return result

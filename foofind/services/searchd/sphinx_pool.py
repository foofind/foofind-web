# -*- coding: utf-8 -*-

from collections import deque
from threading import Lock
from time import time

from foofind.utils import sphinxapi2, logging

class SphinxClientPool():
    def __init__(self, server, maintenance_interval, min_clients, recycle_uses, connect_timeout):
        self.server = server
        self.maintenance_interval = maintenance_interval
        self.recycle_uses = recycle_uses
        self.connect_timeout = connect_timeout

        self.clients = deque()
        self.access = Lock()
        self.connection_failed = False

        self.clients_counter = 0
        self.max_clients_counter = float(min_clients)
        self.adhoc = 0

        self.maintenance()

    def get_sphinx_client(self):

        client = None
        self.access.acquire()

        # aumenta la cuenta de clientes en uso, antes de intentar un ad-hoc
        self.clients_counter+=1

        if self.clients_counter > self.max_clients_counter:
            self.max_clients_counter = float(self.clients_counter)

        # obtiene un cliente conectado, si hay
        if self.clients:
            client = self.clients.popleft()
        self.access.release()


        # si no hay cliente, lo crea ad-hoc
        if not client and not self.connection_failed:
            self.adhoc += 1
            try:
                client = self.create_sphinx_client()
                client.Open()
                client.uses = 0
            except Exception as e:
                logging.error("Ad-hoc connection to search server %s:%d failed."%self.server)
                self.connection_failed = True
                client = None

        # si aun no hay cliente, actualiza la cuenta de clientes en uso
        if not client:
            self.clients_counter-=1

        return client

    def return_sphinx_client(self, client, discard=False):
        client.uses += 1

        # si ha alcanzado el limite de usos, lo descarta
        if discard or client.uses>=self.recycle_uses:
            uses = client.uses
            recycle = self.recycle_uses
            client.Close()
            self.clients_counter -= 1
            return

        # reinicia el cliente para el próximo uso
        client.ResetFilters()
        client.ResetGroupBy()

        # renueva el timeout de la conexion
        client.timeout = self.connect_timeout+time()

        # actualiza el timeout del cliente y lo añade a la lista de clientes de nuevo
        self.access.acquire()
        self.clients_counter -= 1
        self.clients.append(client)
        self.access.release()


    def maintenance(self):
        client = None
        self.max_clients_counter -= 0.05

        # si se necesitan conexiones extras, añade clientes
        if len(self.clients) + self.clients_counter < self.max_clients_counter + 2:
            try:
                client = self.create_sphinx_client()
                client.Open()
                client.timeout = self.connect_timeout+time()
                self.connection_failed = False
            except:
                logging.error("Connection to search server %s:%d failed."%self.server)
                self.connection_failed = True

        now = time()
        # descarta conexiones caducadas
        while self.clients and self.clients[0].timeout and self.clients[0].timeout<now:
            self.clients.popleft().Close()

        # va a tocar los clients
        self.access.acquire()

        # añade clientes conectados
        if client:
            self.clients.append(client)

        # ya no necesita los clientes
        self.access.release()

    def create_sphinx_client(self):
        client = sphinxapi2.SphinxClient()
        client.SetConnectTimeout(self.connect_timeout)
        client.SetServer(self.server[0], self.server[1])
        client.uses = 0
        client.timeout = None
        return client

    def destroy(self):
        # va a tocar los clients
        self.access.acquire()

        # cierra conexiones
        for client in self.clients:
            client.Close()

        # ya no necesita los clientes
        self.access.release()

# -*- coding: utf-8 -*-
import argparse, redis
from common import *
from time import time

# parsea argumentos
parser = argparse.ArgumentParser(description='Sphinx search service.')
parser.add_argument('part', type=int, help='Server number.')
params = parser.parse_args()

# obtiene el servidor redis de la configuración de producción
from os import environ
environ["FOOFIND_NOAPP"] = "1"
config = __import__("production").settings.__dict__
redis_servers = config["SPHINX_REDIS_SERVER"]

# conecta a redis y envia mensaje
for redis_server in redis_servers:
    redisc = redis.StrictRedis(host=redis_server[0], port=redis_server[1], db=WORKER_VERSION)
    redisc.pipeline().set(CONTROL_KEY+"lr_%d"%params.part, time()).publish(CONTROL_CHANNEL+chr(params.part), "lr").execute()

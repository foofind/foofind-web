# -*- coding: utf-8 -*-
from flask import Blueprint, request, jsonify, abort, current_app

from functools import wraps

'''
from hashlib import sha256
from os import urandom
from collections import OrderedDict
from time import time, sleep
'''

control = Blueprint('control', __name__)
'''
shared_key = "foo"
keychain = OrderedDict()
key_lifetime = 60 # seconds
key_length = bigger is better

Client             Server

               >>   /key
cid, key1      <<   generate key1 and id
               >>   /control ( action, args, id, sha(key1+password) )
result, key2   <<   calling actions
               >>   /control ( action, args, id, sha(key2+password) )
result, key3   <<   calling actions

keys expire on server when:
    * Expiration time
    * Control is called with key

keys regenerates only when password is correct

def check_auth():
    cid = request.json["client_id"]
    key, created = keychain[cid]

    if time() - created > key_lifetime:
        return jsonify({"status":"error","return":"Control: Expired key."})

    cpass = request.json["password"]
    if cpass != sha256(key + shared_key):
        return jsonify({"status":"error","return":"Control: Wrong password."})

    # Llegados a este punto, sabemos que el cliente tiene una password correcta
    # cifrada junto a una clave reciente.

    key_new = generate_key() # Clave para siguiente acción
    keychain[cid] = key_new

def generate_key():
    return urandom(100).encode("hex")
'''

def admin_host_required(fn):
    '''
    Decorador que se asegura de que el usuario sea admin

    @param fn: función

    @return función decorada
    '''

    @wraps(fn)
    def decorated_view(*args, **kwargs):
        if not reqquest.remote_addr in current_app.config["ADMIN_HOSTS"]:
            abort(403)
        return fn(*args, **kwargs)
    return decorated_view

@control.route('/info')
@admin_host_required
def info():
    '''
    Handler para recibir los comandos remotos.

    '''
    check_auth()

    return jsonify(
        key=None
        )


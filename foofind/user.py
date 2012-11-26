# -*- coding: utf-8 -*-
from flask import session, request
from flask.ext.login import UserMixin, AnonymousUser as AnonymousUserMixin
from datetime import datetime
from hashlib import md5
from uuid import uuid1, UUID
from struct import Struct
from time import time
from foofind.services import *
from foofind.utils import userid_parse

import logging

class UserBase(object):
    _timepack = Struct("d")

    id = None
    type = None
    username = None
    email = None
    created = None
    lang = None
    location = None
    active = None
    has_data = False

    def session_id_as_int(self):
        if self.session_id:
            return int(self._timepack.unpack(self.session_id.decode("hex"))[0] * 1000)
        return None

    def __init__(self, data):
        self.session_id = data.get("session_id", None) if data else None
        self.session_ip = data.get("session_ip", None) if data else None

    @classmethod
    def generate_session_id(cls):
        return cls._timepack.pack(time()).encode("hex")

    @classmethod
    def current_user(cls, userid = None):
        anonymous = False
        user_changed = False

        if userid is None: # Usuario anónimo
            data = session.get("user", None)
            anonymous = True
        else: # Usuario logeado
            data = session.get("user", None)
            if data is None or data.get("anonymous", False): # Usuario sin sesión
                data = usersdb.find_userid(userid)
                user_changed = True

        if data is None: # Sin datos de usuario, datos de usuario anónimo
            data = {"anonymous": True}
            anonymous = True
            user_changed = True
        elif not anonymous: # Si tengo datos y no se si soy anónimo, compruebo
            anonymous = data.get("anonymous", False)

        if user_changed: # Los datos de usuario han cambiado
            data["session_id"] = cls.generate_session_id()
            data["session_ip"] = md5(request.remote_addr or "127.0.0.1").hexdigest()
            session["user"] = data # Cuardamos en sesión

        a = AnonymousUser(data) if anonymous else User(userid, data)

        # TODO(felipe): borrar con error solucionado
        if a.id < 0 and not anonymous:
            logging.error("Inconsistencia de usuario logeado id negativo.", extra=locals())
            a = AnonymousUser(data)

        return a

class User(UserMixin, UserBase):
    '''
    Guarda los datos de usuario en sesión.
    '''

    def __init__(self, user_id, data = None, is_current=False):
        UserBase.__init__(self, data)
        self.id = userid_parse(user_id)

        if not data is None:
            self.load_data(data)

    def load_data(self, data):
        self.has_data = True

        for attr in ("username", "email", "karma", "lang", "location","active","type"):
            if attr in data:
                setattr(self, attr, data[attr])

        if "created" in data:
            self.created = data["created"].strftime("%d-%m-%Y, %H:%M:%S")

    def set_lang(self, lang):
        session["user"]["lang"] = lang
        session.modified = True

class AnonymousUser(AnonymousUserMixin, UserBase):
    def __init__(self, data):
        UserBase.__init__(self, data)
        self.id = - self.session_id_as_int() # IDs de usuario anónimos son negativas

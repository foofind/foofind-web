# -*- coding: utf-8 -*-
from flask import session
from flaskext.login import UserMixin
from datetime import datetime

class User(UserMixin):
    '''
    Guarda los datos de usuario en sesión.
    '''
    id = None
    type = None
    username = None
    email = None
    created = None
    lang = None
    location = None
    active = None
    has_data = False

    def __init__(self, user_id, data = None):
        self.id = user_id
        if not data is None:
            self.load_data(data)
        elif "user" in session:
            self.load_data(session["user"])

    def load_data(self, data):
        self.username = data["username"]
        self.has_data = True
        if "email" in data:
            self.email = data["email"]
        if "karma" in data:
            self.karma = data["karma"]
        if "created" in data:
            self.created = data["created"].strftime("%d-%m-%Y, %H:%M:%S")
        if "lang" in data:
            self.lang = data["lang"]
        if "location" in data:
            self.location = data["location"]
        if "active" in data:
            self.active = data["active"]
        if "type" in data:
            self.type = data["type"]

    def set_data(self, data):
        session["user"] = data
        self.load_data(data)

    def set_lang(self, lang):
        session["user"]["lang"] = lang
        session.modified = True

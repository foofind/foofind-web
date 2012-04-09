# -*- coding: utf-8 -*-
"""
    Servicios utilizados por la aplicación web de Foofind
"""
from foofind.services.db.filesstore import FilesStore
from foofind.services.db.pagesstore import PagesStore
from foofind.services.db.usersstore import UsersStore
from foofind.services.db.feedbackstore import FeedbackStore
from foofind.utils.profiler import Profiler
from foofind.utils.event import EventManager
from extensions import *

__all__ = ['babel', 'cache', 'auth', 'mail', 'send_mail', 'profiler', 'sentry', 'filesdb', 'usersdb', 'pagesdb', 'feedbackdb', 'eventmanager']

filesdb = FilesStore()
usersdb = UsersStore()
pagesdb = PagesStore()
feedbackdb = FeedbackStore()

eventmanager = EventManager()
profiler = Profiler()

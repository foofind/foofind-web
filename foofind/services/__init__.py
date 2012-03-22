# -*- coding: utf-8 -*-
"""
    Servicios utilizados por la aplicación web de Foofind
"""
from foofind.services.db.filesstore import FilesStore
from foofind.services.db.pagesstore import PagesStore
from foofind.services.db.usersstore import UsersStore
from foofind.services.db.feedbackstore import FeedbackStore
from foofind.utils.countupdater import CountUpdater
import extensions

__all__ = ['babel', 'cache', 'auth', 'mail', 'sentry', 'filesdb', 'usersdb', 'pagesdb', 'feedbackdb', 'countupdater']

babel = extensions.babel
cache = extensions.cache
auth = extensions.auth
mail = extensions.mail
sentry = extensions.sentry

filesdb = FilesStore()
usersdb = UsersStore()
pagesdb = PagesStore()
feedbackdb = FeedbackStore()

countupdater = CountUpdater()


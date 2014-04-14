# -*- coding: utf-8 -*-
"""
    Servicios utilizados por la aplicación web de Foofind
"""
# activacion de la nueva busqueda por variable de entorno
from os import environ
from foofind.services.search import Searchd

from foofind.services.db.filesstore import FilesStore
from foofind.services.db.pagesstore import PagesStore
from foofind.services.db.usersstore import UsersStore
from foofind.services.db.feedbackstore import FeedbackStore
from foofind.services.db.configstore import ConfigStore
from foofind.services.db.entitiesstore import EntitiesStore
from foofind.services.db.pluginstore import PluginStore
from foofind.utils.profiler import Profiler
from foofind.utils.event import EventManager
from foofind.utils.taming import TamingClient
from .ip_ranges import IPRanges
from extensions import *

__all__=['filesdb', 'usersdb', 'pagesdb', 'feedbackdb', 'configdb', 'entitiesdb', 'spanish_ips',
                'taming', 'eventmanager', 'profiler', 'searchd', 'plugindb', 'local_cache']

__all__.extend(extensions.__all__)

filesdb = FilesStore()
usersdb = UsersStore()
pagesdb = PagesStore()
feedbackdb = FeedbackStore()
configdb = ConfigStore()
entitiesdb = EntitiesStore()
plugindb = PluginStore()
spanish_ips = IPRanges()
taming = TamingClient()
eventmanager = EventManager()
profiler = Profiler()
searchd = Searchd()
local_cache = {}

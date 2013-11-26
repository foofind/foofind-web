# -*- coding: utf-8 -*-

import pymongo
import collections
import time
import os.path

try:
    import simplejson as json
except ImportError:
    import json

from foofind.utils import check_collection_indexes, logging
from flask import url_for, g

class Plugin(object):
    '''
    Plugin object abstraction object
    '''
    def __init__(self, name, title, user, summary="", description=None, category=None, images=(), installer=False, ct=None, ut=None, checksum=0, minversion="", params=None):
        self.name = name
        self.title = title
        self.user = user
        self.summary = summary
        self.description = summary if description is None else description
        self.installer = installer
        self.images = images
        self.created = ct or (time.time()-time.timezone)
        self.updated = ut or (time.time()-time.timezone)
        self.category = category
        self.checksum = checksum
        self.minversion = minversion
        self.params = params or {}

    @classmethod
    def from_data(cls, d, category_fnc):
        category = category_fnc(d.get("category"))
        plugin = cls(
            d["name"],
            d["title"],
            d["user"],
            d.get("summary", ""),
            d.get("description"),
            category,
            [(i, "") if isinstance(i, basestring) else i for i in d.get("images", ())] ,
            d.get("installer", False),
            d.get("ct", 0),
            d.get("ut", 0),
            d.get("checksum", 0),
            d.get("minversion", ""),
            d.get("params", {})
            )
        category.plugins.append(plugin)
        return plugin

    def get_download_json(self, request_os="all"):
        '''

        '''
        platform = request_os if self.installer else "all"
        return json.dumps({
            "name": self.name,
            "title": self.title,
            "summary": self.summary,
            "url": url_for('extras.download', lang=g.lang, name=self.name, platform=platform, _external=True),
            "installer": self.installer,
            "params": self.params.get(request_os, ()),
            })

    def to_data(self):
        '''

        '''
        # Normal properties
        data = {k: v for k, v in self.__dict__.iteritems() if not k.startswith("_") and not callable(v)}
        # Renames properties
        data["ct"] = data.pop("created")
        data["ut"] = data.pop("updated")
        # Changed values
        if isinstance(self.category, Category):
            data["category"] = self.category.name
        return data

    def __eq__(self, x):
        return id(self) == id(x) or \
               isinstance(x, self.__class__) and self.__dict__ == x.__dict__

class Category(object):
    def __init__(self, name, title):
        self.name = name
        self.title = title
        self.plugins = []

    @classmethod
    def from_name(cls, name, titles=None):
        return cls(
            name,
            titles.get(name, name) if titles else name
            )

    def __eq__(self, x):
        return id(self) == id(x) or \
               isinstance(x, self.__class__) and self.__dict__ == x.__dict__


class PluginStore(object):
    '''
    Clase para acceder a los plugins de foofind download manager
    '''
    _indexes = {
        "plugins": (
            {"key": [("ut", -1)]},
            {"key": [("name", 1)], "unique": True},
            {"key": [("category", 1)]},
            {"key": [("category", 1), ("ut", -1)]},
            {"key": [("category", 1), ("name", 1)]},
            ),
        }
    def init_app(self, app):
        self.max_pool_size = app.config["DATA_SOURCE_MAX_POOL_SIZE"]

        if app.config.get("DATA_SOURCE_EXTRAS_RS", None):
            self.options = {
                "replicaSet": app.config["DATA_SOURCE_EXTRAS_RS"],
                "read_preference": pymongo.read_preferences.ReadPreference.SECONDARY_PREFERRED,
                "secondary_acceptable_latency_ms": app.config.get("SECONDARY_ACCEPTABLE_LATENCY_MS", 15),
                "tag_sets":app.config.get("DATA_SOURCE_EXTRAS_RS_TAG_SETS",[{}])
                }
        else:
            self.options = {"slave_okay":True}

        self.image_path = app.config["EXTRAS_IMAGE_PATH"]
        self.download_path = app.config["EXTRAS_DOWNLOAD_PATH"]

        self.titles = app.config.get("EXTRAS_CATEGORY_TITLES", {None:"all"})
        self.conn = pymongo.MongoClient(app.config["DATA_SOURCE_EXTRAS"], max_pool_size=self.max_pool_size, **self.options)
        self.db = self.conn.plugins

        check_collection_indexes(self.db, self._indexes)

    def get_category(self, name):
        '''
        Get category from given name
        '''
        return Category.from_name(name, self.titles)

    def get_plugin(self, name):
        '''
        Get plugin given a name

        @type name: basestring
        @param name: plugin name

        @rtype Plugin object or None
        @return Plugin object or None if not found
        '''
        d = self.db.plugins.find_one({"name": name})
        category_fnc =  self.get_category
        return Plugin.from_data(d, category_fnc) if d else None

    def get_plugins_with_categories(self, category=None, page=0, size=10, order_by="name"):
        '''
        Get a category object and list of plugin objects
        '''
        order = [("category", 1)] if category is None else []
        order.append((order_by, -1 if order_by == "ut" else 1))
        categories = {}

        if category is None:
            select = None
        elif isinstance(category, basestring):
            select = {"category": category}
        elif hasattr(category, "__iter__"):
            select = {"category": {"$in": category}}
        category_fnc = lambda name: categories[name] if name in categories else categories.setdefault(name, self.get_category(name))
        cursor = self.db.plugins.find(select, skip=size*page, limit=size, sort=order)
        return [Plugin.from_data(d, category_fnc) for d in cursor], categories

    def count_plugins(self, categories=None):
        '''
        Get number of plugins.

        @type categories: list of strings or None
        @param categories: list of categories or None for all categories.

        @rtype int
        @return number of plugins in given categories or total count.
        '''
        if categories is None:
            return self.db.plugins.count()
        return self.db.plugins.find({"category": {"$in": categories}}).count()

    def get_plugins(self, *args, **kwargs):
        '''
        Same as `PluginStore.get_plugins_with_categories` without returning
        categories (they're already available into plugin objects).

        @rtype list
        @return list of `Plugin` instances.
        '''
        return self.get_plugins_with_categories(*args, **kwargs)[0]

    def get_plugins_by_name(self, names):
        '''
        Gets plugins for a list of names

        @type names: iterable of strings
        @param names: list of plugin names

        @rtype list
        @return list of `Plugin` instances.
        '''
        print names
        if isinstance(names, basestring):
            select = {"name": names}
        elif hasattr(names, "__iter__"):
            select = {"name": {"$in": names}}

        categories = {}
        category_fnc = lambda name: categories[name] if name in categories else categories.setdefault(name, self.get_category(name))
        cursor = self.db.plugins.find(select)
        return [Plugin.from_data(d, category_fnc) for d in cursor]

    def save_plugin(self, plugin):
        '''
        Save `Plugin` object or dict in plugin collection

        @type plugin: dict-like or Plugin
        @param plugin: `Plugin` object or dict representing plugin object.
        '''
        if isinstance(plugin, Plugin):
            plugin = plugin.to_data()
        self.db.plugins.save(plugin)

    def update_plugin(self, plugin):
        '''
        Update `Plugin` object or dict in plugin collection

        @type plugin: dict-like or Plugin
        @param plugin: `Plugin` object or dict representing plugin object.
        '''
        if isinstance(plugin, Plugin):
            plugin = plugin.to_data()
        self.db.plugins.update({"name":plugin["name"]}, plugin, upsert=True, multi=False)

    def get_categories(self):
        '''
        Get a list of all categories on plugins database

        @rtype `Plugin` object or None
        @return `Plugin` object or None if not found
        '''
        return map(
            self.get_category,
            self.db.plugins.find(sort=[("category", 1)]).distinct("category")
            )

    def get_image(self, name, image):
        '''
        Get image path for given plugin and platform

        @type name: string
        @param name: plugin name

        @type platform: string
        @param platform: used values: "all" (default), "windows"

        @rtype basestring
        @return plugin path in filesystem
        '''
        path = os.path.join(self.image_path, "%s.%s.png" % (name, image))
        return path if os.path.exists(path) else None

    def get_download(self, name, platform="all"):
        '''
        Get download file path for given plugin and platform

        @type name: string
        @param name: plugin name

        @type platform: string
        @param platform: used values: "all" (default), "windows"

        @rtype basestring
        @return plugin path in filesystem
        '''

        if platform == "windows":
            # Windows executable
            name += ".exe"
        path = os.path.join(self.download_path, name)
        return path if os.path.exists(path) else None

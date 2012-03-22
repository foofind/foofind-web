# -*- coding: utf-8 -*-
"""
    Módulo principal de la aplicación
"""

import os, os.path, logging
from flask import Flask, g, request, url_for, session, _request_ctx_stack, render_template
from babel import localedata

from flaskext.assets import Environment, Bundle

from foofind.user import User
from foofind.blueprints.index import index
from foofind.blueprints.page import page
from foofind.blueprints.user import user,init_oauth
from foofind.blueprints.files import files
from foofind.blueprints.control import control
from foofind.blueprints.api import api
from foofind.services import *
from foofind.templates import register_filters

from flaskext.babel import get_translations, gettext as _
from flaskext.login import current_user
from raven.contrib.flask import Sentry
from raven.conf import setup_logging
from raven.handlers.logging import SentryHandler

import defaults

def create_app(config=None, debug=False):
    '''
    Inicializa la aplicación Flask. Carga los siguientes módulos:
     - index: página de inicio
     - page: páginas estáticas
     - user: gestión del usuario
     - files: búsqueda y obtención de ficheros
     - status: servicio de monitorización de la aplicación

    Y además, inicializa los siguientes servicios:
     - Configuración: carga valores por defecto y modifica con el @param config
     - Web Assets: compilación y compresión de recursos estáticos
     - i18n: detección de idioma en la URL y catálogos de mensajes
     - Cache y auth: Declarados en el módulo services
     - Files y users: Clases para acceso a datos
    '''
    app = Flask(__name__)
    app.config.from_object(defaults)
    app.debug=debug

    # Configuración
    if config:
        app.config.from_object(config)

    # Gestión centralizada de errores
    sentry.init_app(app)
    logging.getLogger().setLevel(logging.DEBUG if debug else logging.INFO)
    setup_logging(SentryHandler(sentry.client))

    # Registra filtros de plantillas
    register_filters(app)

    # Oauth
    init_oauth(app)

    # Blueprints
    app.register_blueprint(index)
    app.register_blueprint(page)
    app.register_blueprint(user)
    app.register_blueprint(files)
    app.register_blueprint(control)
    app.register_blueprint(api)

    # Web Assets
    if not os.path.isdir(app.static_folder+"/gen"): os.mkdir(app.static_folder+"/gen")
    assets = Environment(app)
    assets.debug=app.debug

    assets.register('js_all', Bundle('js/jquery.js', 'js/jquery-ui.js', 'js/files.js', filters='rjsmin', output='gen/packed.js'))
    assets.register('css_all', Bundle('css/main.css', filters='cssutils', output='gen/packed.css'))
    assets.register('css_ie7', Bundle('css/ie7.css', filters='cssutils', output='gen/ie7.css'))

    assets.register('js_admin', Bundle('js/jquery.js', 'js/admin.js', filters='rjsmin', output='gen/admin_packed.js'))
    assets.register('css_admin', Bundle('css/admin.css', filters='cssutils', output='gen/admin_packed.css'))

    # Detección de idioma
    @app.url_defaults
    def add_language_code(endpoint, values):
        '''
        Añade el código de idioma a una URL que lo incluye.
        '''
        if 'lang' in values or not g.lang:
            return
        if app.url_map.is_endpoint_expecting(endpoint, 'lang'):
            values['lang'] = g.lang

    @app.url_value_preprocessor
    def pull_lang_code(endpoint, values):
        '''
        Carga el código de idioma en la variable global.
        '''
        # lista de idiomas permitidos
        available_langs=app.config["ALL_LANGS"]
        # se pone el idioma por defecto
        g.lang = app.config["LANGS"][0]
        # si el usuario esta logueado y tiene establecido el idioma se asigna ese
        if "user" in session and "lang" in session["user"]:
            g.lang = values.pop('lang', session["user"]["lang"])
        # sino se coge el que mas convenga dependiendo del que tenga establecido en su navegador
        elif request.accept_languages.values()!=[]:
            g.lang = values.pop('lang', request.accept_languages.best_match(available_langs))

        # se carga la lista de idiomas en el idioma actual
        g.languages=[(code,localedata.load(code)["languages"][code].capitalize(), code in app.config["BETA_LANGS"]) for code in available_langs]
        g.beta_lang=g.lang in app.config["BETA_LANGS"]

    # Traducciones
    babel.init_app(app)

    @babel.localeselector
    def get_locale():
        '''
        Devuelve el código del idioma activo.
        '''
        try: return g.lang
        except: return "en"

    # Cache
    cache.init_app(app)

    # Autenticación
    auth.setup_app(app)
    auth.login_view="user.login"
    auth.login_message="login_required"

    @auth.user_loader
    def load_user(userid):
        user = User(userid)
        if not user.has_data:
            data = usersdb.find_userid(userid)
            if data is None:
                return None
            user.set_data(data)
        return user

    # Mail
    mail.init_app(app)

    # Acceso a bases de datos
    filesdb.init_app(app)
    usersdb.init_app(app)
    pagesdb.init_app(app)
    feedbackdb.init_app(app)

    # Actualizador del contador de ficheros
    countupdater.init_app(filesdb, app.config["COUNT_UPDATE_INTERVAL"])
    countupdater.start()

    # Carga la traducción alternativa
    from babel import support
    fallback_lang = support.Translations.load(os.path.join(app.root_path, 'translations'), ["en"])

    @app.before_request
    def before_request():
        # ignora peticiones sin blueprint
        if request.blueprint is None: return
        # dominio de la web
        g.domain = request.url_root[7:-1] if "https" not in request.url_root else request.url_root[8:-1]
        # título de la página por defecto
        g.title = g.domain
        g.count_files = countupdater.lastcount

        # si no es el idioma alternativo, lo añade por si no se encuentra el mensaje
        if g.lang!="en": get_translations().add_fallback(fallback_lang)

    # Páginas de error
    @app.errorhandler(404)
    def page_not_found(e):
        return render_template('error.html',
            title=_("error_404_message"),
            error_message=_("error_404_message"),
            error_description=_("error_404_description")
            ), 404

    @app.errorhandler(502)
    def bad_gateway(e):
        return render_template('error.html',
            title=_("error_502_message"),
            error_message=_("error_502_message"),
            error_description=_("error_502_description")
            ), 502

    @app.context_processor
    def insert_title():
        return {"title": g.title}

    return app


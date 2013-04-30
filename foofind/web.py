# -*- coding: utf-8 -*-
"""
    Módulo principal de la aplicación Web
"""
import foofind.globals
import os, os.path, defaults

from collections import OrderedDict
from flask import Flask, g, request, session, render_template, redirect, abort, url_for, make_response, current_app
from flask.ext.assets import Environment, Bundle
from flask.ext.babel import get_translations, gettext as _
from flask.ext.login import current_user
from babel import support, localedata, Locale
from raven.contrib.flask import Sentry
from webassets.filter import register_filter
from hashlib import md5

from foofind.user import User
from foofind.blueprints.index import index
from foofind.blueprints.page import page
from foofind.blueprints.user import user,init_oauth
from foofind.blueprints.files import files
from foofind.blueprints.api import api
from foofind.blueprints.downloads import downloads, track_downloader_info
from foofind.blueprints.labs import add_labs, init_labs
from foofind.services import *
from foofind.templates import register_filters
from foofind.utils.webassets_filters import JsSlimmer, CssSlimmer
from foofind.utils import u, logging
from foofind.forms.files import SearchForm
from foofind.utils.exceptions import allerrors, get_error_code_information
from foofind.utils.bots import is_search_bot, is_full_browser, check_rate_limit

try:
    from uwsgidecorators import postfork
    @postfork
    def start_eventmanager():
        # Inicio del eventManager
        eventmanager.start()
except ImportError:
    pass

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
    app.debug = debug

    # Configuración
    if config:
        app.config.from_object(config)

    # Gestión centralizada de errores
    if app.config["SENTRY_DSN"]:
        sentry.init_app(app)
    logging.getLogger().setLevel(logging.DEBUG if debug else logging.INFO)

    # Configuración dependiente de la versión del código
    revision_filename_path = os.path.join(os.path.dirname(app.root_path), "revision")
    if os.path.exists(revision_filename_path):
        f = open(revision_filename_path, "r")
        data = f.read()
        f.close()
        revisions = tuple(
            tuple(i.strip() for i in line.split("#")[0].split())
            for line in data.strip().split("\n")
            if line.strip() and not line.strip().startswith("#"))
        revision_hash = md5(data).hexdigest()
        app.config.update(
            CACHE_KEY_PREFIX = "%s%s/" % (
                app.config["CACHE_KEY_PREFIX"] if "CACHE_KEY_PREFIX" in app.config else "",
                revision_hash
                ),
            REVISION_HASH = revision_hash,
            REVISION = revisions
            )
    else:
        app.config.update(
            REVISION_HASH = None,
            REVISION = ()
            )

    # Registra filtros de plantillas
    register_filters(app)

    # Registra valores/funciones para plantillas
    app.jinja_env.globals["u"] = u
    app.jinja_env.auto_reload = debug

    # Oauth
    init_oauth(app)

    # Blueprints
    app.register_blueprint(index)
    app.register_blueprint(page)
    app.register_blueprint(user)
    app.register_blueprint(files)
    app.register_blueprint(api)
    app.register_blueprint(downloads)
    add_labs(app) # Labs (blueprints y alternativas en pruebas)

    # Web Assets
    if not os.path.isdir(os.path.join(app.static_folder,"gen")):
        os.mkdir(os.path.join(app.static_folder,"gen"))
    app.assets = assets = Environment(app)
    assets.debug = app.debug
    assets.versions = "timestamp"

    register_filter(JsSlimmer)
    register_filter(CssSlimmer)

    assets.register('css_all', 'css/jquery-ui.css', Bundle('css/main.css', filters='pyscss', output='gen/main.css', debug=False), filters='css_slimmer', output='gen/foofind.css')
    assets.register('css_ie', 'css/ie.css', filters='css_slimmer', output='gen/ie.css')
    assets.register('css_ie7', 'css/ie7.css', filters='css_slimmer', output='gen/ie7.css')
    assets.register('css_labs', 'css/jquery-ui.css', Bundle('css/labs.css', filters='pyscss', output='gen/l.css', debug=False), filters='css_slimmer', output='gen/labs.css')
    assets.register('css_admin', Bundle('css/admin.css', 'css/jquery-ui.css', filters='css_slimmer', output='gen/admin.css'))
    assets.register('css_foodownloader', Bundle('css/foodownloader.css', filters='css_slimmer', output='gen/foodownloader.css'))

    assets.register('js_all', Bundle('js/jquery.js', 'js/jquery-ui.js', 'js/jquery.ui.selectmenu.js', 'js/files.js', filters='rjsmin', output='gen/foofind.js'), )
    assets.register('js_ie', Bundle('js/html5shiv.js', 'js/jquery-extra-selectors.js', 'js/jquery.ba-hashchange.js', 'js/selectivizr.js', filters='rjsmin', output='gen/ie.js'))
    assets.register('js_search', Bundle('js/jquery.hoverIntent.js', 'js/search.js', filters='rjsmin', output='gen/search.js'))
    assets.register('js_labs', Bundle('js/jquery.js', 'js/jquery-ui.js', 'js/labs.js', filters='rjsmin', output='gen/labs.js'))
    assets.register('js_admin', Bundle('js/jquery.js',  'js/jquery-ui-admin.js', 'js/admin.js', filters='rjsmin', output='gen/admin.js'))
    assets.register('js_foodownloader', Bundle('js/jquery.js', 'js/foodownloader.js', filters='rjsmin', output='gen/foodownloader.js'))

    # proteccion CSRF
    csrf.init_app(app)

    # Detección de idioma
    @app.url_defaults
    def add_language_code(endpoint, values):
        '''
        Añade el código de idioma a una URL que lo incluye.
        '''
        if 'lang' in values or not g.lang:
            return
        #if endpoint in app.view_functions and hasattr(app.view_functions[endpoint], "_fooprint"):
        #    return
        if app.url_map.is_endpoint_expecting(endpoint, 'lang'):
            values['lang'] = g.lang

    all_langs = app.config["ALL_LANGS"]
    pull_lang_code_languages = OrderedDict((code, (localedata.load(code)["languages"], code in app.config["BETA_LANGS"])) for code in all_langs)
    @app.url_value_preprocessor
    def pull_lang_code(endpoint, values):
        '''
        Carga el código de idioma en la variable global.
        '''
        # obtiene el idioma de la URL
        g.url_lang = None
        if values is not None:
            g.url_lang = values.pop('lang', None)

        # si esta lista de idiomas permitidos
        if g.url_lang and g.url_lang in all_langs:
            g.lang = g.url_lang
        # si el usuario esta logueado y tiene establecido el idioma se asigna ese
        elif "user" in session and "lang" in session["user"]:
            g.lang = session["user"]["lang"]
        # si no esta logueado y ha elegido un idioma
        elif "lang" in session:
            g.lang = session["lang"]
        else:
            accept = request.accept_languages.values()
            # si viene, se coge el que mas convenga dependiendo del que tenga establecido en su navegador o el idioma por defecto
            locale = Locale.negotiate((option.replace("-","_") for option in accept), all_langs) if accept else None

            if locale:
                g.lang = locale.language
            else:
                g.lang = app.config["LANGS"][0] # valor por defecto si todo falla

        if g.lang not in all_langs:
            logging.warn("Wrong language choosen.")
            g.lang = app.config["LANGS"][0]

        # se carga la lista de idiomas como se dice en cada idioma
        g.languages = pull_lang_code_languages
        g.beta_lang = g.lang in app.config["BETA_LANGS"]

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
    configdb.register_action("flush_cache", cache.clear, _unique=True)

    # Autenticación
    auth.setup_app(app)
    auth.login_view="user.login"
    auth.login_message="login_required"
    auth.user_loader(User.current_user)
    auth.anonymous_user = User.current_user

    # Mail
    mail.init_app(app)

    # Acceso a bases de datos
    filesdb.init_app(app)
    usersdb.init_app(app)
    pagesdb.init_app(app)
    feedbackdb.init_app(app)
    configdb.init_app(app)
    entitiesdb.init_app(app)
    downloadsdb.init_app(app)

    # Servicio de búsqueda
    @app.before_first_request
    def init_process():
        if not eventmanager.is_alive():
            # Fallback inicio del eventManager
            eventmanager.start()

    # Taming
    taming.init_app(app)

    # Profiler
    profiler.init_app(app, feedbackdb)

    eventmanager.once(searchd.init_app, hargs=(app, filesdb, entitiesdb, profiler))

    # Refresco de conexiones
    eventmanager.once(filesdb.load_servers_conn)
    eventmanager.interval(app.config["FOOCONN_UPDATE_INTERVAL"], filesdb.load_servers_conn)
    eventmanager.interval(app.config["FOOCONN_UPDATE_INTERVAL"], entitiesdb.connect)

    # Refresco de configuración
    eventmanager.once(configdb.pull)
    eventmanager.interval(app.config["CONFIG_UPDATE_INTERVAL"], configdb.pull)

    # guarda registro de downloader
    eventmanager.interval(app.config["CONFIG_UPDATE_INTERVAL"], track_downloader_info)

    # Carga la traducción alternativa
    fallback_lang = support.Translations.load(os.path.join(app.root_path, 'translations'), ["en"])

    # Unittesting
    unit.init_app(app)

    # Inicializa
    init_labs(app)

    if app.config["UNITTEST_INTERVAL"]:
        eventmanager.timeout(20, unit.run_tests)
        #eventmanager.interval(app.config["UNITTEST_INTERVAL"], unit.run_tests)

    @app.before_request
    def before_request():

        # No preprocesamos la peticiones a static
        if request.path.startswith("/static"):
            return

        # default values for g object
        init_g()

        # comprueba limite de ratio de peticiones
        check_rate_limit(g.search_bot)

        # si el idioma de la URL es inválido, devuelve página no encontrada
        if g.url_lang and not g.url_lang in all_langs:
            abort(404)

        # si no es el idioma alternativo, lo añade por si no se encuentra el mensaje
        if g.lang!="en":
            get_translations().add_fallback(fallback_lang)

        # si hay que cambiar el idioma
        if request.args.get("setlang",None):
            session["lang"]=g.lang
            # si el idioma esta entre los permitidos y el usuario esta logueado se actualiza en la base de datos
            if g.lang in all_langs and current_user.is_authenticated():
                current_user.set_lang(g.lang)
                usersdb.update_user({"_id":current_user.id,"lang":g.lang})

            return redirect(request.base_url)

        g.keywords = set(_(keyword) for keyword in ['download', 'watch', 'files', 'submit_search', 'audio', 'video', 'image', 'document', 'software', 'P2P', 'direct_downloads'])
        descr = _("about_text")
        g.page_description = descr[:descr.find("<br")]

        g.foodownloader = app.config["FOODOWNLOADER"] and (request.user_agent.platform == "windows")

        # ignora peticiones sin blueprint
        if request.blueprint is None and request.path.endswith("/"):
            if "?" in request.url:
                root = request.url_root[:-1]
                path = request.path.rstrip("/")
                query = u(request.url)
                query = query[query.find(u"?"):]
                return redirect(root+path+query, 301)
            return redirect(request.url.rstrip("/"), 301)

    @app.after_request
    def after_request(response):
        if request.user_agent.browser == "msie": response.headers["X-UA-Compatible"] = "IE-edge"
        return response

    # Páginas de error

    @allerrors(app, 400, 401, 403, 404, 405, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 429, 500, 501, 502, 503)
    def all_errors(e):
        error_code, error_title, error_description = get_error_code_information(e)
        try:
            init_g()
            g.page_description = g.title = "%d %s" % (error_code, error_title)
            return render_template('error.html', zone="error", error=error_code, description=error_description, search_form=SearchForm()), error_code
        except BaseException as ex: #si el error ha llegado sin contexto se encarga el servidor de él
            logging.warn(ex)
            return make_response("", error_code)

    return app

def init_g():
    # argumentos de busqueda por defecto
    g.args = {}
    g.active_types = {}
    g.active_srcs = {}

    # caracteristicas del cliente
    g.full_browser=is_full_browser()
    g.search_bot=is_search_bot()

    # peticiones en modo preproduccion
    g.beta_request = request.url_root[request.url_root.index("//")+2:].startswith("beta.")

    # prefijo para los contenidos estáticos
    if g.beta_request:
        app_static_prefix = current_app.static_url_path
    else:
        app_static_prefix = current_app.config["STATIC_PREFIX"] or current_app.static_url_path
    g.static_prefix = app_static_prefix
    current_app.assets.url = app_static_prefix + "/"

    g.autocomplete_disabled = "false" if current_app.config["SERVICE_TAMING_ACTIVE"] else "true"

    # dominio de la web
    g.domain = "foofind.is"

    # informacion de la página por defecto
    g.title = g.domain
    g.keywords = set()
    g.page_description = g.title

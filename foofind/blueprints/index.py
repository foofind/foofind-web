# -*- coding: utf-8 -*-
"""
    Controlador de la portada.
"""
import os
import urllib
import datetime
import httplib
import time
import threading
from urlparse import urlparse
from werkzeug import url_unquote
from flask import Blueprint, render_template, redirect, url_for, g, make_response, current_app, request, send_from_directory, abort, get_flashed_messages, session
from flask.ext.babel import get_translations, gettext as _
from flask.ext.login import current_user
from flask.ext.seasurf import SeaSurf
from urlparse import urlparse

from foofind.forms.files import SearchForm
from foofind.services import *
from foofind.utils import u, logging
from foofind.utils.fooprint import Fooprint

index = Fooprint('index', __name__, template_folder="template", dup_on_startswith="/<lang>")

def gensitemap(server, urlformat):
    '''
    Crea la ruta del índice de sitemap para el servidor de archivos dado.
    Se conecta a los índices de segundo nivel y obtiene su fecha de modificación.

    @type server: dict-like
    @param server: Documento del servidor tal cual viene de MongoDB

    @rtype tuple (str, datetime) o None
    @return tupla con la url y su fecha de modificación, o None si no se puede
            obtener la url.
    '''
    subdomain = server["ip"].split(".")[0]
    serverno = int(subdomain[6:])
    url = urlformat % serverno
    domain = urlparse(url)[1]
    con = httplib.HTTPConnection(domain)
    con.request("HEAD", url)
    response =  con.getresponse()

    if response.status == 200:
        mtime = time.mktime(time.strptime(
           response.getheader("last-Modified"),
            "%a, %d %b %Y %H:%M:%S %Z"))
        return (url, datetime.datetime.fromtimestamp(mtime))

    return None

# contenidos de la raiz del sitio
@index.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(current_app.root_path, 'static'), 'favicon.ico', mimetype='image/vnd.microsoft.icon')

@index.route('/robots.txt')
def robots():
    return send_from_directory(os.path.join(current_app.root_path, 'static'), 'robots.txt')

@index.route('/BingSiteAuth.xml')
def bing():
    return '<?xml version="1.0"?><users><user>8AC0F33B6CD35133906047D3976BD2F5</user></users>'

@index.route('/yandex_710d63b404ebcae8.txt')
def yandex():
    return ''

@cache.cached(
    timeout=86400, # Un día
    unless=lambda:True
    )
@index.route('/sitemap.xml')
def sitemap():
    urlformat = current_app.config["FILES_SITEMAP_URL"]
    servers = filesdb.get_servers()
    rules = []
    threads = [
        threading.Thread(
            target = lambda x: rules.append( gensitemap(x, urlformat ) ),
            args = ( server, ) )
        for server in servers ]
    for t in threads:
        t.start()
    for t in threads:
        if t.is_alive():
            t.join()
    if None in rules:
        rules.remove(None)
        logging.error("Hay sitemaps no disponibles", extra=(servers, rules))
    return render_template('sitemap.xml', rules=rules)

@index.route('/<lang>/opensearch.xml')
def opensearch():
    response = make_response(render_template('opensearch.xml',shortname = "Foofind",description = _("opensearch_description")))
    response.headers['content-type']='application/opensearchdescription+xml'
    return response

@index.route('/')
@index.route('/<lang>')
@cache.cached(
    timeout=50,
    key_prefix=lambda: "view/index_%s" % g.lang,
    unless=lambda: current_user.is_authenticated() or bool(get_flashed_messages())
    )
def home():
    '''
    Renderiza la portada.
    '''
    return render_template('index.html',form=SearchForm(),lang=current_app.config["ALL_LANGS_COMPLETE"][g.lang],zone="home")

@index.route("/status")
def status():
    '''
    Comprueba si existe un token csrf valido
    '''
    if csrf._get_token(): #si hay token no hace nada
        return ""
    else: #sino genera uno nuevo y lo devuelve
        session["_csrf_token"]=csrf._generate_token()
        return session["_csrf_token"]

@index.route("/error/<int:num>")
def error(num):
    '''
    Devuelve una página de error con el número que se le mande
    '''
    if num>499 and num<505:
        return abort(num)

    return make_response()

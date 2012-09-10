# -*- coding: utf-8 -*-
"""
    Controlador de la portada.
"""
import os
from werkzeug import url_unquote
from flask import Blueprint, render_template, redirect, url_for, g, make_response, current_app, request, send_from_directory, abort, get_flashed_messages, session
from flask.ext.babel import get_translations, gettext as _
from flask.ext.login import current_user
from foofind.forms.files import SearchForm
from foofind.services import *
from foofind.forms.captcha import generate_image
from foofind.utils import u
from urlparse import urlparse
import urllib
import logging

import datetime
import httplib
import time
import threading

from foofind.utils.fooprint import Fooprint

index = Fooprint('index', __name__, template_folder="template")

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
    domain = urlparse.urlparse(url)[1]
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

@cache.cached(
    timeout=86400, # Un día
    unless=lambda:True
    )
#@index.route('/sitemap.xml')
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

@index.route('/<lang>/setlang')
def setlang():
    '''
    Cambia el idioma
    '''
    session["lang"]=g.lang
    # si el idioma esta entre los permitidos y el usuario esta logueado se actualiza en la base de datos
    if g.lang in current_app.config["ALL_LANGS"] and current_user.is_authenticated():
        current_user.set_lang(g.lang)
        usersdb.update_user({"_id":current_user.id,"lang":g.lang})

    # si se puede se redirige a la pagina en la que se estaba
    if request.referrer:
        parts = url_unquote(request.referrer).split("/")
        if parts[0] in ("http:","https:"):
            parts = parts[3:]

        query_string=urlparse(request.url).query
        return redirect("/%s/%s%s" % (g.lang, "/".join(parts[1:]), "?"+query_string if query_string!="" else ""))
    else:
        return redirect(url_for("index.home"))

@index.route("/captcha/<captcha_id>")
def captcha(captcha_id):
    try:
        code = cache.get("captcha/%s" % captcha_id)
        if code is None:
            abort(404)
    except BaseException as e:
        logging.error(e)
        abort(404)
    response = make_response(generate_image(code))
    response.headers['Content-Type'] = 'image/png'
    return response

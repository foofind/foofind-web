# -*- coding: utf-8 -*-
"""
    Controlador de la portada.
"""
import os
from werkzeug import url_unquote
from flask import Blueprint, render_template, redirect, url_for, g, make_response, current_app, request, send_from_directory, abort
from flaskext.babel import get_translations, gettext as _
from flaskext.login import current_user
from foofind.forms.files import SearchForm
from foofind.services import *
from foofind.forms.captcha import generate_image
from urlparse import urlparse
import urllib
import logging

index = Blueprint('index', __name__, template_folder="template")

# contenidos de la raiz del sitio
@index.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(current_app.root_path, 'static'), 'favicon.ico', mimetype='image/vnd.microsoft.icon')

@index.route('/robots.txt')
def robots():
    return send_from_directory(os.path.join(current_app.root_path, 'static'), 'robots.txt')

#@index.route('/sitemap.xml')
def sitemap():
    return render_template('sitemap.xml', url_root=request.url_root[:-1], rules=current_app.url_map.iter_rules())

@index.route('/<lang>/opensearch.xml')
def opensearch():
    response = make_response(render_template('opensearch.xml', shortname="Foofind", description=urllib.quote_plus(_("opensearch_description"))))
    response.headers['content-type']='application/opensearchdescription+xml'
    return response


@index.route('/')
@index.route('/<lang>')
@cache.cached(timeout=50)
def home():
    '''
    Renderiza la portada.
    '''
    return render_template('index.html',form=SearchForm(),zone="home")

@index.route('/<lang>/setlang')
def setlang():
    '''
    Cambia el idioma
    '''
    # si el idioma esta entre los permitidos y el usuario esta logueado se actualiza en la base de datos
    if g.lang in current_app.config["ALL_LANGS"] and current_user.is_authenticated():
        current_user.set_lang(g.lang)
        usersdb.update_user({"_id":current_user.id,"lang":g.lang})

    # si se puede se redirige a la pagina en la que se estaba
    if request.referrer:
        parts = url_unquote(request.referrer).split("/")
        if parts[0] in ("http:","https:"):
            parts = parts[3:]
        return redirect("/%s/%s" % (g.lang, "/".join(parts[1:])))
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

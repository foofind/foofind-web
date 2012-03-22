# -*- coding: utf-8 -*-
"""
    Controlador de la portada.
"""
import os
from werkzeug import url_unquote
from flask import Blueprint, render_template, redirect, url_for, g, make_response, current_app, request, send_from_directory
from flaskext.babel import get_translations
from flaskext.login import current_user
from foofind.forms.files import SearchForm
from foofind.services import *
from foofind.forms.captcha import generate_image
from urlparse import urlparse

index = Blueprint('index', __name__, template_folder="template")

# contenidos de la raiz del sitio
@index.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(current_app.root_path, 'static'), 'favicon.ico', mimetype='image/vnd.microsoft.icon')

@index.route('/robots.txt')
def robots():
    return send_from_directory(os.path.join(current_app.root_path, 'static'), 'robots.txt')

@index.route('/sitemap.xml')
def sitemap():
    return render_template('sitemap.xml', url_root=request.url_root[:-1], rules=current_app.url_map.iter_rules())

@index.route('/<lang>/opensearch.xml')
def opensearch():
    return render_template('opensearch.xml', shortname="Foofind")

@index.route('/<lang>')
def homeRedirect():
    '''
    Los accesos a portada de un idioma sin la / final se redirigen.
    '''
    return redirect(url_for("index.home"))

@cache.cached(timeout=50)
@index.route('/')
@index.route('/<lang>/')
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
    url = None if request.referrer is None else request.referrer.split("/")[3]
    if url:
        return redirect(url_unquote(g.lang+request.referrer[request.referrer.find("/"+url+"/")+len(url)+1:]))
    else:
        return redirect(url_for("index.home"))

@index.route("/captcha/<captcha_id>")
def captcha(captcha_id):
    response = make_response(cache.get("captcha_"+captcha_id))
    cache.delete("captcha_"+captcha_id)
    response.headers['Content-Type'] = 'image/png'
    return response

# -*- coding: utf-8 -*-
"""
    Controladores de páginas de usuario.
"""
from flask import Blueprint, request, render_template, redirect, flash, url_for, session, abort, current_app, g, jsonify
from flask.ext.babel import gettext as _
from flask.ext.login import login_required, login_user, logout_user, current_user
from flask.ext.oauth import OAuth

from foofind.forms.user import RegistrationForm, LoginForm, ForgotForm, EditForm
from foofind.services import *
from foofind.user import User
from foofind.blueprints.files import get_file_metadata, fill_data, get_files
from foofind.utils import hex2mid,url2mid
from foofind.utils.fooprint import Fooprint

from hashlib import md5
from urllib import unquote

import uuid
import re
import logging

user = Fooprint('user', __name__, template_folder="template", dup_on_startswith="/<lang>")
o_twitter = None
o_facebook = None

@user.context_processor
def user_globals():
    return {"zone": "user"}

@user.before_request
def set_search_form():
    g.title+=" - "

def init_oauth(app):
    global o_twitter, o_facebook, oauth_authorized

    oauth = OAuth()
    o_twitter = oauth.remote_app('twitter',
        base_url='https://api.twitter.com/1/',
        request_token_url='https://api.twitter.com/oauth/request_token',
        access_token_url='https://api.twitter.com/oauth/access_token',
        authorize_url='https://api.twitter.com/oauth/authenticate',
        consumer_key=app.config["OAUTH_TWITTER_CONSUMER_KEY"],
        consumer_secret=app.config["OAUTH_TWITTER_CONSUMER_SECRET"],
        access_token_method='POST'
    )
    user.add_url_rule('/oauth/twitter', "twitter_authorized", o_twitter.authorized_handler(twitter_authorized))
    o_twitter.tokengetter(oauth_token)

    o_facebook = oauth.remote_app('facebook',
        base_url='https://graph.facebook.com/',
        request_token_url=None,
        access_token_url='/oauth/access_token',
        authorize_url=app.config["OAUTH_FACEBOOK_SITE_URL"],
        consumer_key=app.config["OAUTH_FACEBOOK_CONSUMER_KEY"],
        consumer_secret=app.config["OAUTH_FACEBOOK_CONSUMER_SECRET"],
        request_token_params={'scope': 'email'}
    )
    user.add_url_rule('/oauth/facebook', "facebook_authorized", o_facebook.authorized_handler(facebook_authorized))
    o_facebook.tokengetter(oauth_token)

@user.route('/<lang>/auth/<pname>')
def old_show(pname):
    '''
    Si la url es antigua se redirecciona con un 301 a la nueva
    '''
    try:
        return redirect(url_for("user."+pname),301)
    except:
        return abort(401)

@user.route('/<lang>/user/register', methods=['GET', 'POST'])
def register():
    '''
    Página para registrar usuario.
    '''
    error=None
    data=dict()
    form = RegistrationForm(request.form)
    if request.method=='POST' and form.validate():
        if not usersdb.find_username(form.username.data) is None:
            flash("username_taken")
        elif not usersdb.find_email(form.email.data) is None:
            flash("email_taken")
        else:
            data["token"]=md5(str(uuid.uuid4())).hexdigest();
            for field in form:
                data[field.id]=field.data

            if send_mail(data["username"]+_("confirm_email"),data["email"],'register',token=data["token"]):
                usersdb.create_user(data)
                flash("check_inbox_email_finish")
                return redirect(url_for('index.home'))

    g.title+=_("new_user").capitalize()
    return render_template('user/register.html',form=form)

def login_redirect(data,rememberme=True):
    '''
    Realiza el login de usuario y la redirección con mensaje a la home
    '''
    flash(_("logged_in")+" "+data["username"])
    login_user(User(data["_id"], data),rememberme)
    if "next" in request.args:
        return redirect(unquote(request.args["next"]))

    return redirect(url_for('index.home',lang=None))

@user.route('/<lang>/user/login', methods=['GET', 'POST'])
def login():
    '''
    Página para loguear usuario.
    '''
    error=None
    form=LoginForm(request.form)
    if request.method=='POST' and form.validate():
        data=usersdb.find_login(form.email.data, form.password.data)
        if data is not None:
            return login_redirect(data,form.rememberme.data)
        else:
            flash("wrong_email_password")

    g.title+=_("user_login")
    return render_template('user/login.html',form=form)

@user.route('/<lang>/ajax/login')
def ajax_login():
    '''
    Login cargado por ajax,
    '''
    signin_form = RegistrationForm()
    signin_form.action = url_for(".register")
    loging_form = LoginForm()
    loging_form.action = url_for(".login")

    return jsonify(
        html = render_template("user/ajax_login.html", signin=signin_form, login=loging_form)
        )

def clean_username(username):
    '''
    Adaptador para los nombres de usuario que se usan en los registros por oauth
    '''
    # quitar caracteres no alfanumericos
    username=re.sub("\W","",username)
    # añadir "u" si el nombre de usuario empieza por numero o "_"
    if re.match("^[0-9_]",username):
        username="u"+username
    # concatenar Foofy si la longitud del usuario es menor 3
    if len(username)<3:
        username+="Foofy"
    # se traen todos los nombre de usuario que comiencen por el nombre de usuario y opcionalmente continuen por un numero
    user=usersdb.find_username_start_with(username)
    users_repeat=[]
    for user_repeat in user:
        users_repeat.append(user_repeat["username"])
    # si el nombre de usuario ya existe se recorren todos para ponerle el siguiente
    if username in users_repeat:
        i=1
        while username+'_'+str(i) in users_repeat:
            i=i+1

        username=username+'_'+str(i)

    return username

def oauth_token():
    '''
    Devuelve el token para los logueos por oauth
    '''
    return session.get('oauth_token')

def oauth_redirect():
    '''
    Realiza la redirección a la página que corresponda al hacer un login por oauth
    '''
    return request.args.get('next') or (request.referrer if request.referrer not in (url_for('.login',_external=True),url_for('.register',_external=True)) else url_for('index.home'))

@user.route('/<lang>/user/login/twitter')
def twitter():
    '''
    Acceso a traves de twitter
    '''
    try:
        logout_oauth()
        return o_twitter.authorize(url_for('.twitter_authorized',next=oauth_redirect()))
    except BaseException as e:
        logging.warn(e)

    flash(_("technical_problems", service="twitter"))
    return redirect(url_for('.login'))

def twitter_authorized(resp):
    '''
    Manejador de la autorizacion a traves de twitter
    '''
    if resp is None:
        flash("token_not_exist")
        return redirect(url_for('.login'))

    session['oauth_token']=(resp['oauth_token'],resp['oauth_token_secret'])
    # peticion para obtener los datos extra
    extra=o_twitter.get('account/verify_credentials.json')
    # se busca el usuario por id y si no existe en la base de datos se crea el usuario
    data=usersdb.find_oauthid(resp["user_id"]+"@twitter.com")
    if data is None:
        data={}
        data["username"]=clean_username(resp['screen_name'])
        data["email"]=""
        data["password"]=""
        data["token"]=""
        # se crea el usuario guardando el id para luego poder actualizarlo
        data["_id"]=usersdb.create_user(data)
        data["oauthid"]=resp["user_id"]+"@twitter.com"
        data["location"]=extra.data["location"]
        data["lang"]=extra.data["lang"]
        data['active']=1
        usersdb.update_user(data,["token","password"])

    return login_redirect(data)

@user.route('/<lang>/user/login/facebook')
def facebook():
    '''
    Acceso a traves de facebook
    '''
    try:
        logout_oauth()
        return o_facebook.authorize(url_for('.facebook_authorized',next=oauth_redirect(),_external=True))
    except BaseException as e:
        logging.warn(e)

    flash(_("technical_problems", service="facebook"))
    return redirect(url_for('.login'))

def facebook_authorized(resp):
    '''
    Manejador de la autorizacion a traves de facebook
    '''
    if resp is None:
        flash("token_not_exist")
        return redirect(url_for('.login'))

    session['oauth_token']=(resp['access_token'],'')
    me=o_facebook.get('/me')
    # se busca el usuario por email y si no existe en la base de datos se crea el usuario
    data=usersdb.find_email(me.data["email"])
    # comprobacion para actualizar los usuarios antiguos que no tuvieran correo
    if data is None:
        data=usersdb.find_oauthid(me.data['id']+"@facebook.com")

    if data is None:
        data={}
        # si no tiene nick se utiliza el nombre completo
        if me.data["link"].find("profile.php?id=") > 0:
            data["username"]=clean_username(me.data["name"])
        else:
            data["username"]=clean_username(me.data["link"][me.data["link"].find("/",12):])

        data["email"]=me.data["email"]
        data["password"]=""
        data["token"]=""
        # se crea el usuario guardando el id para luego poder actualizarlo
        data["_id"]=usersdb.create_user(data)
        data["oauthid"]=me.data['id']+"@facebook.com"
        data["lang"]=me.data["locale"][:2]
        data['active']=1
        usersdb.update_user(data,["token","password"])

    return login_redirect(data)

def logout_oauth():
    '''
    En caso de existir en sesion algun token de oauth se borra
    '''
    if "oauth_token" in session:
        del(session['oauth_token'])

@user.route("/<lang>/user/logout")
@login_required
def logout():
    '''
    Página para desloguear usuario.
    '''
    if current_user.is_authenticated():
        logout_user()
        logout_oauth()
        del(session["user"])

    return redirect(url_for('index.home',lang=None))

def get_files_metadata(files, block_on_sphinx=True):
    names = {hex2mid(i["id"]):i["name"] for i in files}
    for f in get_files(
      [(i["id"], i["server"]) for i in files],
      block_on_sphinx = block_on_sphinx):
        yield fill_data(f, names[f["_id"]])

def list_type_parser(txt):
    return txt if txt in ("fav","user","search") else "fav"

@user.route("/<lang>/user")
@login_required
def profile():
    '''
    Página de perfil de usuario
    '''
    g.title+=_("user_profile")

    list_type = request.args.get("list", "fav", list_type_parser)
    list_name = request.args.get("name", None)

    favlist = ()

    user_fav_lists = usersdb.list_fav_lists(current_user)

    if list_type == "fav":
        filelist = usersdb.get_fav_files(current_user)
        if filelist: favlist = get_files_metadata(filelist, False)
    elif list_type == "user":
        filelist = usersdb.get_fav_user_list(current_user, name)
        if filelist: favlist = get_files_metadata(filelist, False)
    elif list_type == "search":
        favlist = usersdb.get_fav_searches(current_user)

    return render_template(
        'user/profile.html',
        user_fav_lists = user_fav_lists,
        list_type = list_type,
        list_name = list_name,
        favlist = favlist,
        )

@user.route("/<lang>/favorite/<favorite_type>/<action>/<server>/<file_id>/<file_name>/<redirection>")
@user.route("/<lang>/favorite/<favorite_type>/<action>/<favorite_name>/<server>/<file_id>/<file_name>/<redirection>")
@user.route("/<lang>/favorite",methods=['POST'])
@login_required
def favorite(favorite_type=None,action=None,server=0,file_id=None,file_name=None,favorite_name=None,redirection=0):
    try:
        favorite_type=request.form.get("type",favorite_type)
        action=request.form.get("action",action)
        server=int(request.form.get("where",server)) #where
        file_id=url2mid(request.form.get("file",file_id))
        file_name=request.form.get("name",file_name)
        favorite_name=request.form.get("favorite_name",favorite_name)
        redirection=int(request.form.get("login",redirection)) #login
        if not favorite_type or not action or server<=0 or not file_id or not file_name or redirection<=0:
            raise Exception
    except:
        return _("error_500_message"),404

    if action=="add":
        if favorite_type == "file":
            usersdb.add_fav_file(current_user, file_id, server, file_name)
        elif favorite_type == "user" and favorite_name:
            usersdb.add_fav_user_file(current_user, file_id, server, file_name, favorite_name)
        elif favorite_type == "search":
            pass # TODO (felipe): implementar
        else:
            return _("error_500_message"),404
    elif action=="delete":
        if favorite_type == "file":
            usersdb.remove_fav_file(current_user, file_id)
        elif favorite_type == "user":
            usersdb.remove_fav_user_file(current_user, file_id, favorite_name)
        elif favorite_type == "search":
            pass # TODO (felipe): implementar
        else:
            return _("error_500_message"),404

    if redirection==0:
        return redirect(url_for("files.download",file_id=file_id,file_name=file_name))
    elif redirection==1:
        return jsonify(OK = True)
    elif redirection==2:
        return redirect(url_for(".profile"))

@user.route('/<lang>/vote',methods=['POST'])
def vote():
    '''
    Gestiona las votaciones de archivos y comentarios
    '''
    try:
        vote_type=request.form.get("type",None)
        server=int(request.form.get("where",None))
        file_id=url2mid(request.form.get("file",None))
        file_name=request.form.get("name",None)
        comment_id=request.form.get("comment",None)
        vote=int(request.form.get("vote",0))
        if not vote_type or server<=0 or not file_id or not file_name or vote not in (0,1):
            raise Exception
    except:
        return _("error_500_message"),404

    json={}
    if current_user.type in (None, 0): #comprobación de usuario normal o anónimo
        if vote_type=="file": #voto para archivos: se guarda y actualiza el archivo
            results=usersdb.set_file_vote(file_id, current_user, g.lang,vote)
            filesdb.update_file({"_id":file_id, "vs":results, "s":server}, direct_connection=True)
            json=results.get(g.lang, None)
            if json is None and None in results:
                json = results[None]

        elif vote_type=="comment": #voto para comentarios solo usuarios normales
            json=usersdb.set_file_comment_vote( comment_id, current_user, file_id, vote )["1"]

    json["vote"]=1 if vote==1 else 0
    return jsonify(json)

@user.route("/<lang>/user/edit", methods=['GET', 'POST'])
@login_required
def edit():
    '''
    Página para editar el perfil de usuario
    '''
    error=None
    form=EditForm(request.form,obj=current_user)
    if request.method=='POST' and form.validate():
        if (current_user.username != form.username.data and usersdb.find_username(form.username.data) is None) or current_user.username == form.username.data:
            data=dict()
            data["_id"]=current_user.id
            data["username"]=form.username.data
            data["location"]=form.location.data
            if len(form.password.data)>0:
                data["password"]=form.password.data

            usersdb.update_user(data)
            del(session["user"])
            flash('profile_edited_succesfully')
            return redirect(url_for('user.profile'))
        else:
            flash("username_taken")

    g.title+=_("edit_your_profile")
    return render_template('user/edit.html',form=form)

@user.route("/<lang>/validate/<token>", methods=['GET', 'POST'])
def validate(token):
    '''
    Página de perfil de usuario
    '''
    data=usersdb.find_token(token)
    if data is not None:
        del data["password"]
        data['active'] = 1
        usersdb.update_user(data,["token"])
        login_user(User(data["_id"], data))
        flash("welcome")
    else:
        flash("token_not_exist")

    return redirect(url_for('index.home'))

@user.route("/<lang>/forgot", methods=['GET', 'POST'])
def forgot():
    '''
    Recuperar contraseña
    '''
    error=None
    form=ForgotForm(request.form)
    if request.method=='POST' and form.validate():
        data=usersdb.find_email(form.email.data)
        if data is not None:
            data["token"]=md5(str(uuid.uuid4())).hexdigest();
            if send_mail(data["username"]+_("restore_access"),data["email"],'forgot',token=data["token"]):
                usersdb.update_user(data)
                flash("check_inbox_email_restore")
                return redirect(url_for('index.home',lang=None))
        else:
            flash("email_not_database")

    g.title+=_("forgot")
    return render_template('user/forgot.html',form=form)

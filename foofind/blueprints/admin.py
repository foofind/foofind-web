# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, render_template, request, redirect, url_for, flash, current_app, abort, send_file
from flaskext.babel import gettext as _
from wtforms import BooleanField, TextField, TextAreaField, HiddenField
from foofind.utils import lang_path, expanded_instance, fileurl2mid, mid2hex, hex2mid, url2mid
from foofind.services import *
from foofind.services.search import block_files as block_files_in_sphinx
from foofind.forms.admin import *
from foofind.utils.pogit import pomanager
from flaskext.login import current_user
from functools import wraps
from collections import OrderedDict

import polib
import itertools
import deploy.fabfile as fabfile

import multiprocessing
import time

class DeployTask(object):
    '''
    Ejecuta una tarea en un thread, comprobando que sea única vía flask-cache (memcache)

    Para comprobar si está disponible o no, usar el atributo 'busy'.
    '''
    class DummyValue(object):
        value = 0
        def __init__(self, v=0):
            self.value = v

    class MemcachedBuffer(object):
        def __init__(self, memcache_identifier, syncmanager = None):
            #self._lock = syncmanager.Lock() if syncmanager else Lock()
            self._tmp = syncmanager.list() if syncmanager else []
            self._tim = syncmanager.Value("f", 0) if syncmanager else DummyValue(0)
            self._memid = memcache_identifier

        def write(self, x):
            w = None
            self._tmp.append(x)
            if "\n" in x:
                now = time.time()
                if now-self._tim.value > 2.:
                    self._tim.value = now
                    w = "".join(self._tmp)
            if not w is None:
                cache.cache.set(self._memid, w)

        def flush(self): pass
        def truncate(self, x=0): pass
        def isatty(self): return False

        def clean(self):
            while self._tmp: self._tmp.pop()
            cache.cache.set(self._memid, "")

        def get_data(self):
            return "".join(self._tmp) or cache.cache.get(self._memid) or ""

    class DeployThread(multiprocessing.Process):
        def __init__(self, mode, action, stdout, stderr, callback, config):
            multiprocessing.Process.__init__(self, None, None, "DeployThread")
            self.error = None
            self.mode = mode
            self.action = action
            self.stdout = stdout
            self.stderr = stderr
            self.callback = callback
            self.config = config
            self.start()

        def run(self):
            self.stderr.clean()
            self.stdout.clean()
            try:
                fabfile.run_task(self.action,
                    mode=self.mode,
                    stdout=self.stdout,
                    stderr=self.stderr,
                    use_lock=False,
                    config=self.config)
            except BaseException as e: self.error = e
            self.callback()
            if self.error: raise self.error

    class SingleTaskException(Exception): pass

    _task = None

    @property
    def busy(self):
        return cache.cache.get(self._memid)

    def __init__(self, memcache_prefix):
        self._syncmanager = multiprocessing.Manager()
        self._lock = self._syncmanager.Lock()
        self._memid = "%sbusy" % memcache_prefix
        self.stdout = self.MemcachedBuffer("%sstdout" % memcache_prefix, self._syncmanager)
        self.stderr = self.MemcachedBuffer("%sstderr" % memcache_prefix, self._syncmanager)

    def _release(self):
        cache.cache.set(self._memid, False)

    def run(self, mode, action, config = None):
        self._lock.acquire()
        if self.busy:
            self._lock.release() # Imprescindible liberar el lock antes de hacer run raise
            raise self.SingleTaskException("Ya hay un thread funcionando, revisa el atributo 'busy' antes.")
        cache.cache.set(self._memid, True)
        self.DeployThread(mode, action,
            self.stdout,
            self.stderr,
            self._release,
            config)
        self._lock.release()

admin = Blueprint('admin', __name__)

def deploy_backups():
    '''
    Lista y agrupa los archivos de backup que retorna el fabfile
    '''
    backups = fabfile.get_backups()
    backups.sort(key=lambda x:x.split("/")[-1])
    return itertools.groupby(backups, lambda x: x[x.rfind("/")+1:-4 if x.endswith(".txt") else x.rfind("_")])

def admin_required(fn):
    '''
    Decorador que se asegura de que el usuario sea admin

    @param fn: función

    @return función decorada
    '''

    @wraps(fn)
    def decorated_view(*args, **kwargs):
        if not current_user.is_authenticated():
            return current_app.login_manager.unauthorized()
        elif current_user.type != 1:
            abort(403)
        return fn(*args, **kwargs)
    return decorated_view


def admin_title(msgid):
    '''
    Añade el str localizado de administración al id de localización dado

    @type msgid: str
    @param msgid: id del mensaje

    @rtype str
    @return
    '''
    return "%s - %s" % (_("admin_prefix"), _(msgid))


def pagination(num, page=0, items_per_page=15):
    '''
    Helper para el sistema de paginación, los valores por defecto son tomados
    por parámetro, pero tendrá en cuenta los atributos 'page' y 'size' pasados
    por GET si son válidos.

    @type num: int
    @param num: número de registros a paginar

    @type page: int
    @param page: valor por defecto de la página actual

    @type items_per_page: int
    @param items_per_page: valor por defecto del número de elementos por página

    @rtype tuple of ints
    @return tupla como (elementos a omitir al inicio, elementos por página, página, número de páginas)
    '''
    if "page" in request.args and request.args["page"].isdigit():
        page = request.args.get("page", page, int)
    if "size" in request.args and request.args["size"].isdigit():
        items_per_page = request.args.get("size", items_per_page, int)
    return (items_per_page*page, items_per_page, page, (num/items_per_page)+bool(num%items_per_page))

@admin.route('/<lang>/admin')
@admin_required
def index():
    '''
    Administración, vista general
    '''
    return render_template('admin/overview.html',
        page_title=_('admin_overview'),
        title=admin_title('admin_overview'),
        page_size=request.args.get("size", 15, int),
        new_locks=pagesdb.count_complaints(False, limit=1),
        new_translations=pagesdb.count_translations(False, limit=1),
        role=None)

@admin.route('/<lang>/admin/locks', methods=("GET","POST"))
@admin_required
def locks():
    '''
    Administración de peticiones para ficheros bloqueados
    '''
    arg_show = request.args.get("show","new",str)
    searchform = BlockFileSearchForm(request.form)
    processed = None if arg_show == "all" else (arg_show == "old")
    num_items = pagesdb.count_complaints(processed)
    skip, limit, page, num_pages = pagination(num_items)
    complaints = pagesdb.get_complaints(skip=skip, limit=limit, processed=processed) if num_items > 0 else ()
    return render_template('admin/locks.html',
        page_title=_('admin_locks'),
        title=admin_title('admin_locks'),
        searchform=searchform,
        complaints=complaints,
        num_items=num_items,
        num_pages=num_pages,
        list_mode=arg_show,
        list_modes=("new","old","all"),
        page_size=limit,
        page=page)


@admin.route('/<lang>/admin/lockfiles', methods=("GET","POST"))
@admin.route('/<lang>/admin/locks/<complaint_id>', methods=("GET","POST"))
@admin_required
def lock_file(complaint_id=None):
    '''
    Información y bloqueo de ficheros, puede recibir un id de queja, o una lista de ids (en hex) de ficheros separados por la letra "g"
    '''
    page = request.args.get("page", 0, int)
    mode = request.args.get("show", "old", str)
    size = request.args.get("size", 15, int)

    fileids = ()
    if request.method == 'POST':
        if not "fileids" in request.form:
            searchform = BlockFileSearchForm(request.form)
            identifiers = searchform.identifier.data.split()
            if searchform.mode.data == "hexid":
                fileids = [mid2hex(hex2mid(i)) for i in identifiers if
                    all(x in "0123456789abcdef" for x in i)]
            elif searchform.mode.data == "b64id":
                fileids = [mid2hex(url2mid(i)) for i in identifiers if
                    all(x in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!-" for x in i)
                    and (len(i)*8)%6 == 0]
            elif searchform.mode.data == "url":
                fileids = [mid2hex(fileurl2mid(i)) for i in identifiers if
                    i.startswith("http") and len(i.split("//")[1].split("/")) > 3]
            if not fileids:
                return redirect(url_for('admin.locks', page=page, mode=mode, size=size))
        else:
            block = request.form.get("block", False, bool)
            unblock = request.form.get("unblock", False, bool)
            if block or unblock: # submit confirmar
                if complaint_id: pagesdb.update_complaint({"_id":complaint_id,"processed":True})
                fileids = dict(i.split(":") if i.count(":") == 1 else (i, None)
                        for i in request.form["fileids"].split(","))
                sphinx_block = []
                sphinx_unblock = []
                for fileid, server in fileids.iteritems():
                    (sphinx_block if block and not unblock else sphinx_unblock).append(fileid)
                    req = {"_id":fileid, "bl": int(block and not unblock)}
                    if server: req["s"] = int(server) # si recibo el servidor, lo uso por eficiencia
                    filesdb.update_file(req, direct_connection=True, update_sphinx=False)
                if sphinx_block:
                    block_files_in_sphinx(mongo_ids=sphinx_block, block=True)
                if sphinx_unblock:
                    block_files_in_sphinx(mongo_ids=sphinx_unblock, block=False)
                flash("admin_locks_locked" if block else "admin_locks_unlocked", "success")
            elif request.form.get("cancel", False, bool): # submit cancelar
                if complaint_id:
                    pagesdb.update_complaint({"_id":complaint_id,"processed":True})
                flash("admin_locks_not_locked", "success")
            return redirect(url_for('admin.locks', page=page, mode=mode, size=size))

    complaint_data = None # Hay un único o ningún registro de queja por formulario
    files_data = OrderedDict() # Pueden haber varios ficheros por formulario
    if complaint_id: # Si hay queja, sólo hay una url, la de la queja
        complaint_data = pagesdb.get_complaint(complaint_id)
        if complaint_data and "urlreported" in complaint_data:
            # extracción el id de complaint["urlreported"] de base64 a hexadecimal
            files_data[mid2hex(fileurl2mid(complaint_data["urlreported"]))] = None
    elif fileids: # Si no hay queja, los ficheros se sacan de la url
        files_data.update((i,None) for i in fileids)

    # Suponemos si queremos bloquear, desbloquear, o mostrar las dos opciones
    # dependiendo de sí de los ficheros están bloqueados o no
    # además rellenamos la información de los ficheros
    blocked = 0
    unblocked = 0
    for fileid in files_data.iterkeys():
        files_data[fileid] = filesdb.get_file(fileid, bl=None) or {}
        if not "bl" in files_data[fileid] or files_data[fileid]["bl"] == 0: unblocked += 1
        else: blocked += 1

    return render_template('admin/lock_file.html',
        page_title=_('admin_locks_fileinfo'),
        complaint_data=complaint_data,
        files_data=files_data,
        fileids=",".join(("%s:%s" % (fileid, prop["s"])) if "s" in prop else fileid for fileid, prop in files_data.iteritems()),
        blocked=None if blocked and unblocked else blocked > 0,
        page_size=size,
        list_mode=mode,
        page=page,
        title=admin_title('admin_losfdcks_fileinfo'))


@admin.route('/<lang>/admin/users', methods=("GET","POST"))
@admin.route('/<lang>/admin/users/<userid>', methods=("GET","POST"))
@admin_required
def users():
    '''
    Administración de usuarios
    '''
    # Esta plantilla tiene 2 formularios, filtramos según cuál haya sido enviado
    searchformrequest = request.form if request.form.get("searchform_submit", False) else None
    userformrequest = request.form if request.form.get("userform_submit", False) else None

    searchform = SearchUserForm(searchformrequest, prefix="searchform_")
    userform = None

    fieldnames = ("karma", "token", "username", "email", "password", "lang", "location", "karma", "active", "type", "oauthid")

    properties = ()

    if request.method == "POST":
        user_data = None
        if searchformrequest:
            mode = searchform.mode.data
            identifier = searchform.identifier.data
            if mode == "username": user_data = usersdb.find_username(identifier)
            elif mode == "email": user_data = usersdb.find_email(identifier)
            elif mode == "hexid": user_data = usersdb.find_userid(identifier)
            elif mode == "oauth": user_data = usersdb.find_oauthid(identifier)
        elif userformrequest:
            user_data = {i.strip():None for i in request.form.get("userform_props","").split(";")}

        if user_data:
            password_hash = user_data.pop("password") if "password" in user_data else None
            created = ""
            if "created" in user_data:
                if user_data["created"] is None: created = user_data["created"]
                else: created = user_data["created"].strftime("%Y-%m-%d %H:%M:%S.%f")
            user_data.update((i, "") for i in fieldnames if not i in user_data)
            properties = user_data.keys()
            if "password" in user_data:
                properties.remove("password") # Añadimos password al formulario manualmente
            if "created" in user_data:
                properties.remove("created") # Añadimos password al formulario manualmente
            if "_id" in user_data: properties.remove("_id")
            fields = {"field_%s" % key:
                TextField(key, default=user_data[key], description=key)
                for key in properties}
            fields["field_password"] = TextField("password", default="", description=password_hash)
            userform = expanded_instance(EditUserForm, fields, userformrequest, prefix="userform_")
            if "_id" in user_data: userform.userid.data = user_data["_id"]
            if created: userform.created.data = created
            userform.props.data = ";".join(properties)

            if userformrequest and userform.submit.data:
                data = {key[6:]: userform[key].data for key, value in fields.iteritems() if userform[key].data.strip()}
                data["_id"] = userform.userid.data
                usersdb.update_user(data)
                if data["username"]:
                    searchform["identifier"].data = data["username"]
                    searchform["mode"].data = "username"
                flash("admin_users_updated","success")
        else:
            flash("admin_users_not_found","error")

    return render_template('admin/users.html',
        page_title=_('admin_users'),
        blocked=False,
        user_properties=properties,
        search_form=searchform,
        user_form=userform,
        title=admin_title('admin_locks_fileinfo'))

@admin.route('/<lang>/admin/translations')
@admin_required
def translations():
    '''
    Administración de notificaciones de error en la traducción
    '''
    arg_show = request.args.get("show","new",str)
    processed = None if arg_show == "all" else (arg_show == "old")
    num_items = pagesdb.count_translations(processed)
    skip, limit, page, num_pages = pagination(num_items)
    translations = pagesdb.get_translations(skip=skip, limit=limit, processed=processed) if num_items > 0 else ()
    return render_template('admin/translations.html',
        page_title=_('admin_translation'),
        title=admin_title('admin_translation'),
        translations=translations,
        num_pages=num_pages,
        num_items=num_items,
        list_mode=arg_show,
        list_modes=("new","old","all"),
        page_size=limit,
        page=page)

translation_field_prefix = "translate_"
translation_field_prefix_len = len(translation_field_prefix)

@admin.route('/<lang>/admin/translations/<translation_id>', methods=("GET","POST"))
@admin_required
def review_translation(translation_id):
    '''
    Formulario de revisión de traducción
    '''
    page = request.args.get("page", 0, int)
    mode = request.args.get("show", "old", str)
    size = request.args.get("size", 15, int)
    select = request.args.get("select", "default", str)

    # Optimización: si envío en un campo la lista de strids, no la pido a BBDD
    data = None
    if request.form.get("field_keys", None):
        data_fields = {i:"" for i in request.form.get("fields").replace("&sbquo;",",").replace("&amp;","&").split(",")}
    else:
        data = pagesdb.get_translation(translation_id)
        data_fields = data["texts"]

    def denuke(o):
        '''Evita nukeos de msgstrs malformados'''
        try: return _(o) # TODO: Investigar porqué esto hace falta
        except: return o

    # Campos de traducción
    fields = {"field_%s" % key:
        TextAreaField(key, default=value, description=denuke(key))
        if len(value) > 40 else
        TextField(key, default=value, description=denuke(key))
        for key, value in data_fields.iteritems()}

    # Checkboxes
    fields.update(
        ("check_%s" % key,  BooleanField(default=(select==all)))
        for key in data_fields.iterkeys())

    form = expanded_instance(ValidateTranslationForm, fields, request.form, prefix="translation_")

    # Guardo en el Hiddenfield la lista de strids
    form.field_keys = "&".join(i.replace("&","&amp;").replace(",","&sbquo;") for i in data_fields.iterkeys())

    if request.method=='POST' and form.validate():
        if form.submit.data:
            pomanager.update_lang(data["dest_lang"],
                {key: form["field_%s" % key].data
                    for key in data_fields.iterkeys() if form["check_%s" % key].data})
        pagesdb.update_translation({"_id":translation_id,"processed":True})
        return redirect(url_for('admin.translations', page=page, mode=mode, size=size))
    elif data is None:
        # Si no he pedido a base de datos la traducción por optimizar
        data = pagesdb.get_translation(translation_id)

    dest_lang = {
        key: value
        for key, value in pomanager.get_lang(data["dest_lang"]).iteritems()
        if key in data["texts"]
        }

    user_lang_path = lang_path(data["user_lang"])
    user_lang = (
        {i.msgid: i.msgstr for i in polib.pofile(user_lang_path) if i.msgid in data["texts"]}
        if user_lang_path else {})

    def try_current(o):
        try: return _(o)
        except: return None

    translation = {
        key:(user_lang[key] if key in user_lang else None,
             dest_lang[key] if key in dest_lang else None,
             try_current(key)) for key, value in data.pop("texts").iteritems()}

    return render_template('admin/translation_review.html',
        page_title=_('admin_translation_review'),
        title=admin_title('admin_translation_review'),
        form=form,
        select=select,
        data=data,
        page=page,
        list_mode=mode,
        page_size=size,
        field_keys=",".join(translation.iterkeys()),
        fields=translation)

deployTask = DeployTask("admin/deploy/task/")
@admin.route('/<lang>/admin/deploy/status')
@admin_required
def deploy_status():
    '''
    Para obtener el estado del deploy por JSON
    '''
    available = not deployTask.busy
    return jsonify(
        available = available,
        backups=[(i,tuple(j)) for i, j in deploy_backups()],
        stdout=deployTask.stdout.get_data(),
        stderr=deployTask.stderr.get_data()
        )

@admin.route('/<lang>/admin/deploy', methods=("GET","POST"))
@admin_required
def deploy():
    '''
    Pantalla de deploy
    '''
    page = request.args.get("page", 0, int)
    mode = request.args.get("show", "old", str)
    size = request.args.get("size", 15, int)
    form = DeployForm(request.form)
    form.mode.choices = [(i,i) for i in fabfile.get_modes()]
    form.publish_mode.choices = [
        (i, "%s | %s" % (j.isoformat("T").replace("T", " | ").split(".")[0], i.split("/")[-1].split("_")[0]))
        for i, j in fabfile.get_backups_datetimes().iteritems()
        ]
    # Ordeno por fecha, de más reciente a más antigua
    form.publish_mode.choices.sort(key=lambda x:x[1], reverse=True)

    if request.method == "POST" and not deployTask.busy:
        flash("admin_deploy_in_progress", "message in_progress_message")
        mode = form.mode.data
        config = None
        task = (
            "deploy" if form.deploy.data else
            "clean-local" if form.clean_local.data else
            "clean-remote" if form.clean_remote.data else
            "restart" if form.restart.data else
            "package" if form.package.data else
            "package-rollback" if form.rollback.data else
            "prepare-deploy" if form.prepare.data else
            "commit-deploy" if form.commit.data else
            "publish" if form.publish.data else None
            )
        if task == "deploy" and fabfile.is_self_target(mode, task):
            task = "deploy-safely"
            flash("admin_deploy_manual_restart", "message")
        elif task == "publish":
            path = form.publish_mode.data
            message = form.publish_message.data.strip()
            version = form.publish_version.data.strip()
            for i in (path, message, version):
                if not i: break
            else:
                config = {"path":path,"message":message,"version":version}
        if task: deployTask.run(mode, task, config)
        else: abort(502)

    stdout = deployTask.stdout.get_data()
    stderr = deployTask.stderr.get_data()

    return render_template('admin/deploy.html',
        page_title=_('admin_deploy'),
        title=admin_title('admin_deploy'),
        backups=deploy_backups(),
        form=form,
        page=page,
        list_mode=mode,
        deploy_available=not deployTask.busy,
        stdout=stdout,
        stderr=stderr,
        page_size=size)

@admin.route('/<lang>/admin/deploy/file/<path:filename>')
@admin_required
def deploy_file(filename):
    '''
    Para obtener los ficheros de deploy, muy dependiente del fabfile
    '''
    foo, bar = filename.split("/") if filename.count("/") == 1 else (None, None)
    if foo in (fabfile.env.rollback, fabfile.env.kitchen): return send_file("../%s" % filename)
    abort(403)

@admin.route('/<lang>/admin/restart/<mode>')
@admin_required
def deploy_restart(mode):
    '''
    Para reiniciar un servidor si el deploy falla.
    '''
    flash("admin_deploy_in_progress", "message in_progress_message")
    deployTask.run(mode, "restart")
    return redirect(url_for("admin.deploy"))

@admin.route('/<lang>/admin/origins')
@admin_required
def origins():
    '''
    Gestión de orígenes
    '''

    page = request.args.get("page", 0, int)
    grps = request.args.get("mode", "all", str)
    mode = request.args.get("show", "current", str)
    size = request.args.get("size", 15, int)

    all_groups = tuple(filesdb.get_sources_groups())
    origin_filter = None if grps == "all" else tuple(grps)
    crbl_filter = None if mode == "all" else mode == "blocked"
    num_items = filesdb.count_sources(crbl_filter, origin_filter, True)
    skip, limit, page, num_pages = pagination(num_items)
    origin_list = filesdb.get_sources(skip, limit, crbl_filter, origin_filter, True)

    return render_template('admin/origins.html',
        page_title=_('admin_origins'),
        title=admin_title('admin_origins'),
        num_pages=num_pages,
        num_items=num_items,
        show_modes=("current","blocked","all"),
        show_mode=mode,
        origins=origin_list,
        list_modes=all_groups,
        list_mode=grps,
        list_mode_add="" if grps == "all" else grps,
        list_mode_substracted={} if origin_filter is None else {i:"".join(j for j in origin_filter if j != i) or "all" for i in all_groups},
        page_size=limit,
        page=page)

@admin.route('/<lang>/admin/origins/<int:originid>', methods=("GET","POST"))
@admin_required
def manage_origin(originid):
    '''
    Edición de origen
    '''
    page = request.args.get("page", 0, int)
    grps = request.args.get("mode", "all", str)
    mode = request.args.get("show", "current", str)
    size = request.args.get("size", 15, int)

    if "props" in request.form:
        origin = None
        props = request.form["props"].split(",")
    else:
        origin = filesdb.get_source_by_id(originid)
        if not "crbl" in origin: origin["crbl"] = 0
        props = origin.keys()
        props.remove("_id")

    fn = "field_%s"
    fields = {fn % i: TextField(i,
            default=(", ".join(origin[i]) if i == "g" else origin[i])
            if origin and i in origin else None)
        for i in props}
    originform = expanded_instance(OriginForm, fields, request.form)
    originform.props.data = ",".join(props)

    if request.method == "POST" and originform.validate():
        req = {i: originform[fn % i].data for i in props}
        req["_id"] = originid
        filesdb.update_source(req)
        return redirect(url_for('admin.origins', page=page, mode=grps, show=mode, size=size))

    return render_template('admin/origin.html',
        page_title=_('admin_origins_info'),
        title=admin_title('admin_origins_info'),
        originid = originid,
        origin_properties = props,
        origin_form = originform,
        list_mode=grps,
        mode=mode,
        page_size=size,
        page=page)

def add_admin(app):
    pomanager.init_lang_repository(app)
    app.register_blueprint(admin)

# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, render_template, request, redirect, url_for, flash, current_app, abort, send_file
from werkzeug.datastructures import MultiDict
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
import json
import hashlib
import datetime
import types

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

def deploy_backups_datetimes():
    '''
    Lista los archivos de backup que retorna el fabfile con fechas
    '''
    tr = [
        (i, "%s | %s" % (j.isoformat("T").replace("T", " | ").split(".")[0], i.split("/")[-1].split("_")[0]))
        for i, j in fabfile.get_backups_datetimes().iteritems()]
    # Ordeno por fecha, de más reciente a más antigua
    tr.sort(key=lambda x:x[1], reverse=True)
    return tr

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

    searchform = SearchUserForm(request.form, prefix="searchform_")

    if request.method == "POST":
        user_data = None
        mode = searchform.mode.data
        identifier = searchform.identifier.data
        if mode == "username": user_data = usersdb.find_username(identifier)
        elif mode == "email": user_data = usersdb.find_email(identifier)
        elif mode == "hexid": user_data = usersdb.find_userid(identifier)
        elif mode == "oauth": user_data = usersdb.find_oauthid(identifier)

        if user_data:
            return redirect(url_for("admin.db_edit", collection="user", document_id=mid2hex(user_data["_id"])))
        else:
            flash("admin_users_not_found","error")

    return render_template('admin/users.html',
        page_title=_('admin_users'),
        blocked=False,
        search_form=searchform)

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
        publish=deploy_backups_datetimes(),
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
    form.publish_mode.choices = deploy_backups_datetimes()

    if request.method == "POST" and not deployTask.busy:
        flash("admin_deploy_in_progress", "message in_progress_message")
        mode = form.mode.data
        config = None
        task = (
            "deploy" if form.deploy.data else
            "deploy-rollback" if form.deploy_rollback.data else
            "clean-local" if form.clean_local.data else
            "clean-remote" if form.clean_remote.data else
            "restart" if form.restart.data else
            "package" if form.package.data else
            "package-rollback" if form.rollback.data else
            "prepare-deploy" if form.prepare.data else
            "commit-deploy" if form.commit.data else
            "publish" if form.publish.data else None
            )
        if task.startswith("deploy") and fabfile.is_self_target(mode, task):
            task += "-safely"
            flash("admin_deploy_manual_restart", "message")
        elif task == "publish":
            path = form.publish_mode.data
            message = form.publish_message.data.strip()
            version = form.publish_version.data.strip()
            for i in (path, message, version):
                if not i:
                    flash("admin_deploy_publish_empty", "error")
                    break
            else:
                if " " in version or any(not i.isdigit() for i in version.split("-")[0].split(".")):
                    flash("admin_deploy_publish_bad_version", "error")
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

# Pares de parsers para diccionario de parsers
db_data_int = (
    lambda x: (
        "%d" % x if isinstance(x, (int,float)) else
        x if isinstance(x, basestring) and x.count(".") in (0,1) and x.replace(".","").isdigit() else "0"
        ),
    lambda x: int(float(x)) if x.count(".") in (0,1) and x.replace(".","").isdigit() else 0
    )
db_data_float = (
    lambda x: (
        "%g" % x if isinstance(x, (int,float)) else
        x if isinstance(x, basestring) and x.count(".") in (0,1) and x.replace(".","").isdigit() else "0.0"
        ),
    lambda x: float(x) if x.count(".") in (0,1) and x.replace(".","").isdigit() else 0.0
    )
db_data_bool = (
    lambda x: "1" if x else "0",
    lambda x: 1 if x.lower() in ("1","true") else 0,
    )
db_data_list_str = (
    lambda x: ",".join(x) if isinstance(x, list) else "",
    lambda x: [i.strip() for i in x.split(",")]
    )
# Diccionario de parsers [collection][prop] = (data to form, form to data)
db_parsers = {
    "origin": {
        "g": db_data_list_str,
        "crbl": db_data_int,
        },
    "user":{
        "karma": db_data_float,
        "active": db_data_bool,
        "type": db_data_float,
        "token": (
            lambda x: x if x else "",
            lambda x: x if x else None
            )
        }
    }
# MongoDB data to form
db_dtf = lambda a, b, c: db_parsers[a][b][0](c) if a in db_parsers and b in db_parsers[a] else c
# form to MongoDB data
db_ftd = lambda a, b, c: db_parsers[a][b][1](c) if a in db_parsers and b in db_parsers[a] else c
# fieldname
db_fnm = lambda x: "field_%s" % x.replace(" ","_")
# json_fixer
fjson = lambda x: x if isinstance(x,( dict, list, unicode, int, long, float, bool, types.NoneType)) else str(x)
@admin.route('/<lang>/admin/db/edit/<collection>/<document_id>', methods=("GET","POST"))
@admin_required
def db_edit(collection, document_id=None):
    '''
    Edición de base de datos
    '''
    page = request.args.get("page", 0, int)
    grps = request.args.get("mode", "all", str)
    mode = request.args.get("show", "current", str)
    size = request.args.get("size", 15, int)

    page_title = "admin_edit"
    form_title = "admin_edit"
    data = None

    form_force_fields = ()
    form_readonly_fields = ("_id",)
    document = {}
    if collection == "user":
        page_title = 'admin_users'
        form_title = 'admin_users_info'
        form_force_fields += ("karma", "token", "username", "email", "new password", "lang", "location", "karma", "active", "type", "oauthid")
        form_readonly_fields += ("created","password")
        data = usersdb.find_userid(document_id) if document_id else {}
    elif collection == "origin":
        page_title = 'admin_origins_info'
        form_title = 'admin_origins_info'
        form_force_fields += ("tb", "crbl", "d", "g")
        #form_readonly_fields += ()
        data = filesdb.get_source_by_id(float(document_id)) if document_id else {}
    else:
        abort(500)

    document.update((i, db_ftd(collection, i,"")) for i in form_force_fields)
    document.update(data)

    document_defaults = {fjson(i):fjson(j) for i, j in document.iteritems()}
    document_writeable = [k for k in document.iterkeys() if k not in form_readonly_fields]

    edit_form = expanded_instance(EditForm,
        {db_fnm(k): TextField(k, default=db_dtf(collection, k, document[k])) for k in document_writeable},
        request.form)

    edit_form.defaults.data = json.dumps(document_defaults)
    edit_form.editable.data = json.dumps(document_writeable)

    return render_template('admin/edit.html',
        collection=collection,
        document_id=document_id,
        title = admin_title(page_title),
        page_title = _(page_title),
        edit_form = edit_form,
        form_title = _(form_title),
        fieldname = db_fnm,
        document_writeable = [(k, document[k]) for k in document_writeable],
        document_readonly = [(k, document[k]) for k in document if not k in document_writeable],
        list_mode = grps, mode = mode, page_size = size, page = page)

@admin.route('/<lang>/admin/db/confirm/<collection>/<document_id>', methods=("POST",))
@admin_required
def db_confirm(collection, document_id=None):
    '''
    Confirmación de edición de base de datos
    '''
    page = request.args.get("page", 0, int)
    grps = request.args.get("mode", "all", str)
    mode = request.args.get("show", "current", str)
    size = request.args.get("size", 15, int)

    goback = lambda: redirect(url_for(url_id, page = page, mode = grps, show = mode, size = size))

    document = json.loads(request.form.get("defaults"))
    document_writeable = json.loads(request.form.get("editable"))

    request_form_dict = MultiDict(request.form)

    success_msgid = "admin_saved"
    unchanged_msgid = "admin_nochanges"

    if collection == "user":
        page_title = 'admin_users'
        form_title = 'admin_users_info'
        success_msgid = "admin_users_updated"
        url_id = "admin.users"
        save_fnc = lambda data: usersdb.update_user(data)
        new_password = request_form_dict.pop(db_fnm("new password"), None)
        if new_password:
            document_writeable.append("password")
            request_form_dict[db_fnm("password")] = new_password
    elif collection == "origin":
        page_title = 'admin_origins_info'
        form_title = 'admin_origins_info'
        url_id = "admin.origins"
        save_fnc = lambda data: filesdb.update_source(data)

    if request.form.get("confirmed", "False")=="True":
        # La petición ha sido realizada por el formulario de confirmación,
        # lo procesamos.
        check_form = expanded_instance(EditForm,
            {db_fnm(k): BooleanField(k) for k in document_writeable},
            request_form_dict)
        data = {k: document[k] for k in document_writeable if check_form[db_fnm(k)].data}
        if data:
            data["_id"] = hex2mid(document_id)
            save_fnc(data)
            flash(success_msgid, "success")
            return goback()
        else:
            flash(unchanged_msgid, "error")
            return goback()
    else:
        # No se trata del petición confirmada, procesamos el formulario como
        # viene de db_edit, generamos el formulario de confirmación.

        edit_form = expanded_instance(EditForm,
            {db_fnm(k): TextField(k, default=db_dtf(collection, k, document[k])) for k in document_writeable},
            request_form_dict)
        document_changes = [
            (k, document[k], db_ftd(collection, k, edit_form[db_fnm(k)].data))
            for k in document_writeable if edit_form[db_fnm(k)].data != db_dtf(collection, k, document[k])
            ]
        if document_changes:
            check_form = expanded_instance(EditForm,
                {db_fnm(k): BooleanField(k, default=False) for k, w, w in document_changes})
            check_form.defaults.data = json.dumps({k:v for k, w, v in document_changes})
            check_form.editable.data = json.dumps([k for k, w, v in document_changes])
            check_form.confirmed.data = True
        else:
            flash(unchanged_msgid, "error")
            return goback()

    return render_template('admin/confirm.html',
        collection=collection,
        document_id=document_id,
        title = admin_title(page_title),
        page_title = _(page_title),
        check_form = check_form,
        form_title = _(form_title),
        fieldname = db_fnm,
        repr=repr,
        document_changes = document_changes,
        list_mode = grps, mode = mode, page_size = size, page = page)


def add_admin(app):
    pomanager.init_lang_repository(app)
    app.register_blueprint(admin)

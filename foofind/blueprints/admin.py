# -*- coding: utf-8 -*-

import polib
import itertools
import json
import hashlib
import datetime
import types
import bson
import os

import urllib2
import multiprocessing
import time
import traceback
import logging

import deploy.fabfile as fabfile

from flask import Blueprint, jsonify, render_template, request, redirect, url_for, flash, current_app, abort, send_file, g
from werkzeug.datastructures import MultiDict
from flaskext.babel import gettext as _
from flaskext.login import current_user
from wtforms import BooleanField, TextField, TextAreaField, HiddenField
from functools import wraps
from collections import OrderedDict, defaultdict

from foofind.utils import lang_path, expanded_instance, fileurl2mid, mid2hex, hex2mid, url2mid, u
from foofind.utils.translations import unfix_lang_values
from foofind.utils.fooprint import ManagedSelect
from foofind.services import *
from foofind.services.search import block_files as block_files_in_sphinx, get_id_server_from_search
from foofind.forms.admin import *
from foofind.utils.pogit import pomanager

class DeployTask(object):
    '''
    Ejecuta una tarea de fabric en un thread, comprobando que sea única vía flask-cache (memcache)

    Para comprobar si está disponible o no, usar el atributo 'busy'.

    Para capturar la salida de fabric, se reemplazan el sys.stdout y el sys.stderr
    en el nuevo ámbito de proceso por MemcachedBuffer.

    '''

    class DummyValue(object):
        value = 0
        def __init__(self, v=0):
            self.value = v

    class MemcachedBuffer(object):
        '''
        Buffer que escribe en memcache, con caché local para evitar excesos.

        A la hora de leer, comprueba primero el caché local y, si está vacío
        (se vacía después de cada uso) busca en memcache.
        '''
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
                    cache.cache.set(self._memid, "".join(self._tmp))

        def flush(self): pass
        def truncate(self, x=0): pass
        def isatty(self): return False

        def clean(self):
            while self._tmp: self._tmp.pop()
            cache.cache.set(self._memid, "")

        def get_data(self):
            return "".join(self._tmp) or cache.cache.get(self._memid) or ""

    class DeployThread(multiprocessing.Process):
        '''
        Proceso de multiprocessing para mantener la ejecución del fabric en
        un ámbito controlado.
        '''
        def __init__(self, action, mode, stdout, stderr, callback, config, lockfile):
            multiprocessing.Process.__init__(self, None, None, "DeployThread")
            self.error = None
            self.mode = mode
            self.action = action
            self.stdout = stdout
            self.stderr = stderr
            self.callback = callback
            self.config = config
            self.lockfile = lockfile
            self.start()

        def run(self):
            self.stderr.clean()
            self.stdout.clean()
            try:
                fabfile.run_task(self.action,
                    mode=self.mode,
                    stdout=self.stdout,
                    stderr=self.stderr,
                    lockfile=self.lockfile,
                    config=self.config)
            except BaseException as e:
                logging.exception("Error dentro del DeployThread.")
            self.stderr.flush()
            self.stdout.flush()
            self.callback()

    class SingleTaskException(Exception): pass
    class UnknownBufferException(ValueError): pass

    _task = None

    @property
    def busy(self):
        '''
        Retorna True si hay algún proceso de fabric ejecutándose. Usa
        memcache para evitar problemas con múltiples instancias.
        '''
        return os.path.exists(self._lockfile) or cache.cache.get(self._memid)

    def release_lock(self):
        cache.delete(self._memid)
        if os.path.isfile(self._lockfile):
            try:
                os.remove(self._lockfile)
            except BaseException as e:
                logging.exception("Error forzando la liberación de locks.")
                raise e

    def __init__(self, memcache_prefix):
        self._syncmanager = multiprocessing.Manager()
        self._lock = self._syncmanager.Lock()
        self._memid = "%sbusy" % memcache_prefix
        self._stdout = self.MemcachedBuffer("%sstdout" % memcache_prefix, self._syncmanager)
        self._stderr = self.MemcachedBuffer("%sstderr" % memcache_prefix, self._syncmanager)
        self._lockfile = "%s/.%s.lock" % (
            fabfile.env.abskitchen,
            memcache_prefix.replace("/","_")
            )

    def get_stdout_data(self):
        '''
        Obtiene la salida estándar del proceso de fabric guardada en el buffer
        con el id dado.

        @rtype str
        @return Salida estándar del buffer dado.
        '''
        return self._stdout.get_data()

    def get_stderr_data(self):
        '''
        Obtiene la salida de error del proceso de fabric guardada en el buffer
        con el id dado.

        @rtype str
        @return Salida de error del buffer dado.
        '''
        return self._stderr.get_data()

    def clean_stdout_data(self):
        '''
        Limpia stdout
        '''
        self._stdout.clean()

    def clean_stderr_data(self):
        '''
        Limpia stderr
        '''
        self._stderr.clean()

    def _release(self):
        cache.cache.set(self._memid, False)

    def run(self, action = None, mode = None, config = None):
        '''
        Ejecuta una tarea de fabric.

        @type mode: str o None
        @param mode: modo en el caso de los despliegues, en otro caso None

        @type action: str
        @param action: código de la acción de fabric a realizar

        @type config: dict o None
        @param config: configuración específica dependiente de la tarea.

        '''
        self._lock.acquire()
        if self.busy:
            self._lock.release() # Imprescindible liberar el lock antes de hacer run raise
            raise self.SingleTaskException(
                "Ya hay un thread funcionando, revisa el atributo 'busy' antes.")

        cache.cache.set(self._memid, True)
        self.DeployThread(
            action,
            mode,
            self._stdout,
            self._stderr,
            self._release,
            config,
            self._lockfile)
        self._lock.release()

admin = Blueprint('admin', __name__)

@admin.context_processor
def admin_processor():
    return {
        "page_size": request.args.get("size", 15, int)
        }

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

@cache.memoize(timeout=3600) # 60 minutos
def deploy_list_scripts():
    '''
    Devuelve un diccionario de scripts disponibles.

    @rtype dict
    @return diccionario en formato {
        script (str) : hosts (list)
        }

    '''
    tr = fabfile.list_scripts()
    if tr is None: cache.cacheme = False
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

def simple_pyon(x):
    '''
    Convierte una cadena representando un objeto de python en un objeto de
    python con el tipo apropiado. Muy simplificado.
    '''
    x = x.strip()
    if x.lower() in ("","None","null","none"): return None
    if x.replace("+","",x.startswith("+")).replace("-","",x.startswith("-")).replace(".","",x.count(".")==1).isdigit():
        if "." in x: float(x)
        return int(x)
    if x.endswith("j"):
        return complex(x)
    if len(x)>1:
        if x[0] == x[-1] and x[0] in "\"'": return x[1:-1]
        if x[0] == "u" and x[1] == x[-1] and x[1] in "\"'": return u(x[2:-1])
        if x[0] == "[" and x[-1] == "]": return list(simple_pyon(i) for i in x[1:-1].split(","))
        if x[0] == "(" and x[-1] == ")": return tuple(simple_pyon(i) for i in x[1:-1].split(","))
        if x[0] == "{" and x[-1] == "}": return dict((simple_pyon(i.split(":")[0]), simple_pyon(i.split(":")[1])) for i in x[1:-1].split(","))
    return x

@admin.route('/<lang>/admin')
@admin_required
def index():
    '''
    Administración, vista general
    '''
    return render_template('admin/overview.html',
        page_title=_('admin_overview'),
        title=admin_title('admin_overview'),
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

    filenames = {}
    bugged = []
    fileids = ()
    if request.method == 'POST':
        if not "fileids" in request.form:
            searchform = BlockFileSearchForm(request.form)
            identifiers = searchform.identifier.data.split()
            if searchform.mode.data == "hexid":
                fileids = [ mid2hex(hex2mid(i))
                    for i in identifiers
                    if all(x in "0123456789abcdef" for x in i)
                    ]
            elif searchform.mode.data == "b64id":
                fileids = [
                    mid2hex(url2mid(i))
                    for i in identifiers
                    if all(x in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!-" for x in i)
                        and (len(i)*8)%6 == 0
                    ]
            elif searchform.mode.data == "url":
                filenames.update(
                    (
                        mid2hex(fileurl2mid(i)),
                        u".".join(urllib2.unquote(i.split("/")[-1]).split(".")[:-1])
                        )
                    for i in identifiers
                    if i.startswith("http") and len(i.split("//")[1].split("/")) > 3
                    )
                fileids = filenames.keys()
            if not fileids:
                return redirect(url_for('admin.locks', page=page, mode=mode, size=size))
        else:
            block = request.form.get("block", False, bool)
            unblock = request.form.get("unblock", False, bool)
            if block or unblock: # submit confirmar
                if complaint_id: pagesdb.update_complaint({"_id":complaint_id,"processed":True})
                fileids = dict(i.split(":") for i in request.form["fileids"].split(","))
                sphinx_block = []
                sphinx_unblock = []
                for fileid, server in fileids.iteritems():
                    (sphinx_block if block and not unblock else sphinx_unblock).append(fileid)
                    req = {"_id":fileid, "bl": int(block and not unblock)}
                    if server: req["s"] = int(server) # si recibo el servidor, lo uso por eficiencia
                    try:
                        # TODO(felipe): comprobar en qué casos se puede llegar aquí sin "s"
                        filesdb.update_file(req, direct_connection=True, update_sphinx=False)
                    except:
                        flash("No se ha podido actualizar el fichero con id %s" % fileid, "error")
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
        data = filesdb.get_file(fileid, bl=None)
        if data is None and fileid in filenames:
            bugged.append(fileid)
            sid = get_id_server_from_search(fileid, filenames[fileid])
            if sid:
                data = filesdb.get_file(fileid, sid = sid, bl = None)
        files_data[fileid] = data or {}
        if not "bl" in files_data[fileid] or files_data[fileid]["bl"] == 0: unblocked += 1
        else: blocked += 1

    return render_template('admin/lock_file.html',
        page_title=_('admin_locks_fileinfo'),
        complaint_data=complaint_data,
        files_data=files_data,
        filenames = filenames,
        bugged = bugged,
        fileids=",".join(
            "%s:%s" % (fileid, prop["s"] if "s" in prop else "")
            for fileid, prop in files_data.iteritems()),
        blocked=None if blocked and unblocked else blocked > 0,
        list_mode=mode,
        page=page,
        title=admin_title('admin_losfdcks_fileinfo'))

@admin.route('/<lang>/admin/getserver/<fileid>', methods=("GET","POST"))
@admin.route('/<lang>/admin/getserver/<fileid>/<filename>', methods=("GET","POST"))
def getserver(fileid, filename=None):
    '''
    Apaño porque Fer tarda mucho en arreglar indir
    '''
    # TODO(felipe): posibilidad de bloquear

    form = GetServerForm(request.form)

    data = None
    if request.method == 'POST':
        fname = form.filename.data
        sid = get_id_server_from_search(hex2mid(fileid), fname)
        if sid:
            try:
                data = filesdb.get_file(fileid, sid = sid, bl = None)

            except filesdb.BogusMongoException as e:
                logging.exception(e)
                flash(e, "error")
        else:
            flash("admin_file_search_server_not_found", "error")
    elif filename:
        form.filename.data = filename

    return render_template('admin/getserver.html',
        fileid = fileid,
        searchform = form,
        file_data = data
        )



@admin.route('/<lang>/admin/users', methods=("GET","POST"))
@admin.route('/<lang>/admin/users/<userid>', methods=("GET","POST"))
@admin_required
def users():
    '''
    Administración de usuarios
    '''

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
        user_count=usersdb.count_users(),
        blocked=False,
        search_form=searchform)

@admin.route('/<lang>/admin/translations', methods=("GET","POST"))
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
    rform = ReinitializeTranslationForm(request.form)
    if request.method == "POST" and rform.validate():
        if rform.submit.data:
            flash("admin_translation_reinitialized")
            pomanager.init_lang_repository()
    return render_template('admin/translations.html',
        page_title=_('admin_translation'),
        title=admin_title('admin_translation'),
        translations=translations,
        num_pages=num_pages,
        num_items=num_items,
        list_mode=arg_show,
        list_modes=("new","old","all"),
        rform=rform,
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

    # Optimización: si envío en un campo la lista de strids, no la pido a BBDD
    data = None
    if request.form.get("field_keys", None):
        data_fields = {i:"" for i in request.form.get("fields").replace("&sbquo;",",").replace("&amp;","&").split(",")}
    else:
        data = pagesdb.get_translation(translation_id)
        data_fields = data["texts"]

    # Lenguaje base
    base_lang = pomanager.get_lang("en")

    # Campos de traducción
    fields = {"field_%s" % key:
        TextAreaField(key, default=unfix_lang_values(value, base_lang[key]) or value)
        if len(value) > 40 else
        TextField(key, default=unfix_lang_values(value, base_lang[key]) or value)
        for key, value in data_fields.iteritems() if key in base_lang}

    # Checkboxes
    fields.update(
        ("check_%s" % key,  BooleanField(default=False))
        for key in data_fields.iterkeys())

    form = expanded_instance(ValidateTranslationForm, fields, request.form, prefix="translation_")

    # Guardo en el Hiddenfield la lista de strids
    form.field_keys = "&".join(i.replace("&","&amp;").replace(",","&sbquo;") for i in data_fields.iterkeys())

    if request.method=='POST' and form.validate():
        if form.submit.data:
            # Actualizamos el lenguaje de destino con los valores con checkbox marcado
            pomanager.update_lang(data["dest_lang"],
                {key: form["field_%s" % key].data
                    for key in data_fields.iterkeys() if form["check_%s" % key].data})
        pagesdb.update_translation({"_id":translation_id,"processed":True})
        flash("admin_saved")
        return redirect(url_for('admin.translations', page=page, mode=mode, size=size))
    elif data is None:
        # Si no he pedido a base de datos la traducción por optimizar, pero no queda otra
        data = pagesdb.get_translation(translation_id)

    pomanager.preload((g.lang, data["dest_lang"], data["user_lang"]))

    cur_lang = pomanager.get_lang(g.lang)
    user_lang = pomanager.get_lang(data["user_lang"])
    dest_lang = pomanager.get_lang(data["dest_lang"])

    translation = {
        key:(
             cur_lang[key] if key in cur_lang else None,
             user_lang[key] if key in user_lang else None,
             dest_lang[key] if key in dest_lang else None)
        for key, value in data.pop("texts").iteritems()
        if key in base_lang
        }

    return render_template('admin/translation_review.html',
        page_title=_('admin_translation_review'),
        title=admin_title('admin_translation_review'),
        form=form,
        langs=(g.lang, data["user_lang"], data["dest_lang"]),
        data=data,
        page=page,
        list_mode=mode,
        field_keys=",".join(translation.iterkeys()),
        fields=translation)

scriptTaskMaxTimeout = 600
scriptTaskPrefix = "admin/deploy/script/"
scriptTask = DeployTask(scriptTaskPrefix)
scriptTaskRefresh = "%s/refresh" % scriptTaskPrefix
@admin.route('/<lang>/admin/deploy/scripts_view')
@admin_required
def deploy_script_view():
    '''
    Devuelve el estado del view de un script.
    Si no está en caché, se ejecuta en todos los servidores y se cachea la
    salida.

    Devuelve JSON con hosts como claves (contiene punto, acceder con corchetes)
    y líneas de salida como array.
    Incluye atributo time con el timestamp de cuando ha sido generado.
    Si se ha leído desde caché incluye un atributo "cached" a True.
    '''

    script = request.args.get("script", None)
    hosts = request.args.get("hosts", None)

    # Validación
    if script is None:
        # Si script es None, no hay nada que procesar
        return jsonify({"cached":True})

    if hosts:
        # Pasamos los hosts a tupla ordenada para el hash del cacheid
        hosts.sort()
        hosts = tuple(hosts)

    cacheid = "%s/output/%s_%s" % (scriptTaskPrefix, hash(script), hash(hosts))

    now = time.time()
    cached_data = cache.get(cacheid)
    task_running = scriptTask.busy

    # Si se ha forzado un refresco de caché posterior a la información
    # cacheada que he encontrado, la ignoro.
    if (
      cached_data and
      cached_data.get("time", 0) < (cache.get(scriptTaskRefresh) or 0)
      ):
        cached_data = None

    # Si hay caché y la información de caché es definitiva.
    if cached_data and cached_data.get("cached", False):
        return jsonify(cached_data)

    # Si el script está funcionando o ha terminado y el caché no es definitivo
    if task_running or cached_data:
        # Extracción de salida por host
        raw_data = scriptTask.get_stdout_data()
        md5 = hash(raw_data) # hash de los datos de stdout

        if cached_data and cached_data.get("hash", None) == md5:
            # Si no hay cambios en el hash, no vuelvo a procesar
            data = cached_data
        else:
            data = defaultdict(list)
            data["md5"] = md5
            for line in raw_data.split(os.linesep):
                if line.startswith("[") and "] out:" in line:
                    start = line.find("@") + 1
                    end = min(line.find(":", start), line.find("]", start))
                    data[line[start:end]].append(
                        line[line.find("out: ")+5:].strip()
                        )
        data["time"] = now # Actualizo el timestamp
        if task_running: # Si el script está funcionado
            cache.set(cacheid, data, scriptTaskMaxTimeout)
            data["fresh"] = True
        elif cached_data: # Si la tarea ha terminado hago el caché definitivo
            data["cached"] = True
            cache.set(cacheid, data, scriptTaskMaxTimeout)
        return jsonify(data)

    # Si no se han proporcionado hosts para el script
    if hosts is None:
        hosts = deploy_list_scripts().get(script, None)
        if not hosts:
            # No se hace nada si no hay hosts para el script
            data = {"time":now,"cached":True}
            cache.set(cacheid, data, scriptTaskMaxTimeout)
            return jsonify(data)

    # El script no está funcionando, lo iniciamos
    scriptTask.run("script", None, {"hosts":hosts,"script":script})
    data = {"time":now}
    cache.set(cacheid, data, scriptTaskMaxTimeout)
    data["started"] = True
    return jsonify(data)

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
        stdout=deployTask.get_stdout_data(),
        stderr=deployTask.get_stderr_data()
        )

@admin.route('/<lang>/admin/deploy', methods=("GET","POST"))
@admin_required
def deploy():
    '''
    Pantalla de deploy
    '''
    page = request.args.get("page", 0, int)
    mode = request.args.get("show", "deploy", str)
    if not mode in ("deploy","script","publish","recover"): mode = "deploy"
    form = DeployForm(request.form)
    dls = None

    force_busy = False

    if mode == "deploy":
        form.mode.choices = [(i,i) for i in fabfile.get_modes()]
        form.mode.choices.sort()
    elif mode == "publish":
        form.publish_mode.choices = deploy_backups_datetimes()
    elif mode == "script":
        dls = deploy_list_scripts()
        if dls:
            form.script_mode.choices = [(i,i) for i in dls]
            form.script_mode.choices.sort()
            form.script_hosts.choices = [(i,i) for i in set(
                k for j in dls.itervalues() for k in j)]
            form.script_hosts.choices.sort()
            form.script_available_hosts.data = json.dumps(dls)
        else:
            form.script_mode.choices = ()
            form.script_hosts.choices = ()

    if request.method == "POST":
        if form.script_clean_cache.data:
            # Botón de borrar caché de scripts
            cache.set(scriptTaskRefresh, time.time())
            cache.delete(deploy_list_scripts.make_cache_key())
        elif form.remove_lock.data:
            for i in (scriptTask, deployTask):
                try:
                    i.release_lock()
                except BaseException as e:
                    flash(e, "error")
        elif form.clean_log.data:
            # Botón de borrar caché de stdout y stderr de deploy
            deployTask.clean_stdout_data()
            deployTask.clean_stderr_data()
        elif not deployTask.busy:
            config = None
            do_task = True
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
                "publish" if form.publish.data else
                "script" if form.script.data else None
                )
            if task == "publish":
                path = form.publish_mode.data
                message = form.publish_message.data.strip()
                version = form.publish_version.data.strip()
                for i in (path, message, version):
                    if not i:
                        flash("admin_deploy_publish_empty", "error")
                        do_task = False
                        break
                else:
                    if " " in version or any(not i.isdigit() for i in version.split("-")[0].split(".")):
                        flash("admin_deploy_publish_bad_version", "error")
                        do_task = False
                    else:
                        config = {
                            "path":path,
                            "message":message,
                            "version":version
                            }
            elif task == "script":
                if dls is None: dls = deploy_list_scripts()
                available_hosts = dls[form.script_mode.data]
                config = {
                    "hosts":tuple(
                        i for i in form.script_hosts.data
                        if i in available_hosts
                        ),
                    "script":form.script_mode.data
                    }
                if not config["hosts"]:
                    flash("admin_deploy_no_hosts","error")
                    do_task = False

            if task and do_task:
                flash("admin_deploy_in_progress", "message in_progress_message")
                force_busy = True
                deployTask.run(
                    task,
                    None if task in ("publish","script") else form.mode.data,
                    config)
            elif task is None:
                abort(502)

    return render_template('admin/deploy.html',
        page_title=_('admin_deploy'),
        title=admin_title('admin_deploy'),
        backups=deploy_backups(),
        show=mode,
        form=form,
        page=page,
        deploy_available=not (force_busy or deployTask.busy),
        stdout=deployTask.get_stdout_data(),
        stderr=deployTask.get_stderr_data()
        )

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
        removable_origins=origin_list if current_app.debug else (),
        list_modes=all_groups,
        list_mode=grps,
        list_mode_add="" if grps == "all" else grps,
        list_mode_substracted={} if origin_filter is None else {i:"".join(j for j in origin_filter if j != i) or "all" for i in all_groups},
        page=page)

@admin.route('/<lang>/admin/alternatives')
@admin_required
def alternatives():
    '''
    Gestión de alternativas
    '''

    page = request.args.get("page", 0, int)
    grps = request.args.get("mode", "all", str)
    mode = request.args.get("show", "current", str)

    num_items = configdb.count_alternatives()
    skip, limit, page, num_pages = pagination(num_items)
    alternative_list = configdb.list_alternatives(skip, limit)
    num_items = max(num_items, len(alternative_list))

    return render_template('admin/alternatives.html',
        page_title=_('admin_alternatives'),
        title=admin_title('admin_alternatives'),
        num_pages=num_pages,
        num_items=num_items,
        alternatives=alternative_list,
        page=page)

# Parsers para diccionario de parsers
db_types = {
    int : (
        lambda x: (
            "%d" % x if isinstance(x, (int,float)) else
            x if isinstance(x, basestring) and x.count(".") in (0,1) and x.replace(".","").isdigit() else "0"
            ),
        lambda x: int(float(x)) if x.count(".") in (0,1) and x.replace(".","").isdigit() else 0
        ),
    float : (
        lambda x: (
            "%g" % x if isinstance(x, (int,float)) else
            x if isinstance(x, basestring) and x.count(".") in (0,1) and x.replace(".","").isdigit() else "0.0"
            ),
        lambda x: float(x) if x.count(".") in (0,1) and x.replace(".","").isdigit() else 0.0
        ),
    bool : (
        lambda x: "1" if x else "0",
        lambda x: 1 if x.lower() in ("1","true") else 0
        ),
    datetime.datetime : (
        lambda x: x.isoformat(" "),
        lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
        ),
    str: ( u, u ),
    unicode: ( u, u ),
    bson.ObjectId : ( mid2hex, hex2mid ),
    "json" : (
        json.dumps,
        lambda x: json.loads(x) if x else None
        ),
    "str_list" : (
        lambda x: ",".join(x) if isinstance(x, list) else "",
        lambda x: [i.strip() for i in x.split(",")]
        ),
    "str_none": (
        lambda x: x if x else "",
        lambda x: x if x else None
        ),
    "pyon" : (repr, simple_pyon)
    }
# Diccionario de parsers [collection][prop] = (data_to_form, form_to_data [, data_to_json [, json_to_data]])
db_parsers = {
    "origin": {
        "_id": db_types[float],
        "g": db_types["str_list"],
        "crbl": db_types[int],
        "ig": db_types["json"],
        },
    "user":{
        "_id": db_types[bson.ObjectId],
        "karma": db_types[float],
        "active": db_types[bool],
        "type": db_types[float],
        "token": db_types["str_none"],
        "created": db_types[datetime.datetime]
        },
    "alternatives":{
        "_id": (str, str),
        "default": db_types["pyon"],
        "methods": (str, str),
        "param_name": (str, str),
        "param_type": (
            lambda x: x.__name__ if hasattr(x, "__name__") else str(x),
            str
            ),
        "remember_id": (str, str),
        "probability": (
            lambda x: "\n".join("%s: %s" % (repr(k),repr(v)) for k, v in x.iteritems()),
            lambda x: dict((simple_pyon(p) for p in line.split(":")) for line in x.replace(";","\n").split("\n") if line.strip()),
            lambda x: [j for i in x.iteritems() for j in i],
            lambda x: dict((x[i], x[i+1]) for i in xrange(0, len(x)-1, 2))
            )
        }
    }
db_removable = ("user", "alternatives")
#
db_serialize = lambda collection, data: json.dumps({repr(i):db_dtj(collection, i, j) for i, j in data.iteritems()})
db_unserialize = lambda collection, data: {simple_pyon(k): db_jtd(collection, simple_pyon(k), v) for k, v in json.loads(data).iteritems()}
# MongoDB data to form
db_dtf = lambda a, b, c: db_parsers[a][b][0](c) if a in db_parsers and b in db_parsers[a] else c
# form to MongoDB data
db_ftd = lambda a, b, c: db_parsers[a][b][1](c) if a in db_parsers and b in db_parsers[a] else c
# MongoDB data to json
db_dtj = lambda a, b, c: db_parsers[a][b][2 if len(db_parsers[a][b])>2 else 0](c) if a in db_parsers and b in db_parsers[a] else c
# json to MongoDB data
db_jtd = lambda a, b, c: db_parsers[a][b][3 if len(db_parsers[a][b])>3 else 1](c) if a in db_parsers and b in db_parsers[a] else c
# fieldname
db_fnm = lambda x: "field_%s" % x.replace(" ","_")

@admin.route('/<lang>/admin/db/edit/<collection>/<document_id>', methods=("GET","POST"))
@admin.route('/<lang>/admin/db/edit/<collection>', methods=("GET","POST"))
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

    form_force_fields = ("_id",) if document_id is None else ()
    form_readonly_fields = ("_id",) if document_id else ()
    form_ignored_fields = ()
    form_fieldtypes = {}
    form_fieldkwargs = {}
    form_fieldparams = {}

    document = {}
    deleteable = bool(document_id) # Mostrar o no el botón de borrar

    # Especificidades de las colecciones
    if collection == "user":
        # Nota: El campo "password" es un hash, de modo que lo establezco de
        #       sólo lectura y creo un nuevo campo "new password" para
        #       cambiarla (ver endpoint "db_confirm").
        page_title = 'admin_users'
        form_title = 'admin_users_info'
        form_force_fields += ("karma", "token", "username", "email", "new password", "lang", "location", "karma", "active", "type", "oauthid")
        form_readonly_fields += ("created","password")
        data = usersdb.find_userid(document_id) if document_id else {}
    elif collection == "origin":
        deleteable = current_app.debug
        page_title = 'admin_origins_info'
        form_title = 'admin_origins_info'
        form_force_fields += ("tb", "crbl", "d", "g", "ig")
        #form_readonly_fields += ()
        data = filesdb.get_source_by_id(float(document_id)) if document_id else {}
    elif collection == "alternatives":
        url_id = "admin.alternatives"
        available_methods = configdb.list_alternatives_methods()
        available_endpoints = configdb.list_alternatives_endpoints(document_id) if document_id else []
        available_param_types = configdb.list_alternatives_param_types()
        form_force_fields += ("default", "methods", "param_name", "param_type", "remember_id", "probability")
        form_fieldtypes["probability"] = TextAreaField
        form_fieldtypes["param_type"] = SelectField

        if document_id:
            form_fieldtypes["default"] = SelectField
            form_fieldkwargs["default"] = {"choices": ((repr(i), repr(i)) for i in available_endpoints)}
            data = {"default":available_endpoints[0] if available_endpoints else None}
            data.update(configdb.get_alternative_config(document_id))
            if not "_id" in data: data["_id"] = document_id
            if not "remember_id" in data: data["remember_id"] = document_id
            if not "param_name" in data: data["param_name"] = "alt"
        else:
            data = {}

        data["available_methods"] = ", ".join(available_methods)
        form_fieldkwargs["param_type"] = {"choices": ((i, i) for i in available_param_types)}
        form_fieldparams["available_methods"] = form_fieldparams["available_endpoints"] = {"readonly":"readonly"}
        form_fieldparams["probability"] = {"rows": len(available_endpoints), "class":"monospaced"}
    else:
        abort(404)

    document.update((i, db_ftd(collection, i, "")) for i in form_force_fields)
    if data: document.update(data)

    document_defaults = document
    document_writeable = sorted(k for k in document.iterkeys() if not (k in form_readonly_fields or k in form_ignored_fields))

    edict = {}
    edit_form = expanded_instance(EditForm, {
        db_fnm(k): form_fieldtypes.get(k, TextField)(k, default=db_dtf(collection, k, document[k]), **form_fieldkwargs.get(k, edict))
        for k in document_writeable
        }, request.form)

    edit_form.defaults.data = db_serialize(collection, document_defaults)
    edit_form.editable.data = json.dumps(document_writeable)

    return render_template('admin/edit.html',
        deleteable = deleteable,
        collection=collection,
        document_id=document_id,
        title = admin_title(page_title),
        page_title = _(page_title),
        edit_form = edit_form,
        form_title = _(form_title),
        fieldname = db_fnm,
        fieldparams = form_fieldparams,
        document_writeable = [(k, document[k]) for k in document_writeable],
        document_readonly = [(k, document[k]) for k in document if not (k in document_writeable or k in form_ignored_fields)],
        list_mode = grps, mode = mode, page = page)

db_confirm_debug = False

@admin.route('/<lang>/admin/db/confirm/<collection>/<document_id>', methods=("POST",))
@admin.route('/<lang>/admin/db/confirm/<collection>', methods=("POST",))
@admin_required
def db_confirm(collection, document_id=None):
    '''
    Confirmación de edición de base de datos
    '''
    page = request.args.get("page", 0, int)
    grps = request.args.get("mode", "all", str)
    mode = request.args.get("show", "current", str)
    size = request.args.get("size", 15, int)

    document = db_unserialize(collection, request.form.get("defaults"))
    document_writeable = json.loads(request.form.get("editable"))

    request_form_dict = MultiDict(request.form)

    # Valores por defecto

    page_title = "admin_edit"
    form_title = "admin_edit"

    success_msgid = "admin_saved"
    unchanged_msgid = "admin_nochanges"

    form_fieldtypes = {}
    form_fieldkwargs = {}

    url_id = "admin.index"

    goback = lambda : redirect(url_for(url_id, page = page, mode = grps, show = mode, size = size))

    # Especificaciones de las colecciones
    if collection == "user":
        page_title = 'admin_users'
        form_title = 'admin_users_info'
        success_msgid = "admin_users_updated"
        url_id = "admin.users"
        save_fnc = lambda data: (
            usersdb.create_user(data)
            if document_id is None else
            usersdb.update_user(data)
            )
        # La contraseña se mueve del campo "new password" a "password"
        new_password = request_form_dict.pop(db_fnm("new password"), None)
        if new_password:
            document_writeable.append("password")
            request_form_dict[db_fnm("password")] = new_password
    elif collection == "origin":
        page_title = 'admin_origins_info'
        form_title = 'admin_origins_info'
        url_id = "admin.origins"
        save_fnc = lambda data: (
            filesdb.create_source(data)
            if document_id is None else
            filesdb.update_source(data)
            )
    elif collection == "alternatives":
        url_id = "admin.alternatives"
        save_fnc = lambda data: configdb.update_alternative_config(document_id or data["_id"], data)
    else:
        abort(404)

    if db_confirm_debug: # debug mode
        def dummy_save(x):
            logging.debug(("Update:" if document_id else "Create: ") + repr(x))
        save_fnc = dummy_save

    # Procesamiento de formulario
    if request.form.get("confirmed", "False") == "True":
        # La petición ya ha sido confirmada, procesamos
        check_form = expanded_instance(EditForm,
            {db_fnm(k): BooleanField(k) for k in document_writeable},
            request_form_dict)
        data = {k: document[k] for k in document_writeable if check_form[db_fnm(k)].data}
        if data:
            # Hay datos que modificar, se ejecuta save_fnc y redirigimos
            if not document_id is None:
                data["_id"] = db_ftd(collection, "_id", document_id)
            try:
                save_fnc(data)
                flash(success_msgid, "success")
                return goback()
            except BaseException as e:
                flash(traceback.format_exc(e), "error")
                return goback()
        else:
            # No hay datos que modificar, redirigimos
            flash(unchanged_msgid, "error")
            return goback()
    else:
        # No se trata de la petición confirmada, procesamos el formulario como
        # viene de db_edit, generamos el formulario de confirmación.
        edict = {}
        edit_form = expanded_instance(EditForm, {
            db_fnm(k): form_fieldtypes.get(k, TextField)( k,
                default=db_dtf(collection, k, document[k]) if k in document else None,
                **form_fieldkwargs.get(k, edict))
            for k in document_writeable
            }, request_form_dict)
        document_changes = [
            (k, document.get(k, None), db_ftd(collection, k, edit_form[db_fnm(k)].data))
            for k in document_writeable
            if document.get(k, None) != db_ftd(collection, k, edit_form[db_fnm(k)].data)
            ]
        if document_changes:
            # Si hay cambios, generamos el formulario de confirmación
            check_form = expanded_instance(EditForm,
                {db_fnm(k): BooleanField(k, default=False) for k, w, w in document_changes})
            check_form.defaults.data = db_serialize(collection, {k: w for k, v, w in document_changes})
            check_form.editable.data = json.dumps([k for k, w, v in document_changes])
            check_form.confirmed.data = True
        else:
            # Si no hay cambios, redirigimos
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
        list_mode = grps, mode = mode, page = page)

@admin.route('/<lang>/admin/db/remove/<collection>/<document_id>', methods=("POST","GET"))
def db_remove(collection, document_id):
    page = request.args.get("page", 0, int)
    grps = request.args.get("mode", "all", str)
    mode = request.args.get("show", "current", str)
    size = request.args.get("size", 15, int)

    success_msgid = "admin_saved"
    unchanged_msgid = "admin_nochanges"

    url_id = "admin.index"
    goback = lambda: redirect(url_for(url_id, page = page, mode = grps, show = mode, size = size))

    if collection == "user":
        url_id = "admin.users"
        data = usersdb.find_userid(document_id)
        remove_fnc = lambda data: usersdb.remove_userid(data["_id"])
    elif collection == "origin":
        if not current_app.debug:
            # Sólo se pueden borrar orígenes en modo debug
            abort(403)
        url_id = "admin.origins"
        data = filesdb.get_source_by_id(float(document_id))
        remove_fnc = lambda data: filesdb.remove_source_by_id(data["_id"])
    elif collection == "alternatives":
        url_id = "admin.alternatives"
        data = {"default":configdb.list_alternatives_endpoints(document_id)}
        data.update(configdb.get_alternative_config(document_id))
        remove_fnc = lambda data: configdb.remove_alternative(data["_id"])
    else:
        abort(404)

    check_form = RemoveForm(request.form)

    if request.method == "POST":
        if check_form.confirmed.data:
            flash(_(success_msgid))
        else:
            flash(_(unchanged_msgid))
        return goback()

    return render_template('admin/remove.html',
        check_form = check_form,
        db_values = data
        )


def add_admin(app):
    pomanager.init_lang_repository(app)
    app.register_blueprint(admin)

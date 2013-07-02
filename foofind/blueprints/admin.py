# -*- coding: utf-8 -*-

import polib
import itertools
import json
import hashlib
import datetime
import types
import bson
import os
import inspect
import urllib2
import multiprocessing
import time
import traceback
import zlib
import mimetypes

from flask import Blueprint, jsonify, render_template, request, redirect, url_for, flash, current_app, abort, send_file, g, session
from werkzeug import secure_filename
from werkzeug.datastructures import MultiDict

from flask.ext.babelex import gettext as _
from flask.ext.login import current_user
from wtforms import BooleanField, TextField, TextAreaField, HiddenField
from functools import wraps
from collections import OrderedDict, defaultdict

import deploy.fabfile as fabfile
import foofind.utils.pyon as pyon
from foofind.utils import expanded_instance, fileurl2mid, mid2hex, hex2mid, url2mid, u, mid2url, logging
from foofind.utils.translations import unfix_lang_values
from foofind.utils.fooprint import ManagedSelect
from foofind.utils.flaskutils import send_gridfs_file
from foofind.utils.downloader import get_file_metadata
from foofind.services import *
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
            self._tmp.append(u(x))
            if "\n" in x:
                now = time.time()
                if now-self._tim.value > 2.:
                    self._tim.value = now
                    cache.cache.set(self._memid, u"".join(self._tmp))

        def flush(self): pass
        def truncate(self, x=0): pass
        def isatty(self): return False

        def clean(self):
            #while self._tmp: self._tmp.pop()
            del self._tmp[:]
            cache.cache.set(self._memid, "")

        def get_data(self):
            return u"".join(self._tmp) or u(cache.cache.get(self._memid) or u"")

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
                logging.error("Error dentro del DeployThread.", extra={"DeployTask":self})
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
                logging.error("Error forzando la liberación de locks.")
                raise e

    def __init__(self, memcache_prefix):
        self._syncmanager = multiprocessing.Manager()
        self._lock = self._syncmanager.Lock()
        self._lastmode = "%slastmode" % memcache_prefix
        self._memid = "%sbusy" % memcache_prefix
        self._stdout = self.MemcachedBuffer("%sstdout" % memcache_prefix, self._syncmanager)
        self._stderr = self.MemcachedBuffer("%sstderr" % memcache_prefix, self._syncmanager)
        self._lockfile = "%s/.%s.lock" % (
            fabfile.env.abskitchen,
            memcache_prefix.replace("/","_")
            )

    _ownlastmode = None
    def get_last_mode(self):
        '''
        Obtiene el último modo con el que ha sido llamado

        @rtype str
        @return cadena del último modo
        '''
        if self.busy:
            return self._ownlastmode
        return cache.cache.get(self._lastmode)

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

        if mode:
            self._ownlastmode = mode
            cache.cache.set(self._lastmode, mode)

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

logOrder = {"overview":-1,"staging.1":256,"staging.2":257,"admin.1":258,"admin.2":259}
logTask = DeployTask("admin/deploy/log/")
logRefresh = "admin/log/refresh"

scriptTask = DeployTask("admin/deploy/script/")
scriptRefresh = "admin/deploy/script/refresh"

task_maxtimeout = 600
def task_output(**kwargs):
    '''
    Devuelve la salida de una tarea.

    La tarea a realizar viene dada por los argumentos con clave

    Tareas:
        script:
            @type script: str
            @param script: nombre del script a ejecutar
            @type host: lista de str
            @param hosts: lista de hosts en los que ejecutar el script
        log:
            @type log: str
            @param log: ruta del log
            @type n: int
            @param n: Número de líneas a mostrar desde el inicio del fichero.
                      Si es negativo se refiere a las últimas líneas.

    '''
    # Modos de ejecución
    run_args = ()
    run_kwargs = {}
    mode = None
    if "script" in kwargs:
        mode = "script"
        script = kwargs["script"]
        hosts = kwargs.get("hosts", None)
        # Validación
        if script is None:
            # Si script es None, no hay nada que procesar
            return {"cached":True}

        if hosts:
            # Pasamos los hosts a tupla ordenada para el hash del cacheid
            hosts.sort()
            hosts = tuple(hosts)

        task = scriptTask
        run_args = ("script", None)
        run_kwargs["config"] = {"hosts": hosts, "script": script}
        cacheid = "admin/deploy/script/output/%s_%s" % (hash(script), hash(hosts))
        refreshid = scriptRefresh
    elif "log" in kwargs:
        mode = "log"
        logid = kwargs["log"]
        lognum = kwargs.get("n", -10)
        task = logTask
        run_args = ("log", None)
        run_kwargs["config"] = {"id": logid, "n": lognum}
        cacheid = "admin/log/output/%s_%d" % (hash(logid), lognum)
        refreshid = logRefresh
    else:
        raise ValueError

    now = time.time()
    cached_data = cache.get(cacheid)
    task_running = task.busy

    if cached_data:
        # Si se ha forzado un refresco de caché posterior a la información
        # cacheada que he encontrado, la ignoro.
        if cached_data.get("time", 0) < (cache.get(refreshid) or 0):
            cached_data = None
        # Si hay caché y la información de caché es definitiva.
        elif cached_data.get("cached", False):
            return cached_data

    # Si la tarea está funcionando o ha terminado y tengo caché no definitivo
    if task_running or cached_data:
        # Extracción de salida por host
        raw_data = task.get_stdout_data()
        md5 = hash(raw_data) # hash de los datos de stdout
        error = task.get_stderr_data()

        if cached_data and cached_data.get("hash", None) == md5:
            # Si no hay cambios en el hash, no vuelvo a procesar
            data = cached_data
        else:
            data = defaultdict(list)
            data["hash"] = md5
            # Separo la salida en líneas
            for line in raw_data.split(os.linesep):

                # Línea es salida de fabric
                if line.startswith("[") and 0 < line.find("] out: ") == line.find("]"):
                    host = line[1:line.find("]")]
                    if "@" in host: host = host[host.find("@")+1:] # Elimino el usuario
                    if ":" in host: host = host[:host.find(":")] # Elimino el puerto
                    data[host].append(
                        line[line.find("] out: ") + 7:].strip()
                        )
        data["time"] = now # Actualizo el timestamp
        if error:
            data["error"] = error
        if task_running: # Si la tarea está funcionado
            cache.set(cacheid, data, task_maxtimeout)
            data["fresh"] = True
        elif cached_data: # Si la tarea ha terminado hago el caché definitivo
            data["cached"] = True
            cache.set(cacheid, data, task_maxtimeout)
        return data

    # Caso específico para script: hay que deducir los hosts
    if mode == "script" and not hosts:
        hosts = deploy_list_scripts().get(script, None)
        if not hosts:
            # No se hace nada si no hay hosts para el script
            data = {"time":now,"cached":True}
            cache.set(cacheid, data, task_maxtimeout)
            return data
        run_kwargs["config"]["hosts"] = hosts

    # La tarea no está funcionando, lo iniciamos
    task.run(*run_args, **run_kwargs)
    data = {"time":now}
    cache.set(cacheid, data, task_maxtimeout)
    data["started"] = True
    return data

@admin.route('/<lang>/admin/task/output')
@admin_required
def task_output_endpoint():
    '''
    Devuelve el estado de una tarea.

    Devuelve JSON con hosts como claves (contiene punto, acceder con corchetes)
    y líneas de salida como array.
    Incluye atributo time con el timestamp de cuando ha sido generado.
    Si se ha leído desde caché incluye un atributo "cached" a True.
    '''
    tr = {}
    if "script" in request.args:
        tr.update(task_output(
            script = request.args["script"],
            hosts = request.args.get("hosts", None)
            ))
    elif "log" in request.args:
        tr.update(task_output(
            log = request.args["log"],
            n = int(request.args.get("n", -10))
            ))
    return jsonify(tr)


@admin.route('/<lang>/admin', methods=("GET","POST"))
@admin_required
def index():
    '''
    Administración, vista general
    '''
    mode = request.args.get("show","overview")
    modes = [(v.capitalize(), k) for k, v in fabfile.list_logs()]
    modes.sort()
    modes.insert(0, (_('admin_overview'),"overview"))
    form = None

    stdsep = "\n\n---\n\n"
    log_data = ""
    if mode != "overview":
        if request.method == "POST":
            cache.set(logRefresh, time.time())
        form = LogForm(request.form)
        number = form.number.data
        if form.mode.data == "tail":
            number *= -1
        data = task_output(log = mode, n = number)
        cached = data.get("cached", False)
        stdout = "\n".join(i
            for k, v in data.iteritems() if isinstance(v, list)
            for i in v
            ).strip()
        log_data = []
        if "error" in data: log_data.append(data["error"])
        if stdout: log_data.append(stdout)
        if not cached:
            log_data.append(_("admin_status_processing"))
            form.processing.data = True

    return render_template('admin/overview.html',
        page_title=_('admin_overview'),
        title=admin_title('admin_overview'),
        new_locks=pagesdb.count_complaints(False, limit=1),
        new_translations=pagesdb.count_translations(False, limit=1),
        form=form,
        log_data=stdsep.join(log_data),
        stdsep=stdsep.encode("hex"),
        show_modes=modes,
        show_mode=mode,
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

def lock_file_id_and_filename_from_url(url):
    '''
    Usada por el blueprint de bloqueo de ficheros para extraer el id de fichero
    en hexadecimal y el nombre de fichero a partir de una url.

    @type url: basestring
    @param url: url de fichero

    @rtype tupla (str, str)
    @return id en hexadecimal y nombre de fichero
    '''
    return (mid2hex(fileurl2mid(url)), urllib2.unquote(url.split("/")[-1]).rsplit(".",1)[0])

@admin.route('/<lang>/admin/locks/<complaint_id>', methods=("GET","POST"))
@admin.route('/<lang>/admin/lockfiles', methods=("GET","POST"))
@admin_required
def lock_file(complaint_id=None, url_file_ids=None):
    '''
    Información y bloqueo de ficheros, puede recibir un id de queja, o una lista de ids (en hex) de ficheros separados por la letra "g"
    '''
    page = request.args.get("page", 0, int)
    mode = request.args.get("show", "old", str)
    size = request.args.get("size", 15, int)

    filenames = {}
    bugged = []
    fileids = ()
    permalink = None
    if request.method == 'POST':
        if not "fileids" in request.form:
            searchform = BlockFileSearchForm(request.form)
            identifiers = searchform.identifier.data.split()
            if searchform.mode.data == "hexid":
                fileids = [
                    mid2hex(hex2mid(i))
                    for i in identifiers
                    if all(x in "0123456789abcdef" for x in i.lower())
                    ]
            elif searchform.mode.data == "b64id":
                fileids = [
                    mid2hex(url2mid(i))
                    for i in identifiers
                    if all(x in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!-" for x in i)
                        and (len(i)*6)%8 == 0
                    ]
            elif searchform.mode.data == "url":
                filenames.update(
                    lock_file_id_and_filename_from_url(i)
                    for i in identifiers
                    if i.startswith("http") and len(i.split("//")[1].split("/")) > 3
                    )
                fileids = filenames.keys()
            if fileids:
                permalink = url_for('admin.lock_file',
                    fileids = zlib.compress((
                        u"%s;%s" % (
                            "\0".join(fileids),
                            "\0".join(filenames.itervalues())
                            )).encode("utf-8")
                        ).encode("hex")
                    )
            else:
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
                    (sphinx_block if block and not unblock else sphinx_unblock).append((fileid, server, filenames[fileid] if fileid in filenames else None))
                    req = {"_id":fileid, "bl": int(block and not unblock)}
                    if server: req["s"] = int(server) # si recibo el servidor, lo uso por eficiencia
                    try:
                        # TODO(felipe): cuando se arregle el bug de indir: borrar
                        # TODO(felipe): comprobar en qué casos se puede llegar aquí sin "s"
                        filesdb.update_file(req, direct_connection=True, update_sphinx=False)
                    except:
                        flash("No se ha podido actualizar el fichero con id %s" % fileid, "error")
                if sphinx_block:
                    searchd.block_files(sphinx_block, True)
                if sphinx_unblock:
                    searchd.block_files(sphinx_unblock, False)
                flash("admin_locks_locked" if block else "admin_locks_unlocked", "success")
            elif request.form.get("cancel", False, bool): # submit cancelar
                if complaint_id:
                    pagesdb.update_complaint({"_id":complaint_id,"processed":True})
                flash("admin_locks_not_locked", "success")
            return redirect(url_for('admin.locks', page=page, mode=mode, size=size))
    elif "fileids" in request.args:
        permalink = request.url
        fileids, filenames = zlib.decompress(request.args["fileids"].decode("hex")).decode("utf-8").rsplit(";",1)
        fileids = fileids.split("\0")
        filenames = dict(itertools.izip(fileids, filenames.split("\0")))

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
        data = filesdb.get_file(hex2mid(fileid), bl=None)
        # Arreglo para bug de indir: buscar los servidores en sphinx
        # y obtener los datos del servidor encontrado.
        # TODO(felipe): cuando se arregle el bug de indir: borrar
        if data is None and fileid in filenames:
            bugged.append(fileid)
            sid = searchd.get_id_server_from_search(fileid, filenames[fileid])
            if sid:
                data = filesdb.get_file(fileid, sid = sid, bl = None)
                if data is None:
                    data = {"s":sid}
        files_data[fileid] = data or {}
        if not "bl" in files_data[fileid] or files_data[fileid]["bl"] == 0: unblocked += 1
        else: blocked += 1

    return render_template('admin/lock_file.html',
        page_title=_('admin_locks_fileinfo'),
        complaint_data=complaint_data,
        files_data=files_data,
        filenames = filenames,
        permalink = permalink,
        bugged = bugged,
        mid2url = mid2url,
        fileids=",".join(
            "%s:%s" % (fileid, prop["s"] if "s" in prop else "")
            for fileid, prop in files_data.iteritems()),
        blocked=None if blocked and unblocked else blocked > 0,
        list_mode=mode,
        page=page,
        title=admin_title('admin_locks_fileinfo'))

# TODO(felipe): cuando se arregle el bug de indir: borrar
@admin.route('/<lang>/admin/getserver/<fileid>', methods=("GET","POST"))
@admin.route('/<lang>/admin/getserver/<fileid>/<filename>', methods=("GET","POST"))
def getserver(fileid, filename=None):
    '''
    Apaño porque Fer tarda mucho en arreglar indir
    '''
    # TODO(felipe): posibilidad de bloquear

    form = GetServerForm(request.form)

    mfileid = hex2mid(fileid)
    data = None
    if request.method == 'POST':
        fname = form.filename.data
        sid = searchd.get_id_server_from_search(mfileid, fname)
        if sid:
            try:
                data = filesdb.get_file(mfileid, sid = sid, bl = None)
            except BaseException as e:
                logging.exception(e)
                flash(e, "error")
        else:
            flash("admin_file_search_server_not_found", "error")
    elif filename:
        form.filename.data = filename

    return render_template('admin/getserver.html',
        fileid = fileid,
        search_form = form,
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
        TextAreaField(key, default=unfix_lang_values(value, base_lang[key]))
        if len(value) > 40 else
        TextField(key, default=unfix_lang_values(value, base_lang[key]))
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
    if not mode in ("deploy", "script", "publish", "recover", "downloader"):
        mode = "deploy"
    form = DeployForm(request.form)
    dls = None

    force_busy = False

    if mode == "deploy":
        form.mode.choices = [(i,i) for i in fabfile.get_modes()]
        form.mode.choices.sort()
        lastmode = deployTask.get_last_mode()
        if request.method == "GET" and lastmode:
            form.mode.data = lastmode
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
            if request.method == "GET" and "admin_script_select" in session:
                form.script_mode.data = session["admin_script_select"]
        else:
            form.script_mode.choices = ()
            form.script_hosts.choices = ()

    if request.method == "POST":
        if mode == "script":
            # Para recordar la opción de script
            session["admin_script_select"] = form.script_mode.data
        if form.script_clean_cache.data:
            # Botón de borrar caché de scripts
            cache.set(scriptRefresh, time.time())
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
        elif form.downloader_upload.data:
            # Subir archivos a la carpeta downloads
            for k, f in request.files.iteritems():
                if f.filename and hasattr(form, k): # Verify this file comes from our form
                    fabfile.save_downloader_file(secure_filename(f.filename), f.stream)
        elif not deployTask.busy:
            config = None
            do_task = True
            task = (
                "deploy" if form.deploy.data else
                "deploy-rollback" if form.deploy_rollback.data else
                "clean-local" if form.clean_local.data else
                "clean-remote" if form.clean_remote.data else
                "restart" if form.restart.data else
                "restart_beta" if form.restart_beta.data else
                "package" if form.package.data else
                "package-rollback" if form.rollback.data else
                "prepare-deploy" if form.prepare.data else
                "commit-deploy" if form.commit.data else
                "confirm-deploy" if form.confirm.data else
                "publish" if form.publish.data else
                "script" if form.script.data else
                "distribute-downloader" if form.downloader_submit.data else None
                )
            task_has_mode = not task in ("publish", "script", "distribute-downloader")
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
            elif task == "distribute-downloader":
                config = {"servers":"all"}
            if task and do_task:
                flash("admin_deploy_in_progress", "message in_progress_message")
                force_busy = True
                deployTask.run(task, form.mode.data if task_has_mode else None, config)
            elif task is None:
                abort(502)
        return redirect(url_for("admin.deploy", page=page, show=mode))

    fmd = ()
    if mode == "downloader":
        stp = len(fabfile.env.downloads) + 1
        fmd = {path[stp:]: get_file_metadata(path) for path in fabfile.list_downloader_files()}

    return render_template('admin/deploy.html',
        page_title=_('admin_deploy'),
        title=admin_title('admin_deploy'),
        backups=deploy_backups(),
        file_metadata=fmd,
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
    embed_mode = request.args.get("embed", "all", str)

    cache.skip = True
    all_groups = tuple(filesdb.get_sources_groups())
    origin_filter = None if grps == "all" else tuple(grps)
    crbl_filter = None if mode == "all" else mode == "blocked"
    embed_filter = None if embed_mode == "all" else embed_mode == "enabled"
    num_items = filesdb.count_sources(crbl_filter, origin_filter, True, None, embed_filter)
    skip, limit, page, num_pages = pagination(num_items)
    origin_list = filesdb.get_sources(skip, limit, crbl_filter, origin_filter, True, embed_filter)

    return render_template('admin/origins.html',
        page_title=_('admin_origins'),
        title=admin_title('admin_origins'),
        embed_mode=embed_mode,
        embed_modes=("enabled","disabled","all"),
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

@admin.route('/<lang>/admin/servers')
@admin_required
def servers():
    '''
    Gestión de servers
    '''
    page = request.args.get("page", 0, int)
    grps = request.args.get("mode", "all", str)
    mode = request.args.get("show", "current", str)

    server_list = filesdb.get_servers()

    # Int fix
    to_int = {k for k, v in db_parsers["server"].iteritems() if v == db_types[int]}
    to_int.add("_id")
    for server in server_list:
        for i in to_int:
            if i in server:
                server[i] = int(server[i])

    num_items = max(server["_id"] for server in server_list)
    skip, limit, page, num_pages = pagination(num_items)
    num_items = max(num_items, len(server_list))

    return render_template('admin/servers.html',
        page_title=_('admin_servers'),
        title=admin_title('admin_servers'),
        num_pages=num_pages,
        num_items=num_items,
        alternatives=server_list,
        page=page)

@admin.route('/<lang>/admin/downloads', defaults={"filename":None}, methods=("GET","POST"))
@admin.route('/<lang>/admin/downloads/<path:filename>', methods=("GET","POST"))
@admin_required
def downloads(filename=None):
    '''
    Gestión de servers
    '''
    page = request.args.get("page", 0, int)
    grps = request.args.get("mode", "all", str)
    mode = request.args.get("show", "current", str)

    num_items = downloadsdb.count_files()
    skip, limit, page, num_pages = pagination(num_items)
    file_list = downloadsdb.list_files(skip, limit)

    form = DownloadForm(request.form)

    if filename:
        form.filename.data = filename

    file_data = None

    if request.method == "POST":
        if form.remove.data:
            downloadsdb.remove_file(filename)
            flash("admin_saved", "success")
            return redirect(url_for("admin.downloads"))
        else:
            if form.filename.data and form.version.data:
                old_version = (
                    form.old_version.data or
                    downloadsdb.get_last_version(form.filename.data) or
                    ""
                    )
                if form.version.data:
                    downloadsdb.store_file(
                        secure_filename(form.filename.data),
                        request.files["upfile"],
                        request.files["upfile"].content_type
                            or mimetypes.guess_type(form.filename.data)[0],
                        form.version.data)
                    flash("admin_saved", "success")
                    return redirect(url_for("admin.downloads"))
                else:
                    flash("admin_version_error", "error")
            else:
                flash("admin_nochanges", "error")
    elif not filename is None:
        file_data = downloadsdb.get_file(filename)
        form.old_version.data = file_data["version_code"]
        form.version.data = file_data["version_code"]

    return render_template('admin/downloads.html',
        file_data=file_data,
        page_title=_('admin_downloads'),
        title=admin_title('admin_downloads'),
        form=form,
        num_pages=num_pages,
        num_items=num_items,
        downloads=file_list,
        page=page)

@admin.route('/<lang>/admin/download/<path:filename>', defaults={"version":None})
@admin.route('/<lang>/admin/download/<version>/<path:filename>')
@admin_required
def download(version=None, filename=None):
    f = downloadsdb.stream_file(filename, version)
    return send_gridfs_file(f)

# Parsers para diccionario de parsers (data_to_form, form_to_data [, data_to_json [, json_to_data]])
db_types = {
    int : (
        lambda x: (
            "%d" % x if isinstance(x, (int, float, long)) else
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
        lambda x: 1 if x.lower() in ("1","true","yes","ok", u"sí") else 0
        ),
    datetime.datetime : (
        lambda x: x.isoformat(" ") if x else None,
        lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f") if x else None
        ),
    str: ( u, u ),
    unicode: ( u, u ),
    bson.ObjectId : (
        mid2hex,
        lambda x: hex2mid(x) if x else "" # _id está vacío al crear nuevos registros defecto
        ),
    "json" : (
        json.dumps,
        lambda x: json.loads(x) if x else None
        ),
    "str_list" : (
        lambda x: ",".join(x) if isinstance(x, list) else "",
        lambda x: [i.strip() for i in x.split(",")]
        ),
    "int_list": (
        lambda x: ",".join("%d" % i for i in x if isinstance(i, (int, long, float))) if isinstance(x, list) else "",
        lambda x: [int(i.strip()) for i in x.split(",") if i.isdigit()]
        ),
    "str_none": (
        lambda x: x if x else "",
        lambda x: x if x else None
        ),
    "pyon" : (
        pyon.dumps,
        lambda x: pyon.loads(x.encode("utf-8")) if x else None
        )
    }
# Diccionario de campos y parsers
db_parsers = {
    "server":{
        '_id': db_types[float],
        'c': db_types[int],
        'ip': db_types[str],
        'p': db_types[int],
        'rip': db_types[str],
        'rp': db_types[int],
        'lt': db_types[datetime.datetime],
        'mc': db_types[int],
        'sp': db_types[str],
        'spp': db_types[int],
        'ss': db_types[int],
        'rs': db_types[str],
        },
    "origin":{
        "_id": db_types[float],
        "g": db_types["str_list"],
        "crbl": db_types[int],
        "ig": db_types["json"],
        "url_lastparts_indexed": db_types[int],
        "embed_active": db_types[bool],
        "embed": db_types["str_none"],
        "embed_enabled": db_types["str_none"],
        "embed_disabled": db_types["str_none"],
        "embed_cts": db_types["int_list"],
        "url_embed_regexp": db_types["str_none"],
        "icons": db_types["pyon"],
        "quality": db_types["pyon"],
        "hidden_extensions": db_types[bool],
        "tb": db_types["str_none"],
        "d": db_types[str],
        "url_pattern": db_types[str],
        "ct": db_types["int_list"],
        },
    "user":{
        "_id": db_types[int],
        "karma": db_types[float],
        "active": db_types[bool],
        "type": db_types[float],
        "token": db_types["str_none"],
        "created": db_types[datetime.datetime],
        "username": db_types[str],
        "email": db_types[str],
        "lang": db_types[str],
        "location": db_types[str],
        "oauthid": db_types[str],
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
            lambda x: dict((pyon.loads(p.strip()) for p in line.split(":")) for line in x.replace(";","\n").split("\n") if line.strip()),
            lambda x: [j for i in x.iteritems() for j in i],
            lambda x: dict((x[i], x[i+1]) for i in xrange(0, len(x)-1, 2))
            )
        }
    }
db_removable = ("user", "alternatives")
#
db_serialize = lambda collection, data: pyon.dumps({k: db_dtj(collection, k, v) for k, v in data.iteritems()})
db_unserialize = lambda collection, data: {k: db_jtd(collection, k, v) for k, v in pyon.loads(data).iteritems()}
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

    cache.skip = True # Es importante que los datos estén actualizados

    page = request.args.get("page", 0, int)
    grps = request.args.get("mode", "all", str)
    mode = request.args.get("show", "current", str)
    size = request.args.get("size", 15, int)

    page_title = "admin_edit"
    form_title = "admin_edit"
    data = None

    form_force_fields = {"_id"} if document_id is None else set()
    if collection in db_parsers:
        form_force_fields.update( db_parsers[collection] )

    form_readonly_fields = {"_id"} if document_id else set()
    form_ignored_fields = ()
    form_fieldtypes = {}
    form_fieldkwargs = {}
    form_fieldparams = {}

    document = {}
    deleteable = bool(document_id) # Mostrar o no el botón de borrar
    sort_fnc = lambda x: x

    # Especificidades de las colecciones
    if collection == "user":
        # Nota: El campo "password" es un hash, de modo que lo establezco de
        #       sólo lectura y creo un nuevo campo "new password" para
        #       cambiarla (ver endpoint "db_confirm").
        page_title = 'admin_users'
        form_title = 'admin_users_info'
        form_force_fields.add("new password")
        form_readonly_fields.update(("created","password"))
        data = usersdb.find_userid(document_id) if document_id else {}
    elif collection == "origin":
        deleteable = deleteable and current_app.debug
        page_title = 'admin_origins_info'
        form_title = 'admin_origins_info'
        data = filesdb.get_source_by_id(float(document_id)) if document_id else {}
    elif collection == "alternatives":
        url_id = "admin.alternatives"
        available_methods = configdb.list_alternatives_methods()
        available_endpoints = configdb.list_alternatives_endpoints(document_id) if document_id else []
        available_param_types = configdb.list_alternatives_param_types()

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
    elif collection == "server":
        form_force_fields.add("rs")
        deleteable = deleteable and current_app.debug
        page_title = 'admin_server'
        form_title = 'admin_server'
        data = filesdb.get_server(float(document_id)) if document_id else {}
        fixed_order = ("ip","p","rip","rp")
        sort_fnc = lambda x: fixed_order.index(x) if x in fixed_order else x
    else:
        abort(404)

    if data: document.update(data)
    document.update((i, db_ftd(collection, i, "")) for i in form_force_fields if i not in data)

    document_defaults = document
    document_writeable = sorted(
        (k for k in document.iterkeys() if not (k in form_readonly_fields or k in form_ignored_fields)),
        key=sort_fnc
        )

    edict = {}
    edit_form = expanded_instance(EditForm, {
        db_fnm(k): form_fieldtypes.get(k, TextField
            )(k, default=db_dtf(collection, k, document[k]), **form_fieldkwargs.get(k, edict))
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
    elif collection == "server":
        url_id = 'admin.servers'
        save_fnc = lambda data: filesdb.update_server(data)
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

@admin.route("/<lang>/admin/actions", methods=("POST","GET"))
def actions(actionid = None):

    form = ActionForm(request.form)
    form.target.choices = [(i,i) for i in configdb.get_current_profiles()]

    actions = tuple(configdb.list_actions())

    form.submitlist.choices = [
        (actionid, _('admin_actions_run'))
        for actionid, fnc, unique, args, kwargs in actions
        ]

    if request.method == "POST":
        flash(_("admin_actions_updated"))
        configdb.run_action(actionid)
        return redirect(url_for(".actions"))

    return render_template("admin/action.html",
        title = admin_title("admin_actions"),
        page_title = _("admin_actions"),
        form = form,
        interval = current_app.config.get("CONFIG_UPDATE_INTERVAL", -1),
        actions = [(
            submit,
            actionid,
            "%s(%s)" % (
                "%s.%s" % (
                    fnc.im_class().__class__.__name__ if hasattr(fnc.im_class(), "__class__") else fnc.im_class().__name__,
                    fnc.__name__
                    ) if hasattr(fnc, "im_class") else fnc.__name__,
                ", ".join(itertools.chain(
                    (repr(i) for i in args),
                    ("%s=%s" % (k, repr(v)) for k, v in kwargs.iteritems()))),
                ),
            inspect.getdoc(fnc).decode("utf-8"),
            unique
            ) for (actionid, fnc, unique, args, kwargs), submit in itertools.izip(actions, iter(form.submitlist))]
        )


def add_admin(app):
    pomanager.init_app(app)
    app.register_blueprint(admin)

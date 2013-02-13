# -*- coding: utf-8 -*-
"""
    Soporte para Foofind download manager
"""

from flask import g, render_template, request, abort, current_app
from foofind.utils.fooprint import Fooprint
from foofind.utils.flaskutils import send_gridfs_file
from foofind.services import *
from functools import wraps

downloads = Fooprint("downloads", __name__, template_folder="template", dup_on_startswith="/<lang>")

installer_filename = "foofind_download_manager_setup.exe"
installer_dependencies = ("setup.exe", "vcredist_x86.exe")
setup_filename = installer_dependencies[0]
source_filename = "foofind_download_manager_source.tar.gz"

downloader_opened = 0
downloader_success = 0

def track_downloader_info():
    global downloader_opened

    # reinicia contador y guarda
    temp, downloader_opened = downloader_opened, 0
    profiler.save_data({"downloader_opened":temp})

def restrict_url(fnc):
    '''
    Hace que el endpoint sólo sea visible con el useragent del
    instalador o el actualizador del downloader.
    '''
    @wraps(fnc)
    def wrapped(*args, **kwargs):
        ua = request.headers.get("user_agent", "").split("/")[0].strip()
        if ua in current_app.config["DOWNLOADER_UA"]:
            return fnc(*args, **kwargs)
        abort(404)
    return wrapped

@cache.memoize(600) # 10 minutos
def get_foodownloader_data():
    d = {}
    pdata = downloadsdb.get_file(installer_filename)
    if pdata:
        d["length"] = pdata["length"]
        d["version_code"] = pdata["version_code"]

    #
    #rdata = downloadsdb.get_file(setup_filename)
    #if rdata:
    #    d["version_code"] = rdata["version_code"]

    sdata =  downloadsdb.get_file(source_filename)
    if sdata:
        d["source_length"] = sdata["length"]

    d["available"] = bool(pdata) #pdata and rdata
    d["source_available"] = bool(sdata)
    return d

@downloads.route("/<lang>/downloader")
def foodownloader_microsite():
    g.title = "Foofind download manager"
    props = get_foodownloader_data()
    return render_template(
        'microsite/foodownloader.html',
        properties = props,
        mode = "download",
        style_alternative = request.args.get("a", 0, int)
        )

@downloads.route("/<lang>/downloader/success")
def foodownloader_success():
    global downloader_success
    downloader_success += 1
    return render_template(
        'microsite/foodownloader.html',
        mode = "success",
        style_alternative = 0
        )

@downloads.route("/<lang>/downloader/version")
def foodownloader_version():
    global downloader_opened
    downloader_opened += 1
    version = downloadsdb.get_last_version(installer_filename)
    # TODO(felipe): cambiar a setup_filename con el nuevo instalador
    if version is None:
        return "0"
    return version

@downloads.route("/<lang>/downloader/installer/<version>/<instfile>")
@restrict_url
def foodownloader_dependency_download(version, instfile):
    '''
    Ficheros relacionados con el instalador y el actualizador
    '''
    global downloader_opened
    if instfile == "version":
        # Obtiene la versión de setup_filename (el instalador real)
        downloader_opened += 1
        version = downloadsdb.get_last_version(setup_filename)
        if version is None:
            return "0"
        return version
    elif instfile not in installer_dependencies:
        abort(404)
    f = downloadsdb.stream_file(instfile)
    if f is None:
        abort(404)
    return send_gridfs_file(f)

@downloads.route("/<lang>/downloader/%s" % installer_filename)
def foodownloader_download():
    # TODO(felipe): descomentar para descargar el instalador real al actualizar
    '''
    if "python" in request.headers.get("user_agent", "").lower():
        # Transición actualizador: descarga el instalador real
        f = downloadsdb.stream_file(setup_filename)
    else:
        # Descarga el instalador
        f = downloadsdb.stream_file(installer_filename)
    '''
    f = downloadsdb.stream_file(installer_filename)
    if f is None:
        abort(404)
    return send_gridfs_file(f)

@downloads.route("/<lang>/downloader/%s" % source_filename)
def foodownloader_source_download():
    f = downloadsdb.stream_file(source_filename)
    if f is None:
        abort(404)
    return send_gridfs_file(f)

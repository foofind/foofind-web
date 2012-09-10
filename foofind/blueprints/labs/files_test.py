# -*- coding: utf-8 -*-
from flask import Blueprint, request, g, current_app, render_template, jsonify
from foofind.utils.pagination import Pagination
from foofind.services import *
from foofind.utils import sphinxapi, hex2mid, mid2bin, mid2hex, bin2hex, hex2mid, mid2bin, mid2hex, bin2hex, u
#from foofind.services.search import get_ids, block_files
from foofind.blueprints.files import taming_search, block_files, save_visited, fill_data, download as files_download
from foofind.forms.files import SearchForm
import itertools

files_test = Blueprint('files_test', __name__, template_folder="template")

all_langs = {}
def init_searchd(app):
    global all_langs
    all_langs = {l:i for i,l in enumerate(app.config["ALL_LANGS"])}

@files_test.context_processor
def file_var():
    return {"zone":"files","search_form":SearchForm(request.args),"args":request.args}

@files_test.route('/<lang>/search_stats/')
def search_stats():
    return jsonify(searchd.proxy.stats)
    
@files_test.route('/<lang>/search_info/')
def search_info():
    query = request.args.get("q", None)
    info = searchd.get_search_info(query)
    return jsonify({"stats":[{"-":info[0]}, {"-":info[1]}]})
    
@files_test.route('/<lang>/searcht')
def search():
    query = request.args.get("q", None)
    if not query:
        flash("write_something")
        return redirect(url_for("index.home"))
    results = {"total_found":0,"total":0,"time":0}

    g.title = "%s - %s" % (query, g.title)

    from time import time

    a=time()
    # obtener los resultados y sacar la paginación
    s = searchd.search(query, None, all_langs.get(g.lang))
    print "a", time()-a; a=time()
    ids = list(s.get_results())
    print "b", time()-a; a=time()
    results["time"] = 1
    stats = s.get_stats()
    results["total_found"] = int(stats["cs"])
    print "c", time()-a; a=time()
    files_dict = {mid2hex(file_data["_id"]):fill_data(file_data, False, query) for file_data in get_files(ids)}
    print "d", time()-a; a=time()
    files=({"file":files_dict[file_id[0]], "search":file_id} for file_id in ids if file_id[0] in files_dict)
    print "e", time()-a; a=time()

    return render_template('files/search.html',
        results=results,
        search=request.args["q"].split(" "),
        files=files,
        pagination=Pagination(1, 10, min(results["total_found"], 1000)),
        didyoumean=None,
        tags=None)
        
@files_test.route('/<lang>/download/<file_id>',methods=['GET','POST'])
@files_test.route('/<lang>/download/<file_id>/<path:file_name>.html',methods=['GET','POST'])
@files_test.route('/<lang>/download/<file_id>/<path:file_name>',methods=['GET','POST'])
def download(file_id,file_name=None):
    return files_download(file_id,file_name)


def get_files(ids):
    '''
    Recibe lista de tuplas de tamaño 3 (como devueltas por get_ids de search)
    y devuelve los ficheros del mongo correspondiente que no estén bloqueados.
    Si se recibe un fichero bloqueado, lo omite y bloquea en el sphinx.

    @type ids: iterable de tuplas de tamaño 3 o mayor
    @param ids: lista de tuplas (mongoid, id servidor, id sphinx)

    @yield: cada uno de los resultados de los ids en los mongos

    '''
    toblock = []
    already = False
    for f in filesdb.get_files(ids, servers_known = True, bl = None):
        if f["bl"] == 0 or f["bl"] is None:
            yield f
        else:
            toblock.append(mid2hex(f["_id"]))

    # bloquea en sphinx los ficheros bloqueados
    if toblock:
        cache.cacheme = False
        id_list = {i[0]:i[2] for i in ids}
        block_files( sphinx_ids=[id_list[i] for i in toblock] )

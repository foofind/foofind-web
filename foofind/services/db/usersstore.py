# -*- coding: utf-8 -*-
import pymongo, sys
from bson.code import Code
from hashlib import sha256
from datetime import datetime
from time import time

from foofind.utils import hex2mid, check_collection_indexes, userid_parse, mid2hex, logging
from foofind.services.extensions import cache


class UsersStore(object):
    '''
    Clase para acceder a los datos de los usuarios.
    '''
    _indexes = {
        "favfile": (
            {"key": [("user_id", 1)]},
            {"key": [("files.server_id", 1)]},
            {"key": [("user_id", 1), ("name", 1), ("type",1)], "unique":1},
            ),
        "favsearch": (
            {"key": [("user_id", 1)]},
            {"key": [("user_id", 1), ("q", 1), ("filter", 1)], "unique":1},
            ),
        "users": (
            {"key": [("email", 1)]},
            {"key": [("token", 1)]},
            {"key": [("username", 1)]},
            {"key": [("oauthid", 1)]},
            ),
        "comment": (
            {"key": [("f", 1), ("l", 1)]},
            ),
        "comment_vote": (
            {"key": [("f", 1)]},
            )
        }

    def __init__(self):
        '''
        Inicialización de la clase.
        '''
        self.users_conn = None

    def init_app(self, app):
        '''
        Apply users database access configuration.

        @param app: Flask application.
        '''
        if app.config["DATA_SOURCE_USER"]:
            if "DATA_SOURCE_USER_RS" in app.config:
                self.user_conn = pymongo.MongoReplicaSetClient(app.config["DATA_SOURCE_USER"],
                                                                max_pool_size=app.config["DATA_SOURCE_MAX_POOL_SIZE"],
                                                                replicaSet = app.config["DATA_SOURCE_USER_RS"],
                                                                read_preference = pymongo.read_preferences.ReadPreference.SECONDARY_PREFERRED,
                                                                tag_sets = app.config.get("DATA_SOURCE_USER_RS_TAG_SETS",[{}]),
                                                                secondary_acceptable_latency_ms = app.config.get("SECONDARY_ACCEPTABLE_LATENCY_MS", 15))
            else:
                self.user_conn = pymongo.MongoClient(app.config["DATA_SOURCE_USER"], max_pool_size=app.config["DATA_SOURCE_MAX_POOL_SIZE"], slave_okay=True)

            self.init_user_conn()

    def share_connections(self, user_conn=None):
        '''
        Allows to share data source connections with other modules.
        '''
        if user_conn:
            self.user_conn = user_conn
            self.init_user_conn()

    def init_user_conn(self):
        '''
        Inits users database before its first use.
        '''
        check_collection_indexes(self.user_conn.users, self._indexes)
        self.user_conn.end_request()

    def remove_userid(self, userid):
        '''
        Borra un usuario con el userid dado

        @type userid: hex u ObjectID
        @param userid: identificador del usuario
        '''
        self.user_conn.users.users.remove({"_id":userid_parse(userid)})
        self.user_conn.end_request()

    def create_user(self,data):
        '''
        Guarda los datos del usuario.

        @param data: Diccionario con los datos del usuario a guardar.
        '''
        data = self.user_conn.users.users.insert({
            "username":data["username"],
            "email":data["email"],
            "password":sha256(data["password"]).hexdigest(),
            "karma":0.2,
            "token":data["token"],
            "created": datetime.utcnow()
            })
        self.user_conn.end_request()
        return data

    _userParse = { # Parseo de datos para base de datos
        "active": lambda x:int(float(x)),
        "type": lambda x:int(float(x)),
        "karma": float,
        "created": lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f") if isinstance(x, basestring) else x, # formato ISO: YYYY-MM-DD HH:MM:SS[.mmmmmm][+HH:MM],
        "*": unicode
        }
    def update_user(self, data, remove=None):
        '''
        Actualiza los datos del usuario

        @param data: Diccionario con los datos del usuario a guardar.
        @param remove: Lista de campos a quitar del registro.
        '''
        update = {"$set":data.copy()}
        if "password" in data:
            update["$set"]["password"] = sha256(data["password"]).hexdigest()

        if remove is not None:
            update["$unset"]={}
            for rem in remove:
                del update["$set"][rem]
                update["$unset"][rem]=1

        del update["$set"]["_id"]

        parser = self._userParse
        update["$set"].update(
            (key, parser[key](value) if key in parser else parser["*"](value))
            for key, value in update["$set"].iteritems())

        self.user_conn.users.users.update({"_id":data["_id"]}, update)
        self.user_conn.end_request()

    def find_login(self,email,password):
        '''
        Busca los datos de usuario (activo) a partir de los datos de login
        '''
        return self.__search({"email":email,"password":sha256(password).hexdigest(),"active":1})

    def find_userid(self,userid):
        '''
        Busca los datos del usuario a partir del id
        '''
        return self.__search({"_id":userid_parse(userid)})

    def find_username(self,username):
        '''
        Busca por un nombre de usuario
        '''
        return self.__search({"username":username})

    def find_username_start_with(self,username):
        '''
        Busca por un nombre de usuario
        '''
        cursor = self.user_conn.users.users.find({"username":{'$regex':'^'+username+'(_\\d+)?$'}})
        for document in cursor:
            yield document
        self.user_conn.end_request()

    def find_email(self,email):
        '''
        Busca por un email
        '''
        return self.__search({"email":email})

    def find_token(self,token):
        '''
        Busca por un token
        '''
        return self.__search({"token":token})

    def find_oauthid(self,oauthid):
        '''
        Busca por oauthid
        '''
        return self.__search({"oauthid":oauthid})

    def __search(self, data):
        '''
        Busqueda por los campos que se reciban
        '''
        user = self.user_conn.users.users.find_one(data)
        self.user_conn.end_request()
        return user

    def set_file_vote(self, file_id, user, lang, vote):
        '''
        Guarda el voto en la colección y actualiza el archivo correspondiente con los nuevos datos
        '''
        data  = {
            "u": user.id,
            "k": 1 if vote == 1 else -1,
            "d": datetime.utcnow(),
            "l": lang,
            }
        # TODO(felipe): borrar con error solucionado
        if user.id < 0 and user.is_authenticated():
            logging.error("Inconsistencia de usuario votando logeado id negativo.", extra=locals())
        else:
            if user.is_authenticated():
                data["_id"] =  "%s_%s" % (mid2hex(file_id), user.id)
                self.user_conn.users.vote.update(
                    {"_id": data["_id"]}, data, upsert=True)
            else:
                data["_id"] = "%s:%s" % (mid2hex(file_id), user.session_ip)
                self.user_conn.users.vote.update(
                    {"_id": data["_id"], "u": data["u"]},
                    data, upsert=True)

        # Para cada idioma guarda el karma, la cuenta total y la suma
        map_function = Code('''
            function()
            {
                emit(this.l,{
                    k:this.k,
                    c:new Array((this.k>0)?1:0,(this.k<0)?1:0),
                    s:new Array((this.k>0)?this.k:0,(this.k<0)?this.k:0)
                })
            }''')
        # Suma todo y aplica la funcion 1/1+E^(-X) para que el valor este entre 0 y 1
        reduce_function = Code('''
            function(lang, vals)
            {
                c=new Array(0,0);
                s=new Array(0,0);
                for (var i in vals)
                {
                    c[0]+=vals[i].c[0];
                    c[1]+=vals[i].c[1];
                    s[0]+=vals[i].s[0];
                    s[1]+=vals[i].s[1];
                }
                return {t:1/(1+Math.exp(-((s[0]*c[0]+s[1]*c[1])/(c[0]+c[1])))), c:c, s:s};
            }''')
        # Tercer parametro para devolverlo en vez de generar una coleccion nueva
        votes = self.user_conn.users.vote.map_reduce(
            map_function,
            reduce_function,
            {"inline": 1},
            query = {"_id": {"$regex": "^%s" % mid2hex(file_id)}}
            )
        # Devolver un diccionario de la forma idioma:valores
        data = {values["_id"]: values["value"] for values in votes["results"]}
        self.user_conn.end_request()
        return data

    @cache.memoize(timeout=60*60)
    def list_fav_lists(self, user):
        '''
        Obtener los nombres de lista de favoritos con nombre.

        @param user: objeto usuario
        @rtype list
        @return Lista de nombres de listas de favoritos.
        '''
        doc = [
            i["name"]
            for i in self.user_conn.users.favfile.find({
                "user_id": user.id, "type": 0}, {"files": 0})]
        self.user_conn.end_request()
        return doc
    list_fav_lists.make_cache_key = lambda self, user: "memoized/usersstore.list_fav_list/%s" % user.id

    @cache.memoize(timeout=60*60)
    def list_fav_files(self, user):
        '''
        Obtener todos los ficheros de todas las listas de usuario

        @param user: objeto usuario
        @rtype set
        @return Set de diccionarios con "id", "name" y "server".
        '''
        doc = {f for doc in self.user_conn.users.favfile.find({"user_id": user.id}) for f in doc["files"]}
        self.user_conn.end_request()
        return doc
    list_fav_files.make_cache_key = lambda self, user: "memoized/usersstore.list_fav_files/%s" % user.id

    def _get_fav_files(self, user, category=0, name=None, skip=0, limit=None):
        ''' Interfaz común para obtener favoritos

        @param user: objeto usuario
        @param category: tipo de favorito, puede ser
                          - 0 : Lista de favoritos con nombre
                          - 1 : Lista de favoritos por defecto (sin nombre)
        @param name: Nombre de la lista para los favoritos con nombre (category = 0)
        @param skip:  número de ficheros a omitir por el principio.
        @param limit: número de ficheros a obtener
        '''
        qslice = None
        if skip:
            qslice = {"files":{"$slice":(skip, limit if limit else sys.maxint)}}
        elif limit:
            qslice = {"files":{"$slice":limit}}
        doc = self.user_conn.users.favfile.find_one(
            {"user_id": user.id, "type": category, "name": name if category == 0 else None},
            qslice
            )
        self.user_conn.end_request()
        return doc.get("files", ()) if doc else []

    def _add_fav_file(self, user, fileid, server, filename = None, category = 0, listname = None):
        ''' Interfaz común para añadir favoritos

        @param user: objeto usuario
        @param fileid: id de fichero
        @param server: id de servidor
        @param filename: nombre del fichero
        @param category: tipo de favorito, puede ser
                          - 0 : Lista de favoritos con nombre
                          - 1 : Lista de favoritos por defecto (sin nombre)
        @param listname: Nombre de la lista para los favoritos con nombre (category = 0)
        '''
        self.list_fav_files.flush(self, user)
        self.user_conn.users.favfile.update(
            {"user_id": user.id, "name": listname if category == 0 else None, "type": category},
            {"$addToSet":{
                "files":{
                    "id": fileid,
                    "name": filename,
                    "server": server,
                    }
                }},
            upsert=True,
            safe=True)
        self.user_conn.end_request()

    def _del_fav_file(self, user, fileid, category = 0, listname = None):
        ''' Interfaz común para borrar favoritos

        @param user: objeto usuario
        @param fileid: id de fichero
        @param category: tipo de favorito, puede ser
                          - 0 : Lista de favoritos con nombre
                          - 1 : Lista de favoritos por defecto (sin nombre)
        @param listname: Nombre de la lista para los favoritos con nombre (category = 0)
        '''
        self.list_fav_files.flush(self, user)
        self.user_conn.users.favfile.update(
            {"user_id": user.id, "name":listname if category == 0 else None, "type":category},
            {"$pull": {"files": {"id": fileid}}})
        self.user_conn.end_request()

    def get_fav_user_list(self, user, name, skip=0, limit=None):
        return self._get_fav_files(user, 0, name, skip, limit)

    def get_fav_files(self, user, skip=0, limit=None):
        return self._get_fav_files(user, 1, None, skip, limit)

    def get_fav_searches(self, user, skip=0, limit=None):
        data = tuple(self.user_conn.users.favsearch.find({"user":user.id}))
        self.user_conn.end_request()
        return data

    def add_fav_file(self, user, fileid, server, filename = None):
        self._add_fav_file(user, fileid, server, filename, 1)

    def move_fav_file(self, user, fileid, origin_category = 0, origin_name = None, destiny_category = 0, destiny_name = None):
        self._del_fav_file(self, user, fileid, origin_category, origin_name)
        self._add_fav_file(self, user, fileid, origin_category, origin_name)

    def remove_fav_file(self, user, fileid):
        self._del_fav_file(user, fileid, 1)

    def add_fav_user_file(self, user, fileid, server, filename = None, listname = None):
        assert not listname is None
        self._add_fav_file(user, fileid, server, filename, 0, listname)

    def remove_fav_user_file(self, user, fileid, listname = None):
        assert not listname is None
        self._add_fav_file(user, fileid, 0, listname)

    def add_fav_user_list(self, user, name):
        # Borrado de caché
        self.list_fav_lists.flush(self, user)
        self.list_fav_files.flush(self, user)
        self.user_conn.users.favfile.insert(
            {"user_id": user.id, "type":0, "name": name, "files":[]})

    def remove_fav_user_list(self, user, name):
        # Borrado de caché
        self.list_fav_lists.flush(self, user)
        self.list_fav_files.flush(self, user)
        self.user_conn.users.favfile.remove(
            {"user_id": user.id, "type":0, "name": name})

    def get_file_vote(self, file_id, user, lang):
        '''
        Recupera el voto de un usuario para un archivo
        '''
        if user.is_authenticated():
            data = self.user_conn.users.vote.find_one({
                "_id": "%s_%s" % (mid2hex(file_id), user.id),
                "l": lang
                })
        else:
            data = self.user_conn.users.vote.find_one({
                "_id": "%s:%s" % (mid2hex(file_id), user.session_ip),
                "l": lang,
                "u": user.id
                })
        self.user_conn.end_request()
        return data

    def set_file_comment(self, file_id, user, lang, comment):
        '''
        Guarda un comentario de un archivo
        '''
        data = self.user_conn.users.comment.insert({
            "_id": "%s_%s" % (user.id, int(time())),
            "f": file_id,
            "l": lang,
            "d": datetime.utcnow(),
            "k": user.karma,
            "t": comment
            })
        self.user_conn.end_request()
        return data

    def get_file_comments_sum(self, file_id):
        '''
        Cuenta los comentarios que hay para cada idioma
        '''
        data = {
            lang["l"]: lang["c"]
            for lang in self.user_conn.users.comment.group(
                {"l": 1}, {'f': hex2mid(file_id)}, {"c": 0},
                Code("function(o,p){p.c++}"))
            }
        self.user_conn.end_request()
        return data

    def get_file_comments(self,file_id,lang):
        '''
        Recupera los comentarios de un archivo
        '''
        cursor = self.user_conn.users.comment.find({"f": hex2mid(file_id),"l": lang})
        for document in cursor:
            yield document
        self.user_conn.end_request()

    def set_file_comment_vote(self,comment_id,user,file_id,vote):
        '''
        Guarda el comentario en la colección y actualiza el archivo correspondiente con los nuevos datos
        '''
        self.user_conn.users.comment_vote.save({"_id":"%s_%s"%(str(comment_id), str(user.id)),"u":user.id,"f":file_id,"k":user.karma if vote==1 else -user.karma,"d":datetime.utcnow()})
        #guarda el karma, la cuenta total, la suma y el usuario
        map_function=Code('''
            function()
            {
                var pos=this._id.lastIndexOf('_');
                emit('1',{
                        k:this.k,
                        c:new Array((this.k>0)?1:0, (this.k<0)?1:0),
                        s:new Array((this.k>0)?this.k:0,(this.k<0)?this.k:0)/*,
                        u:(this.u=="%d")?this.k:0*/
                })
            }''' % user.id)
        #suma todo y aplica la funcion 1/1+E^(-X) para que el valor este entre 0 y 1
        reduce_function=Code('''
            function(lang, vals)
            {
                var c=new Array(0,0);
                var s=new Array(0,0);
                var u=0;
                for(var i in vals)
                {
                    c[0]+=vals[i].c[0];
                    c[1]+=vals[i].c[1];
                    s[0]+=vals[i].s[0];
                    s[1]+=vals[i].s[1];
                    //u+=vals[i].u;
                }
                return {t:1/(1+Math.exp(-((s[0]*c[0]+s[1]*c[1])/(c[0]+c[1])))), c:c, s:s/*, u:u*/};
            }''')
        #tercer parametro para devolverlo en vez de generar una coleccion nueva
        votes=self.user_conn.users.comment_vote.map_reduce(map_function,reduce_function,{"inline":1},query={'_id':{'$regex':"^%s"%comment_id}})
        #crear diccionario de la forma idioma:valores, actualizar el comentario con el y devolverlo
        data={values["_id"]:values["value"] for values in votes["results"]}
        self.user_conn.users.comment.update({"_id":comment_id},{"$set":{"vs":data}})
        self.user_conn.end_request()
        return data

    def count_users(self):
        return self.user_conn.users.users.find({"active":1}).count()

    def get_file_comment_votes(self,file_id):
        '''
        Recuper los votos de los comentarios de un archivo
        '''
        cursor = self.user_conn.users.comment_vote.find({"f":hex2mid(file_id)})
        for document in cursor:
            yield cursor
        self.user_conn.end_request()

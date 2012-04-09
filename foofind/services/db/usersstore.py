# -*- coding: utf-8 -*-
import pymongo
from bson.code import Code
from foofind.utils import hex2mid, end_request
from hashlib import sha256
from datetime import datetime
from time import time


class UsersStore(object):
    '''
    Clase para acceder a los datos de los usuarios.
    '''
    def __init__(self):
        '''
        Inicialización de la clase.
        '''
        self.max_pool_size = 0
        self.users_conn = None

    def init_app(self, app):
        '''
        Inicializa la clase con la configuración de la aplicación.

        @param app: Aplicación de Flask.
        '''
        self.max_pool_size = app.config["DATA_SOURCE_MAX_POOL_SIZE"]

        # Inicia conexiones
        self.user_conn = pymongo.Connection(app.config["DATA_SOURCE_USER"], slave_okay=True, max_pool_size=self.max_pool_size)

    def create_user(self,data):
        '''
        Guarda los datos del usuario.

        @param data: Diccionario con los datos del usuario a guardar.
        '''
        return end_request(self.user_conn.foofind.users.insert({"username":data["username"],"email":data["email"],"password":sha256(data["password"]).hexdigest(),"karma":0.2,"token":data["token"],"created": datetime.utcnow()}),self.user_conn)

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

        self.user_conn.foofind.users.update({"_id":hex2mid(data["_id"])}, update)
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
        return self.__search({"_id":hex2mid(userid)})

    def find_username(self,username):
        '''
        Busca por un nombre de usuario
        '''
        return self.__search({"username":username})

    def find_username_start_with(self,username):
        '''
        Busca por un nombre de usuario
        '''
        return end_request(
            self.user_conn.foofind.users.find({"username":{'$regex':'^'+username+'(_\\d+)?$'}}))

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

    def __search(self,data):
        '''
        Busqueda por los campos que se reciban
        '''
        return end_request(self.user_conn.foofind.users.find_one(data), self.user_conn)

    def set_file_vote(self,file_id,user,lang,vote):
        '''
        Guarda el voto en la colección y actualiza el archivo correspondiente con los nuevos datos
        '''
        self.user_conn.foofind.vote.save({"_id":"%s_%s"%(file_id,user.id),"u":hex2mid(user.id),"k":user.karma if vote==1 else -user.karma,"d":datetime.utcnow(),"l":lang})
        #para cada idioma guarda el karma, la cuenta total y la suma
        map_function=Code('''
            function()
            {
                emit(this.l,{
                    k:this.k,
                    c:new Array((this.k>0)?1:0,(this.k<0)?1:0),
                    s:new Array((this.k>0)?this.k:0,(this.k<0)?this.k:0)
                })
            }''')
        #suma todo y aplica la funcion 1/1+E^(-X) para que el valor este entre 0 y 1
        reduce_function=Code('''
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
        #tercer parametro para devolverlo en vez de generar una coleccion nueva
        votes=self.user_conn.foofind.vote.map_reduce(map_function,reduce_function,{"inline":1},query={'_id':{'$regex':"^%s"%file_id}})
        #devolver un diccionario de la forma idioma:valores
        return end_request({values["_id"]:values["value"] for values in votes["results"]}, self.user_conn)

    def get_file_vote(self,file_id,user,lang):
        '''
        Recupera el voto de un usuario para un archivo
        '''
        return end_request(
            self.user_conn.foofind.vote.find_one({"_id":"%s_%s"%(file_id,user.id),"l":lang}),
            self.user_conn)

    def set_file_comment(self,file_id,user,lang,comment):
        '''
        Guarda un comentario de un archivo
        '''
        return end_request(
            self.user_conn.foofind.comment.insert({
                "_id": "%s_%s" % (user.id,int(time())),
                "f": file_id,
                "l": lang,
                "d": datetime.utcnow(),
                "k": user.karma,
                "t": comment}),
            self.user_conn)

    def get_file_comments_sum(self,file_id):
        '''
        Cuenta los comentarios que hay para cada idioma
        '''
        return end_request({
            lang["l"]:lang["c"] for lang in self.user_conn.foofind.comment.group({"l":1},{'f':hex2mid(file_id)},{"c":0},Code("function(o,p){p.c++}"))},
            self.user_conn)

    def get_file_comments(self,file_id,lang):
        '''
        Recupera los comentarios de un archivo
        '''
        return end_request(self.user_conn.foofind.comment.find({"f":hex2mid(file_id),"l":lang}))

    def set_file_comment_vote(self,comment_id,user,file_id,vote):
        '''
        Guarda el comentario en la colección y actualiza el archivo correspondiente con los nuevos datos
        '''
        self.user_conn.foofind.comment_vote.save({"_id":"%s_%s"%(comment_id,user.id),"u":user.id,"f":file_id,"k":user.karma if vote==1 else -user.karma,"d":datetime.utcnow()})
        #guarda el karma, la cuenta total, la suma y el usuario
        map_function=Code('''
            function()
            {
                pos=this._id.lastIndexOf('_');
                emit('1',{
                        k:this.k,
                        c:new Array((this.k>0)?1:0, (this.k<0)?1:0),
                        s:new Array((this.k>0)?this.k:0,(this.k<0)?this.k:0)/*,
                        u:(this.u=="'''+user.id+'''")?this.k:0*/
                })
            }''')
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
        votes=self.user_conn.foofind.comment_vote.map_reduce(map_function,reduce_function,{"inline":1},query={'_id':{'$regex':"^%s"%comment_id}})
        #crear diccionario de la forma idioma:valores, actualizar el comentario con el y devolverlo
        data={values["_id"]:values["value"] for values in votes["results"]}
        self.user_conn.foofind.comment.update({"_id":comment_id},{"$set":{"vs":data}})
        self.user_conn.end_request()
        return data

    def get_file_comment_votes(self,file_id):
        '''
        Recuper los votos de los comentarios de un archivo
        '''
        return end_request(self.user_conn.foofind.comment_vote.find({"f":hex2mid(file_id)}))

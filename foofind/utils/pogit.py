# -*- coding: utf-8 -*-

import os.path
import git
import polib
import collections
import time
import logging

from . import touch_path

'''
mkdir myrepo
cd myrepo
git ini
git config core.sparsecheckout ...
git checkout [branchname]


Cuando cambie los sparse-checkout
git read-tree -mu HEAD
'''

__all__ = ("pomanager", "CommitException")

class CommitException(Exception):
    def __init__(self, msg, status):
        Exception.__init__(self, msg)
        self.status = status

    def __str__(self):
        return "%s\n\t%s" % (
            Exception.__str__(self),
            "\n\t".join("%s: %s" % i for i in self.status.iteritems())
            )

class LangRepoManager(object):
    '''
    Esta clase aprovecha la opción de sparse-checkout (git 1.7+) para obtener
    del repositorio sólo los archivos de traducción que hagan falta.

    Trabaja internamente con una lista de ficheros conocidos (que serán los
    únicos descargados y actualizados), si se pide un fichero de lenguaje
    desconocido se lo intentará bajar del servidor y lo añadirá a la lista.
    '''

    git_author = None
    git_email = None
    branch = None
    lang_folder = None
    base = None
    local_dir = None
    repo = None

    def __init__(self, attemps_on_fail = 3, file_lifetime=3600):

        self._attemps_on_fail = 3
        self._time = 0
        self._file_lifetime = file_lifetime

    def get_current_langs(self):
        # Retornamos la lista de lenguajes conocidos (cuyo directorio
            # seguimos) con los lenguajes que ya tengamos
        return tuple(i for i in os.listdir(self.local_dir)
            if os.path.isdir(os.path.join(self.local_dir, i)))

    def is_current_lang(self, x):
        return os.path.isdir(os.path.join(self.local_dir, x))

    def init_lang_repository(self, app):
        '''Inicializa la clase y los repositorios aprovechando la
        configuración de aplicación.
        '''

        #self.remote = app.config["ADMIN_LANG_REMOTE"]
        self.git_author = app.config["ADMIN_GIT_AUTHOR"]
        self.git_email = app.config["ADMIN_GIT_EMAIL"]
        self.repo_url = app.config["ADMIN_LANG_REMOTE_REPO"]

        self.branch = app.config["ADMIN_LANG_REMOTE_BRANCH"]

        # Directorio remoto de traducciones
        self.lang_folder = app.config["ADMIN_LANG_FOLDER"]

        # Directorio del repositorio local
        self.base = app.config["ADMIN_LANG_LOCAL_REPO"]

        # Directorio local de traducciones (git replica el árbol de directorios
        # completo con sparse-checkout)
        self.local_dir = os.path.join(self.base, self.lang_folder)

        touch_path(self.local_dir) # nos aseguramos que el árbol existe

        self.repo = git.Repo.init(self.base, mkdir=False)
        if len(self.repo.remotes) == 0:
            self.repo.create_remote("origin", self.repo_url)

        assert self.repo.git.version_info[:2] >= (1,7), "Se requiere git 1.7 como mínimo."

        config = self.repo.config_writer()
        config.set_value("core","sparsecheckout","true")
        config.write()

        # Localizo el fichero de configuración de sparse-checkout
        info = os.path.join(self.base, ".git", "info")
        touch_path(info) # info puede no existir
        self.sparsepath = os.path.join(info, "sparse-checkout")

    def _refresh_tree(self, new_langs=()):
        '''Actualiza las rutas seguidas'''
        f = open(self.sparsepath, "w")
        f.writelines("%s/%s\n" % (self.lang_folder, c) for c in (self.get_current_langs() + new_langs))
        f.close()
        if self.repo.active_branch.is_valid():
            # Si ya está inicializado HEAD relee la ruta, hace merge and update.
            for attemp in xrange(self._attemps_on_fail):
                try:
                    self.repo.git.read_tree("-mu", self.repo.active_branch.name)
                except AssertionError as a:
                    logging.error("pogit.py _refresh_tree intento %d:\n\t%s" % (attemp, a))
                finally: break
            else:
                raise Exception("No se ha podido refrescar la rama de traducciones.")

    def _refresh_langs(self):
        '''Pull de los ficheros conocidos'''
        for attemp in xrange(self._attemps_on_fail):
            print "%s:%s" % (self.branch, self.repo.active_branch.path)
            try:
                self.repo.remotes[0].pull(
                    "%s:%s" % (self.branch, self.repo.active_branch.path))
            except AssertionError as a:
                logging.error("pogit.py _refresh_langs intento %d:\n\t%s" % (attemp, a))
            finally:
                self._time = time.time()
                break
        else:
            raise Exception("No se pueden descargar los ficheros de idioma")

    def get_pofile(self, code, refresh=None):
        '''Retorna un pofile para en lenguaje dado.'''
        if not self.is_current_lang(code):
            self._refresh_tree()
            self._refresh_langs((code,))
        elif refresh or (refresh is None and time.time() - self._time > self._file_lifetime):
            self._refresh_langs()

        podir = os.path.join(self.local_dir, code, "LC_MESSAGES")
        popath = os.path.join(podir, "messages.po")
        if not os.path.isfile(popath):
            assert code != "en", "No se puede encontrar el idioma %s." % code
            en = self.get_pofile("en")
            touch_path(podir)
            pofile = polib.POFile(fpath=popath)
            pofile.metadata = en.metadata
            pofile.metadata.update({
                "Last-Translator":"Foofind <hola@foofind.com>",
                "Language-Team":"%s <hola@foofind.com>" % code,
                "Generated-By":"Foofind's pogit"
                })
            return pofile
        return polib.pofile(popath)

    def update_lang(self, code, dictionary, refresh=True, commit=True):
        '''Actualiza el fichero de lenguaje con el diccionario dado.

        @type code: str 2 bytes
        @param code: código de lenguaje

        @type dictionary: dict
        @param dictionary: diccionario con msgid : msgstr

        @type refresh: bool
        @param refresh: True por defecto, si se hace pull

        @rype commit: bool
        @param commit: True por defecto, si se hace push
        '''
        en = self.get_pofile("en", refresh) # Fichero base
        new = self.get_pofile(code, False)

        esi = 0 # Contador para insertar en new
        for enitem in en:
            msgid = enitem.msgid
            entry = new.find(msgid)
            if entry:
                esi += 1 # si el campo está en new, incrementamos el indice
            if msgid in dictionary:
                if entry:
                    entry.msgstr = dictionary[msgid]
                else:
                    esi += 1
                    new.insert(esi, polib.POEntry(
                        msgid = msgid,
                        msgstr = dictionary[msgid]))
        new.save()
        if commit: self.commit()

    def get_lang(self, code, base=None):
        if base is None:
            return collections.OrderedDict((i.msgid, i.msgstr) for i in self.get_pofile(code))
        tr = collections.OrderedDict((i.msgid, i.msgstr) for i in self.get_pofile(base))
        tr.update((i.msgid, i.msgstr) for i in self.get_pofile(code))
        return tr

    def commit(self):
        '''Sube al servidor todos los cambios

        @raises CommitException: si falla add, commit o push (ver su atributo status, diccionario)
        '''
        tr = collections.OrderedDict(add="No branch.", commit="Not reached.", push="Not reached.")
        error = bool(self.repo.branches)
        if error is False:
            try:
                tr["add"] = self.repo.git.add("--all") or "OK"
            except Exception as e:
                tr["add"] = str(e)
                error = True
        if error is False:
            try:
                tr["commit"] = self.repo.git.commit("-a",
                    "--message=\"Update of languages on admin\"",
                    "--author=\"%s <%s>\"" % (self.git_author, self.git_email)
                    ) or "OK"
            except Exception as e:
                tr["commit"] = str(e)
                error = True
        if error is False:
            try:
                tr["push"] = self.repo.git.push() or "OK"
            except Exception as e:
                tr["push"] = str(e)
                error = True
        if error:
            raise CommitException("Error al hacer commit.", tr)

    def __contains__(self, x):
        '''Comprueba si se sigue el lenguaje dado, uso: "en" in objeto

        @type x: str 2
        @param x:
        @rtype bool
        @return True si x está en los lenguajes conocidos
        '''
        return x in self._langs

pomanager = LangRepoManager()


# -*- coding: utf-8 -*-
import pymongo
import gridfs

from foofind.utils import logging

class DownloadStore(object):
    def __init__(self):
        '''
        Inicialización de la clase.
        '''
        self.max_pool_size = 0
        self.feedback_conn = None

    def init_app(self, app):
        '''
        Inicializa la clase con la configuración de la aplicación.
        '''
        self.max_pool_size = app.config["DATA_SOURCE_MAX_POOL_SIZE"]

        # Inicia conexiones
        self.download_conn = pymongo.Connection(app.config["DATA_SOURCE_DOWNLOADS"], slave_okay=True, max_pool_size=self.max_pool_size)
        self.download_fs = gridfs.GridFS(self.download_conn.downloads, "download")

    def get_last_version(self, filename):
        '''
        **No confundir con get_last_version de GridFS**

        Devuelve version_code de la última revisión de un fichero de GridFS.

        @type filename: basestring
        @param filename: nombre de fichero

        @rtype None o basestring
        @return Valor asignado al attributo version_code.
        '''
        data = self._get_closed_file(filename=filename)
        if data:
            return data.version_code
        return None

    def list_files(self, skip=0, limit=-1):
        data = self.download_fs.list()
        if limit > -1:
            return data[skip:limit]
        return data[skip:]

    def count_files(self):
        return len(self.download_fs.list())

    def stream_file(self, filename, version=None):
        try:
            if version is None:
                return self.download_fs.get_last_version(filename=filename)
            return self.download_fs.get_last_version(filename=filename, version_code=version)
        except gridfs.errors.NoFile as e:
            return None

    def _get_closed_file(self, filename, version=None):
        try:
            if not version is None:
                f = self.download_fs.get_last_version(filename=filename, version_code=version)
            else:
                f = self.download_fs.get_last_version(filename=filename)
            f.close()
            return f
        except gridfs.errors.NoFile as e:
            logging.warn("Requested download not found: %s" % filename)
        return None

    def get_file(self, filename, version=None):
        f = self._get_closed_file(filename, version)
        if f:
            r = {i:getattr(f, i) for i in (
                'aliases', 'chunk_size', 'content_type', 'length', 'md5',
                'metadata', 'name', 'upload_date',
                'filename', 'version_code' # Custom
                )}
        else:
            r = {}
        return r

    def remove_file(self, filename, version=None):
        f = self._get_closed_file(filename, version)
        if f:
            self.download_fs.delete(f._id)

    def store_file(self, filename,  fileobj, content_type, version):
        assert version is None or isinstance(version, basestring), "Version must be None or basestring"
        self.download_fs.put(fileobj, filename=filename, content_type=content_type, version_code=version)


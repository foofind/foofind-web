#!/usr/bin/env python
# -*- coding: utf-8 -*-

import flask
import functools
import logging
import os
import os.path

def is_downloader_useragent():
    ua = flask.request.headers.get("user_agent", "").split("/")[0].strip()
    return ua in flask.current_app.config["DOWNLOADER_UA"]

def downloader_url(fnc):
    '''
    Hace que el endpoint sólo sea visible con el useragent del
    instalador o el actualizador del downloader.
    '''
    @functools.wraps(fnc)
    def wrapped(*args, **kwargs):
        if True or is_downloader_useragent():
            return fnc(*args, **kwargs)
        abort(404)
    return wrapped

from hachoir_core.error import HachoirError
from hachoir_core.cmd_line import unicodeFilename
from hachoir_parser import createParser
from hachoir_metadata import extractMetadata


def get_file_metadata(path):
    rdata = {}
    if os.path.isfile(path):
        parser = createParser(unicodeFilename(path), path)
        rdata["size"] = os.stat(path).st_size
        if parser:
            try:
                metadata = extractMetadata(parser)
                if metadata:
                    rdata.update(
                        (md.key,
                            md.values[0].value
                            if len(md.values) == 1 else
                            [value.value for value in md.values]
                            )
                        for md in metadata if md.values
                        )
            except HachoirError as e:
                logging.exception(e)
    return rdata


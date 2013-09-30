#!/usr/bin/env python
# -*- coding: utf-8 -*-

from flask import request, abort, current_app
import functools
import logging
import os
import os.path

def is_downloader_useragent():
    ua = request.headers.get("user_agent", "").split("/")[0].strip()
    return ua in current_app.config["DOWNLOADER_UA"]

def downloader_url(fnc):
    '''
    Hace que el endpoint sólo sea visible con el useragent del
    instalador o el actualizador del downloader.
    '''
    @functools.wraps(fnc)
    def wrapped(*args, **kwargs):
        if is_downloader_useragent():
            return fnc(*args, **kwargs)
        abort(404)
    return wrapped

from hachoir_core.error import HachoirError
from hachoir_core.stream.input import NullStreamError

from hachoir_core.cmd_line import unicodeFilename
from hachoir_parser import createParser
from hachoir_metadata import extractMetadata

def get_file_metadata(path):
    rdata = {}
    if os.path.isfile(path):
        try:
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
        except NullStreamError:
            rdata["size"] = 0
        except BaseException as e:
            logging.exception(e)
        finally:
            if parser and parser.stream and parser.stream._input and not parser.stream._input.closed:
                parser.stream._input.close()
    return rdata

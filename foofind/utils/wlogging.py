# -*- coding: utf-8 -*-
import logging as base

DEBUG = base.DEBUG
INFO = base.INFO
WARNING = base.WARNING
WARN = base.WARN
ERROR = base.ERROR
FATAL = base.FATAL
CRITICAL = base.CRITICAL

def _forcekwargs(kwargs):
    if kwargs.get("extra", None) is None:
        kwargs["extra"] = {"stack": True}
    else:
        kwargs["extra"]["stack"] = True
    kwargs["exc_info"] = True

def debug(msg, *args, **kwargs):
    _forcekwargs(kwargs)
    base.debug(msg, *args, **kwargs)

def info(msg, *args, **kwargs):
    _forcekwargs(kwargs)
    base.info(msg, *args, **kwargs)

def warning(msg, *args, **kwargs):
    _forcekwargs(kwargs)
    base.warning(msg, *args, **kwargs)

def warn(msg, *args, **kwargs):
    _forcekwargs(kwargs)
    base.warn(msg, *args, **kwargs)

def error(msg, *args, **kwargs):
    _forcekwargs(kwargs)
    base.error(msg, *args, **kwargs)

def critical(msg, *args, **kwargs):
    _forcekwargs(kwargs)
    base.critical(msg, *args, **kwargs)

def log(lvl, msg, *args, **kwargs):
    _forcekwargs(kwargs)
    base.log(lvl, msg, *args, **kwargs)

def exception(msg, *args):
    base.exception(msg, *args)

def getLogger(*args, **kwargs):
    return base.getLogger(*args, **kwargs)


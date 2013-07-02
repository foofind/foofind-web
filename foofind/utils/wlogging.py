# -*- coding: utf-8 -*-
try:
    from flask import current_app
except ImportError:
    current_app = None

import logging as base

DEBUG = base.DEBUG
INFO = base.INFO
WARNING = base.WARNING
WARN = base.WARN
ERROR = base.ERROR
FATAL = base.FATAL
CRITICAL = base.CRITICAL

def _logger(name=None):
    if current_app:
        return current_app.logger
    return base.getLogger(name)

def _forcekwargs(kwargs):
    if kwargs.get("extra", None) is None:
        kwargs["extra"] = {"stack": True}
    else:
        kwargs["extra"]["stack"] = True
    kwargs["exc_info"] = True

def debug(msg, *args, **kwargs):
    _forcekwargs(kwargs)
    _logger().debug(msg, *args, **kwargs)

def info(msg, *args, **kwargs):
    _forcekwargs(kwargs)
    _logger().info(msg, *args, **kwargs)

def warning(msg, *args, **kwargs):
    _forcekwargs(kwargs)
    _logger().warning(msg, *args, **kwargs)

def warn(msg, *args, **kwargs):
    _forcekwargs(kwargs)
    _logger().warn(msg, *args, **kwargs)

def error(msg, *args, **kwargs):
    _forcekwargs(kwargs)
    _logger().error(msg, *args, **kwargs)

def critical(msg, *args, **kwargs):
    _forcekwargs(kwargs)
    _logger().critical(msg, *args, **kwargs)

def log(lvl, msg, *args, **kwargs):
    _forcekwargs(kwargs)
    _logger().log(lvl, msg, *args, **kwargs)

def exception(msg, *args):
    _logger().exception(msg, *args)

def getLogger(name=None):
    return _logger(name)

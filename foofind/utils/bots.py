import newrelic.agent
from . import logging
from hashlib import md5
from flask import g, request, abort, current_app
from foofind.services import *

_FULL_BROWSERS_USER_AGENTS=("chrome", "firefox", "msie", "opera", "safari", "webkit")
def is_search_bot():
    '''
    Detecta si la peticion es de un robot de busqueda
    '''
    if request.user_agent.browser in _FULL_BROWSERS_USER_AGENTS:
        return False

    user_agent = request.user_agent.string.lower()
    for i, bot in enumerate(current_app.config["ROBOT_USER_AGENTS"]):
        if bot in user_agent:
            return current_app.config["SAFE_ROBOT_USER_AGENTS"][i]
    return False

def is_full_browser():
    '''
    Detecta si la peticion es de un robot de busqueda
    '''
    return request.user_agent.browser in _FULL_BROWSERS_USER_AGENTS

def check_rate_limit(search_bot):
    '''
    Hace que se respeten los limites de peticiones.
    '''
    if search_bot: # robots
        if not cache.add("rlimit_bot_"+search_bot, 1, timeout=60):
            rate_limit = current_app.config["ROBOT_USER_AGENTS_RATE_LIMIT"].get(search_bot, current_app.config["ROBOT_DEFAULT_RATE_LIMIT"])
            current = cache.inc("rlimit_bot_"+search_bot) # devuelve None si no existe la clave
            if current and current > rate_limit:
                if (current%rate_limit)==1:
                    logging.warn("Request rate over limit %d times from bot %s."%(int(current/rate_limit),search_bot))
                newrelic.agent.ignore_transaction()
                abort(429)
    else: # resto
       ip = request.headers.getlist("X-Forwarded-For")[0] if request.headers.getlist("X-Forwarded-For") else request.remote_addr
       client_id = md5(ip).hexdigest()
       if not cache.add("rlimit_user_"+client_id, 1, timeout=60):
            current = cache.inc("rlimit_user_"+client_id) # devuelve None si no existe la clave
            rate_limit = current_app.config["USER_RATE_LIMIT"]
            if current and current > rate_limit:
                if (current%rate_limit)==1:
                    user_agent = request.user_agent.string
                    logging.warn("Request rate over limit %d times from user %s."%(int(current/rate_limit),client_id))
                abort(429)

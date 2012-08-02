# -*- coding: utf-8 -*-
"""
    Configuraciones por defecto para la aplicación.
"""

SECRET_KEY = "supersecret"

JOBS_EMAIL = ""
CONTACT_EMAIL = ""
DEFAULT_MAIL_SENDER = "noreply@foofind.com"

MAIL_SERVER = "admin1.foofind.com"
MAIL_PORT = 25
MAIL_USE_TLS = False
MAIL_USE_SSL = False
MAIL_USERNAME = ""
MAIL_PASSWORD = ""

DATA_SOURCE_SERVER = "mongodb://mongo.foofind.com:27017"
DATA_SOURCE_USER = "mongodb://mongo.foofind.com:27017"
DATA_SOURCE_FEEDBACK = "mongodb://mongo.foofind.com:27017"
DATA_SOURCE_MAX_POOL_SIZE = 50
DATA_SOURCE_FOO_THREADS = 30
GET_FILES_TIMEOUT = 2
AUTORECONNECT_FOO_INTERVAL = 300

SERVICE_SPHINX = "sphinx.foofind.com"
SERVICE_SPHINX_PORT = 33000
SERVICE_SPHINX_CONNECT_TIMEOUT = 1000.0
SERVICE_SPHINX_MAX_QUERY_TIME = 1000
SERVICE_SPHINX_WORKERS_PER_SERVER = 3

SENTRY_DSN = None

SERVICE_TAMING_SERVERS = (("taming.foofind.com",24642))
SERVICE_TAMING_TIMEOUT = 1.0

CACHE_SEARCHES = True
CACHE_FILES = True
CACHE_TAMING = True

CACHE_KEY_PREFIX = "foofind/"
CACHE_MEMCACHED_SERVERS = ()
CACHE_TYPE = "memcached"

REMOTE_MEMCACHED_SERVERS = ()

PROFILER_KEYS = ["taming_dym","taming_tags","mongo","sphinx"]

OAUTH_TWITTER_CALLBACK_URL = "http://foofind.com/es/user/oauth/tw/callback"
OAUTH_TWITTER_SITE_URL = "https://api.twitter.com/oauth"
OAUTH_TWITTER_QUERY_URL = "http://dev.twitter.com/account/verify_credentials.json"
OAUTH_TWITTER_CONSUMER_KEY = "xxxxxxxxxxxxxxxxxxxxx"
OAUTH_TWITTER_CONSUMER_SECRET = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

OAUTH_FACEBOOK_CALLBACK_URL = "http://foofind.com/es/user/oauth/fb/callback"
OAUTH_FACEBOOK_SITE_URL = "http://www.facebook.com/dialog/oauth/"
OAUTH_FACEBOOK_ACCESS_URL = "https://graph.facebook.com/oauth/access_token"
OAUTH_FACEBOOK_QUERY_URL = "https://graph.facebook.com/me"
OAUTH_FACEBOOK_CONSUMER_KEY = "xxxxxxxxxxxxxxx"
OAUTH_FACEBOOK_CONSUMER_SECRET = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

TRANSLATE_LANGS=['aa','ab','ae','af','ak','am','an','ar','as','av','ay','az','ba','be','bg','bh','bi','bm','bn','bo','br','bs','ca','ce','ch','co','cr','cs','cu','cv','cy','da','de','dv','dz','ee','el','en','eo','es','et','eu','fa','ff','fi','fj','fo','fr','fy','ga','gd','gl','gn','gu','gv','ha','he','hi','ho','hr','ht','hu','hy','hz','ia','id','ie','ig','ii','ik','io','is','it','iu','ja','jv','ka','kg','ki','kj','kk','kl','km','kn','ko','kr','ks','ku','kv','kw','ky','la','lb','lg','li','ln','lo','lt','lu','lv','mg','mh','mi','mk','ml','mn','mr','ms','mt','my','na','nb','nd','ne','ng','nl','nn','no','nr','nv','ny','oc','oj','om','or','os','pa','pi','pl','ps','pt','qu','rm','rn','ro','ru','rw','sa','sc','sd','se','sg','si','sk','sl','sm','sn','so','sq','sr','ss','st','su','sv','sw','ta','te','tg','th','ti','tk','tl','tn','to','tr','ts','tt','tw','ty','ug','uk','ur','uz','ve','vi','vo','wa','wo','xh','yi','yo','za','zh','zu']

ALL_LANGS = ('en', 'es', 'fr', 'it', 'pt', 'tr', 'zh', 'ca', 'gl', 'eu', 'eo') # Orden usado en foof.in
ALL_LANGS_COMPLETE = {'en':'en_GB', 'es':'es_ES', 'fr':'fr_FR', 'it':'it_IT', 'pt':'pt_PT', 'tr':'tr_TR', 'zh':'zh_CN', 'ca':'ca_ES', 'gl':'gl_ES', 'eu':'eu_ES', 'eo':'eo_EO'}
LANGS = ('en', 'es')
BETA_LANGS = [lang for lang in ALL_LANGS if lang not in LANGS]
PRIVATE_MSGID_PREFIXES = ("safe_", "admin_", "newhome_")

COUNT_UPDATE_INTERVAL = 300
FOOCONN_UPDATE_INTERVAL = 300
CONFIG_UPDATE_INTERVAL = 60
UNITTEST_INTERVAL = 0

ADMIN_HOSTS = ()
ADMIN_LANG_LOCAL_REPO = "repo/lang"
ADMIN_LANG_REMOTE_REPO = ""
ADMIN_LANG_REMOTE_BRANCH = "refs/heads/master"
ADMIN_LANG_FOLDER = "foofind/translations"

ADMIN_GIT_AUTHOR = "admin"
ADMIN_GIT_EMAIL = "admin@foofind.com"

FILES_SITEMAP_URL = "http://sitemap.foofind.is/%s/sitemap_index1.xml.gz"

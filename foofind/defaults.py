# -*- coding: utf-8 -*-
"""
    Configuraciones por defecto para la aplicación.
"""

SECRET_KEY = "supersecret"

APPLICATION_ID = "default"

JOBS_EMAIL = ""
CONTACT_EMAIL = ""
DEFAULT_MAIL_SENDER = "noreply@foofind.com"

MAIL_SERVER = "mail.foofind.com"
MAIL_PORT = 25
MAIL_USE_TLS = False
MAIL_USE_SSL = False
MAIL_USERNAME = ""
MAIL_PASSWORD = ""

DATA_SOURCE_SERVER = "mongodb://mongo.foofind.com:27017"
DATA_SOURCE_USER = "mongodb://mongo.foofind.com:27017"
DATA_SOURCE_FEEDBACK = "mongodb://mongo.foofind.com:27017"
DATA_SOURCE_ENTITIES = "mongodb://mongo.foofind.com:27017"
DATA_SOURCE_DOWNLADS = "mongodb://mongo.foofind.com:27017"
DATA_SOURCE_MAX_POOL_SIZE = 50
DATA_SOURCE_FOO_THREADS = 30
GET_FILES_TIMEOUT = 1
AUTORECONNECT_FOO_INTERVAL = 300

SERVICE_SPHINX = "sphinx.foofind.com"
SERVICE_SPHINX_PORT = 33000
SERVICE_SPHINX_CONNECT_TIMEOUT = 20.0
SERVICE_SPHINX_MAX_QUERY_TIME = 800
SERVICE_SPHINX_CLIENT_MIN = 5

SERVICE_SPHINX_SEARCH_MAX_RETRIES = 5
SERVICE_SPHINX_WORKERS_PER_SERVER = 3
SERVICE_SPHINX_CLIENT_RECYCLE = 1000
SERVICE_SEARCH_MAINTENANCE_INTERVAL = 1
SERVICE_SEARCH_PROFILE_INTERVAL = 60*5
SERVICE_SPHINX_DISABLE_QUERY_SEARCH = False

SERVERS_REFRESH_INTERVAL = 60*60 # actualiza servidores cada hora

SENTRY_AUTO_LOG_STACKS = True
SENTRY_DSN = None

SERVICE_TAMING_SERVERS = (("taming.foofind.com",24642))
SERVICE_TAMING_TIMEOUT = 1.0
SERVICE_TAMING_ACTIVE = True

FOODOWNLOADER = False
DOWNLOADER_UA = ()

CACHE_SEARCHES = True
CACHE_FILES = True
CACHE_TAMING = True

CACHE_KEY_PREFIX = "foofind/"
CACHE_MEMCACHED_SERVERS = ()
CACHE_TYPE = "memcached"

STATIC_PREFIX = None

REMOTE_MEMCACHED_SERVERS = ()

# Extracted from http://www.monperrus.net/martin/list+of+robot+user+agents (CC-SA license)
ROBOT_USER_AGENTS=["googlebot/","Googlebot-Mobile","Googlebot-Image","bingbot","slurp","java","wget","curl",
                    "Commons-HttpClient","Python-urllib","libwww","httpunit","nutch","phpcrawl","msnbot",
                    "Adidxbot","blekkobot","teoma","ia_archiver","GingerCrawler","webmon ","httrack","webcrawler",
                    "FAST-WebCrawler","FAST Enterprise Crawler","convera","biglotron","grub.org",
                    "UsineNouvelleCrawler","antibot","netresearchserver","speedy","fluffy","jyxobot","bibnum.bnf",
                    "findlink","exabot","gigabot","msrbot","seekbot","ngbot","panscient","yacybot","AISearchBot",
                    "IOI","ips-agent","tagoobot","MJ12bot","dotbot","woriobot","yanga","buzzbot","mlbot","yandex",
                    "purebot","Linguee Bot","Voyager","CyberPatrol","voilabot","baiduspider","citeseerxbot","spbot",
                    "twengabot","postrank","turnitinbot","scribdbot","page2rss","sitebot","linkdex","ezooms","dotbot",
                    "mail.ru","discobot","heritrix","findthatfile","europarchive.org","NerdByNature.Bot","sistrix crawler",
                    "ahrefsbot","Aboundex","domaincrawler","wbsearchbot","summify","ccbot","edisterbot","seznambot",
                    "ec2linkfinder","gslfbot","aihitbot","intelium_bot","facebookexternalhit","yeti","RetrevoPageAnalyzer",
                    "lb-spider","sogou","lssbot","careerbot","wotbox","wocbot","ichiro","DuckDuckBot","lssrocketcrawler",
                    "drupact","webcompanycrawler","acoonbot","openindexspider","gnam gnam spider","web-archive-net.com.bot",
                    "help.yahoo.co.jp"]

ROBOT_USER_AGENTS=[robot.lower() for robot in ROBOT_USER_AGENTS]
SAFE_ROBOT_USER_AGENTS = [s.strip().replace(" ","_").replace("/", "").replace(".","_").replace("-","_") for s in ROBOT_USER_AGENTS]

ROBOT_USER_AGENTS_RATE_LIMIT = {}
ROBOT_DEFAULT_RATE_LIMIT = 200
USER_RATE_LIMIT = 200


PROFILER_GRAPHS = { 1:("Search page", 'TIMING', ["taming","mongo","sphinx","visited","entities"]),
                    2:("Mongo master accesses", 'TIMING',["mongo%dm"%s for s in xrange(1,20)]),
                    3:("Mongo slave accesses", 'TIMING', ["mongo%ds"%s for s in xrange(1,20)]),
                    4:("Search services", 'MEAN', ["updates","pendings"]),
                    5:("Search pending tasks", 'MEAN', ["sp_tasks%d"%s for s in xrange(1,20)]),
                    6:("Search open conns", 'MEAN', ["sp_conns%d"%s for s in xrange(1,20)]),
                    7:("Search free conns", 'MEAN', ["sp_freeconns%d"%s for s in xrange(1,20)]),
                    8:("Search max conns", 'MEAN', ["sp_preconns%d"%s for s in xrange(1,20)]),
                    9:("Search adhoc conns", 'MEAN', ["sp_adhoc%d"%s for s in xrange(1,20)]),
                    10:("Search waiting timeouts", 'SUM', ["sp_timeout%d"%s for s in xrange(1,20)]),
                    11:("Bots results", 'SUM', ["bot_%s"%s for s in SAFE_ROBOT_USER_AGENTS]),
                    12:("Bots not results", 'SUM', ["bot_no_%s"%s for s in SAFE_ROBOT_USER_AGENTS]),
                    13:("Downloader", 'SUM', ["downloader_opened"])
                    }

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

ALL_LANGS = ('en', 'es', 'fr', 'it', 'pt', 'de', 'tr', 'zh', 'ja', 'ko', 'ca', 'gl', 'eu', 'eo') # Orden usado en foof.in
ALL_LANGS_COMPLETE = {'en':'en_GB', 'es':'es_ES', 'fr':'fr_FR', 'it':'it_IT', 'pt':'pt_PT', 'de':'de_DE', 'tr':'tr_TR', 'zh':'zh_CN', 'ja':'ja_JP', 'ko':'ko_KR', 'ca':'ca_ES', 'gl':'gl_ES', 'eu':'eu_ES', 'eo':'eo_EO'}
LANGS = ('en', 'es')
BETA_LANGS = [lang for lang in ALL_LANGS if lang not in LANGS]
PRIVATE_MSGID_PREFIXES = ("safe_", "admin_", "newhome_")

COUNT_UPDATE_INTERVAL = 300
MAX_AUTORECONNECTIONS = 20
FOOCONN_UPDATE_INTERVAL = 120
CONFIG_UPDATE_INTERVAL = 60
UNITTEST_INTERVAL = 0

CSRF_ENABLED=False #deshabilita CSRF de Flask-WTF

ADMIN_HOSTS = ()
ADMIN_LANG_LOCAL_REPO = "repo/lang"
ADMIN_LANG_REMOTE_REPO = ""
ADMIN_LANG_REMOTE_BRANCH = "refs/heads/master"
ADMIN_LANG_FOLDER = "foofind/translations"

ADMIN_GIT_AUTHOR = "admin"
ADMIN_GIT_EMAIL = "admin@foofind.com"

FILES_SITEMAP_URL = "http://sitemap.foofind.is/%s/sitemap_index1.xml.gz"

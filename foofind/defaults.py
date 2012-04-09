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

SERVICE_SPHINX = "sphinx.foofind.com"
SERVICE_SPHINX_PORT = 33000
SERVICE_SPHINX_MAX_QUERY_TIME = 1500

SERVICE_TAMING_SERVERS = (("taming.foofind.com",24642))
SERVICE_TAMING_TIMEOUT = 1.0

CACHE_SEARCHES = True
CACHE_FILES = True
CACHE_TAMING = True

CACHE_KEY_PREFIX = "foofind/"
CACHE_MEMCACHED_SERVERS = ()
CACHE_TYPE = "memcached"

PROFILER_KEYS = ["taming_dym","taming_tags","mongo","sphinx"]

EXTENSIONS = {"264":2, "3d":5, "3ds":5, "3dr":5, "3g2":2, "3gp":2, "7z":7, "7zip":7,
    "aac":1, "abr":5, "abw":9, "ace":7, "act":5, "aeh":3, "afp":9, "ai":5, "aif":1,
    "aifc":1, "aiff":1, "air":6, "alx":6, "alz":7, "amr":1, "ani":5, "ans":9, "ape":6,
    "apk":6, "aplibrary":5, "app":6, "arc":7, "arj":7, "art":5, "arw":5, "asf":2,
    "asx":2, "at3":7, "au":1, "aup":1, "avi":2, "awg":5, "aww":9, "azw":3, "bat":6,
    "big":7, "bik":2, "bin":7, "bke":7, "bkf":7, "blp":5, "bmp":5, "bw":5, "bzip2":7,
    "cab":7, "caf":1, "cbr":3, "cbz":3, "ccd":8, "cda":1, "cdr":5, "cgm":5, "chm":3,
    "cit":5, "class":6, "cmx":5, "cod":6, "com":6, "cpt":5, "cr2":5, "crw":5, "csv":10,
    "cut":5, "cwk":9, "daa":8, "dao":8, "dat":2, "dcr":5, "dds":7, "deb":7, "dib":5,
    "divx":2, "djvu":3, "dll":6, "dmg":8, "dng":5, "dnl":3, "doc":9, "docm":9, "docx":9,
    "dot":9, "dotm":9, "dotx":9, "drw":5, "dwg":5, "dxf":5, "ecab":7, "eea":7, "egt":5,
    "emf":5, "emz":5, "eps":9, "epub":3, "erf":5, "ess":7, "exe":6, "exif":5, "fax":9,
    "fb2":3, "fff":5, "fla":6, "flac":1, "flv":2, "flw":2, "fpx":5, "ftm":9, "ftx":9,
    "gadget":6, "gho":7, "gif":5, "gz":7, "gzip":7, "hqx":7, "htm":9, "html":9, "hwp":9,
    "ibm":5, "icb":5, "ico":5, "icon":5, "icns":5, "iff":5, "ilbm":5, "img":8, "ind":5,
    "info":9, "int":5, "ipa":6, "iso":8, "isz":8, "j2k":5, "jar":6, "jng":5, "jpeg":5,
    "jp2":5, "jpg":5, "kdc":5, "keynote":11, "kml":9, "la":1, "lbr":7, "lha":7, "lit":3,
    "lqr":7, "lrf":3, "lrx":3, "lwp":9, "lzo":7, "lzx":7, "m2ts":2, "m4a":1, "m4b":1,
    "m4p":1, "m4v":2, "mcw":9, "mdf":8, "mds":8, "mef":5, "mht":9, "midi":1, "mkv":2,
    "mobi":3, "mod":1, "mos":5, "mov":2, "mp+":1, "mp2":1, "mp3":1, "mp4":2, "mpa":1,
    "mpc":1, "mpe":2, "mpeg":2, "mpg":2, "mpp":1, "mrw":5, "msi":6, "nb":9, "nbp":9,
    "nds":6, "nef":5, "nes":6, "nrg":8, "nsv":2, "numbers":10, "ocx":6, "odg":5,
    "odp":11, "ods":10, "odt":9, "ogg":1, "ogm":2, "ogv":2, "opf":3, "orf":5, "otp":11,
    "ots":10, "ott":9, "pages":9, "pak":7, "pac":1, "pap":9, "par":7, "par2":7, "pbm":5,
    "pcd":5, "pcf":5, "pcm":1, "pct":5, "pcx":5, "pdb":3, "pdd":5, "pdf":3, "pdn":5,
    "pef":5, "pgm":5, "pk4":7, "pkg":7, "pix":5, "pnm":5, "png":5, "potx":11, "ppm":5,
    "pps":11, "ppsm":11, "ppsx":11, "ppt":11, "pptm":11, "pptx":11, "prc":3, "prg":6,
    "ps":9, "psb":5, "psd":5, "psp":5, "ptx":5, "px":5, "pxr":5, "qfx":5, "r3d":5,
    "ra":1, "raf":5, "rar":7, "raw":5, "rgb":5, "rgo":3, "rka":1, "rm":2, "rma":1,
    "rom":8, "rtf":9, "sav":6, "scn":6, "scr":6, "sct":5, "scx":6, "sdw":9, "sea":7,
    "sgi":5, "shn":1, "shp":5, "sisx":6, "sit":7, "sitx":7, "skp":5, "snd":1, "sng":1,
    "sr2":5, "srf":5, "srt":9, "sti":9, "stw":9, "sub":9, "svg":5, "svi":2, "swf":6,
    "sxc":10, "sxi":9, "sxw":9, "tao":8, "tar":7, "targa":5, "tb":7, "tex":9, "text":9,
    "tga":5, "tgz":7, "theme":6, "themepack":6, "thm":5, "thmx":11, "tib":7, "tif":5,
    "tiff":5, "toast":8, "torrent":4, "tr2":3, "tr3":3, "txt":9, "uha":7, "uif":8,
    "uoml":9, "vbs":6, "vcd":8, "vda":5, "viff":5, "vob":2, "vsa":7, "vst":5, "wav":1,
    "webarchive":9, "wma":1, "wmf":5, "wmv":2, "wol":3, "wpd":9, "wps":9, "wpt":9,
    "wrap":2, "wrf":9, "wri":9, "wv":1, "x3f":5, "xar":5, "xbm":5, "xcf":5, "xls":10,
    "xlsm":10, "xlsx":10, "xdiv":2, "xhtml":9, "xls":9, "xml":9, "xpi":6, "xpm":5,
    "xps":9, "yuv":5, "z":7, "zip":7, "zipx":7, "zix":7, "zoo":7 }

CONTENT_AUDIO = 1;
CONTENT_VIDEO = 2;
CONTENT_BOOK = 3;
CONTENT_TORRENT = 4;
CONTENT_IMAGE = 5;
CONTENT_APPLICATION = 6;
CONTENT_ARCHIVE = 7;
CONTENT_ROM = 8;
CONTENT_DOCUMENT = 9;
CONTENT_SPREADSHEET = 10;
CONTENT_PRESENTATION = 11;

CONTENTS={  CONTENT_AUDIO:'audio', CONTENT_VIDEO:'video', CONTENT_BOOK:'document',
            CONTENT_TORRENT:'archive', CONTENT_IMAGE:'image', CONTENT_APPLICATION:'software',
            CONTENT_ARCHIVE:'archive', CONTENT_ROM:'software', CONTENT_DOCUMENT:'document',
            CONTENT_SPREADSHEET:'document', CONTENT_PRESENTATION:'document'}

CONTENTS_CATEGORY={ 'audio':[CONTENT_AUDIO],
                    'video':[CONTENT_VIDEO],
                    'document':[CONTENT_BOOK, CONTENT_DOCUMENT, CONTENT_SPREADSHEET, CONTENT_PRESENTATION],
                    'archive':[CONTENT_TORRENT, CONTENT_ARCHIVE, CONTENT_ROM],
                    'image':[CONTENT_IMAGE],
                    'software':[CONTENT_APPLICATION]}

TAMING_TYPES = ["", "(a", "(v", "(e", "(t", "(i", "(n", "(z", "(r", "(d", "(s", "(p"]

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

ALL_LANGS = ('en', 'es', 'fr', 'it', 'pt', 'tr', 'zh', 'ca', 'gl') # Orden usado en foof.in
LANGS = ('en', 'es')
BETA_LANGS = [lang for lang in ALL_LANGS if lang not in LANGS]

COUNT_UPDATE_INTERVAL = 300
FOOCONN_UPDATE_INTERVAL = 300

ADMIN_HOSTS = ()
ADMIN_LANG_LOCAL_REPO = "repo/lang"
ADMIN_LANG_REMOTE_REPO = ""
ADMIN_LANG_REMOTE_BRANCH = "refs/heads/master"
ADMIN_LANG_FOLDER = "foofind/translations"

ADMIN_GIT_AUTHOR = "admin"
ADMIN_GIT_EMAIL = "admin@foofind.com"

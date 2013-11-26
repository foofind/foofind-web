# -*- coding: utf-8 -*-

from collections import defaultdict
from splitter import SEPPER, seppersplit
from . import to_seconds, content_types as ct, u

import operator
import itertools
import functools
import sys

'''
import inspect
CONTENT_TYPES_NAME = {v:k for k,v in ct.__dict__.iteritems() if k.startswith("CONTENT_")}
class Chivato(list):
    def __setitem__(self, x, y):
        list.__setitem__(self, x, y)
        if x:
            print "> %19s = %s" % (CONTENT_TYPES_NAME[x], ("%g" % y).ljust(11))
            print "\n".join("%25s:%03d%25s > %s" % ((frame[1].split("/")[-1],) + frame[2:-1]) for frame in inspect.getouterframes(inspect.currentframe())[1:2])

    def __repr__(self):
        return "= %s" % ", ".join("%s: %g" % (k,v) for (n, k), v in itertools.izip(CONTENT_TYPES_NAME.iteritems(), self) if n in CONTENT_TYPE_SUBSET)

    def __getslice__(self, x, y):
        return self.__class__(list.__getslice__(self, x, y))
'''

def mdcheck(d, require=(), blacklist=()):
    return (
        all(itertools.imap(d["md"].__contains__, require)) and
        not any(itertools.imap(d["md"].__contains__, blacklist))
        )

def mdcontains(d, md_dict=None):
    for mdkey, mdvalue_list in mdict.iteritems():
        mdvalue = d["md"][mdkey].lower()
        if any(v in mdv for v in mdvalue_list):
            return True
    return False

def minpixels(a, minimum):
    if isinstance(a, basestring):
        try:
            return int("".join(i for i in a.strip() if i.isdigit()) or "0") >= minimum
        except ValueError:
            return False
    try:
        return int(a) >= minimum
    except ValueError:
        return False

def minpixelpair(a, minx, miny):
    try:
        if isinstance(a, basestring):
            x, y = a.split("x")
            return minpixels(x, minx) and minpixels(y, miny)
        elif isinstance(a, (tuple, list)) and len(a) == 2:
            if all(isinstance(c, (int, float, long)) for c in a):
                return a[0] >= minx and a[1] >= miny
            else:
                return minpixels(a[0], minx) and minpixels(a[1], miny)
    except:
        pass
    return False

# Content types
CONTENT_UNKNOWN_THRESHOLD = 0.2

CONTENT_TYPE_SET = {
    v for k, v in ct.__dict__.iteritems() if k.startswith("CONTENT_") and isinstance(v, int)
    }

EXTENSION_BLACKLIST_CT = {ct.CONTENT_UNKNOWN, ct.CONTENT_TORRENT, ct.CONTENT_ARCHIVE}

CONTENT_TYPE_SUBSET = {
    ct.CONTENT_UNKNOWN,
    ct.CONTENT_AUDIO,
    ct.CONTENT_VIDEO,
    ct.CONTENT_IMAGE,
    ct.CONTENT_APPLICATION,
    ct.CONTENT_DOCUMENT,
    }
CONTENT_TYPE_ASSIMILATION = {
    # Content types que engloban a otros, con peso y tags para el trasvase
    ct.CONTENT_DOCUMENT: {
        ct.CONTENT_BOOK: (1, {"ebook"}),
        ct.CONTENT_SPREADSHEET: (1, {"spreadsheet"}),
        ct.CONTENT_PRESENTATION: (1, {"presentation"}),
        },
    ct.CONTENT_APPLICATION: {
        ct.CONTENT_ROM: (0.2, {"rom", "game"}),
        },
    ct.CONTENT_UNKNOWN: {
        ct.CONTENT_ARCHIVE: (0.5, {}),
        ct.CONTENT_TORRENT: (0.5, {}),
        },
    }
ARCHIVE_CONTENT_PRIORITY = (
    ct.CONTENT_APPLICATION, ct.CONTENT_ROM, ct.CONTENT_VIDEO, ct.CONTENT_AUDIO,
    ct.CONTENT_ARCHIVE, ct.CONTENT_BOOK, ct.CONTENT_SPREADSHEET,
    ct.CONTENT_PRESENTATION, ct.CONTENT_IMAGE, ct.CONTENT_DOCUMENT,
    ct.CONTENT_TORRENT, ct.CONTENT_UNKNOWN
    )
ARCHIVE_CONTENT_PRIORITY_WEIGHTS = list(ARCHIVE_CONTENT_PRIORITY)
for i, ict in enumerate(ARCHIVE_CONTENT_PRIORITY):
    ARCHIVE_CONTENT_PRIORITY_WEIGHTS[ict] = 2./(i+2)

ARCHIVE_CONTENT_PRIORITY_STEP = 0.75

# Confianza del tipo de contenido según orígen
SOURCE_CT_CONFIDENCE_DEFAULT = 0.25
SOURCE_CT_CONFIDENCE = {
    # torrent
     3: 0.1, 7: 0.1, 79: 0.75, 80: 0.75, 81: 0.75, 82: 0.75, 83: 0.75, 90: 0.75, 94:0.75,
    # audio y vídeo
    16: 0.75, 17: 0.75, 18: 0.75, 21: 0.75, 33: 0.75, 36: 0.75, 39: 0.75, 42: 0.75, 45: 0.75, 47: 0.75, 49: 0.75, 50: 0.75, 51: 0.75, 57: 0.75, 60: 0.75, 62: 0.75, 63: 0.75, 64: 0.75, 65: 0.75, 69: 0.75, 71: 0.75, 72: 0.75, 73: 0.75, 75: 0.75, 78: 0.75, 85: 0.75, 86: 0.75, 87: 0.75, 88: 0.75, 89: 0.75, 91: 0.75, 93: 0.75,
    # tipo evidente
     9: 0.75, # ftp
    13: 0.5,
     43:1,
    # tipo no tan evidente
    14: 0.25, 15: 0.25,
     8: 0.5,  # http
    # tipo erróneo
    59 : 0.1
    }
# Tipo de contenido según url
SOURCE_CT_URL = {
    13: lambda url: (
        (ct.CONTENT_VIDEO, 1) if "/video/" in url else
        (ct.CONTENT_IMAGE, 1) if "/photo/" in url else
        (ct.CONTENT_DOCUMENT, 1) if "/office/" in url else
        (ct.CONTENT_DOCUMENT, 0.5) if "/document/" in url else
        (ct.CONTENT_AUDIO, 1) if any(word in url for word in ("/music/", "/mp3/")) else
        (ct.CONTENT_APPLICATION, 1) if any(word in url for word in ("/mobile/", "/android/", "/game/")) else
        (ct.CONTENT_UNKNOWN, 0)
        ),
    }
SOURCE_TAG_URL = {
    13: {
        "game": lambda url: "/game/" in url
        },
    91: {
        "porn": lambda url: True
        }
    }
SOURCES_WITH_RELEVANT_URLS = {key for surl in (SOURCE_CT_URL, SOURCE_TAG_URL) for key in surl}
# Confianza del tipo de contenido según servidor
_server_ctcfnumber = 10
_server_ctcfstep = 0.75 / _server_ctcfnumber
SERVER_CT_CONFIDENCE_DEFAULT = 0.5
SERVER_CT_CONFIDENCE = {
    i+1: min(round(0.25 + _server_ctcfstep * i, 2), 1) for i in xrange(_server_ctcfnumber)
    }
# Dominios que podrían ser confundidos por extensiones
TOP_LEVEL_DOMAINS = {
    "aero", "asia", "biz", "cat", "com", "coop", "info", "int",
    "jobs", "mobi", "museum", "name", "net", "org", "pro", "tel",
    "travel", "xxx", "edu", "gov", "mil", "ac", "ad", "ae", "af",
    "ag", "ai", "al", "am", "an", "ao", "aq", "ar", "as", "at",
    "au", "aw", "ax", "az", "ba", "bb", "bd", "be", "bf", "bg",
    "bh", "bi", "bj", "bm", "bn", "bo", "br", "bs", "bt", "bv",
    "no", "bw", "by", "bz", "ca", "cc", "cd", "cf", "cg", "ch",
    "ci", "ck", "cl", "cm", "cn", "co", "cr", "cs", "cu", "cv",
    "cx", "cy", "cz", "dd", "de", "dj", "dk", "dm", "do", "dz",
    "ec", "ee", "eg", "eh", "er", "es", "et", "eu", "fi", "fj",
    "fk", "fm", "fo", "fr", "ga", "gb", "uk", "gd", "ge", "gf",
    "gg", "gh", "gi", "gl", "gm", "gn", "gp", "gq", "gr", "gs",
    "gt", "gu", "gw", "gy", "hk", "hm", "hn", "hr", "ht", "hu",
    "id", "ie", "il", "im", "in", "io", "iq", "ir", "is", "it",
    "je", "jm", "jo", "jp", "ke", "kg", "kh", "ki", "km", "kn",
    "kp", "kr", "kw", "ky", "kz", "la", "lb", "lc", "li", "lk",
    "lr", "ls", "lt", "lu", "lv", "ly", "ma", "mc", "md", "me",
    "mg", "mh", "mk", "ml", "mm", "mn", "mn", "mo", "mp", "mq",
    "mr", "ms", "mt", "mu", "mv", "mw", "mx", "my", "mz", "na",
    "nc", "ne", "nf", "ng", "ni", "nl", "no", "np", "nr", "nu",
    "nz", "om", "pa", "pe", "pf", "pg", "ph", "pk", "pl", "pm",
    "pn", "pr", "ps", "pt", "pw", "py", "qa", "re", "ro", "rs",
    "ru", "su", "рф", "rw", "sa", "sb", "sc", "sd", "se", "sg",
    "sh", "si", "sj", "no", "sk", "sl", "sm", "sn", "so", "sr",
    "ss", "st", "su", "sv", "sx", "sy", "sz", "tc", "td", "tf",
    "tg", "th", "tj", "tk", "tl", "tp", "tm", "tn", "to", "tp",
    "tl", "tr", "tt", "tv", "tw", "tz", "ua", "ug", "uk", "us",
    "gov", "uy", "uz", "va", "vc", "ve", "vg", "vi", "vn", "vu",
    "wf", "ws", "ye", "yt", "yu", "za", "zm", "zw", "dz", "বাংলা",
    "bd", "中国", "cn", "中國", "cn", "eg", "გე", "ge", "香港", "hk",
    "भारत", "in", "in", "భారత్", "in", "ભારત", "in", "ਭਾਰਤ", "in",
    "இந்தியா", "in", "ভারত", "in", "ir", "jo", "қаз", "kz", "my",
    "mn", "ma", "om", "pk", "ps", "qa", "рф", "ru", "sa", "срб",
    "rs", "新加坡", "sg", "சிங்கப்பூர்", "sg", "한국", "kr", "ලංකා", "lk",
    "இலங்கை", "lk", "sy", "台湾", "tw", "台灣", "tw", "ไทย", "th",
    "tn", "укр", "ua", "ae", "ye", "бг", "bg", "ελ", "gr", "il",
    "jp", "la", "ly", "arpa"
    }

# Confianza del tipo dado por extensión
EXTENSION_CONFIDENCE_DEFAULT = 0.5
EXTENSION_CONFIDENCE = {
    # Ambiguous
    "swf": 0.25, "ogg": 0.25,
    # Bad
    "nfo":0, "s":0,
    }
# Extensiones ambiguas por ser también dominios de primer nivel
EXTENSION_CONFIDENCE.update((i, CONTENT_UNKNOWN_THRESHOLD) for i in TOP_LEVEL_DOMAINS)
EXTENSION_IMPORTANCE_POSITION = 0.75
# Tipos dados por palabras clave en el nombre
FILENAME_KEYWORDS = {
    ct.CONTENT_IMAGE: {
        "photo": 0.1,
        "photos": 0.2,
        "wallpapers": 0.2,
        },
    ct.CONTENT_AUDIO: {
        "audio": 0.5,
        "kbps": 0.75,
        "mp3": 0.5,
        "lossless": 0.75,
        "radio": 0.5,
        "remix": 0.5,
        "feat": 0.5,
        "ogg": 0.75,
        "aac": 0.5,
        "vbr": 0.5,
        "cbr": 0.25,
        "kbps": 0.5,
        ("full", "album"): 1,
        },
    ct.CONTENT_VIDEO: {
        "hdtv": 1,
        "video": 0.5,
        "movie": 0.5,
        "film": 0.5,
        "hd": 0.1, # problemas con apks
        "fullhd": 0.75,
        "720": 0.5,
        "720p": 1,
        "720i": 1,
        "1080": 0.5,
        "1080p": 1,
        "1080i": 1,
        "dvd": 1,
        "blueray": 0.5,
        "bdray": 0.5,
        "bdrip": 1,
        "brrip": 1,
        "hdrip": 1,
        "dvdrip": 1,
        "rip": 0.25,
        "screener": 1,
        "hdscreener": 1,
        "xvid": 0.75,
        "h264": 0.75,
        "aac": 0.5,
        "mp4": 1,
        "ogv": 1,
        "matroska": 1,
        "mkv": 0.75,
        "avi": 1,
        "m4v": 1,
        },
    }

# Formato
FORMAT_KEYWORDS_WEIGHT = 1
FORMAT_KEYWORDS = {
    ct.CONTENT_VIDEO: {
        ("h264", "H.264/MPEG-4 AVC"): {"h264"},
        ("xvid", "XviD"): {"xvid"},
        ("mkv", "Matroska"): {"mkv"},
        },
    ct.CONTENT_AUDIO: {
        ("mp3", "MPEG-1 or MPEG-2 Audio Layer III"): {"mp3"},
        ("oga", "Ogg Vorbis"): {"ogg"},
        },
    }

FORMAT_EXTENSIONS = {
    ct.CONTENT_VIDEO: {
        ("xvid", "XviD"): {"xvid"},
        ("divx", "DivX"): {"dvx"},
        ("webm", "WebM"): {"webm"},
        ("3gp", "3GPP"): {"3gp", "3gp2", "3gpp", "3gpp2", "3p2"},
        ("mkv", "Matroska"): {"mkv", "mk3d"},
        ("ogv", "Theora"): {"ogv", "ogg"},
        ("ogm", "Ogg Media"): {"ogm", "ogx"},
        ("flv", "Flash Video"): {"flv"},
        ("f4p", "Adobe Flash Protected Media"): {"f4p"},
        ("asf", "Advanced Systems Format"): {"asf"},
        ("asx", "Microsoft ASF Redirector"): {"asx"},
        ("mov", "Apple QuickTime Movie"): {"mov", "moov", "movie"},
        ("mp4", "MPEG-4 Video"): {"mp4", "mp4v", "mpeg4", "m4v", "f4v"},
        ("rm", "Real Media"): {"rm", "rmp", "rmvb", "rms", "rv"},
        ("swf", "Shockwave Flash Movie"): {"swf"},
        ("vob", "DVD Video Object"): {"vob"},
        ("wmv", "Windows Media Video"): {"wmv"},
        ("wmx", "Windows Media Redirector"): {"wmx"},
        ("amv", "AMC Video"): {"amv"},
        ("amc", "Anime Music Video"): {"amc"},
        ("asf", "Advanced Systems Format"): {"asf"},
        ("vcd", "VideoCD"): {"vcd"},
        ("fli", "Autodesk Animator"): {"fli", "flh", "flc", "flx"},
        ("mpeg", "MPEG Video"): {"mpeg", "mpg", "m15", "m1v", "m1pg", "m21", "m2p", "m2v", "mp21"},
        ("m2ts", "Blu-ray BDAV Video") : {"m2ts"},
        ("mgv", "PSP Video"): {"mgv"},
        ("mnv", "Playstation Video"): {"mnv"},
        ("mqv", "Sony Movie Format"): {"mqv"},
        ("smk", "Smacker Compressed Movie"): {"smk"},
        ("svi", "Samsung Video"): {"svi"},
        ("thp", "Wii/GameCube Video"): {"thp"},
        ("usm", "USM Game Video"): {"usm"},
        ("vpN", "On2/Truemotion Video"): {"vp3", "vp6", "vp7"},
        ("zmv", "ZSNES Movie"): {"zm1", "zm2", "zm3", "zmv"},
        ("yuv", "YUV Video"): {"yuv"},
        ("bsf", "Blu-ray AVC Video"): {"bsf"},
        ("bik", "Bink Video"): {"bik"},
        },
    ct.CONTENT_AUDIO: {
        ("m1a", "MPEG-1 audio"): {"m1a"},
        ("amr", "Adaptive Multi-Rate"): {"amr"},
        ("oga", "Ogg Vorbis"): {"oga", "ogg"},
        ("flac", "Free Lossless Audio"): {"flac"},
        ("wav", "Wave Audio"): {"wav"},
        ("aac", "Advanced Audio Coding"): {"m4a", "m4b", "m4p", "m4v", "m4r", "3gp", "mp4", "aac"},
        ("mp3", "MPEG-1 or MPEG-2 Audio Layer III"): {"mp3"}
        },
    ct.CONTENT_APPLICATION: {
        ("msi", "Windows Installer Package"): {"msi"},
        ("exe", "Windows Executable"): {"exe"},
        ("apk", "Android Package"): {"apk"},
        ("sis", "Symbian Installer"): {"sis"},
        ("dmg", "Mac OS X Disk"): {"dmg"},
        ("img", "Disc Image Data File"): {"img"}
        },
    ct.CONTENT_ARCHIVE: {
        ("iso", "Disc Image"): {"iso"},
        ("cue", "Cue Sheet"): {"cue"},
        ("bwi", "BlindWrite Disk"): {"bwa", "bwi", "bwt"},
        ("cso", "Compressed ISO Disk Image"): {"cso"},
        ("toast", "Toast Disc Image"): {"toast"},
        ("nrg", "Nero CD/DVD Image"): {"nrg"},
        ("uif", "Universal Image Format Disc"): {"uif"},
        ("vaporcd", "Norum Vapor CD"): {"vaporcd"},
        ("vcd", "Virtual CD"): {"vc4", "vc6", "vc8", "vcd"},
        ("wmt", "WinMount Virtual Disk"): {"wmt"},
        ("cdr", "Macintosh DVD/CD Master"): {"cdr"},
        }
    }
FORMAT_EXTENSIONS_WEIGHT = 1
FORMAT_EXTENSIONS_AUTO_WEIGHT = 0.5
FORMAT_EXTENSIONS_AUTO_BLACKLIST = {
    "nfo", "mdinfo", "md5", "crc"
    }
FORMAT_EXTENSIONS_AUTO_BLACKLIST.update(TOP_LEVEL_DOMAINS)
FORMAT_EXTENSIONS_AUTO_BLACKLIST_CT = {ct.CONTENT_VIDEO, ct.CONTENT_TORRENT, ct.CONTENT_ARCHIVE}

for content_type in CONTENT_TYPE_SET:
    # Añadimos None (formato desconocido) a todos los ct de FORMAT_EXTENSIONS
    if content_type in FORMAT_EXTENSIONS:
        FORMAT_EXTENSIONS[content_type][None] = ()
    else:
        FORMAT_EXTENSIONS[content_type] = {None: ()}

FORMAT_SET = {
    # Todos los formatos por tipo de contenido
    actype: {fileformat for ctype, fileformat in fileformats}
    for ctype, fileformats in itertools.groupby(sorted(
        (ctype, fileformat)
        for fsource in (FORMAT_KEYWORDS, FORMAT_EXTENSIONS)
            for ctype, fileformats in fsource.iteritems()
                for fileformat in fileformats.iterkeys() if fileformat
        ), key=operator.itemgetter(0))
        for actype in (
            CONTENT_TYPE_ASSIMILATION[ctype] if ctype in CONTENT_TYPE_ASSIMILATION else
            (ctype,) if ctype in CONTENT_TYPE_SUBSET else
            (ct.CONTENT_UNKNOWN,)
            )
    }

# Tags dados por palabras clave en el nombre
TAG_KEYWORDS = {
    ct.CONTENT_VIDEO: {
        "hd": {
            word for word in FILENAME_KEYWORDS[ct.CONTENT_VIDEO]
            if any(word.startswith(subword) for subword in ("hd", "1080", "720", "bd", "br", "blueray"))
               or any(word.endswith(subword) for subword in ("hd",))
            },
        "game": {
            "dlc",
            }
        },
    ct.CONTENT_AUDIO: {
        "full_album": {
            ("full", "album"),
            },
        }
    }
# Tags dados por metadatos
_movie_check = functools.partial(mdcheck, blacklist={"video:season", "video:series", "video:episode", "ntt:schema"})
_check_os = lambda words, doc: (
    isinstance(doc["md"]["application:os"], basestring) and
    any(i in doc["md"]["application:os"].lower() for i in words)
    )

TAG_METADATA = {
    ct.CONTENT_AUDIO: {
        "ntt:schema": {
            "full_album": lambda doc: doc["md"]["ntt:schema"]=="album",
            },
        },
    ct.CONTENT_VIDEO: {
        "ntt:schema": {
            "movie": lambda doc: doc["md"]["ntt:schema"]=="movies",
            "series": lambda doc: doc["md"]["ntt:schema"]=="episode",
            "documentary": lambda doc: doc["md"]["ntt:schema"]=="documentary",
            },
        "video:width": {
            "hd": lambda doc: (
                minpixels(doc["md"]["video:width"], 1280) and
                minpixels(doc["md"]["video:height"], 720)
                ),
            },
        "video:resolution": {
            "hd": lambda doc: (
                minpixelpair(doc["md"]["video:resolution"], 1280, 720)
                ),
            },
        "video:director": {
            "movie": _movie_check,
            },
        "video:year": {
            "movie": _movie_check,
            },
        #"video:duration": {
        #    "movie": lambda doc: to_seconds(doc["md"]["video:duration"]) >
        #    },
        #"video:genre": {
        #    "series": lambda doc: doc["md"]["video:genre"].lower().startswith("serie")
        #    },
        "video:series": {
            "series": True,
            },
        #"video:documental"
        },
    ct.CONTENT_APPLICATION: {
        "application:os": {
            "linux": functools.partial(_check_os, ("linux",)),
            "windows": functools.partial(_check_os, ("win32", "win64")),
            "mac": functools.partial(_check_os, ("osx", "mac")),
            },
        },
    ct.CONTENT_BOOK: {
        "ntt:schema": {
            "ebook": lambda doc: doc["md"]["ntt:schema"]=="books",
            },
        "book:title": {
            "ebook": True,
            },
        }
    }
# Tags dados por extensión
TAG_EXTENSIONS = {
    ct.CONTENT_APPLICATION: {
        "android": {"apk"},
        "mobile": {"apk", "ipa"},
        "linux": {"deb", "rpm", "tgz", "bz2"},
        "mac": {"pkg", "dmg"},
        "windows": {"msi", "exe"},
        "gamecube": {"dol"},
        "game": {"dol"},
        },
    ct.CONTENT_AUDIO: {
        "karaoke": {"kar"},
        },
    }

TORRENT_CATEGORY_TAG = {
        u"adult": u"porn",
        u"xxx": u"porn",
        u"asian": u"porn",
        u"movies": u"movie",
}
TAG_CONTENT_TYPE = {
        u"movie": ct.CONTENT_VIDEO,
        u"series": ct.CONTENT_VIDEO,
        u"documentary": ct.CONTENT_VIDEO,
        u"ebook": ct.CONTENT_DOCUMENT,
        u"software": ct.CONTENT_APPLICATION,
        u"linux": ct.CONTENT_APPLICATION,
        u"mac": ct.CONTENT_APPLICATION,
        u"windows": ct.CONTENT_APPLICATION,
        u"android": ct.CONTENT_APPLICATION,
        u"game": ct.CONTENT_APPLICATION,
        u"gamecube": ct.CONTENT_APPLICATION,
        u"music": ct.CONTENT_AUDIO,
        u"full_album": ct.CONTENT_AUDIO,
        u"karaoke": ct.CONTENT_AUDIO,
        u"mobile": ct.CONTENT_APPLICATION
    }

TAG_CONTENT_TYPE_GUESS = {
        u"porn": {ct.CONTENT_VIDEO:0.7, ct.CONTENT_BOOK:0.1, ct.CONTENT_IMAGE:0.4},
        u"hd": {ct.CONTENT_VIDEO:0.7, ct.CONTENT_IMAGE:0.4},
        u"anime": {ct.CONTENT_VIDEO:0.9, ct.CONTENT_BOOK:0.1},
    }

# Tags vinculados a tipos concretos
_tag_restrictions = [
    # Tags organizados por content type (palabras clave y extensiones)
    (ctype, tag)
    for tags_by_ct in (TAG_KEYWORDS, TAG_EXTENSIONS)
        for ctype, tags in tags_by_ct.iteritems()
            for tag in tags
    ]
_tag_restrictions.extend(
    # Tags organizados por content type y metadato
    (ctype, tag)
    for ctype, mds in TAG_METADATA.iteritems()
        for tags in mds.itervalues()
            for tag in tags
    )
_tag_restrictions.extend(
    # Tags surgidos de asimilación de tipos de contenido
    (dest, tag)
    for dest, origins in CONTENT_TYPE_ASSIMILATION.iteritems()
        for origin, (weight, tags) in origins.iteritems()
            for tag in tags
    )
_tag_restrictions.extend(
    # Asociación inversa de tags y content types
    (ctype, tag)
    for tag, ctype in TAG_CONTENT_TYPE.iteritems()
    )

_tag_restrictions.extend(
    # Asociación inversa de tags y content types multiples
    (ctype, tag)
    for tag, ctypes in TAG_CONTENT_TYPE_GUESS.iteritems()
        for ctype in ctypes
    )

_tag_restrictions.sort()
TAG_RESTRICTIONS = {
    ctype: {tag for ctype, tag in taglist}
    for ctype, taglist in itertools.groupby(_tag_restrictions, key=operator.itemgetter(0))
    }

#
METADATA_PREFIX = {
    "video":ct.CONTENT_VIDEO,
    "audio":ct.CONTENT_AUDIO,
    "application":ct.CONTENT_APPLICATION,
    "document":ct.CONTENT_DOCUMENT,
    "image":ct.CONTENT_IMAGE,
    }
METADATA_PREFIX_WEIGHT = 0.2

REVERSE_FORMAT_KEYWORDS = {
    #
    ext: {
        fileformat
        for ctype, fileformats in FORMAT_KEYWORDS.iteritems()
            for fileformat, exts in fileformats.iteritems() if ext in exts
        }
    for ext in {
        ext
        for fformats in FORMAT_KEYWORDS.itervalues()
            for exts in fformats.itervalues() if exts
                for ext in exts
        }
    }
REVERSE_FORMAT_EXTENSIONS = {
    #
    ext: {
        fileformat
        for ctype, fileformats in FORMAT_EXTENSIONS.iteritems()
            for fileformat, exts in fileformats.iteritems() if ext in exts
        }
    for ext in {
        ext
        for fformats in FORMAT_EXTENSIONS.itervalues()
            for exts in fformats.itervalues() if exts
                for ext in exts
        }
    }
REVERSE_CONTENT_TYPE_ASSIMILATION = {
    # Organiza la fagocitosis de tipos de contenido
    orig : (dest, mult, tags)
    for dest, origs in CONTENT_TYPE_ASSIMILATION.iteritems()
        for orig, (mult, tags) in origs.iteritems()
    }
REVERSE_TAG_KEYWORDS = {
    # Keywords que implican tags en un diccionario
    keyword: {tag for kw, tag in tag_list}
    for keyword, tag_list in itertools.groupby(
        sorted(
            (keyword, tag)
            for ctype, tags in TAG_KEYWORDS.iteritems()
                for tag, keywords in tags.iteritems()
                    for keyword in keywords
           ), key=operator.itemgetter(0))
    }
REVERSE_TAG_METADATA = {
    # Metadatos que implican tags con función condicional
    md: {tag: cond for md, tag, cond in tag_list}
    for md, tag_list in itertools.groupby(
        sorted(
            (md, tag, cond)
            for ctype, mds in TAG_METADATA.iteritems()
                for md, conds in mds.iteritems()
                    for tag, cond in conds.iteritems()
           ), key=operator.itemgetter(0))
    }
REVERSE_TAG_EXTENSIONS = {
    # Extensiones que implican tags
    ext: {tag for ext, tag in tag_list}
    for ext, tag_list in itertools.groupby(
        sorted(
            (ext, tag)
            for ctype, tags in TAG_EXTENSIONS.iteritems()
                for tag, exts in tags.iteritems()
                    for ext in exts
           ), key=operator.itemgetter(0))
    }
REVERSE_FILENAME_KEYWORDS = {
    # Organiza las palabras claves en un diccionario de diccionarios
    # por palabra clave, content type y peso
    keyword: {ctenum: weight for keyword, ctenum, weight in group}
    for keyword, group in
    itertools.groupby(
        sorted(
            (keyword, ctenum, weight)
            for ctenum, keywords in FILENAME_KEYWORDS.iteritems()
                for keyword, weight in keywords.iteritems()
            ),
        key = operator.itemgetter(0)
        )
    }

# Todos los formatos y tags en un solo set
ALL_FORMATS = set(aformat
                    for aset in FORMAT_SET.itervalues()
                        for aformat, format_desc in aset)

ALL_TAGS = set(tag
                for aset in itertools.chain(REVERSE_TAG_KEYWORDS.itervalues(), REVERSE_TAG_METADATA.itervalues(),
                                            REVERSE_TAG_EXTENSIONS.itervalues())
                    for tag in aset)

ALL_TAGS.update(TAG_CONTENT_TYPE.iterkeys())
ALL_TAGS.update(TAG_CONTENT_TYPE_GUESS.iterkeys())

_scores_empty = [0.0] * len(CONTENT_TYPE_SET)
_depths_empty = [sys.maxint] * len(CONTENT_TYPE_SET)
_scores_initial = _scores_empty[:]
_scores_initial[ct.CONTENT_UNKNOWN] = CONTENT_UNKNOWN_THRESHOLD
_formats_initial = defaultdict(float, ((None, 0),))

def analyze_filenames(filenames, filesizes, skip_ct=False, analyze_extensions=True):
    '''
    Develve una lista de tipos de contenido y tags dependiendo de la profundidad
    y prioridad de cada uno de los ficheros (el tipo de contenido de cada
    fichero se decide uno a uno).

    @type filenames: iterable
    @param filenames: lista de rutas de fichero

    @rtype lista
    @return lista de puntuaciones para cada tipo de contenido
    '''
    tags = set()
    fileformats = _formats_initial.copy()
    numfilenames = len(filenames)
    scores = None if skip_ct else _scores_empty[:]

    # Devolución sin scores por no haber filenames
    if numfilenames == 0:
        return scores, tags, fileformats

    not_skip_ct = not skip_ct
    lower_depths = _depths_empty[:]

    # los tamaños de ficheros deben ser de la misma longitud
    filesizes_reverse=filesizes[:] if len(filesizes)==numfilenames else []
    filesizes_reverse.reverse()
    filesizes_sum = sum(float(asize) for asize in filesizes_reverse) or 1

    # Análisis de extensiones
    for fn, counts in filenames:
        # Puntuaciones locales de fichero
        if not_skip_ct:
            file_scores = _scores_empty[:]
        # No tenemos en cuenta mayúsculas/minúsculas
        fn = fn.strip().lower().replace("\\", "/")
        if "/" in fn:
            path, fn = fn.rsplit("/", 1)
            depth = path.count("/") + 2
        else:
            depth = 1

        # Análisis de extensiones
        if analyze_extensions and "." in fn:
            # Al menos un punto para poder analizar extensiones
            parts = fn.split(".")
            parts.reverse()
            # Extensiones en orden inverso
            exts = tuple(itertools.takewhile(ct.EXTENSIONS.__contains__, parts[:-1]))
            # Nombre de fichero sin extensiones
            fn = ".".join(parts[:len(exts)-1:-1])
            # Tags por extensiones
            tags.update(
                tag
                for ext in exts if ext in REVERSE_TAG_EXTENSIONS
                    for tag in REVERSE_TAG_EXTENSIONS[ext]
                )
            # Formato por la primera extensión válida
            for ext in exts:
                # Formato dado manualmente por extensión
                if ext in REVERSE_FORMAT_EXTENSIONS:
                    for fformat in REVERSE_FORMAT_EXTENSIONS[ext]:
                        fileformats[fformat] += FORMAT_EXTENSIONS_WEIGHT
                    break
                # Formato dado automáticamente por extensión
                if not ext in FORMAT_EXTENSIONS_AUTO_BLACKLIST:
                    sitf = False
                    for extype in ct.EXTENSIONS[ext]:
                        if not extype in FORMAT_EXTENSIONS_AUTO_BLACKLIST_CT:
                            fileformats[ext, None] += FORMAT_EXTENSIONS_AUTO_WEIGHT
                            sitf = True
                    if sitf:
                        break
            # Tipo de contenido por extensión
            if exts and not_skip_ct:
                extweight = 2
                for ext in exts:
                    for extype in ct.EXTENSIONS[ext]:
                        if extype not in EXTENSION_BLACKLIST_CT:
                            file_scores[extype] += EXTENSION_CONFIDENCE.get(ext, EXTENSION_CONFIDENCE_DEFAULT) * counts * extweight
                            extweight *= EXTENSION_IMPORTANCE_POSITION

        # Análisis de nombre de fichero
        if fn:
            # Puntuación según palabras clave
            singlesplit = filter(None, seppersplit(fn))
            doublesplit = itertools.izip(singlesplit, itertools.islice(singlesplit, 1, sys.maxint)) if len(singlesplit) > 1 else ()
            if skip_ct:
                for splitted_words in (singlesplit, doublesplit):
                    for word in splitted_words:
                        if word in REVERSE_TAG_KEYWORDS:
                            tags.update(tag for tag in REVERSE_TAG_KEYWORDS[word])
                        if word in REVERSE_FORMAT_KEYWORDS:
                            for fformat in REVERSE_FORMAT_KEYWORDS[word]:
                                fileformats[fformat] += FORMAT_KEYWORDS_WEIGHT
            else:
                # Contador para saber si hay más de uno
                for splitted_words in (singlesplit, doublesplit):
                    for word in splitted_words:
                        if word in REVERSE_FILENAME_KEYWORDS:
                            for ctenum, weight in REVERSE_FILENAME_KEYWORDS[word].iteritems():
                                file_scores[ctenum] += weight * counts
                        if word in REVERSE_TAG_KEYWORDS:
                            tags.update(tag for tag in REVERSE_TAG_KEYWORDS[word])
                        if word in REVERSE_FORMAT_KEYWORDS:
                            for fformat in REVERSE_FORMAT_KEYWORDS[word]:
                                fileformats[fformat] += FORMAT_KEYWORDS_WEIGHT

        # Análisis de content type del fichero
        if not_skip_ct:
            ict = max(_content_type_xrange, key=file_scores.__getitem__)
            scores[ict] += float(filesizes_reverse.pop())/filesizes_sum if filesizes_reverse else 1
            if lower_depths[ict] > depth:
                lower_depths[ict] = depth

    # Devolución sin scores
    if skip_ct:
        return scores, tags, fileformats

    if scores.count(0.0) == len(scores):
        return scores, tags, fileformats

    # Relativización de cada peso respecto a los pesos por prioridad
    scores = [sc*w for sc, w in itertools.izip(scores, ARCHIVE_CONTENT_PRIORITY_WEIGHTS)]

    # Relativización de cada peso respecto al de mayor prioridad de cada nivel
    '''total = sum(scores)
    n = 1
    for prio, ict, depth in sorted(
      ((n, ict, lower_depths[ict]) for n, ict in enumerate(ARCHIVE_CONTENT_PRIORITY)
      if lower_depths[ict] < sys.maxint)):
        n = n * scores[ict] / total
        scores[ict] = n'''

    # Compensación (para que tienda a 1) y actualización
    total = sum(scores) / PRIORITY_FILENAMES
    for ict in _content_type_xrange:
        if scores[ict]:
            scores[ict] /= total

    # Preparación de scores para se comporte tal cual se necesita en guess_doc_content_type
    scores[ct.CONTENT_UNKNOWN] = CONTENT_UNKNOWN_THRESHOLD

    return scores, tags, fileformats

def _rfm(fformats, ctype, ff):
    '''
    Criterio de ordenación para elegir el mejor formato de fichero
    '''
    if ff is None:
        return 0
    elif (ff[0] in ct.EXTENSIONS and ctype in ct.EXTENSIONS[ff[0]]) or (ctype in FORMAT_SET and ff in FORMAT_SET[ctype]):
        return fformats[ff]
    return -1

_content_type_xrange = xrange(len(CONTENT_TYPE_SET))
def restrict_content_type(scores, tags=(), fformats=(), ctype=None):
    # Asimilación de tipos de contenido y tags correspondientes
    if ctype:
        # Content type fijo
        if ctype in REVERSE_CONTENT_TYPE_ASSIMILATION:
            ctype, multiplier, ntags = REVERSE_CONTENT_TYPE_ASSIMILATION[ctype]
            tags.update(ntags)
        # Validación de tipo de contenido
        elif not ctype in CONTENT_TYPE_SUBSET:
            ctype = ct.CONTENT_UNKNOWN
    else:
        # Content type por puntuaciones
        for sctype, value in enumerate(scores):
            if value > 0:
                if sctype in REVERSE_CONTENT_TYPE_ASSIMILATION:
                    destiny, multiplier, ntags = REVERSE_CONTENT_TYPE_ASSIMILATION[sctype]
                    scores[destiny] += value * multiplier
                    scores[sctype] = 0
                elif not sctype in CONTENT_TYPE_SUBSET:
                    scores[sctype] = 0
        # Selección del mejor tipo de contenido, su fiabilidad, y los tags permitidos para él
        ctype = max(_content_type_xrange, key=scores.__getitem__)
        ntags = REVERSE_CONTENT_TYPE_ASSIMILATION[ctype][2] if ctype in REVERSE_CONTENT_TYPE_ASSIMILATION else None
        if ntags:
            tags.update(ntags)

    # Selección de formato
    if len(fformats) > 1: # fformats incluye el formato None por defecto
        fformat = max(fformats, key=functools.partial(_rfm, fformats, ctype))
    else:
        fformat = None

    # Ahorramos cálculos para el tipo desconocido
    if ctype == ct.CONTENT_UNKNOWN:
        return ctype, [], fformat

    # Restricción de tags
    if tags and ctype in TAG_RESTRICTIONS:
        ftags = filter(TAG_RESTRICTIONS[ctype].__contains__, tags)
    else:
        ftags = []
    return ctype, ftags, fformat

PRIORITY_CT = 1
PRIORITY_FILENAMES = 1
def guess_doc_content_type(doc, sources=None):
    '''
    Obtiene el content type e información relacionada de un documento de fichero.

    @type doc: dict
    @param doc: documento de mongodb de fichero
    @type sources: dict
    @param sources: diccionario de sources

    @rtype tuple
    @return tupla con id de tipo de contenido (int), lista de tags, y formato
            (como tupla de formato o None)
    '''
    # Set de tags
    tags = set()

    # Set de orígenes del fichero
    sourceids = {int(src["t"]) for src in doc["src"].itervalues() if "t" in src} if "src" in doc else {}
    # Los ficheros deben de tener sources (de dónde salen los nombres y la configuración por orígen)
    if not sourceids:
        return ct.CONTENT_UNKNOWN, 0, [], None

    # Extracción de nombres de ficheros
    filesizes = []
    if "md" in doc and "torrent:filepaths" in doc["md"] and isinstance(doc["md"]["torrent:filepaths"], basestring):
        filepaths = [(fn, 1) for fn in doc["md"]["torrent:filepaths"].lstrip("/").split("///")]
        filesizes = doc["md"]["torrent:filesizes"].split(" ") if "torrent:filesizes" in doc["md"] and isinstance(doc["md"]["torrent:filesizes"], basestring) else []
    elif "fn" in doc:
        filepaths = [
            (( "%s.%s" % (fn["n"], fn["x"])
                if "x" in fn and fn["x"].strip() and not fn["n"].endswith(".%s" % fn["x"])
                else fn["n"] # Evitar valores duplicados o incorrectos de fn["x"]
             ).strip(),
             sum((src["fn"][k].get("m", 1) for src in doc["src"].itervalues() if "fn" in src and k in src["fn"]), 1)
             ) for k, fn in doc["fn"].iteritems() if fn.get("n", None)
            ]
    else:
        filepaths = ()

    # Tags por urls
    if "src" in doc:
        for d in doc["src"].itervalues():
            if "t" in d:
                dsid = int(d["t"])
                if dsid in SOURCE_TAG_URL:
                    durl = d["url"]
                    tags.update(tag for tag, cond in SOURCE_TAG_URL[dsid].iteritems() if cond(durl))

    # Configuración definida por source
    has_extensions = True # Si tratar extensiones en los nombres de fichero
    allowed_ct = None # Futuro set de tipos de contenidos permitidos

    if sources:
        for sourceid in sourceids:
            if sourceid in sources:
                source = sources[sourceid]
                if "hidden_extensions" in source and source["hidden_extensions"]:
                    has_extensions = False
                if "ct" in source and source["ct"] and source["ct"] != ct.CONTENT_UNKNOWN:
                    if len(source["ct"]) == 1:
                        # Análisis de nombres de ficheros sin tipos
                        empty_scores, ntags, fileformats = analyze_filenames(filepaths, filesizes, skip_ct=True, analyze_extensions=has_extensions)
                        tags.update(ntags)
                        return restrict_content_type(None, tags, fileformats, int(source["ct"][0])) # Tipo dado por el origen
                    source_ct = int(source["ct"])
                    if allowed_ct is None:
                        allowed_ct = {source_ct}
                    else:
                        allowed_ct.add(source_ct)
                    break
    # Si no se ha especificado una lista de cts permitidos, usamos la genérica
    if not allowed_ct:
        allowed_ct = CONTENT_TYPE_SUBSET
    # Análisis de nombres de ficheros
    scores = None # Lista de puntuaciones para los tipos de contenido
    fileformats = None # Diccionario de puntuaciones para los formatos
    if filepaths:
        scores, ntags, fileformats = analyze_filenames(filepaths, filesizes, analyze_extensions=has_extensions)
        tags.update(ntags)

    # Si no se han sacado fileformats y scores de los nombres de fichero
    if fileformats is None:
        fileformats =  _formats_initial.copy()
        scores = _scores_initial[:]

    # Tipo de contenido por urls
    if "src" in doc:
        for d in doc["src"].itervalues():
            if "t" in d:
                dsid = int(d["t"])
                if dsid in SOURCE_CT_URL:
                    ctid, ctw = SOURCE_CT_URL[dsid](d["url"])
                    scores[ctid] += ctw

    # Puntuaciones por metadatos
    if "md" in doc:
        for mdkey in doc["md"].iterkeys():
            prefix = mdkey.split(":", 1)[0]
            if prefix in METADATA_PREFIX:
                scores[METADATA_PREFIX[prefix]] += METADATA_PREFIX_WEIGHT

    # Suma del tipo de contenido dado del mongo
    if "ct" in doc:
        doc_ct = int(doc["ct"])
        doc_s = int(doc["s"])
        scores[doc_ct] += sum((
            SOURCE_CT_CONFIDENCE.get(sourceid, SOURCE_CT_CONFIDENCE_DEFAULT)
            for sourceid in sourceids), 0.0) * (
            SERVER_CT_CONFIDENCE.get(doc_s, SERVER_CT_CONFIDENCE_DEFAULT) *
            PRIORITY_CT / len(sourceids)
            )

    # Tags por metadatos
    if "md" in doc:
        # tags provenientes de torrents
        torrent_tags = []
        if "torrent:special_tags" in doc["md"]:
            torrent_tags = [tag.strip() for tag in u(doc["md"]["torrent:special_tags"]).split(",") if tag and tag.strip()]

        if "torrent:category" in doc["md"]:
            torrent_category = [tag.strip() for tag in u(doc["md"]["torrent:category"]).lower().replace("/",",").split(",") if tag]
            torrent_tags.extend(TORRENT_CATEGORY_TAG[tag] if tag in TORRENT_CATEGORY_TAG else tag for tag in torrent_category if tag)

        if torrent_tags:
            tags.update(tag for tag in torrent_tags if tag in ALL_TAGS)

            for tag in tags:
                if tag in TAG_CONTENT_TYPE:
                    scores[TAG_CONTENT_TYPE[tag]] += 1.0
                elif tag in TAG_CONTENT_TYPE_GUESS:
                    for tct, w in TAG_CONTENT_TYPE_GUESS[tag].iteritems():
                        scores[tct] += w

        tags.update(
            tag
            for mdkey in doc["md"].iterkeys() if mdkey in REVERSE_TAG_METADATA
                for tag, cond in REVERSE_TAG_METADATA[mdkey].iteritems() if cond is True or callable(cond) and cond(doc)
            )
    return restrict_content_type(scores, tags, fileformats)

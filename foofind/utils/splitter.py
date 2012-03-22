#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from collections import defaultdict

split = re.compile(r"(?:[^\w\']|\_)|(?:[^\_\W]|\')+", re.UNICODE)
sepper = re.compile(r"[^\w\']|\_", re.UNICODE)
wsepsws = {" ":0.9, "_":0.6, ".":0.2, "-":0.1, "&":0.01, "(":-0.1, "[":-0.1, "{":-0.1, "}":-0.1, "]":-0.1, ")":-0.1}

types = {1:"a", 2:"v", 3:"e", 4:"t", 5:"i", 6:"n", 7:"z", 8:"r", 9:"d", 10:"s", 11:"p"}
exts = {"264":2, "3d":5, "3ds":5, "3dr":5, "3g2":2, "3gp":2, "7z":7, "7zip":7, "aac":1, "abr":5, "abw":9, "ace":7, "act":5, "aeh":3, "afp":9, "ai":5, "aif":1, "aifc":1, "aiff":1, "air":6, "alx":6, "alz":7, "amr":1, "ani":5, "ans":9, "ape":6, "apk":6, "aplibrary":5, "app":6, "arc":7, "arj":7, "art":5, "arw":5, "asf":2, "asx":2, "at3":7, "au":1, "aup":1, "avi":2, "awg":5, "aww":9, "azw":3, "bat":6, "big":7, "bik":2, "bin":7, "bke":7, "bkf":7, "blp":5, "bmp":5, "bw":5, "bzip2":7, "cab":7, "caf":1, "cbr":3, "cbz":3, "ccd":8, "cda":1, "cdr":5, "cgm":5, "chm":3, "cit":5, "class":6, "cmx":5, "cod":6, "com":6, "cpt":5, "cr2":5, "crw":5, "csv":10, "cut":5, "cwk":9, "daa":8, "dao":8, "dat":2, "dcr":5, "dds":7, "deb":7, "dib":5, "divx":2, "djvu":3, "dll":6, "dmg":8, "dng":5, "dnl":3, "doc":9, "docm":9, "docx":9, "dot":9, "dotm":9, "dotx":9, "drw":5, "dwg":5, "dxf":5, "ecab":7, "eea":7, "egt":5, "emf":5, "emz":5, "eps":9, "epub":3, "erf":5, "ess":7, "exe":6, "exif":5, "fax":9, "fb2":3, "fff":5, "fla":6, "flac":1, "flv":2, "flw":2, "fpx":5, "ftm":9, "ftx":9, "gadget":6, "gho":7, "gif":5, "gz":7, "gzip":7, "hqx":7, "htm":9, "html":9, "hwp":9, "ibm":5, "icb":5, "ico":5, "icon":5, "icns":5, "iff":5, "ilbm":5, "img":8, "ind":5, "info":9, "int":5, "ipa":6, "iso":8, "isz":8, "j2k":5, "jar":6, "jng":5, "jpeg":5, "jp2":5, "jpg":5, "kdc":5, "keynote":11, "kml":9, "la":1, "lbr":7, "lha":7, "lit":3, "lqr":7, "lrf":3, "lrx":3, "lwp":9, "lzo":7, "lzx":7, "m2ts":2, "m4a":1, "m4b":1, "m4p":1, "m4v":2, "mcw":9, "mdf":8, "mds":8, "mef":5, "mht":9, "midi":1, "mkv":2, "mobi":3, "mod":1, "mos":5, "mov":2, "mp+":1, "mp2":1, "mp3":1, "mp4":2, "mpa":1, "mpc":1, "mpe":2, "mpeg":2, "mpg":2, "mpp":1, "mrw":5, "msi":6, "nb":9, "nbp":9, "nds":6, "nef":5, "nes":6, "nrg":8, "nsv":2, "numbers":10, "ocx":6, "odg":5, "odp":11, "ods":10, "odt":9, "ogg":1, "ogm":2, "ogv":2, "opf":3, "orf":5, "otp":11, "ots":10, "ott":9, "pages":9, "pak":7, "pac":1, "pap":9, "par":7, "par2":7, "pbm":5, "pcd":5, "pcf":5, "pcm":1, "pct":5, "pcx":5, "pdb":3, "pdd":5, "pdf":3, "pdn":5, "pef":5, 
"pgm":5, "pk4":7, "pkg":7, "pix":5, "pnm":5, "png":5, "potx":11, "ppm":5, "pps":11, "ppsm":11, "ppsx":11, "ppt":11, "pptm":11, "pptx":11, "prc":3, "prg":6, "ps":9, "psb":5, "psd":5, "psp":5, "ptx":5, "px":5, "pxr":5, "qfx":5, "r3d":5, "ra":1, "raf":5, "rar":7, "raw":5, "rgb":5, "rgo":3, "rka":1, "rm":2, "rma":1, "rom":8, "rtf":9, "sav":6, "scn":6, "scr":6, "sct":5, "scx":6, "sdw":9, "sea":7, "sgi":5, "shn":1, "shp":5, "sisx":6, "sit":7, "sitx":7, "skp":5, "snd":1, "sng":1, "sr2":5, "srf":5, "srt":9, "sti":9, "stw":9, "sub":9, "svg":5, "svi":2, "swf":6, "sxc":10, "sxi":9, "sxw":9, "tao":8, "tar":7, "targa":5, "tb":7, "tex":9, "text":9, "tga":5, "tgz":7, "theme":6, "themepack":6, "thm":5, "thmx":11, "tib":7, "tif":5, "tiff":5, "toast":8, "torrent":4, "tr2":3, "tr3":3, "txt":9, "uha":7, "uif":8, "uoml":9, "vbs":6, "vcd":8, "vda":5, "viff":5, "vob":2, "vsa":7, "vst":5, "wav":1, "webarchive":9, "wma":1, "wmf":5, "wmv":2, "wol":3, "wpd":9, "wps":9, "wpt":9, "wrap":2, "wrf":9, "wri":9, "wv":1, "x3f":5, "xar":5, "xbm":5, "xcf":5, "xls":10, "xlsm":10, "xlsx":10, "xdiv":2, "xhtml":9, "xls":9, "xml":9, "xpi":6, "xpm":5, "xps":9, "yuv":5, "z":7, "zip":7, "zipx":7, "zix":7, "zoo":7 }
mds = {'audio:artist':1,'audio:composer':1,'archive:folders':1, 'archive:files':1,'video:keywords':1,'audio:album':1, 'audio:title':1, 'document:title':1, 'torrent:name':1, 'image:title':1, 'video:title':1 }

def isallnumeric(l):
    return all(map(unicode.isdigit, l))

def split_phrase(phrase):
    global split, sepper, exts, wsepsws
    #split between words and separators
    parts = split.findall(phrase)
    parts = reduce(lambda x,y: [list(x)+[y], x][len(y)==1 and x[-1]==y and sepper.match(y)!=None], [' '] + parts)[1:]
    ct = None
    #ignore small phrases
    if len(parts)>2:
        #ignore extensions
        for i in range(len(parts)-1,0,-1):
            if parts[i-1]=='.' and parts[i].lower() in exts:
                if not ct or ct in ["t", "z"]: ct = exts[parts[i].lower()]
                del(parts[i])
                del(parts[i-1])
                i-=1
    #ignore small phrases
    if len(parts)<=3: return [[w for w in parts if len(w)>1 or sepper.match(w)==None]], ct
    #identify separators
    seplist = [(w,False)[len(w)>1 or sepper.match(w)==None] for w in parts]
    wseps=defaultdict(int)
    pseps=defaultdict(int)
    for sep in seplist: 
        if sep and sep in wsepsws: wseps[sep] = wsepsws[sep]
    compare = [seplist[:-2], seplist[1:-1], seplist[2:]]
    compare = map(lambda *x:x, *compare)
    for comp in compare:
        if not comp[1]: continue
        if not comp[0] and not comp[2]: 
            wseps[comp[1]] += 2
        elif comp[0]==comp[2]:
            wseps[comp[1]] -= 1
            pseps[comp[1]] += 1
        else:
            wseps[comp[1]] += 1
    wsep = sorted(wseps, reverse=True, key=wseps.__getitem__)[0]
    phrase = []
    ret = []
    for i in range(0, len(parts)):
        cont = join = 0
        if i>=len(parts):continue
        if parts[i] in wseps:
            if i==0 or i==len(parts)-1 or parts[i]==wsep: continue  # starting, ending and word separators
            if len(phrase)>0:
                if parts[i] in [".", ","] and phrase[-1].isdigit() and parts[i+1].isdigit():  # numbers
                    join = (2,1)[len(parts[i+1])==3]
                elif parts[i] in [",", "&"] and parts[i+1]==wsep: # punctuations
                    continue
                elif parts[i]!="|" and sepper.match(parts[i+1])==None: # between words separators
                    join = (2,0)[parts[i] in wsepsws]
                    cont = pseps[parts[i]]>0
                elif parts[i] == "." and parts[i+1]==wsep and not parts[i-1].isdigit(): continue # abreviation
                if join>0:
                    if join==2: phrase[-1]+=parts[i]
                    phrase[-1]+=parts[i+1]
                    del(parts[i+1])
                if join + cont>0: continue
                if not isallnumeric(phrase) and not len(phrase)==len(phrase[0])==1: ret.append(phrase)
            phrase = []
        else:
            phrase.append(parts[i])
    if len(phrase)>0 and not isallnumeric(phrase) and not len(phrase)==len(phrase[0])==1: ret.append(phrase)
    return ret, ct

def split_file(f):
    global mds
    res = []
    cts = {}
    if 't' in f and f['t'] in types: cts[f['t']] = 3
    for ffn in f["fn"]:
        ffnn = f["fn"][ffn][u'n']
        r, c = split_phrase(ffnn)
        res += r
        if c:
            if not c in cts: cts[c]=1
            else: cts[c] += 1
    for fmd in f["md"]:
        if fmd in mds and type(f["md"][fmd]) is unicode:
            res += split_phrase(f["md"][fmd])[0]
    sumcts = sum(cts.values())*1.0
    info = dict(map(lambda x: (u"("+types[x], cts[x]/sumcts), cts))
    return res, info

#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
    Acortador de urls de foof.in
'''
#from flask import Flask, redirect
from werkzeug.wrappers import Request
from werkzeug.utils import redirect
from foofind.defaults import ALL_LANGS as langcodes

nlangcodes = len(langcodes)
domain = "foofind.is"

@Request.application
def application(request):
    part = request.path.split("/")
    partlen = len(part)
    if partlen > 1 and part[1].isdigit():
        langnum = int(part[1])
        langcode = langcodes[langnum-1] if 0 < langnum <= nlangcodes else nlangcodes[0]
        if partlen > 2 and part[2]:
            return redirect("http://%s/%s/download/%s" % (domain, langcode, part[2]), 301)
        return redirect("http://%s/%s" % (domain, langcode), 301)
    return redirect("http://%s" % domain, 301)

# -*- coding: utf-8 -*-
'''
    Acortador de urls de foof.in
'''
from flask import Flask, redirect
from foofind.defaults import ALL_LANGS as langcodes

nlangcodes = len(langcodes)
domain = "foofind.is"

def create_app(config=None, debug=False):
    '''
    Función de creación de aplicación
    '''
    app = Flask(__name__)
    app.debug = debug

    @app.route("/")
    @app.route("/<int:code>")
    @app.route("/<int:code>/<fid>")
    def foofin(code=None, fid=None):
        if code is None: return redirect("http://%s" % domain, 301)
        lang = langcodes[code-1] if 0 < code <= nlangcodes else nlangcodes[0]
        if fid is None: return redirect("http://%s/%s" % (domain, langcode), 301)
        return redirect("http://%s/%s/download/%s" % (domain, lang, fid), 301)

    return app

application = create_app()

if __name__ == "__main__":
    application.run()

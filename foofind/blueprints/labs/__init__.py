# -*- coding: utf-8 -*-
from foofind.blueprints.labs import files_ajax, files_test
from foofind.utils import fooprint
from foofind.services import eventmanager, cache

def add_labs(app):
    # Registro de blueprints
    app.register_blueprint(files_ajax.files_ajax)
    #app.register_blueprint(files_test.files_test)
    
def init_labs(app):
    pass
    #files_test.init_searchd(app)

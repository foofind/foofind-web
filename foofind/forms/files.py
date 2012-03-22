# -*- coding: utf-8 -*-
"""
    Formularios para manejo de datos de ficheros.
"""
from wtforms import Form, TextField, RadioField, TextAreaField, SubmitField, HiddenField
from flaskext.babel import lazy_gettext as _
from foofind.forms.validators import *

class SearchForm(Form):
    '''
    Formulario de búsqueda de ficheros.
    '''
    q = TextField(validators=[length(min=4, max=25)])
    submit = SubmitField(_("submit_search"))
    src = RadioField(default="swftge",choices=[('swftge', _('all')), ('wf', _('direct_downloads')), ('t', 'Torrents'), ('s', 'Streaming'), ('g', 'Gnutella'), ('e', 'Ed2k')])
    type = HiddenField()

class CommentForm(Form):
    '''
    Formulario para añadir comentarios
    '''
    t = TextAreaField(validators=[require()])
    submit_comment = SubmitField(_("post_comment"))

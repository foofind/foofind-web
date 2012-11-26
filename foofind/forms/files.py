# -*- coding: utf-8 -*-
"""
    Formularios para manejo de datos de ficheros.
"""
from flask.ext.wtf import Form, TextField, RadioField, TextAreaField, SubmitField, HiddenField, BooleanField, SelectField

from flask.ext.babel import lazy_gettext as _
from foofind.forms.validators import *
from foofind.forms.fields import VoidSubmitField

class SearchForm(Form):
    '''
    Formulario de búsqueda de ficheros.
    '''
    q = TextField(validators=[length(min=4, max=25)])
    search_submit = SubmitField(_("submit_search"))
    src = RadioField(default="swftge",choices=[('swftge', _('all')), ('wf', _('direct_downloads')), ('t', 'Torrents'), ('s', 'Streaming'), ('g', 'Gnutella'), ('e', 'Ed2k')])
    type = SelectField(default="", choices=[('', _('all')), ('audio', _('audio')), ('video', _('video')), ('image', _('image')), ('document', _('document')), ('software', _('software'))])

class CommentForm(Form):
    '''
    Formulario para añadir comentarios
    '''
    t = TextAreaField(validators=[require()])
    submit_comment = SubmitField(_("post_comment"))

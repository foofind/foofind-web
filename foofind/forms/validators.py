# -*- coding: utf-8 -*-
"""
    Formularios para manejo de datos de usuarios.
"""
from flask.ext.wtf import Required, Email, Length, URL, ValidationError
from flask.ext.babelex import lazy_gettext as _
from functools import partial
import re

def number_letter(form,field):
    '''
    Validador para controlar que solo se ponen numeros y letras
    '''
    if not re.match(r'^^(\w+)*$',field.data or u'',re.UNICODE):
        raise ValidationError(_("non_alphabetic_digits",value=field.data))

require=partial(Required,message=_("required_empty"))
email=partial(Email,message=_("no_valid_email"))
length=partial(Length,message=_("field_length",min="%(min)d",max="%(max)d"))
url=partial(URL,message=_("no_valid_url"))

# -*- coding: utf-8 -*-
"""
    Formularios para manejo de datos de usuarios.
"""
from flask.ext.wtf import Form,BooleanField,PasswordField,TextField,SubmitField,ValidationError
from flask.ext.babel import lazy_gettext as _
from foofind.forms.captcha import CaptchaField
from foofind.forms.validators import *


class LoginForm(Form):
    '''
    Formulario de login de usuario.
    '''
    email = TextField(_('your_email'), [require(), email()])
    password = PasswordField(_('password'), [require()])
    rememberme = BooleanField(_('remember_me'))
    submit = SubmitField(_("submit_login"))

class ForgotForm(Form):
    '''
    Formulario de recordatorio de contraseña
    '''
    email = TextField(_('your_email'), [require(), email()])
    captcha = CaptchaField()
    submit = SubmitField(_("submit"))

class RegistrationForm(Form):
    '''
    Formulario de registro de usuario.
    '''
    username = TextField(_("nickname"), [require(), length(min=3,max=20),number_letter])
    email = TextField(_("your_email"), [require(), length(min=6, max=35),email()])
    password = PasswordField(_("your_password"), [require(), length(min=5, max=20), EqualTo('confirm', message=_("passwords_not_match"))])
    confirm = PasswordField(_("insert_your_password"), [require(), length(min=5, max=20)])
    accept_tos = BooleanField(validators=[require()])
    captcha = CaptchaField()
    submit = SubmitField(_("submit_registration"))

class EditForm(Form):
    '''
    Formulario para editar usuario.
    '''
    username = TextField(_("username"), [require(), length(min=3,max=20),number_letter])
    location = TextField(_("location"), [Optional(), length(min=5,max=50)])
    password = PasswordField(_("new_password"), [Optional(), length(min=5, max=20), EqualTo('confirm', message=_("passwords_not_match"))])
    confirm = PasswordField(_("insert_your_password"), [Optional(), length(min=5, max=20)])
    submit = SubmitField(_("submit_edit"))

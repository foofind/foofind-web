# -*- coding: utf-8 -*-
from flask import render_template, flash, current_app
from flask.ext.babelex import gettext as _, Babel
from flask.ext.login import LoginManager
from flask.ext.mail import Mail, Message
from flask.ext.seasurf import SeaSurf
from smtplib import SMTPRecipientsRefused
from raven.contrib.flask import Sentry

from foofind.services.cache import Cache
from foofind.services.unittest import UnitTester
from foofind.utils import logging

__all__ = ('babel', 'cache', 'auth', 'mail', 'send_mail', 'sentry', 'unit', 'csrf')

babel = Babel()
auth = LoginManager()
mail = Mail()
sentry = Sentry(logging = True)
cache = Cache()
unit = UnitTester()
csrf = SeaSurf()

def send_mail(subject,to,template=None,attachment=None,**kwargs):
    '''
    Envia un correo y trata y loguea los errores
    '''
    try:
        msg=Message(_(subject),to if isinstance(to,list) else [to],html=render_template('email/'+(template if template else subject)+'.html',**kwargs))
        if attachment is not None:
            msg.attach(attachment[0],attachment[1],attachment[2])

        mail.send(msg)
        return True
    except SMTPRecipientsRefused as e:
        # se extrae el código y el mensaje de error
        (code,message)=e[0].values()[0]
        logging.warn("%d: %s"%(code,message))
        flash("error_mail_send")
        return False

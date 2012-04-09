# -*- coding: utf-8 -*-
from flask import render_template, flash, current_app
from flaskext.babel import gettext as _
from flaskext.babel import Babel
from flaskext.cache import Cache
from flaskext.login import LoginManager
from flaskext.mail import Mail, Message
from smtplib import SMTPRecipientsRefused
from raven.contrib.flask import Sentry
import logging

__all__ = ['babel', 'cache', 'auth', 'mail', 'send_mail', 'sentry']

babel = Babel()
cache = Cache()
auth = LoginManager()
mail = Mail()
sentry = Sentry()

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
        flash("error_mail_send")
        # se extrae el código y el mensaje de error
        (code,message)=e[0].values()[0]
        logging.exception("%d: %s"%(code,message))    
        return False

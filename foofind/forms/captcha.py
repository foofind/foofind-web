# -*- coding: utf-8 -*-
from flask import request, session
from flaskext.babel import lazy_gettext as _
from wtforms.fields import Field
from foofind.forms.validators import *
from foofind.services import cache

import Image, ImageFont, ImageDraw, ImageFilter, StringIO
import hashlib
import random

CAPTCHA_KEY = "foofind.com_capt";

def generate_image(txt):
    def random_color(total=False):
        if total == False:
            return (random.randint(20,150), random.randint(20,140), random.randint(160,200))
        return (random.randint(0,255), random.randint(0,255), random.randint(0,255))

    length = len(txt)
    width = 200
    height = 40
    font_size = 36
    interval = width/length
    font = ImageFont.truetype("foofind/static/DejaVuSans.ttf",font_size)
    image = Image.new('RGB', (width, height), (255,255,255))
    draw = ImageDraw.Draw(image)

    # Generar cada letra
    for i in range(0,length):
        # Crear una imagen para guardar un caracter
        char = Image.new('RGB',(font_size, font_size))
        # Añadirle el caracter con la fuente indicada y un color aleatorio
        ImageDraw.Draw(char).text((3, 1), txt[i], font=font, fill= random_color())
        # Girarlo 40 grados hacia cualquier lado
        char = char.rotate(random.randint(-40,40))
        # Crear una mascara para tapar las partes negras que salen al girar el caracter
        mask = Image.new('L',(font_size, font_size),0)
        mask.paste(char,(0,0))
        # Pegarlo en la imagen final en unas coordenadas aleatorias
        image.paste(char,(i*interval + random.randint(0,interval/4),random.randint(-4,-2)),mask)

    # Pintar lineas y puntos en el fondo para confundir
    for i in range(random.randint(2,5)):
        draw.line((random.randint(6,width-6),random.randint(3,height-3),random.randint(6,width-6),random.randint(3,height-3)), fill = random_color())
    for i in range(0,1000):
        draw.point((random.randint(0,width),random.randint(0,height)),fill = random_color(True))

    # Guardar la imagen en el buffer y devolverlo
    buffer = StringIO.StringIO()
    image.save(buffer, "PNG")
    return buffer.getvalue()

def captcha(form,field):
    '''
    Validador para controlar que la coinciden la imagen y el texto enviado por el usuario
    '''
    hasher = hashlib.sha256()
    hasher.update(CAPTCHA_KEY+field.data)
    captcha_id = session.pop("captcha_id",None)
    if hasher.hexdigest()!=captcha_id:
        raise ValidationError(_('captcha_wrong'))

class Captcha(object):
    def __call__(self, field, **kwargs):
        session["captcha_id"]=field.captcha_id
        return u'''
            <img src="/captcha/%(id)s" alt="captcha" />
            <input type="text" name="%(name)s" id="%(name)s" />''' % {'id': field.captcha_id, "name":field.name}

class CaptchaField(Field):
    widget = Captcha()

    def __init__(self, label='', validators=None, **kwargs):
        super(CaptchaField, self).__init__(_("insert_characters"), [captcha], **kwargs)
        self.captcha_id = self.generate_id()

    def generate_id(self):
        # Generar un texto aleatorio para el captcha
        imgtext = ''.join([random.choice('ABCDEFGHIJKLMNOPQRSTUVWZYZ0123456789') for i in range(5)])

        # Encriptarlo
        hasher = hashlib.sha256()
        hasher.update(CAPTCHA_KEY+imgtext)

        captcha_id = hasher.hexdigest()
        cache.set("captcha_"+captcha_id, generate_image(imgtext))

        # Devolverlo codificado para poder usado en una URL
        return captcha_id

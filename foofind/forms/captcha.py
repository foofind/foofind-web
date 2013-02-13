# -*- coding: utf-8 -*-
from flask import request, session, current_app
from flask.ext.wtf import ValidationError
from flask.ext.babel import lazy_gettext as _
from wtforms.fields import Field
from foofind.forms.validators import require
from foofind.services import cache
from hashlib import sha256
import Image, ImageFont, ImageDraw, ImageFilter, StringIO

import random

cache_image_font = ImageFont.truetype("foofind/static/DejaVuSans.ttf", 36)
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
    image = Image.new('RGB', (width, height), (255,255,255))
    draw = ImageDraw.Draw(image)

    # Generar cada letra
    for i in range(0,length):
        # Crear una imagen para guardar un caracter
        char = Image.new('RGB',(font_size, font_size))
        # Añadirle el caracter con la fuente indicada y un color aleatorio
        ImageDraw.Draw(char).text((3, 1), txt[i], font=cache_image_font, fill= random_color())
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

def captcha(form, field):
    '''
    Validador para controlar que la coinciden la imagen y el texto enviado por el usuario
    '''
    captcha_id = request.form["%s_id" % field.name]
    cache_data = cache.get("captcha/" + captcha_id)
    if cache_data is None:
        raise ValidationError(_('captcha_wrong'))
    code, consumed = cache_data
    if field.data != code or not consumed:
        raise ValidationError(_('captcha_wrong'))
    cache.delete("captcha/" + captcha_id)

class Captcha(object):
    def __call__(self, field, **kwargs):
        return u'''
            <img src="/captcha/%(captcha_id)s" alt="captcha" />
            <input type="hidden" name="%(name)s_id" value="%(captcha_id)s" />
            <input type="text" name="%(name)s" id="%(name)s" autocomplete="off" />''' % { "captcha_id":field.captcha_id, "name":field.name}

class CaptchaField(Field):
    widget = Captcha()

    def __init__(self, label='', validators=None, **kwargs):
        super(CaptchaField, self).__init__(_("insert_characters"), [captcha], **kwargs)
        self.captcha_id = self.generate_id()

    def generate_id(self):
        # Generar un texto aleatorio para el captcha
        imgtext = ''.join(random.choice('ABCDEFGHIJKLMNPRSTUVWZYZ123456789') for i in range(5))

        # Encriptarlo
        captcha_id=sha256(current_app.config["SECRET_KEY"]+imgtext).hexdigest()
        cache.set("captcha/%s" % captcha_id, (imgtext, False))

        # Devolverlo codificado para poder usado en una URL
        return captcha_id

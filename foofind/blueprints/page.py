# -*- coding: utf-8 -*-
"""
    Controladores de páginas estáticas.
"""
from flask import Blueprint, g, render_template, request, flash, current_app, redirect, url_for, abort
from flaskext.babel import gettext as _
from wtforms import Form,FieldList,TextField,TextAreaField,SubmitField
from babel import localedata
from foofind.forms.pages import ContactForm, SubmitLinkForm, ReportLinkForm, SelectLanguageForm, TranslateForm, JobsForm
from foofind.services import *
from foofind.translations.samples_values import samples
from foofind.utils import lang_path, expanded_instance, nocache
from functools import cmp_to_key
import locale
import polib
import re
import random

page = Blueprint('page', __name__)

@page.context_processor
def page_globals():
    return {"zone": "page"}

@page.before_request
def set_search_form():
    g.title+=" - "

@page.route('/<lang>/page/<pname>')
def old_show(pname):
    '''
    Si la url es antigua se redirecciona con un 301 a la nueva
    '''
    if callable(pname):
        return redirect(url_for("page."+pname),301)
    else:
        return redirect(url_for("page.show",pname=pname),301)

valid_pages = {"about":"about_text", "legal":"safe_legal", "tos":"safe_tos", "privacy":"safe_privacy"}
@page.route('/<lang>/<pname>')
def show(pname):
    '''
    Página para contenidos sin interacción.
    '''
    if not pname in valid_pages:
        abort(404)

    g.title+=_(pname)
    return render_template('pages/page.html',page_title=_(pname),page_text=valid_pages[pname],domain=g.domain)

@page.route('/<lang>/jobs', methods=['GET', 'POST'])
@nocache
def jobs():
    '''
    Página de oferta de trabajo
    '''
    form = JobsForm(request.form,captcha={'ip_address': request.remote_addr})
    if request.method=='POST' and form.validate():
        attach=None
        ufile=request.files[form.cv.name]
        if ufile.filename:
            attach=(ufile.filename,ufile.content_type,ufile.read())

        if send_mail("Ofertas de empleo",current_app.config["JOBS_EMAIL"],"pages",attach,form=form):
            flash("message_sent")
            return redirect(url_for('index.home'))

    g.title+="Ofertas de empleo"
    return render_template('pages/jobs.html',page_title="Ofertas de empleo",form=form)

@page.route('/<lang>/contact', methods=['GET', 'POST'])
@nocache
def contact():
    '''
    Muestra el formulario de contacto
    '''
    form = ContactForm(request.form)
    if request.method=='POST' and form.validate() and send_mail("contact",current_app.config["CONTACT_EMAIL"],"pages",form=form):
        flash("message_sent")
        return redirect(url_for('index.home'))

    g.title+=_("contact")
    return render_template('pages/contact.html',page_title=_("contact"),form=form)

@page.route('/<lang>/submitlink', methods=['GET', 'POST'])
@nocache
def submit_link():
    '''
    Muestra el formulario para agregar enlaces
    '''
    form = SubmitLinkForm(request.form)
    if request.method=='POST' and form.validate():
        feedbackdb.create_links({"links":[link for link in form.urls.data.splitlines()],"ip":request.remote_addr})
        flash("link_sent")
        return redirect(url_for('index.home'))

    g.title+=_("submit_links")
    return render_template('pages/submit_link.html',page_title=_("submit_links"),form=form)

@page.route('/<lang>/complaint', methods=['GET', 'POST'])
@nocache
def complaint():
    '''
    Muestra el formulario para reportar enlaces
    '''
    form = ReportLinkForm(request.form)
    if request.method=='POST' and form.validate():
        pagesdb.create_complaint(dict([("ip",request.remote_addr)]+[(field.name,field.data) for field in form]))
        flash("message_sent")
        return redirect(url_for('index.home'))

    g.title+=_("complaint")
    return render_template('pages/complaint.html',page_title=_("complaint"),form=form)

@page.route('/<lang>/translate',methods=['GET','POST'])
@nocache
def translate():
    '''
    Edita la traducción a un idioma
    '''
    def fix_values(entry,sample=False):
        '''
        Si la traduccion contiene campos de valores los sustituimos por ______[X] y ponemos un ejemplo de uso,
        además se eliminan los saltos de linea
        '''
        result=re.finditer(r'(%\(([^\)]+)\)([s|d]))', entry.msgstr)
        subs=dict()
        # se cargan los ejemplos si es necesario
        if entry.msgid in samples:
            subs=samples[entry.msgid]

        # para cada valor encontrado se sustituye por _____[X]
        for i,item in enumerate(result):
            entry.msgstr=entry.msgstr.replace(item.group(1),"_____["+str(i+1)+"]")
            # para los ejemplos numericos se utiliza uno aleatorio
            if item.group(3)=="d":
                subs[item.group(2)]=random.randint(2,10)

        if sample:
            if subs!={}:
                return (entry.msgid,(entry.msgstr,_(entry.msgid,**subs)))
            else:
                return (entry.msgid,(entry.msgstr,False))

        # se sustituyen los saltos de linea html y se devuelve todo
        return (entry.msgid,entry.msgstr.replace("<br>","\n").replace("<br />","\n").replace("<br/>","\n") if "<br" in entry.msgstr else entry.msgstr)

    languages = localedata.load(g.lang)["languages"]
    keystrcoll = cmp_to_key(locale.strcoll)
    form = None
    forml = SelectLanguageForm(request.form)
    forml.lang.choices = [("", "-- "+_("choose_language")+" --")] + sorted(
        ((code, localedata.load(code)["languages"][code].capitalize()+" ("+languages[code].capitalize()+")")
            for code, language in languages.items()
            if code in current_app.config["TRANSLATE_LANGS"] and not code in current_app.config["LANGS"] and localedata.exists(code) and code in localedata.load(code)["languages"]),
        key=lambda x: keystrcoll(x[1]))

    total=99999
    no_translation=0
    msgids=[]
    lang_edit = request.args.get("lang")
    if not lang_edit in languages:
        lang_edit = None

    formfields = {}
    # mostrar el idioma a traducir
    if lang_edit is not None:
        forml.lang.default=lang_edit
        # cargar idioma actual
        current_lang = dict(fix_values(entry,True) for entry in polib.pofile(lang_path(g.lang)))

        # si existe el idioma se carga, sino vacio
        lpath = lang_path(lang_edit)
        new_lang = dict(fix_values(entry) for entry in polib.pofile(lpath)) if lpath else {}

        # recorre los ids en ingles y los coge el mensaje del idioma actual y el valor del nuevo
        for i, (msgid, msgstr) in enumerate(fix_values(entry,True) for entry in polib.pofile(lang_path("en"))):
            # se excluyen los textos legales que concluyen con safe_
            if not msgid.startswith(("safe_","admin_")):
                # si no esta traducida la cadena en el idioma actual se deja vacio
                if not msgid in new_lang:
                    no_translation+=1
                    new_lang[msgid]=""

                # si el mensaje esta traducido al idioma actual se usa, sino se usa el ingles
                if msgid in current_lang:
                    msg=current_lang[msgid][0]
                    description=current_lang[msgid][1]
                else:
                    msg=msgstr[0]
                    description=msgstr[1]

                # si la traduccion es mayor de 80 caracteres se utiliza un textarea en vez de un input text
                length=len(new_lang[msgid] or msg)
                if length>80:
                    formfields[msgid]=TextAreaField(msg,default=new_lang[msgid],description=description)
                    # se le establecen las filas al text area dependiendo del tamaño de la traduccion
                    formfields["_args_%s" % msgid]={"rows":length/50}
                else:
                    formfields[msgid]=TextField(msg,default=new_lang[msgid],description=description)
                    formfields["_args_%s" % msgid]={}

                #se añade a la lista que se le envia al formulario
                msgids.append(msgid)

        total=float(len(msgids))
        form=expanded_instance(TranslateForm, formfields, request.form, prefix="translate_")
        # si es el envio de la traducción
        if request.method == 'POST' and form.validate():
            pagesdb.create_translation({"ip":request.remote_addr,"user_lang":g.lang,"dest_lang":lang_edit,"texts":{field.short_name: field.data for field in form if not field.short_name in ("captcha", "submit_form") and field.data!=new_lang[field.short_name]}})
            flash("translation_sent")
            return redirect(url_for('index.home'))

    if lang_edit: forml.lang.data = lang_edit
    # sino se muestra la seleccion de idioma a traducir
    g.title+=_("translate_to_your_language")
    return render_template('pages/translate.html',
        page_title=_("translate_to_your_language"),
        lang=lang_edit,
        forml=forml,
        form=form,
        msgids=msgids,
        complete=round(((total-no_translation)/total)*100,2))

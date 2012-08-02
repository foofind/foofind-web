# -*- coding: utf-8 -*-
"""
    Formularios para admin
"""

from wtforms import *
from wtforms.widgets import *
from flaskext.babel import lazy_gettext as _
from flaskext.babel import gettext

import logging

from .fields import HTMLString, fix_param_name, html_params

class MultiHostField(SelectMultipleField):
    class LoneLabel(object):
        '''
        Label sin atributo 'for', el estándar html especifica que el id en
        'for' debe apuntar a un campo de formulario
        '''
        def __init__(self, field_id, text):
            self.field_id = field_id
            self.text = text

        def __str__(self): return self()
        def __unicode__(self): return self()
        def __html__(self): return self()

        def __call__(self, text=None, **kwargs):
            attributes = widgets.html_params(**kwargs)
            return widgets.HTMLString(u'<label %s>%s</label>' % (attributes, text or self.text))

        def __repr__(self):
            return 'LoneLabel(%r, %r)' % (self.field_id, self.text)

    class StatusWidget(object):
        '''
        SelectMultipleField en un ul con checkboxes.
        '''
        def __init__(self, html_tag='ul'):
            self.html_tag = html_tag
            self.inner_kwarg_prefix = "checkbox_"

        def __call__(self, field, **kwargs):
            kwargs.setdefault('id', field.id)
            likp = len(self.inner_kwarg_prefix)
            inner_kwargs = {fix_param_name(k[likp:]):v for k,v in kwargs.iteritems() if k.startswith(self.inner_kwarg_prefix)}
            outer_kwargs = {fix_param_name(k):v for k,v in kwargs.iteritems() if not k.startswith(self.inner_kwarg_prefix)}
            return HTMLString(u'<%s %s data-status_unknown="%s" data-status_loading="%s" data-status_processing="%s">%s</%s>' % (
                self.html_tag,
                html_params(**outer_kwargs),
                gettext("admin_status_unknown"),
                gettext("admin_status_loading"),
                gettext("admin_status_processing"),
                u''.join(u'<li><span>%s%s</span></li>' % (
                    subfield(**inner_kwargs),
                    subfield.label
                    ) for subfield in field),
                self.html_tag))

    def __init__(self, *args, **kwargs):
        SelectMultipleField.__init__(self, *args, **kwargs)
        self.label = self.LoneLabel(self.id, self.label.text)
        self.status = kwargs.get("status", {})

    widget = StatusWidget()
    option_widget = CheckboxInput()

class ValidateTranslationForm(Form):
    submit = SubmitField(_('admin_translation_confirm'), default="confirm")
    cancel = SubmitField(_('admin_translation_cancel'), default="cancel")
    field_keys = HiddenField()

class ReinitializeTranslationForm(Form):
    submit = SubmitField(_('admin_translation_reinitialize'), default="confirm")

class BlockFileSearchForm(Form):
    identifier = TextAreaField(_("admin_search_field"))
    mode = SelectField(_("admin_locks_search_mode"), choices = (
        ("hexid", _("admin_lock_mode_hexid")),
        ("url", _("admin_lock_mode_url")),
        ("b64id", _("admin_lock_mode_b64"))
        ), default="url")
    submit = SubmitField(_("admin_locks_search"), default="confirm")

class SearchUserForm(Form):
    identifier = TextField(_("admin_search_field"))
    mode = SelectField(_("admin_search_mode"), choices = (
        ("username", _("admin_users_mode_username")),
        ("email", _("admin_users_mode_email")),
        ("hexid", _("admin_users_mode_id")),
        ("oauth", _("admin_users_mode_oauth"))), default="username")
    submit = SubmitField(_("admin_users_search"), default="submit")

class EditUserForm(Form):
    userid = HiddenField()
    props = HiddenField()
    created = HiddenField()
    submit =  SubmitField(_('admin_users_save'), default="submit")

class OriginForm(Form):
    props = HiddenField()
    submit = SubmitField(_('admin_origins_save'), default="submit")

class DeployForm(Form):
    mode = SelectField(_("admin_deploy_mode"), default="production")
    deploy = SubmitField(_("admin_deploy_deploy"), default="deploy")
    deploy_rollback = SubmitField(_("admin_deploy_deploy_rollback"), default="deploy-rollback")
    clean_local = SubmitField(_("admin_deploy_clean_local"), default="clean_local")
    clean_remote = SubmitField(_("admin_deploy_clean_remote"), default="clean_remote")
    restart = SubmitField(_("admin_deploy_restart"), default="restart")
    package = SubmitField(_("admin_deploy_package"), default="package")

    script_available_hosts = HiddenField()
    script_mode = SelectField(_("admin_deploy_script"))
    script_hosts = MultiHostField(_("admin_deploy_script_hosts"))
    script_clean_cache = SubmitField(_("admin_deploy_script_clean_cache"), default="script_cleann_cache")
    script = SubmitField(_("admin_deploy_run_script"), default="script")

    publish = SubmitField(_("admin_deploy_publish"), default="publish")
    publish_mode = SelectField(_("admin_deploy_publish_mode"))
    publish_version = TextField(_("admin_deploy_publish_version"))
    publish_message = TextAreaField(_("admin_deploy_publish_message"))
    rollback = SubmitField(_("admin_deploy_rollback"), default="rollback")
    prepare = SubmitField(_("admin_deploy_prepare"), default="prepare")
    commit = SubmitField(_("admin_deploy_commit"), default="commit")

    clean_log = SubmitField(_("admin_deploy_clean_log"), default="clean_log")
    remove_lock = SubmitField(_("admin_deploy_remove_lock"), default="remove_lock")

class EditForm(Form):
    defaults = HiddenField()
    editable = HiddenField()
    confirmed = HiddenField(default="false", filters=(lambda x: x.strip().lower() == "true" ,))
    submit = SubmitField(_('admin_edit_submit'), default="submit")

class RemoveForm(Form):
    confirmed = BooleanField(_("admin_confirm"))
    submit = SubmitField(_('admin_edit_submit'), default="submit")

class GetServerForm(Form):
    filename = TextField(_("admin_filename"))
    submit = SubmitField(_("admin_locks_search"), default="submit")

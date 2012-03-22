# -*- coding: utf-8 -*-
"""
    Formularios para admin
"""

from wtforms import *
from flaskext.babel import lazy_gettext as _
from flaskext.babel import gettext

class ValidateTranslationForm(Form):
    submit = SubmitField(_('admin_translation_confirm'), default="confirm")
    cancel = SubmitField(_('admin_translation_cancel'), default="cancel")
    field_keys = HiddenField()

class BlockFileSearchForm(Form):
    identifier = TextAreaField(_("admin_search_field"))
    mode = SelectField(_("admin_locks_search_mode"), choices = (
        ("hexid", _("admin_lock_mode_hexid")),
        ("url", _("admin_lock_mode_url")),
        ("b64id", _("admin_lock_mode_b64"))), default="url")
    submit = SubmitField(_("admin_locks_search"), default="confirm")

class SearchUserForm(Form):
    identifier = TextField(_("admin_search_field"))
    mode = SelectField(_("admin_users_search_mode"), choices = (
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
    submit =  SubmitField(_('admin_origins_save'), default="submit")

class DeployForm(Form):
    mode = SelectField(_("admin_deploy_mode"), default="production")
    deploy = SubmitField(_("admin_deploy_deploy"), default="deploy")
    clean_local = SubmitField(_("admin_deploy_clean_local"), default="clean_local")
    clean_remote = SubmitField(_("admin_deploy_clean_remote"), default="clean_remote")
    restart = SubmitField(_("admin_deploy_restart"), default="restart")
    package = SubmitField(_("admin_deploy_package"), default="package")
    publish = SubmitField(_("admin_deploy_publish"), default="publish")
    publish_mode = SelectField(_("admin_deploy_publish_mode"))
    publish_version = TextField(_("admin_deploy_publish_version"))
    publish_message = TextAreaField(_("admin_deploy_publish_message"))
    rollback = SubmitField(_("admin_deploy_rollback"), default="rollback")
    prepare = SubmitField(_("admin_deploy_prepare"), default="prepare")
    commit = SubmitField(_("admin_deploy_commit"), default="commit")




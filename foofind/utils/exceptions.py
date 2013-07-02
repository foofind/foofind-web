# -*- coding: utf-8 -*-

from flask import get_flashed_messages
from flask.ext.babelex import gettext as _
from werkzeug.exceptions import HTTPException, abort, HTTP_STATUS_CODES
class TooManyRequests(HTTPException):
    """*429* `Too Many Requests `

    Status code you should return if the user has sent too many requests in a given amount of time.
    """
    code = 429
    description = (
        '<p>You have sent too many requests in the last minutes, please try again in a while.</p>'
    )
abort.mapping[429] = TooManyRequests
HTTP_STATUS_CODES[429] = 'Too Many Requests'

def allerrors(app, *list_of_codes):
    def inner(f):
        return reduce(lambda f, code: app.errorhandler(code)(f), list_of_codes, f)
    return inner

def get_error_code_information(e):

    error_code = e.code if hasattr(e,"code") else 500

    error_title = _("error_%d_message" % error_code)
    if error_title == "error_%d_message" % error_code:
        if error_code!=500 and error_code in HTTP_STATUS_CODES:
            error_title = _(HTTP_STATUS_CODES[error_code])
        else:
            error_title = _("error_500_message")

    messages = get_flashed_messages()
    if messages:
        error_description = " ".join(_(msg) for msg in messages)
    else:
        error_description = _("error_%d_description" % error_code)
        if error_description == "error_%d_description" % error_code:
            if hasattr(e,"description"):
                error_description = _(e.description[3:-4])
            else:
                error_description = _("error_500_message")

    return error_code, error_title, error_description

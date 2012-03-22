
from flaskext.babel import Babel
from flaskext.cache import Cache
from flaskext.login import LoginManager
from flaskext.mail import Mail
from raven.contrib.flask import Sentry

__all__ = ['babel', 'cache', 'auth', 'mail', 'sentry']

babel = Babel()
cache = Cache()
auth = LoginManager()
mail = Mail()
sentry = Sentry()

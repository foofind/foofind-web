
from .downloads import downloads as _downloads
from .logger import logger as _logger
from .web import web as _web

all_blueprints = (_downloads, _logger, _web)

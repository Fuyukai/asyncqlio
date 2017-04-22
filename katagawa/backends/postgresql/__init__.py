# used for namespace packages
from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)

DEFAULT_CONNECTOR = "asyncpg"

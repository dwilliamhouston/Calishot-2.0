# Make calishot_web a package and expose app for imports
from .app import app  # Flask app instance

__all__ = ["app"]

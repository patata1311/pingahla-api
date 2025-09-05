# app/core/__init__.py
from .config import get_settings, Settings  # re-export
__all__ = ["get_settings", "Settings"]

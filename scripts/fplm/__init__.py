"""Shared ingestion library for FPL-Mentat.

Importable from any script in `scripts/` because Python puts the running
script's directory on sys.path, so no packaging config is needed.
"""

from .api import FPLClient
from .warehouse import Warehouse

__all__ = ["FPLClient", "Warehouse"]

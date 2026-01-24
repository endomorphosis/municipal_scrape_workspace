"""Common Crawl (CC) index pipeline utilities.

This subpackage contains the code used to build and query the Common Crawl
CDX-derived pointer indexes (Parquet + DuckDB) and their meta-indexes.
"""

from . import api
from . import orchestrator_manager

__all__ = ["api", "orchestrator_manager"]


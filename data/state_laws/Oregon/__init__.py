"""Utilities for organizing and downloading Oregon Revised Statutes (ORS).

This package stores downloaded Oregon law artifacts under:

- ``raw_html/``: downloaded ORS chapter HTML files
- ``manifests/``: machine-readable run metadata and chapter manifest
- ``parsed/``: extracted chapter summaries (JSONL)
"""

from .oregon_ors_downloader import OregonORSArchivalDownloader, run

__all__ = ["OregonORSArchivalDownloader", "run"]

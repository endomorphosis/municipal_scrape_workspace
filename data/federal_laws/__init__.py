"""Federal laws data package.

This module exposes paths to JSON-LD artifacts under ``data/federal_laws``.
"""

from pathlib import Path


MODULE_DIR = Path(__file__).resolve().parent
US_CONSTITUTION_JSONLD = MODULE_DIR / "us_constitution.jsonld"


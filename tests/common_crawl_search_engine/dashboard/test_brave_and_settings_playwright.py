"""Deprecated Playwright suite.

This module previously contained Playwright E2E tests for Brave + Settings panels.
It is intentionally reduced to a harmless placeholder so it:

- does not require Playwright at import time
- does not break pytest collection
- remains opt-in via the `integration` marker
"""

import pytest


pytestmark = pytest.mark.integration

def test_deprecated_placeholder() -> None:
    assert True

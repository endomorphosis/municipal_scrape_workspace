"""Test that root wrappers work correctly."""

import subprocess
import sys
from pathlib import Path


def test_wrapper_imports():
    """Test that a root wrapper can import from src."""
    # We test this by importing the wrapper module directly
    import sys
    from pathlib import Path
    
    root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(root))
    
    # Import wrapper - should work without error
    try:
        import search_cc_domain
        assert hasattr(search_cc_domain, '__file__')
    finally:
        sys.path.remove(str(root))


def test_wrapper_search_domain_help():
    """Test that root wrapper search_cc_domain.py displays help."""
    root = Path(__file__).parent.parent.parent
    script = root / "search_cc_domain.py"
    
    if not script.exists():
        pytest.skip(f"Wrapper script not found: {script}")
    
    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        capture_output=True,
        text=True,
        timeout=10
    )
    assert result.returncode == 0
    assert "domain" in result.stdout.lower()

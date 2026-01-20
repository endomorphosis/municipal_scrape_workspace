# Post-Migration Gaps & Action Items

**Date**: 2026-01-19  
**Status**: Documentation of remaining work after successful migration  
**Priority**: HIGH for dependency issue, MEDIUM for testing, LOW for enhancements

---

## 🎯 Overview

The repository refactoring is **✅ COMPLETE** from a structural perspective. All 52 Python files have been properly organized:
- 41 migrated to `src/` with wrappers
- 11 archived in `archive/ccindex/superseded/`

However, there are **4 key gaps** that need to be addressed for full functionality:

1. ⚠️ **HIGH PRIORITY**: ipfs_datasets_py dependency portability
2. ⚠️ **MEDIUM PRIORITY**: Testing infrastructure
3. ⏳ **LOW PRIORITY**: Console script entry points
4. ⏳ **LOW PRIORITY**: Enhanced documentation

---

## ⚠️ Gap 1: ipfs_datasets_py Dependency (HIGH PRIORITY)

### Current Problem

The `ipfs_datasets_py` dependency is **commented out** in `pyproject.toml`:

```toml
[project]
dependencies = [
    # Temporarily commented out - see REFACTORING_ROADMAP.md § Dependency Gaps
    # "ipfs_datasets_py @ file:///home/barberb/ipfs_datasets_py",
]
```

**Why it's commented out:**
- Hardcoded local path: `/home/barberb/ipfs_datasets_py`
- Not portable across development environments
- Breaks installation on other machines
- Prevents clean package distribution

### Impact

**What doesn't work:**
- `ipfs-datasets` CLI command not available
- Municipal scrape functionality that depends on `ipfs_datasets_py`
- Full package installation on fresh systems

**What still works:**
- All Common Crawl (CC) index tools (don't depend on ipfs_datasets_py)
- Basic package installation (but without municipal scrape features)
- Development with manual PYTHONPATH setup (workaround)

### Solution Options

#### Option A: Git URL Dependency (Recommended - Quick Fix)

**Pros:**
- Works immediately
- No code changes needed
- Portable across environments
- Standard Python practice

**Cons:**
- Still depends on GitHub availability
- Version pinning more difficult
- Slower installs (clones repo)

**Implementation:**
```toml
[project]
dependencies = [
    "ipfs_datasets_py @ git+https://github.com/endomorphosis/ipfs_datasets_py.git@main",
]
```

**Steps:**
1. Edit `pyproject.toml`
2. Replace commented line with git URL
3. Test: `pip install -e .`
4. Verify: `ipfs-datasets --help`

#### Option B: Make Optional with Environment Override (Flexible)

**Pros:**
- Flexible for development
- Allows local testing
- Falls back to git URL for prod
- Lightweight core package

**Cons:**
- More complex setup
- Requires documentation
- Users must choose install method

**Implementation:**
```toml
[project]
dependencies = [
    # Core dependencies only
]

[project.optional-dependencies]
ipfs = [
    "ipfs_datasets_py @ git+https://github.com/endomorphosis/ipfs_datasets_py.git@main",
]
```

In code that uses it:
```python
import os
import sys

# Support local dev checkout via environment variable
ipfs_root = os.environ.get("IPFS_DATASETS_PY_ROOT")
if ipfs_root:
    sys.path.insert(0, ipfs_root)

try:
    from ipfs_datasets.unified_scraper import UnifiedScraper
except ImportError:
    raise ImportError(
        "ipfs_datasets_py not found. Install with: pip install -e '.[ipfs]' "
        "or set IPFS_DATASETS_PY_ROOT environment variable"
    )
```

**Usage:**
```bash
# For production
pip install -e '.[ipfs]'

# For local development
export IPFS_DATASETS_PY_ROOT="/path/to/local/ipfs_datasets_py"
pip install -e .
```

#### Option C: Publish to PyPI (Best Long-term)

**Pros:**
- Most professional
- Standard Python ecosystem
- Fast installs
- Version management
- Discoverable

**Cons:**
- Requires PyPI account
- Package name must be available
- Ongoing maintenance
- Release process overhead

**Implementation:**
1. Prepare ipfs_datasets_py for PyPI:
   - Add proper `pyproject.toml`
   - Choose package name (e.g., `ipfs-datasets-py`)
   - Create distributions
   - Upload to PyPI

2. Update this package:
   ```toml
   [project]
   dependencies = [
       "ipfs-datasets-py>=0.1.0",
   ]
   ```

3. Standard installation:
   ```bash
   pip install municipal-scrape-workspace
   ```

### Recommendation

**Short-term (Immediate):** Use **Option A** (Git URL)
- Quick fix
- Unblocks development
- Tested and reliable

**Long-term (Future):** Move to **Option C** (PyPI)
- Better for users
- Professional ecosystem
- Easier maintenance

### Action Items

- [ ] Choose solution (recommend Option A for now)
- [ ] Update `pyproject.toml`
- [ ] Test installation on clean environment
- [ ] Verify `ipfs-datasets` CLI works
- [ ] Test municipal scrape functionality
- [ ] Update documentation
- [ ] Consider long-term PyPI publication

---

## ⚠️ Gap 2: Testing Infrastructure (MEDIUM PRIORITY)

### Current Problem

**No test suite exists** for this repository.

**What's missing:**
- No `tests/` directory
- No test framework configured
- No test dependencies
- No CI/CD pipeline
- No automated validation

### Impact

**Risks:**
- Changes cannot be automatically validated
- Regressions go unnoticed
- Manual testing required
- Difficult to refactor safely
- Lower code quality confidence

**Current workaround:**
- Manual testing of tools
- Running scripts with `--help`
- Ad-hoc verification

### Solution

#### Phase 1: Basic Test Setup

**Add test dependencies:**
```toml
[project.optional-dependencies]
dev = [
    "pytest>=7.0",
    "pytest-cov>=4.0",
    "pytest-asyncio>=0.21",
]
```

**Create test structure:**
```
tests/
├── __init__.py
├── conftest.py
├── test_ccindex/
│   ├── __init__.py
│   ├── test_search_domain.py
│   ├── test_build_index.py
│   ├── test_validation.py
│   └── test_import_patterns.py
└── test_municipal_scrape/
    ├── __init__.py
    └── test_orchestrator.py
```

**Basic test example:**
```python
# tests/test_ccindex/test_import_patterns.py
"""Test that all tools can be imported correctly."""

def test_search_domain_import():
    """Test search_cc_domain imports correctly."""
    from municipal_scrape_workspace.ccindex.search_cc_domain import main
    assert callable(main)

def test_build_index_import():
    """Test build_cc_pointer_duckdb imports correctly."""
    from municipal_scrape_workspace.ccindex.build_cc_pointer_duckdb import main
    assert callable(main)

def test_wrapper_imports():
    """Test root wrappers can import from src."""
    import sys
    from pathlib import Path
    
    # Add root to path temporarily
    root = Path(__file__).parent.parent
    sys.path.insert(0, str(root))
    
    # Import wrapper - should work
    import search_cc_domain
    assert hasattr(search_cc_domain, 'main')
```

**Run tests:**
```bash
pip install -e '.[dev]'
pytest
```

#### Phase 2: Integration Tests

**Add integration tests:**
```python
# tests/test_ccindex/test_integration.py
import tempfile
from pathlib import Path

def test_search_help_command():
    """Test that search tools display help."""
    import subprocess
    result = subprocess.run(
        ["python", "-m", "municipal_scrape_workspace.ccindex.search_cc_domain", "--help"],
        capture_output=True,
        text=True
    )
    assert result.returncode == 0
    assert "domain" in result.stdout.lower()
```

#### Phase 3: CI/CD Setup

**Create `.github/workflows/test.yml`:**
```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.12"]
    
    steps:
    - uses: actions/checkout@v3
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: ${{ matrix.python-version }}
    
    - name: Install dependencies
      run: |
        pip install -e '.[dev,ccindex]'
    
    - name: Run tests
      run: |
        pytest --cov=municipal_scrape_workspace tests/
```

### Action Items

- [ ] Add `[dev]` optional dependencies to pyproject.toml
- [ ] Create `tests/` directory structure
- [ ] Write basic import tests
- [ ] Write CLI help tests
- [ ] Add pytest configuration
- [ ] Document test running in README
- [ ] (Future) Add CI/CD workflow

---

## ⏳ Gap 3: Console Script Entry Points (LOW PRIORITY)

### Current Problem

Only **one console script** is defined: `municipal-scrape`

Users must use:
- Root wrappers: `./search_cc_domain.py --domain example.com`
- Module commands: `python -m municipal_scrape_workspace.ccindex.search_cc_domain --domain example.com`

### Impact

**User experience:**
- Commands are verbose
- Less discoverable
- Not as user-friendly
- Requires being in repo root (wrappers) or remembering module paths

### Solution

Add common tools as console scripts in `pyproject.toml`:

```toml
[project.scripts]
# Municipal scrape
municipal-scrape = "municipal_scrape_workspace.cli:main"

# Common Crawl search tools
ccindex-search = "municipal_scrape_workspace.ccindex.search_cc_via_meta_indexes:main"
ccindex-search-domain = "municipal_scrape_workspace.ccindex.search_cc_domain:main"
ccindex-search-parallel = "municipal_scrape_workspace.ccindex.search_parallel_duckdb_indexes:main"

# Index building tools
ccindex-build-pointer = "municipal_scrape_workspace.ccindex.build_cc_pointer_duckdb:main"
ccindex-build-parallel = "municipal_scrape_workspace.ccindex.build_parallel_duckdb_indexes:main"
ccindex-build-meta = "municipal_scrape_workspace.ccindex.build_year_meta_indexes:main"

# Orchestration tools
ccindex-orchestrate = "municipal_scrape_workspace.ccindex.cc_pipeline_orchestrator:main"
ccindex-watch = "municipal_scrape_workspace.ccindex.cc_pipeline_watch:main"
ccindex-hud = "municipal_scrape_workspace.ccindex.cc_pipeline_hud:main"

# Validation tools
ccindex-validate = "municipal_scrape_workspace.ccindex.validate_collection_completeness:main"
ccindex-validate-parquet = "municipal_scrape_workspace.ccindex.validate_and_sort_parquet:main"
```

**After installation:**
```bash
pip install -e '.[ccindex]'

# Now can use short commands:
ccindex-search --domain example.com
ccindex-build-pointer --help
ccindex-orchestrate --config pipeline_config.json
```

### Action Items

- [ ] Decide which tools warrant console scripts
- [ ] Add entries to `[project.scripts]` in pyproject.toml
- [ ] Test console scripts work after install
- [ ] Update documentation with new commands
- [ ] Add examples to FINAL_LAYOUT_README.md

---

## ⏳ Gap 4: Enhanced Documentation (LOW PRIORITY)

### Current Problem

While **migration documentation is excellent**, operational documentation could be enhanced:

**What's good:**
- ✅ Migration guide (FINAL_LAYOUT_README.md)
- ✅ File location map (FILE_MIGRATION_MAP.md)
- ✅ Import patterns documented
- ✅ Running tools explained

**What could be better:**
- API reference documentation
- Usage examples for each tool
- Common workflows documentation
- Troubleshooting guide
- Performance tuning guide
- Contributing guidelines

### Solution

#### Quick Wins

1. **Add docstrings to main() functions:**
   ```python
   def main(argv=None) -> int:
       """Search Common Crawl indexes for a domain.
       
       Args:
           argv: Command-line arguments (default: sys.argv)
       
       Returns:
           Exit code (0 for success)
       
       Examples:
           >>> from municipal_scrape_workspace.ccindex.search_cc_domain import main
           >>> main(["--domain", "example.com"])
           0
       """
   ```

2. **Create TROUBLESHOOTING.md:**
   - Common errors and solutions
   - Installation issues
   - Import problems
   - Performance issues

3. **Create WORKFLOWS.md:**
   - Building a new index from scratch
   - Updating existing indexes
   - Searching across multiple indexes
   - Municipal scraping workflow

#### Long-term

4. **Sphinx/MkDocs Documentation:**
   - API reference generated from docstrings
   - Hosted documentation
   - Search functionality
   - Version management

5. **Performance Guide:**
   - Benchmarking tools
   - Optimization tips
   - Resource requirements
   - Scaling strategies

### Action Items

- [ ] Add comprehensive docstrings to key functions
- [ ] Create TROUBLESHOOTING.md
- [ ] Create WORKFLOWS.md with common use cases
- [ ] Add CONTRIBUTING.md with development guidelines
- [ ] (Future) Setup Sphinx/MkDocs for hosted docs

---

## 📊 Priority Summary

| Gap | Priority | Effort | Impact | Status |
|-----|----------|--------|--------|--------|
| ipfs_datasets_py dependency | ⚠️ HIGH | 1 hour | Blocks functionality | Not started |
| Testing infrastructure | ⚠️ MEDIUM | 4-8 hours | Quality/safety | Not started |
| Console script entry points | ⏳ LOW | 1 hour | UX improvement | Not started |
| Enhanced documentation | ⏳ LOW | 4-8 hours | Usability | Not started |

---

## 🎯 Recommended Action Plan

### Week 1: Critical Path

1. **Day 1**: Fix ipfs_datasets_py dependency (HIGH)
   - Choose git URL approach
   - Update pyproject.toml
   - Test installation
   - Verify functionality

2. **Day 2-3**: Basic testing infrastructure (MEDIUM)
   - Add dev dependencies
   - Create test structure
   - Write import tests
   - Write help command tests

### Week 2: Enhancements

3. **Day 1**: Console scripts (LOW)
   - Add key entry points
   - Test installation
   - Update docs

4. **Day 2-3**: Documentation improvements (LOW)
   - Add docstrings
   - Create TROUBLESHOOTING.md
   - Create WORKFLOWS.md

### Future: Continuous Improvement

5. **Ongoing**: Expand test coverage
6. **Ongoing**: Improve documentation
7. **Future**: CI/CD pipeline
8. **Future**: Performance optimization

---

## ✅ Success Criteria

Gaps are considered resolved when:

- [x] Repository structure refactored (✅ DONE)
- [x] All files migrated to src/ (✅ DONE)
- [x] Migration documentation complete (✅ DONE)
- [ ] ipfs_datasets_py dependency working
- [ ] Basic test suite implemented
- [ ] Console scripts configured
- [ ] Documentation enhanced

---

## 📞 Questions?

- See [FINAL_LAYOUT_README.md](FINAL_LAYOUT_README.md) for structure guide
- See [MIGRATION_COMPLETE.md](MIGRATION_COMPLETE.md) for migration summary
- See [REFACTORING_INDEX.md](REFACTORING_INDEX.md) for all refactoring docs

---

**Last Updated**: 2026-01-19  
**Status**: Gaps documented and prioritized  
**Next Action**: Fix ipfs_datasets_py dependency (HIGH priority)

import asyncio
import importlib.util
import sys
import types
from pathlib import Path

REPO_ROOT = Path("/home/barberb/municipal_scrape_workspace")
PKG_ROOT = REPO_ROOT / "ipfs_datasets_py"
if str(PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(PKG_ROOT))

LEGAL_SCRAPERS_ROOT = (
    PKG_ROOT / "ipfs_datasets_py" / "processors" / "legal_scrapers"
)


def _ensure_pkg(name: str, path: Path) -> None:
    if name in sys.modules:
        return
    module = types.ModuleType(name)
    module.__path__ = [str(path)]
    sys.modules[name] = module


def _load_module(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_ensure_pkg("ipfs_datasets_py.processors", PKG_ROOT / "ipfs_datasets_py" / "processors")
_ensure_pkg("ipfs_datasets_py.processors.legal_scrapers", LEGAL_SCRAPERS_ROOT)

_cache_mod = _load_module(
    "ipfs_datasets_py.processors.legal_scrapers.url_archive_cache",
    LEGAL_SCRAPERS_ROOT / "url_archive_cache.py",
)
_orchestrator_mod = _load_module(
    "ipfs_datasets_py.processors.legal_scrapers.enhanced_state_admin_orchestrator",
    LEGAL_SCRAPERS_ROOT / "enhanced_state_admin_orchestrator.py",
)
_archiver_mod = _load_module(
    "ipfs_datasets_py.processors.legal_scrapers.parallel_web_archiver",
    LEGAL_SCRAPERS_ROOT / "parallel_web_archiver.py",
)

ParallelStateAdminOrchestrator = _orchestrator_mod.ParallelStateAdminOrchestrator
ParallelStateDiscoveryConfig = _orchestrator_mod.ParallelStateDiscoveryConfig
ParallelWebArchiver = _archiver_mod.ParallelWebArchiver
URLArchiveCache = _cache_mod.URLArchiveCache


def test_url_archive_cache_round_trip(tmp_path):
    cache = URLArchiveCache(metadata_dir=str(tmp_path), persist_to_ipfs=False)

    asyncio.run(
        cache.put(
            url="https://example.com/rules#section-1",
            content="Example administrative rule text.",
            source="unit-test",
            metadata={"state_code": "ZZ"},
        )
    )

    entry = cache.get("https://example.com/rules")
    assert entry is not None
    assert entry.content == "Example administrative rule text."
    assert entry.source == "unit-test"
    assert entry.metadata["state_code"] == "ZZ"


def test_parallel_archiver_accepts_max_concurrent_override():
    archiver = ParallelWebArchiver(max_concurrent=3)
    coro = archiver.archive_urls_parallel(["https://example.com"], max_concurrent=1)
    assert asyncio.iscoroutine(coro)
    coro.close()


def test_orchestrator_parallel_fetch_reuses_cache(tmp_path):
    class FakeArchiver:
        def __init__(self):
            self.calls = 0

        async def archive_urls_parallel(self, urls, progress_callback=None, max_concurrent=None):
            self.calls += 1
            return [
                type(
                    "Result",
                    (),
                    {
                        "url": urls[0],
                        "success": True,
                        "content": "Administrative rule body",
                        "source": "fake-archiver",
                    },
                )()
            ]

    config = ParallelStateDiscoveryConfig(
        cache_dir=str(tmp_path / "cache"),
        cache_to_ipfs=False,
    )
    fake_archiver = FakeArchiver()
    orchestrator = ParallelStateAdminOrchestrator(config=config, parallel_archiver=fake_archiver)

    async def _run():
        first = await orchestrator._parallel_fetch_urls(
            urls=["https://example.com/admin"],
            state_code="ZZ",
            deadline=10**9,
            phase="test",
        )
        second = await orchestrator._parallel_fetch_urls(
            urls=["https://example.com/admin"],
            state_code="ZZ",
            deadline=10**9,
            phase="test",
        )
        return first, second

    first, second = asyncio.run(_run())

    assert fake_archiver.calls == 1
    assert first[0][1] == "Administrative rule body"
    assert second[0][1] == "Administrative rule body"
    assert second[0][2] == "fake-archiver:cache"

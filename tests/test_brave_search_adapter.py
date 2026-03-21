from ipfs_datasets_py.processors.web_archiving.search_engines.base import (
    SearchEngineConfig,
    SearchEngineType,
)
from ipfs_datasets_py.processors.web_archiving.search_engines.brave_adapter import (
    BraveSearchEngine,
)


def _build_engine() -> BraveSearchEngine:
    return BraveSearchEngine(SearchEngineConfig(engine_type="brave", cache_enabled=False))


def test_brave_adapter_normalizes_dict_payload() -> None:
    engine = _build_engine()

    results = engine._normalize_results(
        {
            "web": {
                "results": [
                    {
                        "title": "Example",
                        "url": "https://example.com/rules",
                        "description": "Example snippet",
                    }
                ]
            }
        }
    )

    assert len(results) == 1
    assert results[0].engine == SearchEngineType.BRAVE
    assert results[0].url == "https://example.com/rules"
    assert results[0].domain == "example.com"


def test_brave_adapter_normalizes_list_payload() -> None:
    engine = _build_engine()

    results = engine._normalize_results(
        [
            {
                "title": "List Result",
                "url": "https://agency.example.gov/admin-code",
                "description": "Administrative code page",
            }
        ]
    )

    assert len(results) == 1
    assert results[0].engine == SearchEngineType.BRAVE
    assert results[0].url == "https://agency.example.gov/admin-code"
    assert results[0].domain == "agency.example.gov"

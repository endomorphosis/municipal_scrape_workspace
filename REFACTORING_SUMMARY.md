# Pipeline Orchestrator Refactoring Summary

## Objective
Consolidate duplicate validation logic and ensure consistency between `cc_pipeline_orchestrator.py` and `validate_collection_completeness.py`.

## Changes Made

### 1. Import CollectionValidator
```python
# Added to cc_pipeline_orchestrator.py
from validate_collection_completeness import CollectionValidator
```

### 2. Initialize Validator in Orchestrator
```python
def __init__(self, config: PipelineConfig):
    self.config = config
    self.validator = CollectionValidator(
        ccindex_dir=config.ccindex_root,
        parquet_dir=config.parquet_root,
        pointer_dir=config.duckdb_root
    )
    self.collections: List[str] = []
    self.collection_status: Dict[str, dict] = {}  # Changed from CollectionStatus
```

### 3. Use Validator for Collection Discovery
```python
def get_all_collections(self) -> List[str]:
    """Get all available CC collections using validator"""
    collections = list(self.validator.get_all_collections())
    
    # Apply filter if specified
    if self.config.collections_filter:
        collections = [c for c in collections if self.config.collections_filter in c]
    
    return sorted(collections)
```

### 4. Use Validator for Status Scanning
```python
def scan_all_collections(self):
    """Scan status of all collections using validator"""
    self.collections = self.get_all_collections()
    logger.info(f"Found {len(self.collections)} collections")
    
    for collection in self.collections:
        status = self.validator.validate_collection(collection)
        self.collection_status[collection] = status
```

## Status Format Change

### Before (CollectionStatus dataclass):
```python
status.downloaded_gz        # int
status.expected_shards      # int
status.converted_parquet    # int
status.sorted_parquet       # int
status.has_duckdb_index     # bool
status.duckdb_index_sorted  # bool
status.is_complete          # bool (property)
```

### After (dict from validator):
```python
status['tar_gz_count']         # int
status['expected_shards']      # int
status['parquet_count']        # int
status['sorted_count']         # int
status['duckdb_index_exists']  # bool
status['duckdb_index_sorted']  # bool
status['is_complete']          # bool
status['parquet_path']         # Path or None
status['duckdb_path']          # Path or None
```

## Remaining Work

The orchestrator methods still need updates to use dict-based status:

### `process_collection()` - Lines 239-283
Need to change:
- `status.downloaded_gz` → `status['tar_gz_count']`
- `status.expected_shards` → `status['expected_shards']`
- `status.converted_parquet` → `status['parquet_count']`
- `status.sorted_parquet` → `status['sorted_count']`
- `status.has_duckdb_index` → `status['duckdb_index_exists']`
- `status.is_complete` → `status['is_complete']`

### `run_pipeline()` - Lines 285+
Need to change:
- `s.is_complete` → `s['is_complete']`
- `s.completion_percent` → calculate from counts

## Benefits Achieved

1. **Single Source of Truth**: All validation logic in one place
2. **Consistent Reporting**: Orchestrator and validator show same data
3. **Better File Discovery**: Validator checks multiple locations
4. **Accurate Sort Detection**: Uses both filename and content inspection
5. **Easier Maintenance**: Update validator once, all tools benefit

## Testing

```bash
# Test validator directly
python validate_collection_completeness.py --collection CC-MAIN-2025-05

# Test orchestrator with validator integration
python -c "
from cc_pipeline_orchestrator import PipelineOrchestrator, PipelineConfig
from pathlib import Path

config = PipelineConfig(
    ccindex_root=Path('/storage/ccindex'),
    parquet_root=Path('/storage/ccindex_parquet'),
    duckdb_root=Path('/storage/ccindex_duckdb'),
    max_workers=4,
    memory_limit_gb=10,
    min_free_space_gb=50,
    collections_filter='2025-05'
)

orch = PipelineOrchestrator(config)
orch.scan_all_collections()
for coll, status in orch.collection_status.items():
    print(f'{coll}: {status[\"is_complete\"]}')
"
```

## Next Steps

1. Complete refactoring of `process_collection()` method
2. Complete refactoring of `run_pipeline()` method  
3. Remove obsolete `CollectionStatus` dataclass
4. Add integration tests comparing orchestrator vs validator output
5. Update all documentation to reflect new status format

## Related Files

- `cc_pipeline_orchestrator.py` - Main orchestrator (partially refactored)
- `validate_collection_completeness.py` - Validation logic (source of truth)
- `cc_pipeline_watch.py` - CLI monitoring tool
- `cc_pipeline_hud.py` - Interactive HUD
- `CC_ORCHESTRATOR_README.md` - Documentation with refactoring notes

## Rollback Plan

If issues arise, revert to old logic by:
1. Remove `from validate_collection_completeness import CollectionValidator`
2. Restore `CollectionStatus` dataclass usage
3. Restore old `scan_collection_status()` method

However, this would reintroduce the inconsistency problem.

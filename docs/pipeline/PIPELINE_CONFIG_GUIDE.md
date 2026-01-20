# Pipeline Configuration Guide

## Configuration System

The Common Crawl Pipeline Orchestrator uses a JSON configuration file (`pipeline_config.json`) for easy path and settings management.

## Default Configuration

Created `pipeline_config.json` with these defaults:

```json
{
  "ccindex_root": "/storage/ccindex",
  "parquet_root": "/storage/ccindex_parquet",
  "duckdb_root": "/storage/ccindex_duckdb/cc_pointers_by_collection",
  "max_workers": 8,
  "memory_limit_gb": 10.0,
  "min_free_space_gb": 50.0,
  "collections_filter": null
}
```

## Usage Examples

### Basic Usage (uses config file)
```bash
python cc_pipeline_orchestrator.py --filter 2024
```

Output shows loaded configuration:
```
2026-01-12 03:18:12 [INFO] Loading configuration from pipeline_config.json
2026-01-12 03:18:12 [INFO] 
2026-01-12 03:18:12 [INFO] Active Configuration:
2026-01-12 03:18:12 [INFO]   ccindex_root:  /storage/ccindex
2026-01-12 03:18:12 [INFO]   parquet_root:  /storage/ccindex_parquet
2026-01-12 03:18:12 [INFO]   duckdb_root:   /storage/ccindex_duckdb/cc_pointers_by_collection
2026-01-12 03:18:12 [INFO]   max_workers:   8
2026-01-12 03:18:12 [INFO]   memory_limit:  10.0 GB
2026-01-12 03:18:12 [INFO]   min_free:      50.0 GB
```

### Override Worker Count
```bash
python cc_pipeline_orchestrator.py --filter 2024 --workers 4
```

Output confirms override:
```
2026-01-12 03:18:12 [INFO] Loading configuration from pipeline_config.json
2026-01-12 03:18:12 [INFO] Overriding workers: 4
2026-01-12 03:18:12 [INFO]   max_workers:   4
```

### Override Paths
```bash
python cc_pipeline_orchestrator.py \
  --parquet-root /mnt/nvme/parquet \
  --duckdb-root /mnt/ssd/indexes \
  --filter 2025
```

### Custom Config File
```bash
python cc_pipeline_orchestrator.py --config production_config.json --filter 2024
```

### Verbose Logging
```bash
python cc_pipeline_orchestrator.py --filter 2024 --verbose
```

## Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `ccindex_root` | Path | `/storage/ccindex` | Downloaded .tar.gz files |
| `parquet_root` | Path | `/storage/ccindex_parquet` | Converted .gz.parquet files |
| `duckdb_root` | Path | `/storage/ccindex_duckdb/cc_pointers_by_collection` | DuckDB pointer indexes |
| `max_workers` | int | `8` | Parallel worker processes |
| `memory_limit_gb` | float | `10.0` | Min available memory (GB) |
| `min_free_space_gb` | float | `50.0` | Min free disk space (GB) |
| `collections_filter` | string | `null` | Filter pattern (e.g., "2024") |

## Integration with Other Tools

All pipeline tools now respect the same configuration:

- **cc_pipeline_orchestrator.py** - Main orchestrator
- **cc_pipeline_hud.py** - Real-time monitoring
- **cc_pipeline_watch.py** - Status reporting
- **validate_collection_completeness.py** - Validation

## Customization Tips

### For Development
```json
{
  "ccindex_root": "/tmp/ccindex",
  "parquet_root": "/tmp/parquet",
  "duckdb_root": "/tmp/duckdb",
  "max_workers": 2,
  "memory_limit_gb": 5.0,
  "min_free_space_gb": 10.0,
  "collections_filter": "2025-05"
}
```

### For Production (High Memory System)
```json
{
  "ccindex_root": "/storage/ccindex",
  "parquet_root": "/nvme/ccindex_parquet",
  "duckdb_root": "/ssd/ccindex_duckdb/cc_pointers_by_collection",
  "max_workers": 32,
  "memory_limit_gb": 20.0,
  "min_free_space_gb": 100.0,
  "collections_filter": null
}
```

### For Limited Resources
```json
{
  "ccindex_root": "/storage/ccindex",
  "parquet_root": "/storage/ccindex_parquet",
  "duckdb_root": "/storage/ccindex_duckdb/cc_pointers_by_collection",
  "max_workers": 2,
  "memory_limit_gb": 5.0,
  "min_free_space_gb": 20.0,
  "collections_filter": "2024-10"
}
```

## Migration from Old Scripts

The new configuration system replaces hardcoded paths in:
- `download_cc_indexes_1year.sh`
- `download_cc_indexes_2years.sh`
- `download_cc_indexes_5years.sh`

Instead of editing shell scripts, just modify `pipeline_config.json`.

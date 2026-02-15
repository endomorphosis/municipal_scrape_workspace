# American Law (Kyle slop) reindex

This folder contains a small utility to **re-shard / normalize** Kyle Rose's `american_municipal_law` Parquet dump so it is easier to ingest with the Hugging Face `datasets` API.

## Why reindex?

The raw dump is *already Parquet*, but it is:
- **Very many small files** (per-GNIS), which is painful for Hub uploads and slow to load.
- Includes a pandas artifact column `__index_level_0__`.
- Uses a few awkward Arrow types (e.g. citations have several columns typed as `null`).

The reindex step:
- Drops `__index_level_0__`
- Ensures problematic columns are nullable strings
- Adds `gnis` to html/citation rows (derived from filename)
- Optionally adds `place_name`/`state_code` to html rows from the per-GNIS JSON metadata
- Writes **larger shards** to reduce file count

## Run

From repo root:

```bash
python scripts/american_law/reindex_kyle_slop_to_parquet.py \
  --input-root data/kyle_slop/american_municipal_law/american_law \
  --output-root datasets/american_law_parquet \
  --shard-size-mb 512
```

Debug run (process only first 10 per kind):

```bash
python scripts/american_law/reindex_kyle_slop_to_parquet.py --limit-files 10
```

## Output

`datasets/american_law_parquet/` will contain:
- `places.parquet` (one row per GNIS with metadata)
- `html/*.parquet` (normalized shards)
- `citation/*.parquet` (normalized shards)
- `embeddings/*.parquet` (normalized shards)

## Hugging Face ingestion (high-level)

You can ingest the resulting shards with the HF `datasets` library using one config per table type (recommended), or by loading each table separately.

This repo does **not** currently include the HF dataset loading script; the idea is to generate/upload the normalized Parquet first, then build a small dataset repo with a `datasets` loading script that points at the Parquet on the Hub.

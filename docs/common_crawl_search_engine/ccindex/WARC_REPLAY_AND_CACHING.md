# WARC Replay + Caching

The CCIndex pointers include a WARC filename plus an exact byte-range (`warc_offset`, `warc_length`) inside that `*.warc.gz` file.

This repo supports two ways to retrieve the record bytes, and a shared “WARC → HTTP response” extractor so the dashboard/MCP can render the archived page (Wayback-ish) without re-fetching the live site.

## Modes

### 1) Range mode (default)

- **What it does**: HTTP `Range` GETs only the record’s byte-range from Common Crawl.
- **Pros**: Small downloads; fast for single pages.
- **Cons**: If you fetch many records from the *same* WARC file, you’ll make many network requests.
- **Cache**: Range responses are cached on disk (to avoid re-downloading the same range).

### 2) Full-WARC mode (opt-in / last-ditch)

- **What it does**: Downloads the entire `*.warc.gz` file once to disk, then reads record byte-ranges locally.
- **Pros**: Great for bulk/domain scraping when lots of records live in the same WARC.
- **Cons**: Large downloads (multi-GB); requires disk space; slower first fetch.

### 3) Auto mode

- Uses the local full-WARC cache if the file is already present, otherwise falls back to Range.

## Cache locations

- Range cache (record byte-ranges): `state/warc_cache/` (default)
  - Disable with: `CCINDEX_WARC_CACHE_DIR=''`
- Full WARC cache (whole `*.warc.gz` files): `state/warc_files/` (default)
  - Disable with: `CCINDEX_FULL_WARC_CACHE_DIR=''`

Both are under `state/` and are git-ignored.

## CLI

Download/cache a full WARC:

```bash
ccindex warc cache \
  --warc-filename crawl-data/CC-MAIN-2024-10/segments/.../warc/CC-MAIN-202401...warc.gz
```

Fetch a record (Range by default):

```bash
ccindex warc fetch-record \
  --warc-filename ...warc.gz \
  --warc-offset 123 \
  --warc-length 456
```

Fetch a record using full-WARC mode (downloads the WARC if needed):

```bash
ccindex warc fetch-record \
  --cache-mode full \
  --warc-filename ...warc.gz \
  --warc-offset 123 \
  --warc-length 456
```

## MCP tools (dashboard + stdio server)

The `fetch_warc_record` tool accepts:

- `cache_mode`: `"range" | "auto" | "full"`
- `full_warc_cache_dir` (optional)
- `full_warc_max_bytes` (optional safety guard; default 5GB)

It returns extra fields:

- `source`: `"range"` or `"full_cache"`
- `local_warc_path`: local cached WARC path when `source="full_cache"`
- `http`: parsed HTTP response extracted from the WARC record (best-effort)

## Dashboard

Open a record page (`/record?...`). Use the **cache_mode** selector:

- `range`: normal pointer replay
- `auto`: use local full WARC if already cached
- `full`: download full WARC then render locally

The renderer prefers the parsed `http` payload for HTML replay, and falls back to a string-slice heuristic when parsing fails.

## JavaScript SDK usage

The browser SDK is generic; you just pass the new arguments:

```js
const res = await ccindexMcp.callTool('fetch_warc_record', {
  warc_filename,
  warc_offset,
  warc_length,
  cache_mode: 'full',
  full_warc_max_bytes: 5_000_000_000,
});
```

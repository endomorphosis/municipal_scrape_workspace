# Parallel Agentic Web Archiving for State Administrative Rules

This directory contains an enhanced state admin rules scraper that uses true parallel agentic discovery with integrated corpus gap analysis and PDF processing.

## Architecture Overview

The new system consists of three key components working in parallel:

### 1. **Enhanced State Admin Orchestrator** (`enhanced_state_admin_orchestrator.py`)

Manages parallel agentic discovery for state administrative rules using:
- **ParallelWebArchiver**: 10-25x faster batch fetching with fallback sources
- **Concurrent URL discovery**: Multi-domain parallel exploration per state
- **Semantic filtering**: Rule classification and substantive text validation
- **Dynamic budget allocation**: Per-state timeout with intelligent phase sequencing

**Key Classes:**
- `ParallelStateAdminOrchestrator`: Main orchestrator
- `ParallelStateDiscoveryConfig`: Configurable discovery parameters
- `StateDiscoveryResult`: Per-state discovery results with metrics

**Usage:**
```python
from enhanced_state_admin_orchestrator import (
    ParallelStateAdminOrchestrator,
    ParallelStateDiscoveryConfig,
)

config = ParallelStateDiscoveryConfig(
    max_state_workers=6,
    max_fetch_per_state=12,
    state_timeout=180.0,
)

orchestrator = ParallelStateAdminOrchestrator(config=config)
results = await orchestrator.discover_state_rules_parallel(
    states=["UT", "AZ", "RI"],
    seed_urls_by_state={"UT": ["https://le.utah.gov", ...], ...},
)
```

### 2. **Corpus Gap Analyzer** (`corpus_gap_analyzer.py`)

Analyzes coverage gaps in the discovered corpus by:
- **Domain inventory tracking**: Compares discovered vs. known state agency domains
- **Coverage metrics**: Percent coverage per domain and state
- **Missing content estimation**: Predicts unreached rules based on patterns
- **Intelligent recommendations**: Suggests discovery strategies for weak points

**Key Classes:**
- `CorpusGapAnalyzer`: Gap analysis engine
- `CorpusGapReport`: Detailed per-state gap analysis
- `DomainCoverageSummary`: Coverage metrics per domain

**Usage:**
```python
from corpus_gap_analyzer import CorpusGapAnalyzer

analyzer = CorpusGapAnalyzer()
gap_report = await analyzer.analyze_state_gaps(
    state_code="UT",
    discovered_rules=discovered_rules,
    discovered_domains=discovered_domains,
)

print(analyzer.format_gap_report(gap_report))
```

### 3. **Integrated PDF Processor** (`integrated_pdf_processor.py`)

Extracts and processes administrative rule PDFs using:
- **Multi-method extraction**: pdfplumber → PyPDF2 → OCR fallback
- **Batch processing**: Parallel PDF processing with configurable concurrency
- **Rule segmentation**: Extracting individual rules from large PDFs
- **Metadata enrichment**: Linking PDFs back to state/domain context

**Key Classes:**
- `IntegratedPDFProcessor`: PDF extraction and processing
- `PDFProcessingResult`: Per-PDF processing results with metrics

**Usage:**
```python
from integrated_pdf_processor import IntegratedPDFProcessor

processor = IntegratedPDFProcessor(enable_ocr=True)
results = await processor.process_pdf_batch(
    pdf_urls=["https://example.com/rules.pdf", ...],
    state_code="UT",
    max_concurrent=4,
)
```

## Complete Pipeline Workflow

Run the integrated pipeline script:

```bash
python integrated_state_admin_pipeline.py \
    --states UT AZ RI IN MI \
    --output-dir ./results \
    --max-workers 6
```

### Pipeline Phases:

1. **Phase 1: Parallel Agentic Discovery**
   - Seeds: Fetch official state entrypoints (0-25 seconds per state)
   - Candidates: Discover rule URLs via link extraction (10-50 seconds per state)
   - Fast path: Prioritize known-good URLs (Utah API, etc.)
   - Result: List of discovered rules per state

2. **Phase 2: Corpus Gap Analysis**
   - Compare discovered rules against known state agency domains
   - Identify weak/missing domains
   - Estimate missing content
   - Generate targeted recommendations

3. **Phase 3: PDF Processing** (Optional)
   - Discover PDF documents in state domains
   - Extract text and rules from PDFs
   - Augment corpus with document-based content

4. **Phase 4: Results Aggregation**
   - Combine discovery + gap analysis + PDF results
   - Generate JSON reports with detailed metrics
   - Write per-state rule sets to JSONL

## Configuration

### ParallelStateDiscoveryConfig

```python
@dataclass
class ParallelStateDiscoveryConfig:
    # Concurrency
    max_state_workers: int = 6              # Parallel states
    max_domain_workers_per_state: int = 4   # URLs per state
    max_urls_per_domain: int = 20           # Depth per domain
    
    # Fetch budget
    max_fetch_per_state: int = 12           # Total rules to fetch
    max_candidates_per_state: int = 40      # Candidates to evaluate
    
    # Timeouts (seconds)
    url_fetch_timeout: float = 25.0
    state_timeout: float = 180.0
    domain_timeout: float = 45.0
    
    # Quality
    min_rule_text_chars: int = 220
    require_substantive_text: bool = True
    
    # PDF processing
    enable_pdf_processing: bool = True
    pdf_extract_timeout: float = 15.0
    
    # Gap analysis
    enable_gap_analysis: bool = True
    gap_analysis_sample_size: int = 10
```

## Expected Output

### `pipeline_results.json`
```json
{
  "pipeline_metadata": {
    "timestamp": "2026-03-10T...",
    "states": ["UT", "AZ", "RI"]
  },
  "discovery_results": {
    "UT": {
      "status": "success",
      "rules_count": 47,
      "urls_fetched": 12,
      "duration_seconds": 85.3
    }
  },
  "gap_analysis": {
    "UT": {
      "total_domains_required": 3,
      "domains_discovered": 3,
      "domains_with_rules": 3,
      "domains_with_gaps": 0,
      "recommendations": [...]
    }
  },
  "summary": {
    "total_rules_discovered": 127,
    "states_with_rules": 3
  }
}
```

### `details/{STATE}_rules.jsonl`
```jsonl
{"state_code": "UT", "url": "...", "title": "...", "text": "...", "domain": "..."}
{"state_code": "UT", "url": "...", "title": "...", "text": "...", "domain": "..."}
```

## Performance Characteristics

### Throughput
- **Sequential baseline**: ~1 rule/state/minute (timeout issues)
- **Parallel with agentic discovery**: ~3-5 rules/state/minute
- **With ParallelWebArchiver**: ~5-10 rules/state/minute
- **With gap-awareness**: Skip redundant domains, reduce discovery phases

### Budget utilization
- **Phase 1 (seeds)**: 25-40s per state
- **Phase 2 (candidates)**: 50-100s per state  
- **Phase 3 (PDFs)**: 20-40s per state
- **Phase 4 (analysis)**: 5-10s per state
- **Total**: 100-190s per state (within 180s timeout when optimized)

## Integration with Existing Scraper

These modules work alongside the existing `state_admin_rules_scraper.py`:

```python
# Option 1: Use enhanced orchestrator as replacement
from enhanced_state_admin_orchestrator import ParallelStateAdminOrchestrator

# Option 2: Use existing scraper with gap analysis
from state_admin_rules_scraper import scrape_state_admin_rules
from corpus_gap_analyzer import CorpusGapAnalyzer

# Option 3: Hybrid - scraper handles bulk, orchestrator handles edge cases
```

## Troubleshooting

### Timeout Issues
- Reduce `max_state_workers` from 6 to 4
- Reduce `max_candidates_per_state` from 40 to 20
- Increase `state_timeout` from 180 to 300

### Low Rule Discovery
- Check `enable_gap_analysis` output for weak domains
- Increase `max_fetch_per_state` to explore more candidates
- Enable PDF processing to recover document-based rules

### Memory Issues
- Reduce `max_state_workers` to lower concurrent memory usage
- Reduce `max_domain_workers_per_state` from 4 to 2
- Process states sequentially instead of parallel

## Future Enhancements

1. **LLM-guided discovery**: Use LLM to classify pages and select promising links
2. **Dynamic domain expansion**: Detect new agency subdomains during crawl
3. **ML-based rule classification**: Train model to identify rules vs. non-rule content
4. **Incremental updates**: Detect changed/new rules and update corpus
5. **Cross-state linkage**: Find inter-state legal references
6. **Full PDF processing**: Complete PDF discovery and extraction
7. **API optimization**: Direct state API integration (Utah, etc.)

## Testing

Run unit tests:
```bash
pytest tests/unit/legal_scrapers/test_enhanced_orchestrator.py -v
pytest tests/unit/legal_scrapers/test_corpus_gap_analyzer.py -v
pytest tests/unit/legal_scrapers/test_pdf_processor.py -v
```

Run integration test:
```bash
python integrated_state_admin_pipeline.py \
    --states UT \
    --output-dir /tmp/test_results \
    --max-workers 2
```

## References

- **ParallelWebArchiver**: `legal_scrapers/parallel_web_archiver.py`
- **UnifiedWebArchivingAPI**: `web_archiving/unified_api.py`
- **PDF Processors**: `specialized/pdf/`
- **Original Scraper**: `legal_scrapers/state_admin_rules_scraper.py`

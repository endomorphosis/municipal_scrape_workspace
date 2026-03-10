#!/usr/bin/env python
"""Quick test of parallel agentic discovery components.

This script demonstrates the three key components working together:
1. Parallel discovery orchestrator
2. Corpus gap analysis
3. PDF processing

Run with:
    cd /path/to/municipal_scrape_workspace
    PYTHONPATH=ipfs_datasets_py .venv/bin/python test_parallel_discovery.py
"""

import asyncio
import json
from pathlib import Path


async def test_orchestrator_config():
    """Test ParallelStateDiscoveryConfig."""
    from ipfs_datasets_py.processors.legal_scrapers.enhanced_state_admin_orchestrator import (
        ParallelStateDiscoveryConfig,
    )
    
    print("\n" + "=" * 70)
    print("TEST 1: ParallelStateDiscoveryConfig")
    print("=" * 70)
    
    config = ParallelStateDiscoveryConfig(
        max_state_workers=4,
        max_fetch_per_state=8,
        state_timeout=120.0,
    )
    
    print(f"Config created successfully:")
    print(f"  - Max state workers: {config.max_state_workers}")
    print(f"  - Max fetch per state: {config.max_fetch_per_state}")
    print(f"  - State timeout: {config.state_timeout}s")
    print(f"  - URL fetch timeout: {config.url_fetch_timeout}s")
    print("✓ Config test passed")
    
    return config


async def test_orchestrator():
    """Test ParallelStateAdminOrchestrator initialization."""
    from ipfs_datasets_py.processors.legal_scrapers.enhanced_state_admin_orchestrator import (
        ParallelStateAdminOrchestrator,
        ParallelStateDiscoveryConfig,
    )
    
    print("\n" + "=" * 70)
    print("TEST 2: ParallelStateAdminOrchestrator Initialization")
    print("=" * 70)
    
    config = ParallelStateDiscoveryConfig(max_state_workers=2)
    orchestrator = ParallelStateAdminOrchestrator(config=config)
    
    print(f"Orchestrator created successfully")
    print(f"  - Config: {orchestrator.config}")
    print(f"  - Max state workers: {orchestrator.config.max_state_workers}")
    print("✓ Orchestrator test passed")
    
    return orchestrator


async def test_gap_analyzer():
    """Test CorpusGapAnalyzer."""
    from ipfs_datasets_py.processors.legal_scrapers.corpus_gap_analyzer import (
        CorpusGapAnalyzer,
    )
    
    print("\n" + "=" * 70)
    print("TEST 3: CorpusGapAnalyzer")
    print("=" * 70)
    
    analyzer = CorpusGapAnalyzer()
    
    # Test with sample data
    sample_rules = [
        {
            "state_code": "UT",
            "domain": "adminrules.utah.gov",
            "url": "https://adminrules.utah.gov/rule/R70-101",
            "title": "Sample Rule",
            "text": "A" * 300,
        }
    ]
    
    sample_domains = {"adminrules.utah.gov", "le.utah.gov"}
    
    report = await analyzer.analyze_state_gaps(
        state_code="UT",
        discovered_rules=sample_rules,
        discovered_domains=sample_domains,
    )
    
    print(f"Gap analysis created successfully:")
    print(f"  - State: {report.state_code}")
    print(f"  - Domains required: {report.total_domains_required}")
    print(f"  - Domains discovered: {report.domains_discovered}")
    print(f"  - Domains with rules: {report.domains_with_rules}")
    print(f"  - Estimated missing: {report.estimated_missing_rules}")
    print(f"  - Recommendations: {len(report.recommendations)}")
    
    # Print formatted report
    print("\nFormatted Report:")
    print(analyzer.format_gap_report(report))
    
    print("✓ Gap analyzer test passed")


async def test_pdf_processor():
    """Test IntegratedPDFProcessor."""
    from ipfs_datasets_py.processors.legal_scrapers.integrated_pdf_processor import (
        IntegratedPDFProcessor,
    )
    
    print("\n" + "=" * 70)
    print("TEST 4: IntegratedPDFProcessor")
    print("=" * 70)
    
    processor = IntegratedPDFProcessor(enable_ocr=True)
    
    print(f"PDF processor created successfully:")
    print(f"  - OCR enabled: {processor.enable_ocr}")
    print("✓ PDF processor test passed")


async def test_discovery_result_model():
    """Test StateDiscoveryResult model."""
    from ipfs_datasets_py.processors.legal_scrapers.enhanced_state_admin_orchestrator import (
        StateDiscoveryResult,
    )
    
    print("\n" + "=" * 70)
    print("TEST 5: StateDiscoveryResult Model")
    print("=" * 70)
    
    result = StateDiscoveryResult(
        state_code="UT",
        state_name="Utah",
        status="success",
    )
    
    result.rules = [
        {"url": "https://example.com/rule1", "title": "Rule 1"},
        {"url": "https://example.com/rule2", "title": "Rule 2"},
    ]
    
    result.urls_fetched = 5
    result.domains_visited.add("example.com")
    result.methods_used["common_crawl"] = 3
    result.methods_used["wayback"] = 2
    
    print(f"StateDiscoveryResult created successfully:")
    print(f"  - State: {result.state_code}")
    print(f"  - Status: {result.status}")
    print(f"  - Rules discovered: {len(result.rules)}")
    print(f"  - URLs fetched: {result.urls_fetched}")
    print(f"  - Domains visited: {result.domains_visited}")
    print(f"  - Methods: {result.methods_used}")
    print(f"  - Elapsed time: {result.elapsed():.2f}s")
    print(f"  - Success rate: {result.success_rate():.1%}")
    
    print("✓ StateDiscoveryResult test passed")


async def test_gap_report_model():
    """Test CorpusGapReport model."""
    from ipfs_datasets_py.processors.legal_scrapers.corpus_gap_analyzer import (
        CorpusGapReport,
        DomainCoverageSummary,
    )
    
    print("\n" + "=" * 70)
    print("TEST 6: CorpusGapReport Model")
    print("=" * 70)
    
    report = CorpusGapReport(
        state_code="UT",
        state_name="Utah",
        total_domains_required=3,
    )
    
    report.domain_coverage["adminrules.utah.gov"] = DomainCoverageSummary(
        domain="adminrules.utah.gov",
        state_code="UT",
        urls_fetched=5,
        rules_discovered=3,
        status="good",
    )
    
    report.recommendations = [
        "Investigate le.utah.gov for additional rules",
        "Check for PDF archives at oar.utah.gov",
    ]
    
    print(f"CorpusGapReport created successfully:")
    print(f"  - State: {report.state_code}")
    print(f"  - Domains required: {report.total_domains_required}")
    print(f"  - Domain coverage entries: {len(report.domain_coverage)}")
    print(f"  - Recommendations: {len(report.recommendations)}")
    
    print("✓ CorpusGapReport test passed")


async def test_pdf_result_model():
    """Test PDFProcessingResult model."""
    from ipfs_datasets_py.processors.legal_scrapers.integrated_pdf_processor import (
        PDFProcessingResult,
    )
    
    print("\n" + "=" * 70)
    print("TEST 7: PDFProcessingResult Model")
    print("=" * 70)
    
    result = PDFProcessingResult(
        pdf_url="https://example.com/rules.pdf",
        state_code="UT",
        status="success",
        extracted_text="Sample extracted text from PDF",
        page_count=10,
        text_length=5000,
        method_used="pdfplumber",
        processing_time_seconds=2.5,
    )
    
    result.extracted_rules = [
        {"rule_id": "R70-101", "text": "Rule text"},
        {"rule_id": "R70-102", "text": "Rule text"},
    ]
    
    print(f"PDFProcessingResult created successfully:")
    print(f"  - PDF URL: {result.pdf_url}")
    print(f"  - Status: {result.status}")
    print(f"  - Page count: {result.page_count}")
    print(f"  - Text length: {result.text_length}")
    print(f"  - Method: {result.method_used}")
    print(f"  - Extracted rules: {len(result.extracted_rules)}")
    print(f"  - Processing time: {result.processing_time_seconds:.2f}s")
    
    print("✓ PDFProcessingResult test passed")


async def main():
    """Run all tests."""
    print("\n" + "=" * 70)
    print("PARALLEL AGENTIC DISCOVERY COMPONENTS TEST SUITE")
    print("=" * 70)
    
    try:
        await test_orchestrator_config()
        await test_orchestrator()
        await test_gap_analyzer()
        await test_pdf_processor()
        await test_discovery_result_model()
        await test_gap_report_model()
        await test_pdf_result_model()
        
        print("\n" + "=" * 70)
        print("ALL TESTS PASSED ✓")
        print("=" * 70)
        print("\nNext steps:")
        print("1. Review PARALLEL_AGENTIC_DISCOVERY_README.md for detailed docs")
        print("2. Run integrated pipeline: python integrated_state_admin_pipeline.py --states UT AZ")
        print("3. Check gap analysis output for coverage weak points")
        print("4. Review discovered rules in output directory")
    
    except Exception as exc:
        print(f"\n✗ TEST FAILED: {exc}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

"""Integrated State Admin Rules Pipeline.

This script orchestrates the complete parallel agentic discovery pipeline:
1. Parallel state discovery using agentic web archiving
2. Corpus gap analysis to identify weak points
3. PDF processing for document-based rules
4. Results aggregation and reporting

Usage:
    python integrated_state_admin_pipeline.py --states UT AZ RI IN --output-dir ./results
"""

import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


async def run_integrated_pipeline(
    states: List[str],
    output_dir: Optional[str] = None,
    enable_gap_analysis: bool = True,
    enable_pdf_processing: bool = True,
    max_state_workers: int = 6,
):
    """Run the integrated state admin rules pipeline.
    
    Args:
        states: List of state codes to process
        output_dir: Output directory for results
        enable_gap_analysis: Whether to analyze corpus gaps
        enable_pdf_processing: Whether to process PDFs
        max_state_workers: Max concurrent state workers
    """
    from ipfs_datasets_py.processors.legal_scrapers.enhanced_state_admin_orchestrator import (
        ParallelStateAdminOrchestrator,
        ParallelStateDiscoveryConfig,
    )
    from ipfs_datasets_py.processors.legal_scrapers.corpus_gap_analyzer import (
        CorpusGapAnalyzer,
        analyze_multi_state_gaps,
    )
    from ipfs_datasets_py.processors.legal_scrapers.integrated_pdf_processor import (
        IntegratedPDFProcessor,
    )
    
    # Create output directory
    if output_dir:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory: {output_path}")
    else:
        output_path = None
    
    # Build seed URLs for each state
    seed_urls_by_state = {
        "UT": [
            "https://le.utah.gov",
            "https://adminrules.utah.gov",
            "https://oar.utah.gov",
        ],
        "AZ": [
            "https://azsos.gov",
            "https://azleg.gov",
        ],
        "RI": [
            "https://sos.ri.gov",
            "https://ri.gov",
        ],
        "IN": [
            "https://iga.in.gov",
            "https://admin.in.gov",
        ],
    }
    
    # Filter to requested states
    states_to_process = [s.upper() for s in states]
    seed_urls_by_state = {
        k: v for k, v in seed_urls_by_state.items() if k in states_to_process
    }
    
    logger.info(f"Starting pipeline for states: {states_to_process}")
    logger.info(f"Processing {len(seed_urls_by_state)} states with up to {max_state_workers} workers")
    
    # Phase 1: Parallel agentic discovery
    logger.info("\n" + "=" * 70)
    logger.info("PHASE 1: PARALLEL AGENTIC DISCOVERY")
    logger.info("=" * 70)
    
    config = ParallelStateDiscoveryConfig(
        max_state_workers=max_state_workers,
        max_fetch_per_state=12,
        max_candidates_per_state=40,
        state_timeout=180.0,
        enable_gap_analysis=enable_gap_analysis,
        enable_pdf_processing=enable_pdf_processing,
    )
    
    orchestrator = ParallelStateAdminOrchestrator(config=config)
    discovery_results = await orchestrator.discover_state_rules_parallel(
        states=states_to_process,
        seed_urls_by_state=seed_urls_by_state,
    )
    
    # Log discovery results
    total_rules = 0
    total_time = 0
    for state_code, result in discovery_results.items():
        logger.info(
            f"{state_code}: {result.status} | "
            f"Rules: {len(result.rules)} | "
            f"URLs fetched: {result.urls_fetched} | "
            f"Time: {result.duration_seconds:.1f}s"
        )
        total_rules += len(result.rules)
        total_time += result.duration_seconds
    
    logger.info(f"Total rules discovered: {total_rules} in {total_time:.1f}s")
    
    # Phase 2: Corpus gap analysis
    gap_reports = {}
    if enable_gap_analysis:
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 2: CORPUS GAP ANALYSIS")
        logger.info("=" * 70)
        
        try:
            gap_reports = await analyze_multi_state_gaps(
                results=discovery_results,
                state_codes=states_to_process,
            )
            
            analyzer = CorpusGapAnalyzer()
            for state_code, report in gap_reports.items():
                logger.info(analyzer.format_gap_report(report))
        
        except Exception as exc:
            logger.error(f"Gap analysis failed: {exc}")
    
    # Phase 3: PDF processing
    if enable_pdf_processing:
        logger.info("\n" + "=" * 70)
        logger.info("PHASE 3: PDF PROCESSING")
        logger.info("=" * 70)
        
        pdf_processor = IntegratedPDFProcessor(enable_ocr=True)
        
        for state_code in states_to_process:
            # TODO: Discover PDFs from state domains and process them
            logger.info(f"{state_code}: PDF processing not yet implemented")
    
    # Phase 4: Results aggregation
    logger.info("\n" + "=" * 70)
    logger.info("PHASE 4: RESULTS AGGREGATION")
    logger.info("=" * 70)
    
    aggregated_results = {
        "pipeline_metadata": {
            "timestamp": datetime.now().isoformat(),
            "states": states_to_process,
            "config": {
                "max_state_workers": config.max_state_workers,
                "max_fetch_per_state": config.max_fetch_per_state,
                "state_timeout_seconds": config.state_timeout,
            },
        },
        "discovery_results": {},
        "gap_analysis": {},
        "summary": {
            "total_states": len(states_to_process),
            "total_rules_discovered": 0,
            "states_with_rules": 0,
            "states_with_gaps": 0,
            "total_processing_time_seconds": 0,
        },
    }
    
    # Aggregate discovery results
    for state_code, result in discovery_results.items():
        aggregated_results["discovery_results"][state_code] = {
            "status": result.status,
            "rules_count": len(result.rules),
            "urls_fetched": result.urls_fetched,
            "domains_visited": list(result.domains_visited),
            "methods_used": result.methods_used,
            "duration_seconds": result.duration_seconds,
            "error": result.error_message,
        }
        
        aggregated_results["summary"]["total_rules_discovered"] += len(result.rules)
        if len(result.rules) > 0:
            aggregated_results["summary"]["states_with_rules"] += 1
        aggregated_results["summary"]["total_processing_time_seconds"] += result.duration_seconds
    
    # Aggregate gap analysis
    for state_code, report in gap_reports.items():
        aggregated_results["gap_analysis"][state_code] = {
            "total_domains_required": report.total_domains_required,
            "domains_discovered": report.domains_discovered,
            "domains_with_rules": report.domains_with_rules,
            "domains_with_gaps": report.domains_with_gaps,
            "estimated_missing_rules": report.estimated_missing_rules,
            "recommendations": report.recommendations,
        }
        aggregated_results["summary"]["states_with_gaps"] += report.domains_with_gaps
    
    # Write results
    if output_path:
        results_file = output_path / "pipeline_results.json"
        with open(results_file, "w") as f:
            json.dump(aggregated_results, f, indent=2, default=str)
        logger.info(f"Results written to {results_file}")
        
        # Write discovery results detail
        detail_dir = output_path / "details"
        detail_dir.mkdir(parents=True, exist_ok=True)
        
        for state_code, result in discovery_results.items():
            detail_file = detail_dir / f"{state_code}_rules.jsonl"
            with open(detail_file, "w") as f:
                for rule in result.rules:
                    f.write(json.dumps(rule, default=str) + "\n")
            logger.info(f"Wrote {len(result.rules)} rules for {state_code}")
    
    # Final summary
    logger.info("\n" + "=" * 70)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Total rules discovered: {aggregated_results['summary']['total_rules_discovered']}")
    logger.info(f"States with rules: {aggregated_results['summary']['states_with_rules']}")
    logger.info(f"Total processing time: {aggregated_results['summary']['total_processing_time_seconds']:.1f}s")
    
    return aggregated_results


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Integrated state admin rules discovery pipeline"
    )
    parser.add_argument(
        "--states",
        nargs="+",
        default=["UT", "AZ"],
        help="State codes to process (default: UT AZ)",
    )
    parser.add_argument(
        "--output-dir",
        help="Output directory for results",
    )
    parser.add_argument(
        "--no-gap-analysis",
        action="store_true",
        help="Skip corpus gap analysis",
    )
    parser.add_argument(
        "--no-pdf-processing",
        action="store_true",
        help="Skip PDF processing",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=6,
        help="Max concurrent state workers (default: 6)",
    )
    
    args = parser.parse_args()
    
    # Run pipeline
    try:
        results = asyncio.run(
            run_integrated_pipeline(
                states=args.states,
                output_dir=args.output_dir,
                enable_gap_analysis=not args.no_gap_analysis,
                enable_pdf_processing=not args.no_pdf_processing,
                max_state_workers=args.max_workers,
            )
        )
        sys.exit(0)
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as exc:
        logger.error(f"Pipeline failed: {exc}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Script to run federal law scrapers and generate raw data and JSON-LD files.

This script:
1. Scrapes Federal Register data (recent documents from major agencies)
2. Scrapes US Code data (selected titles)
3. Converts scraped data to JSON-LD format
4. Saves all outputs to data/federal_laws/
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add the ipfs_datasets_py to path
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "ipfs_datasets_py"))

# Import directly from federal_scrapers subpackage
from ipfs_datasets_py.processors.legal_scrapers.federal_scrapers.federal_register_scraper import (
    scrape_federal_register,
)
from ipfs_datasets_py.processors.legal_scrapers.federal_scrapers.us_code_scraper import (
    scrape_us_code,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Output directories
FEDERAL_LAWS_DIR = Path(__file__).parent.parent / "data" / "federal_laws"
FEDERAL_REGISTER_DIR = FEDERAL_LAWS_DIR / "federal_register"
US_CODE_DIR = FEDERAL_LAWS_DIR / "us_code"
MANIFEST_PATH = FEDERAL_LAWS_DIR / "run_manifest.json"


def ensure_directories():
    """Ensure all output directories exist."""
    FEDERAL_REGISTER_DIR.mkdir(parents=True, exist_ok=True)
    US_CODE_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directories ready:")
    logger.info(f"  - {FEDERAL_REGISTER_DIR}")
    logger.info(f"  - {US_CODE_DIR}")


def _load_manifest() -> Dict[str, Any]:
    if not MANIFEST_PATH.exists():
        return {
            "version": 1,
            "updated_at": datetime.now().isoformat(),
            "federal_register": {},
            "us_code": {},
        }
    try:
        return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    except Exception:
        # Never fail the run because of a malformed prior manifest.
        return {
            "version": 1,
            "updated_at": datetime.now().isoformat(),
            "federal_register": {},
            "us_code": {},
        }


def _save_manifest(manifest: Dict[str, Any]) -> None:
    manifest["updated_at"] = datetime.now().isoformat()
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")


def _fr_run_signature(start_date: str, end_date: str, max_documents: Optional[int]) -> str:
    return f"start={start_date}|end={end_date}|max_documents={max_documents}"


def _usc_run_signature(max_sections: Optional[int], year: Optional[int]) -> str:
    return f"max_sections={max_sections}|year={year}"


def _set_phase_interrupted(phase: str, signature: str) -> None:
    manifest = _load_manifest()
    prior = manifest.get(phase, {})
    manifest[phase] = {
        "status": "interrupted",
        "signature": signature,
        "started_at": prior.get("started_at"),
        "finished_at": datetime.now().isoformat(),
        "error": "KeyboardInterrupt",
    }
    # Preserve progress counters if they already exist.
    for key in ("documents_count", "titles_count", "sections_count", "failed_titles_count"):
        if key in prior:
            manifest[phase][key] = prior.get(key)
    _save_manifest(manifest)


def convert_federal_register_to_jsonld(documents: List[Dict[str, Any]], metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Federal Register documents to JSON-LD format."""
    jsonld = {
        "@context": {
            "@vocab": "https://schema.org/",
            "fedreg": "https://www.federalregister.gov/",
            "documentNumber": "fedreg:documentNumber",
            "documentType": "fedreg:documentType",
            "publicationDate": "fedreg:publicationDate",
            "effectiveDate": "fedreg:effectiveDate",
            "agency": "fedreg:agency",
            "cfr": "fedreg:cfrReference"
        },
        "@type": "Dataset",
        "@id": f"urn:federal-register:{metadata.get('scraped_at', datetime.now().isoformat())}",
        "name": "Federal Register Documents",
        "description": f"Federal Register documents scraped from {metadata.get('date_range', {}).get('start_date')} to {metadata.get('date_range', {}).get('end_date')}",
        "datePublished": metadata.get("scraped_at"),
        "publisher": {
            "@type": "Organization",
            "name": "Office of the Federal Register",
            "url": "https://www.federalregister.gov"
        },
        "datasetSize": {
            "@type": "QuantitativeValue",
            "value": len(documents)
        },
        "hasPart": []
    }

    for doc in documents:
        doc_jsonld = {
            "@type": "Legislation",
            "@id": f"urn:federal-register:doc:{doc.get('document_number', 'unknown')}",
            "name": doc.get("title", "Untitled Document"),
            "legislationType": str(doc.get("document_type", "Document")).lower(),
            "identifier": doc.get("document_number"),
            "datePublished": doc.get("publication_date"),
            "legislationDate": doc.get("effective_date") if doc.get("effective_date") else None,
            "sourceOrganization": {
                "@type": "Organization",
                "name": doc.get("agency", "Unknown Agency")
            },
            "text": doc.get("abstract", doc.get("excerpt", "")),
            "url": doc.get("fr_url", doc.get("pdf_url", "")),
            "additionalProperty": []
        }

        # Add CFR references if available
        if doc.get("topics"):
            for cfr in doc.get("topics", []):
                doc_jsonld["additionalProperty"].append({
                    "@type": "PropertyValue",
                    "name": "Topic",
                    "value": cfr
                })

        # Add docket numbers if available
        if doc.get("docket_ids"):
            for docket in doc.get("docket_ids", []):
                doc_jsonld["additionalProperty"].append({
                    "@type": "PropertyValue",
                    "name": "Docket Number",
                    "value": docket
                })

        jsonld["hasPart"].append(doc_jsonld)

    return jsonld


def convert_us_code_to_jsonld(sections: List[Dict[str, Any]], metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Convert US Code sections to JSON-LD format."""
    jsonld = {
        "@context": {
            "@vocab": "https://schema.org/",
            "uscode": "https://uscode.house.gov/",
            "titleNumber": "uscode:titleNumber",
            "sectionNumber": "uscode:sectionNumber",
            "chapter": "uscode:chapter",
            "subchapter": "uscode:subchapter"
        },
        "@type": "Dataset",
        "@id": f"urn:us-code:{metadata.get('scraped_at', datetime.now().isoformat())}",
        "name": "United States Code",
        "description": f"US Code sections from titles: {', '.join(metadata.get('titles_scraped', []))}",
        "datePublished": metadata.get("scraped_at"),
        "publisher": {
            "@type": "Organization",
            "name": "Office of the Law Revision Counsel",
            "url": "https://uscode.house.gov"
        },
        "datasetSize": {
            "@type": "QuantitativeValue",
            "value": len(sections)
        },
        "hasPart": []
    }

    # Group sections by title
    titles = {}
    for section in sections:
        title_num = section.get("title_number", "unknown")
        if title_num not in titles:
            titles[title_num] = {
                "title_name": section.get("title_name", f"Title {title_num}"),
                "sections": []
            }
        titles[title_num]["sections"].append(section)

    # Build hierarchical structure
    for title_num, title_data in sorted(titles.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 999):
        title_jsonld = {
            "@type": "Legislation",
            "@id": f"urn:us-code:title:{title_num}",
            "name": title_data["title_name"],
            "legislationType": "statutory-title",
            "identifier": title_num,
            "hasPart": []
        }

        for section in title_data["sections"]:
            section_jsonld = {
                "@type": "Legislation",
                "@id": f"urn:us-code:title:{title_num}:section:{section.get('section_number', 'unknown')}",
                "name": section.get("heading", f"Section {section.get('section_number', 'unknown')}"),
                "legislationType": "statutory-section",
                "identifier": section.get("section_number"),
                "isPartOf": {
                    "@type": "Legislation",
                    "@id": f"urn:us-code:title:{title_num}"
                },
                "text": section.get("body_text", section.get("text", "")),
                "url": section.get("source_url", "")
            }

            # Add chapter info if available
            if section.get("chapter_number"):
                section_jsonld["chapter"] = {
                    "number": section.get("chapter_number"),
                    "name": section.get("chapter_name", "")
                }

            # Add subchapter info if available
            if section.get("subchapter_letter"):
                section_jsonld["subchapter"] = {
                    "letter": section.get("subchapter_letter"),
                    "name": section.get("subchapter_name", "")
                }

            # Add citations if available
            if section.get("citations"):
                section_jsonld["citation"] = section.get("citations", [])

            title_jsonld["hasPart"].append(section_jsonld)

        jsonld["hasPart"].append(title_jsonld)

    return jsonld


async def scrape_federal_register_data():
    """Scrape Federal Register data comprehensively."""
    logger.info("=" * 80)
    logger.info("SCRAPING FEDERAL REGISTER")
    logger.info("=" * 80)

    # Default to comprehensive range unless caller restricts via env vars.
    start_date = os.getenv("FEDERAL_REGISTER_START_DATE", "1994-01-01")
    end_date = os.getenv("FEDERAL_REGISTER_END_DATE", datetime.now().strftime("%Y-%m-%d"))
    max_documents_env = os.getenv("FEDERAL_REGISTER_MAX_DOCUMENTS", "")
    max_documents = int(max_documents_env) if max_documents_env.strip() else None

    logger.info("Date range: %s to %s", start_date, end_date)
    logger.info("Agencies: all")
    logger.info("Max documents: %s", max_documents if max_documents is not None else "unbounded")

    manifest = _load_manifest()
    signature = _fr_run_signature(start_date, end_date, max_documents)
    force_refresh = os.getenv("FEDERAL_REGISTER_FORCE_REFRESH", "0") == "1"
    fr_state = manifest.get("federal_register", {})

    if (
        not force_refresh
        and fr_state.get("status") == "success"
        and fr_state.get("signature") == signature
        and (FEDERAL_REGISTER_DIR / "federal_register_raw.json").exists()
        and (FEDERAL_REGISTER_DIR / "federal_register.jsonld").exists()
        and (FEDERAL_REGISTER_DIR / "metadata.json").exists()
    ):
        logger.info("Federal Register phase already completed for same parameters; skipping fetch.")
        with open(FEDERAL_REGISTER_DIR / "federal_register_raw.json", "r", encoding="utf-8") as f:
            return json.load(f)

    manifest["federal_register"] = {
        "status": "running",
        "signature": signature,
        "started_at": datetime.now().isoformat(),
        "documents_count": 0,
        "error": None,
    }
    _save_manifest(manifest)

    result = await scrape_federal_register(
        agencies=None,
        start_date=start_date,
        end_date=end_date,
        output_format="json",
        include_full_text=False,
        rate_limit_delay=1.0,
        max_documents=max_documents,
    )

    if result.get("status") in ["success", "partial_success"]:
        documents = result.get("data", [])
        metadata = result.get("metadata", {})

        logger.info(f"Scraped {len(documents)} Federal Register documents")

        # Save raw JSON
        raw_path = FEDERAL_REGISTER_DIR / "federal_register_raw.json"
        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved raw data to: {raw_path}")

        # Save as JSON-LD
        jsonld = convert_federal_register_to_jsonld(documents, metadata)
        jsonld_path = FEDERAL_REGISTER_DIR / "federal_register.jsonld"
        with open(jsonld_path, "w", encoding="utf-8") as f:
            json.dump(jsonld, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved JSON-LD to: {jsonld_path}")

        # Save metadata
        meta_path = FEDERAL_REGISTER_DIR / "metadata.json"
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved metadata to: {meta_path}")

        manifest = _load_manifest()
        manifest["federal_register"] = {
            "status": "success",
            "signature": signature,
            "started_at": manifest.get("federal_register", {}).get("started_at"),
            "finished_at": datetime.now().isoformat(),
            "documents_count": len(documents),
            "error": None,
        }
        _save_manifest(manifest)

        return result
    else:
        logger.error(f"Federal Register scraping failed: {result.get('error')}")

        manifest = _load_manifest()
        manifest["federal_register"] = {
            "status": "error",
            "signature": signature,
            "started_at": manifest.get("federal_register", {}).get("started_at"),
            "finished_at": datetime.now().isoformat(),
            "documents_count": 0,
            "error": result.get("error"),
        }
        _save_manifest(manifest)

        return result


async def scrape_us_code_data():
    """Scrape US Code data for all titles."""
    logger.info("=" * 80)
    logger.info("SCRAPING US CODE")
    logger.info("=" * 80)

    max_sections_env = os.getenv("US_CODE_MAX_SECTIONS", "")
    max_sections = int(max_sections_env) if max_sections_env.strip() else None
    year_env = os.getenv("US_CODE_YEAR", "")
    year = int(year_env) if year_env.strip() else None

    logger.info("Titles to scrape: all")
    logger.info("Max sections: %s", max_sections if max_sections is not None else "unbounded")
    logger.info("Year override: %s", year if year is not None else "auto")

    manifest = _load_manifest()
    signature = _usc_run_signature(max_sections, year)
    force_refresh = os.getenv("US_CODE_FORCE_REFRESH", "0") == "1"
    usc_state = manifest.get("us_code", {})

    if (
        not force_refresh
        and usc_state.get("status") == "success"
        and usc_state.get("signature") == signature
        and (US_CODE_DIR / "us_code_raw.json").exists()
        and (US_CODE_DIR / "us_code.jsonld").exists()
        and (US_CODE_DIR / "metadata.json").exists()
    ):
        logger.info("US Code phase already completed for same parameters; skipping fetch.")
        with open(US_CODE_DIR / "us_code_raw.json", "r", encoding="utf-8") as f:
            return json.load(f)

    manifest["us_code"] = {
        "status": "running",
        "signature": signature,
        "started_at": datetime.now().isoformat(),
        "titles_count": 0,
        "sections_count": 0,
        "failed_titles_count": 0,
        "error": None,
    }
    _save_manifest(manifest)

    result = await scrape_us_code(
        titles=["all"],
        output_format="json",
        include_metadata=True,
        rate_limit_delay=1.0,
        max_sections=max_sections,
        year=year,
        keep_zip_cache=True,
        max_year_fallbacks=12,
        download_retries=5,
        continue_on_error=True,
    )

    if result.get("status") in ["success", "partial_success"]:
        title_payloads = result.get("data", [])
        metadata = result.get("metadata", {})
        sections: List[Dict[str, Any]] = []
        for title_blob in title_payloads:
            if not isinstance(title_blob, dict):
                continue
            for section in title_blob.get("sections", []) or []:
                if isinstance(section, dict):
                    sections.append(section)

        logger.info("Scraped %s US Code sections across %s successful titles", len(sections), metadata.get("titles_count", 0))

        # Save raw JSON
        raw_path = US_CODE_DIR / "us_code_raw.json"
        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved raw data to: {raw_path}")

        # Save as JSON-LD
        jsonld = convert_us_code_to_jsonld(sections, metadata)
        jsonld_path = US_CODE_DIR / "us_code.jsonld"
        with open(jsonld_path, "w", encoding="utf-8") as f:
            json.dump(jsonld, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved JSON-LD to: {jsonld_path}")

        # Save metadata
        meta_path = US_CODE_DIR / "metadata.json"
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved metadata to: {meta_path}")

        # Also save individual title files for easier access
        titles_data: Dict[str, Dict[str, Any]] = {}
        for section in sections:
            title_num = section.get("title_number", "unknown")
            if title_num not in titles_data:
                titles_data[title_num] = {
                    "title_number": title_num,
                    "title_name": section.get("title_name", f"Title {title_num}"),
                    "sections": []
                }
            titles_data[title_num]["sections"].append(section)

        for title_num, title_data in titles_data.items():
            title_path = US_CODE_DIR / f"title_{title_num}.json"
            with open(title_path, "w", encoding="utf-8") as f:
                json.dump(title_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved Title {title_num} ({len(title_data['sections'])} sections) to: {title_path}")

        manifest = _load_manifest()
        manifest["us_code"] = {
            "status": "success",
            "signature": signature,
            "started_at": manifest.get("us_code", {}).get("started_at"),
            "finished_at": datetime.now().isoformat(),
            "titles_count": int(metadata.get("titles_count", 0) or 0),
            "sections_count": int(metadata.get("sections_count", 0) or 0),
            "failed_titles_count": int(metadata.get("failed_titles_count", 0) or 0),
            "error": None,
        }
        _save_manifest(manifest)

        return result
    else:
        logger.error(f"US Code scraping failed: {result.get('error')}")

        manifest = _load_manifest()
        manifest["us_code"] = {
            "status": "error",
            "signature": signature,
            "started_at": manifest.get("us_code", {}).get("started_at"),
            "finished_at": datetime.now().isoformat(),
            "titles_count": 0,
            "sections_count": 0,
            "failed_titles_count": 0,
            "error": result.get("error"),
        }
        _save_manifest(manifest)

        return result


async def main():
    """Main entry point."""
    logger.info("Starting Federal Law Scrapers")
    logger.info(f"Output directory: {FEDERAL_LAWS_DIR}")

    ensure_directories()

    start_date = os.getenv("FEDERAL_REGISTER_START_DATE", "1994-01-01")
    end_date = os.getenv("FEDERAL_REGISTER_END_DATE", datetime.now().strftime("%Y-%m-%d"))
    max_documents_env = os.getenv("FEDERAL_REGISTER_MAX_DOCUMENTS", "")
    max_documents = int(max_documents_env) if max_documents_env.strip() else None
    fr_signature = _fr_run_signature(start_date, end_date, max_documents)

    max_sections_env = os.getenv("US_CODE_MAX_SECTIONS", "")
    max_sections = int(max_sections_env) if max_sections_env.strip() else None
    year_env = os.getenv("US_CODE_YEAR", "")
    year = int(year_env) if year_env.strip() else None
    usc_signature = _usc_run_signature(max_sections, year)

    try:
        # Scrape Federal Register
        fr_result = await scrape_federal_register_data()
    except KeyboardInterrupt:
        _set_phase_interrupted("federal_register", fr_signature)
        logger.error("Interrupted during Federal Register phase; manifest marked interrupted.")
        return 130

    try:
        # Scrape US Code
        usc_result = await scrape_us_code_data()
    except KeyboardInterrupt:
        _set_phase_interrupted("us_code", usc_signature)
        logger.error("Interrupted during US Code phase; manifest marked interrupted.")
        return 130

    # Summary
    logger.info("=" * 80)
    logger.info("SCRAPING COMPLETE")
    logger.info("=" * 80)

    fr_count = fr_result.get("metadata", {}).get("documents_scraped", 0) if fr_result.get("status") in ["success", "partial_success"] else 0
    usc_count = usc_result.get("metadata", {}).get("sections_count", 0) if usc_result.get("status") in ["success", "partial_success"] else 0

    logger.info(f"Federal Register documents: {fr_count}")
    logger.info(f"US Code sections: {usc_count}")
    logger.info("")
    logger.info("Output files:")
    logger.info(f"  Federal Register: {FEDERAL_REGISTER_DIR}")
    logger.info(f"  US Code: {US_CODE_DIR}")

    return 0 if fr_result.get("status") in ["success", "partial_success"] and usc_result.get("status") in ["success", "partial_success"] else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

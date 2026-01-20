# Documentation Index

This directory contains all project documentation organized by topic.

## 📁 Directory Structure

```
docs/
├── README.md (this file)
├── refactoring/          # Refactoring process documentation
├── ccindex/              # Common Crawl index documentation
├── pipeline/             # Pipeline orchestration documentation
├── COMMON_CRAWL_USAGE.md
├── REPO_LAYOUT_PLAN.md
├── CRITICAL_FINDINGS.md
├── TEST_SUITE_DOCUMENTATION.md
├── TEST_SUITE_SUMMARY.txt
└── NEW_SCRIPTS_SUMMARY.txt
```

## 📚 Documentation by Topic

### Getting Started

- **[../README.md](../README.md)** - Main project README
- **[../REFACTORED_STRUCTURE.md](../REFACTORED_STRUCTURE.md)** - Complete repository structure guide
- **[../QUICKSTART.md](../QUICKSTART.md)** - Quick start guide
- **[COMMON_CRAWL_USAGE.md](COMMON_CRAWL_USAGE.md)** - Common Crawl usage guide

### Refactoring Documentation

Located in [refactoring/](refactoring/):

- **[refactoring/REFACTORING_INDEX.md](refactoring/REFACTORING_INDEX.md)** - Complete refactoring documentation index
- **[refactoring/MIGRATION_COMPLETE.md](refactoring/MIGRATION_COMPLETE.md)** - Migration summary
- **[refactoring/FILE_MIGRATION_MAP.md](refactoring/FILE_MIGRATION_MAP.md)** - File location lookup table
- **[refactoring/FINAL_LAYOUT_README.md](refactoring/FINAL_LAYOUT_README.md)** - Post-migration guide
- **[refactoring/REFACTORING_ROADMAP.md](refactoring/REFACTORING_ROADMAP.md)** - Refactoring roadmap
- **[refactoring/REFACTORING_STATUS.md](refactoring/REFACTORING_STATUS.md)** - Status tracking
- **[refactoring/POST_MIGRATION_GAPS.md](refactoring/POST_MIGRATION_GAPS.md)** - Gap analysis

### Common Crawl Index Documentation

Located in [ccindex/](ccindex/):

- **[ccindex/INDEX_ARCHITECTURE.md](ccindex/INDEX_ARCHITECTURE.md)** - Index architecture overview
- **[ccindex/DUCKDB_INDEX_DESIGN.md](ccindex/DUCKDB_INDEX_DESIGN.md)** - DuckDB index design
- **[ccindex/POINTER_INDEX_DESIGN.md](ccindex/POINTER_INDEX_DESIGN.md)** - Pointer index design
- **[ccindex/CC_INDEX_SPECIFICATION.md](ccindex/CC_INDEX_SPECIFICATION.md)** - Index specification
- **[ccindex/DUCKDB_POINTER_TOOLS.md](ccindex/DUCKDB_POINTER_TOOLS.md)** - DuckDB pointer tools
- **[ccindex/PARALLEL_DUCKDB_QUICKSTART.md](ccindex/PARALLEL_DUCKDB_QUICKSTART.md)** - Parallel DuckDB quickstart
- **[ccindex/INDEX_HIERARCHY.md](ccindex/INDEX_HIERARCHY.md)** - Index hierarchy

### Pipeline Documentation

Located in [pipeline/](pipeline/):

- **[pipeline/CC_ORCHESTRATOR_README.md](pipeline/CC_ORCHESTRATOR_README.md)** - Orchestrator documentation
- **[pipeline/CC_PIPELINE_MANAGER_README.md](pipeline/CC_PIPELINE_MANAGER_README.md)** - Pipeline manager docs
- **[pipeline/PIPELINE_CONFIG_GUIDE.md](pipeline/PIPELINE_CONFIG_GUIDE.md)** - Configuration guide
- **[pipeline/COLLECTION_TRACKING_FEATURE.md](pipeline/COLLECTION_TRACKING_FEATURE.md)** - Collection tracking
- **[pipeline/OVERNIGHT_BUILD_STATUS.md](pipeline/OVERNIGHT_BUILD_STATUS.md)** - Build status

### Testing Documentation

- **[TEST_SUITE_DOCUMENTATION.md](TEST_SUITE_DOCUMENTATION.md)** - Test suite documentation
- **[TEST_SUITE_SUMMARY.txt](TEST_SUITE_SUMMARY.txt)** - Test suite summary

### Other Documentation

- **[CRITICAL_FINDINGS.md](CRITICAL_FINDINGS.md)** - Critical findings and issues
- **[NEW_SCRIPTS_SUMMARY.txt](NEW_SCRIPTS_SUMMARY.txt)** - New scripts summary
- **[REPO_LAYOUT_PLAN.md](REPO_LAYOUT_PLAN.md)** - Repository layout plan

## 🔍 Finding Documentation

### By Task

| Task | Documentation |
|------|---------------|
| **Getting Started** | [../README.md](../README.md), [../QUICKSTART.md](../QUICKSTART.md) |
| **Understanding Structure** | [../REFACTORED_STRUCTURE.md](../REFACTORED_STRUCTURE.md) |
| **Finding Files** | [refactoring/FILE_MIGRATION_MAP.md](refactoring/FILE_MIGRATION_MAP.md) |
| **Using Common Crawl** | [COMMON_CRAWL_USAGE.md](COMMON_CRAWL_USAGE.md), [ccindex/](ccindex/) |
| **Running Pipelines** | [pipeline/](pipeline/) |
| **Refactoring History** | [refactoring/](refactoring/) |
| **Testing** | [TEST_SUITE_DOCUMENTATION.md](TEST_SUITE_DOCUMENTATION.md) |

### By Role

**Users:**
- Start with [../README.md](../README.md)
- See [../QUICKSTART.md](../QUICKSTART.md) for quick start
- Check [COMMON_CRAWL_USAGE.md](COMMON_CRAWL_USAGE.md) for usage

**Developers:**
- Read [../REFACTORED_STRUCTURE.md](../REFACTORED_STRUCTURE.md)
- Review [refactoring/FINAL_LAYOUT_README.md](refactoring/FINAL_LAYOUT_README.md)
- Check [TEST_SUITE_DOCUMENTATION.md](TEST_SUITE_DOCUMENTATION.md)

**Maintainers:**
- Review [refactoring/REFACTORING_INDEX.md](refactoring/REFACTORING_INDEX.md)
- Check [refactoring/POST_MIGRATION_GAPS.md](refactoring/POST_MIGRATION_GAPS.md)
- See [CRITICAL_FINDINGS.md](CRITICAL_FINDINGS.md)

## 📝 Documentation Standards

All documentation in this repository follows these standards:

- **Markdown format** for easy reading and version control
- **Clear headings** for easy navigation
- **Code examples** where applicable
- **Links** to related documentation
- **Status indicators** (✅, ⚠️, ⏳) for clarity

## 🔄 Keeping Documentation Updated

When making changes to the repository:

1. Update relevant documentation
2. Update links if files are moved
3. Add new documentation to this index
4. Keep the main [../README.md](../README.md) current

---

**Last Updated**: 2026-01-20  
**Status**: Documentation organized and indexed

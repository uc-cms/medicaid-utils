# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Fixed
- Correct LST_DIAG_CD docs, TAF subtype keys, and other review findings
- Fix TAF .df references, stale docstrings, and dependencies
- Fix broken README URLs, dead CMS links, and API examples
- Use Wayback Machine URLs for dead references in max_file.py
- Fix BETOS typo and add missing citation years in README

### Documentation
- Add 8 new Sphinx pages with examples and guides
- Switch to Furo theme with dark mode, copy buttons, and branding
- Clarify index_col accepts BENE_MSIS, BENE_ID, or MSIS_ID
- Fix BENE_MSIS docs, add cohort column reference, correct MAX RX info
- Add Google Search Console verification and sitemap
- Add maroon medical cross favicon
- Show all three ID options in overview table

### Changed
- Switch GitHub Pages to workflow deployment with manual trigger

## [v2.0.0]

### Added
- PyPI publishing, GitHub Pages deployment, and SEO metadata
- CITATION.cff for GitHub cite button and Google Scholar indexing
- Dask cluster setup guide and CMS data references
- Publications section with APA-formatted citations
- Comprehensive README with badges, license, and contributing section

### Fixed
- Python 3.13, dask 2026.x, pandas 3.0, and pyarrow 23 compatibility
- Handle ArrowInvalid with schema inference fallback in parquet export
- Remove fastparquet fallback, use pyarrow exclusively for parquet I/O
- Make holoviews import optional
- Resolve all flake8/pylint warnings and fix BETOS parsing
- Pandas 2.0 compatibility with type hints, doctests, and test suite
- Include requirements.txt and README.md in sdist for PyPI
- Resolve bugs, security issues, and CI gaps
- Upgrade GitHub Actions to Node.js 24 compatible versions and Python 3.13

### Documentation
- Reorganize Sphinx docs with tutorials and user guide

## [v1.0.0]

### Added
- ICD-10 codes for delivery outcomes in obgyn module
- Preconception care identification, SMM calculation, and delivery ICD codes
- Handle filing date in backlog TAF files
- Add missing_dob filter to TAF PS

### Fixed
- Add admitting diagnosis code column to cleaning routines
- Handle unknown divisions while repartitioning before export
- Increase partition size to reduce the number of divisions
- Fix export folder location in max_file
- Handle fully empty rows when computing dominant_boe_elg_grp
- Various column and reference fixes in filters
- Fix restricted benefits month aggregation in MAX preprocessing
- Handle discrepancies in OT_FIL_DT
- Change filing period cleaning logic for 2018 TAF files

### Changed
- Speed up beneficiary lookup when exporting cohort files using dask
- Move caching to within functions that modify datasets
- Assume MAX files are presorted during preprocessing

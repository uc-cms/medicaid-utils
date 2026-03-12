# medicaid-utils

A Python toolkit for constructing patient-level analytic files from Medicaid claims data. This package implements validated cleaning routines, variable construction methods, and public-domain clinical measure algorithms for both MAX (Medicaid Analytic eXtract) and TAF (Transformed Medicaid Statistical Information System) file formats.

Built on [Dask](https://www.dask.org/) for scalable, distributed processing of large-scale claims datasets.

**Documentation:** [https://uc-cms.github.io/medicaid-utils/](https://uc-cms.github.io/medicaid-utils/)

## Key Features

- **Dual-format support** — seamless handling of both MAX (ICD-9 era) and TAF (ICD-10 era) Medicaid claims data
- **Validated preprocessing** — standardized cleaning, deduplication, and variable construction for inpatient, outpatient, pharmacy, long-term care, and person summary files
- **Risk adjustment algorithms** — Elixhauser comorbidity scoring, CDPS-Rx pharmacy-based risk adjustment, BETOS procedure classification
- **Quality measurement** — ED and inpatient Prevention Quality Indicators (PQI), low-value care detection, NYU/Billings ED classification
- **Domain-specific modules** — covariates and outcomes from published Medicaid research on opioid use disorder (OUD) and obstetric/gynecologic care
- **Cohort extraction** — flexible patient-level filtering by diagnosis, procedure, prescription, and demographic criteria
- **External data integration** — NPI registry, HCRIS provider data, UDS health center data, FQHC lookups, geographic crosswalks (RUCA, RUCC, PCSA)
- **Scalable processing** — Dask-based distributed computing with intermediate result caching and configurable partitioning

## Installation

```bash
pip install medicaid-utils
```

Or install from source:

```bash
git clone https://github.com/uc-cms/medicaid-utils.git
cd medicaid-utils
pip install -e .
```

### Requirements

- Python >= 3.11
- Core: dask, pandas, numpy, pyarrow
- See `requirements.txt` for the full dependency list

## Expected Data Layout

The package expects Medicaid claim files to be stored as **Parquet** datasets, split by **year** and **state**, and **sorted by beneficiary ID** (BENE_MSIS or MSIS_ID). The folder hierarchy under your `data_root` must follow the structure below.

### MAX Files

```
data_root/
  medicaid/
    {YEAR}/
      {STATE}/
        max/
          ip/parquet/      # Inpatient claims
          ot/parquet/      # Outpatient claims
          ps/parquet/      # Person Summary
          cc/parquet/      # Chronic Conditions
```

Example: `data_root/medicaid/2012/WY/max/ip/parquet/`

### TAF Files

TAF claims are split into multiple subtypes per claim type:

```
data_root/
  medicaid/
    {YEAR}/
      {STATE}/
        taf/
          ip/                    # Inpatient
            iph/parquet/         #   Header (base)
            ipl/parquet/         #   Line
            ipoccr/parquet/      #   Occurrence codes
            ipdx/parquet/        #   Diagnosis codes
            ipndc/parquet/       #   NDC codes
          ot/                    # Outpatient (same subtypes: oth, otl, ...)
            oth/parquet/
            otl/parquet/
            otoccr/parquet/
            otdx/parquet/
            otndc/parquet/
          lt/                    # Long-Term Care (same subtypes: lth, ltl, ...)
            lth/parquet/
            ltl/parquet/
            ltoccr/parquet/
            ltdx/parquet/
            ltndc/parquet/
          rx/                    # Pharmacy
            rxh/parquet/         #   Header (base)
            rxl/parquet/         #   Line
            rxndc/parquet/       #   NDC codes
          de/                    # Demographics/Eligibility (Person Summary)
            debse/parquet/       #   Base demographics
            dedts/parquet/       #   Dates
            demc/parquet/        #   Managed care
            dedsb/parquet/       #   Disability
            demfp/parquet/       #   Money Follows the Person
            dewvr/parquet/       #   Waiver
            dehsp/parquet/       #   Home health/SPF
            dedxndc/parquet/     #   Diagnosis & NDC codes
```

Each Parquet dataset can be a single file or a directory of partitioned Parquet files. Files must be **pre-sorted by beneficiary ID** to enable efficient partition-level operations.

## Quick Start

### Loading and Cleaning Claims

```python
from medicaid_utils.preprocessing import max_ip, max_ot, max_ps

# Load and preprocess inpatient claims (cleaning + variable construction)
ip = max_ip.MAXIP(year=2012, state="WY", data_root="/path/to/data")

# Access the cleaned Dask DataFrame
df_ip = ip.df

# Load outpatient claims with IP overlap flagging
ot = max_ot.MAXOT(year=2012, state="WY", data_root="/path/to/data")
ot.flag_ip_overlaps_and_ed(df_ip)

# Load person summary with rural classification
ps = max_ps.MAXPS(year=2012, state="WY", data_root="/path/to/data")
```

TAF files follow the same pattern:

```python
from medicaid_utils.preprocessing import taf_ip, taf_ot, taf_ps

ip = taf_ip.TAFIP(year=2016, state="WY", data_root="/path/to/data")
ps = taf_ps.TAFPS(year=2016, state="WY", data_root="/path/to/data")
```

### Applying Risk Adjustment

```python
from medicaid_utils.adapted_algorithms.py_elixhauser import elixhauser_comorbidity

# Flag Elixhauser comorbidity groups on inpatient claims
df_ip = elixhauser_comorbidity.flag_comorbidities(ip.df, claim_type="max")
```

```python
from medicaid_utils.adapted_algorithms.py_cdpsmrx import cdps_rx_risk_adjustment

# Compute CDPS-Rx risk scores from pharmacy claims
df_risk = cdps_rx_risk_adjustment.cdps_rx_risk_adjust(
    df_rx, year=2012, index_col="MSIS_ID"
)
```

### Classifying Procedure Codes with BETOS

```python
from medicaid_utils.adapted_algorithms.py_betos import betos_proc_codes

# Get CPT-to-BETOS crosswalk and classify claims
df_ot = betos_proc_codes.assign_betos_cat(ot.df, year=2012)
```

### Identifying Preventable ED Visits

```python
from medicaid_utils.adapted_algorithms.py_ed_pqi import ed_pqi

# Flag potentially preventable ED visits
df_ed = ed_pqi.flag_potentially_preventable_ed_visits(ot.df, year=2012)
```

### Extracting Patient Cohorts

```python
from medicaid_utils.filters.patients import cohort_extraction

# Extract cohort by diagnosis codes and eligibility criteria
cohort = cohort_extraction.CohortExtraction(
    year=2012, state="WY", data_root="/path/to/data"
)
```

### Filtering Claims by Diagnosis or Procedure

```python
from medicaid_utils.filters.claims import dx_and_proc

# Flag claims with specific ICD codes
df_flagged = dx_and_proc.flag_diagnoses(
    ot.df, lst_dx_codes=["4939", "49390"], claim_type="max"
)
```

## Package Structure

```
medicaid_utils/
    preprocessing/       # File loading, cleaning, and variable construction
        max_file.py      #   Base class for MAX files
        max_ip.py        #   MAX Inpatient
        max_ot.py        #   MAX Outpatient
        max_ps.py        #   MAX Person Summary
        max_cc.py        #   MAX Chronic Conditions
        taf_file.py      #   Base class for TAF files
        taf_ip.py        #   TAF Inpatient
        taf_ot.py        #   TAF Outpatient
        taf_rx.py        #   TAF Pharmacy
        taf_ps.py        #   TAF Person Summary
        taf_lt.py        #   TAF Long-Term Care

    adapted_algorithms/  # Published clinical algorithms
        py_elixhauser/   #   Elixhauser comorbidity index
        py_cdpsmrx/      #   CDPS-Rx pharmacy risk adjustment
        py_betos/        #   BETOS procedure classification
        py_ed_pqi/       #   ED Prevention Quality Indicators
        py_ip_pqi/       #   Inpatient Prevention Quality Indicators
        py_nyu_billings/ #   NYU/Billings ED visit classification
        py_pmca/         #   Pediatric Medical Complexity Algorithm
        py_low_value_care/ # Low-value care measures

    filters/             # Claim and patient-level filtering
        claims/          #   Diagnosis, procedure, and prescription filters
        patients/        #   Cohort extraction utilities

    topics/              # Domain-specific research modules
        oud/             #   Opioid use disorder measures
        obgyn/           #   Obstetric/gynecologic outcomes

    other_datasets/      # External data integration
        hcris.py         #   HCRIS provider cost reports
        npi.py           #   NPI registry lookups
        uds.py           #   UDS health center data
        fqhc.py          #   FQHC provider data
        zip.py           #   Geographic crosswalks (RUCA, RUCC, PCSA)

    common_utils/        # Shared utilities
        dataframe_utils.py  # DataFrame operations and export
        recipes.py          # Common data transformations
        links.py            # Data linking utilities
        stats_utils.py      # Statistical functions
```

## Preprocessing Details

### What Cleaning Does

Each file type has tailored cleaning routines that run automatically (configurable via `clean=True`):

- **Date standardization** — converts date columns to consistent datetime types
- **Diagnosis code cleaning** — strips whitespace, normalizes formatting, handles ICD-9/10 differences
- **Procedure code cleaning** — validates procedure code systems (CPT, HCPCS, ICD)
- **Demographic derivation** — computes age, gender flags, and date-of-birth validation
- **Duplicate flagging** — identifies exact duplicate claims for exclusion
- **Encounter/capitation classification** — flags FFS, encounter, and capitation claims using PHP_TYPE and TYPE_CLM_CD

### What Preprocessing Adds

Additional derived variables computed via `preprocess=True`:

- **Payment calculation** — standardized payment amount from available payment fields
- **ED use flags** — emergency department utilization indicators
- **IP overlap detection** — flags outpatient claims that overlap with inpatient stays
- **Length of stay** — computed from admission and discharge dates
- **Eligibility patterns** — monthly enrollment strings and gap detection
- **Rural classification** — RUCA (Rural-Urban Commuting Area) or RUCC (Rural-Urban Continuum) codes via ZIP code crosswalk
- **Dual eligibility** — Medicare-Medicaid dual enrollment flags
- **Basis of eligibility** — categorization by eligibility group (aged, blind/disabled, child, adult)

### Caching

Intermediate results can be cached to disk to avoid recomputation:

```python
ip = max_ip.MAXIP(
    year=2012, state="WY", data_root="/path/to/data",
    tmp_folder="/path/to/cache"
)
```

## Adapted Algorithms

| Algorithm | Reference | Module |
|-----------|-----------|--------|
| Elixhauser Comorbidity Index | Elixhauser et al., 1998 | `py_elixhauser` |
| CDPS-Rx Risk Adjustment | Kronick et al., UC San Diego | `py_cdpsmrx` |
| BETOS Classification | CMS Berenson-Eggers Type of Service | `py_betos` |
| ED PQI | Davies et al., 2017 | `py_ed_pqi` |
| IP PQI | AHRQ Prevention Quality Indicators | `py_ip_pqi` |
| NYU/Billings ED Algorithm | Billings, Parikh, Mijanovich, 2000 | `py_nyu_billings` |
| PMCA | Simon et al., Seattle Children's | `py_pmca` |
| Low-Value Care | Charlesworth et al., JAMA Intern Med, 2016 | `py_low_value_care` |

## Topics Modules

The `topics` module packages covariates and outcome definitions developed as part of Medicaid data analyses that resulted in peer-reviewed publications:

- **OUD (Opioid Use Disorder)** — buprenorphine treatment detection (procedure codes and NDC), OUD medication flags, behavioral health service identification, care setting classification (FQHC, outpatient hospital, physician office), and co-occurring mental health conditions
- **OB/GYN** — delivery outcome identification, preterm birth flags, multiple birth detection, religious vs. secular provider classification, and chronic condition comorbidities (diabetes, hypertension, CKD, depression, COPD, tobacco use)

## Testing

```bash
# Run the full test suite
pytest tests/

# Run tests for a specific module
pytest tests/preprocessing/
pytest tests/adapted_algorithms/
```

## License

MIT License. See [LICENSE](LICENSE) for details.

## Authors

Research Computing Group, Biostatistics Laboratory, The University of Chicago

## Citation

If you use this package in your research, please cite the repository:

```
@software{medicaid_utils,
  author = {Research Computing Group, Biostatistics Laboratory, University of Chicago},
  title = {medicaid-utils: Python Toolkit for Medicaid Claims Data Analysis},
  url = {https://github.com/uc-cms/medicaid-utils}
}
```

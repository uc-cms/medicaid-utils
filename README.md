# medicaid-utils

[![CI](https://github.com/uc-cms/medicaid-utils/actions/workflows/pylint.yml/badge.svg)](https://github.com/uc-cms/medicaid-utils/actions/workflows/pylint.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://github.com/uc-cms/medicaid-utils/blob/master/LICENSE)
[![GitHub release](https://img.shields.io/github/v/release/uc-cms/medicaid-utils)](https://github.com/uc-cms/medicaid-utils/releases)
[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://uc-cms.github.io/medicaid-utils/)

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

## Setting Up a Dask Cluster

medicaid-utils uses [Dask](https://www.dask.org/) for distributed computation. All DataFrames in the package are lazy Dask DataFrames — operations are deferred until `.compute()` is called. To get the most out of the package, set up a Dask cluster before loading claims.

### Local Cluster (Single Machine)

For workstations with sufficient RAM (recommended: 64 GB+ for state-level data):

```python
from dask.distributed import Client, LocalCluster

# Create a local cluster with 8 workers, 8 GB each
cluster = LocalCluster(
    n_workers=8,
    threads_per_worker=1,    # 1 thread per worker avoids GIL contention with pandas
    memory_limit="8GB",
)
client = Client(cluster)
print(client.dashboard_link)  # Opens Dask dashboard for monitoring
```

### SLURM / HPC Cluster

For high-performance computing environments, use [dask-jobqueue](https://jobqueue.dask.org/):

```python
from dask_jobqueue import SLURMCluster
from dask.distributed import Client

cluster = SLURMCluster(
    cores=4,
    memory="32GB",
    processes=1,
    walltime="04:00:00",
    queue="standard",
)
cluster.scale(jobs=10)  # Request 10 SLURM jobs
client = Client(cluster)
```

### Without a Cluster

If no distributed client is created, Dask defaults to its **synchronous scheduler**, which processes partitions sequentially in the main thread. This works for small datasets or debugging but will be slow for full state-level claims. You can also use the threaded scheduler:

```python
import dask
dask.config.set(scheduler="threads")  # or "synchronous" for debugging
```

### Tips

- **Monitor progress**: The Dask dashboard (typically at `http://localhost:8787`) shows task progress, memory usage, and worker status
- **Memory management**: Use `tmp_folder` when loading claims to cache intermediate results to disk and reduce memory pressure
- **Partition size**: Aim for partitions of 50--200 MB each. The package handles partitioning automatically based on the input Parquet files

## CMS Data Dictionary References

For detailed documentation on the column names and coding schemes used in Medicaid claims data:

- **MAX documentation**: [CMS MAX General Information](https://www.cms.gov/data-research/statistics-trends-and-reports/medicaid-chip-enrollment-data/medicaid-analytic-extract-max-general-information)
- **TAF documentation**: [ResDAC TAF](https://resdac.org/cms-data/files/taf)
- **TAF data dictionary**: [TAF Research Variables](https://resdac.org/cms-data/variables?tid_2%5B%5D=62)
- **RUCA codes**: [USDA Rural-Urban Commuting Area Codes](https://www.ers.usda.gov/data-products/rural-urban-commuting-area-codes/)
- **RUCC codes**: [USDA Rural-Urban Continuum Codes](https://www.ers.usda.gov/data-products/rural-urban-continuum-codes/)

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
from medicaid_utils.adapted_algorithms.py_elixhauser.elixhauser_comorbidity import ElixhauserScoring

# Flag Elixhauser comorbidity groups on inpatient claims
# lst_diag_col_name: list of diagnosis column names in the DataFrame
lst_diag_cols = [col for col in ip.df.columns if col.startswith("DIAG_CD_")]
df_ip = ElixhauserScoring.flag_comorbidities(ip.df, lst_diag_cols, cms_format="MAX")
```

```python
from medicaid_utils.adapted_algorithms.py_cdpsmrx import cdps_rx_risk_adjustment

# Compute CDPS-Rx risk scores from a DataFrame with diagnosis and NDC columns
df_risk = cdps_rx_risk_adjustment.cdps_rx_risk_adjust(df_rx)
```

### Classifying Procedure Codes with BETOS

```python
from medicaid_utils.adapted_algorithms.py_betos import betos_proc_codes

# Get CPT-to-BETOS crosswalk and classify claims
df_ot = betos_proc_codes.assign_betos_cat(ot.df, year=2012)
```

### Identifying Preventable ED Visits

```python
from medicaid_utils.adapted_algorithms.py_ed_pqi.ed_pqi import EDPreventionQualityIndicators

# Flag potentially preventable ED visits (requires ED claims and person summary)
df_pqi = EDPreventionQualityIndicators.flag_potentially_preventable_ed_visits(
    df_ed=ot.df, df_ps=ps.df
)
```

### Extracting Patient Cohorts

```python
from medicaid_utils.filters.patients.cohort_extraction import extract_cohort

# Define diagnosis codes (ICD-9 and ICD-10 prefixes)
dct_codes = {
    "diag_codes": {"diabetes_t2": {"incl": {9: ["250"], 10: ["E11"]}}},
    "proc_codes": {},
}

# Define filters and paths
dct_filters = {"cohort": {"ip": {"missing_dob": 0}}, "export": {}}
dct_paths = {"source_root": "/path/to/data", "export_folder": "/output/cohort/"}

# Extract and export cohort claim files
extract_cohort(
    state="WY", lst_year=[2012], dct_diag_proc_codes=dct_codes,
    dct_filters=dct_filters, lst_types_to_export=["ip", "ot", "ps"],
    dct_data_paths=dct_paths, cms_format="MAX",
)
```

### Filtering Claims by Diagnosis or Procedure

```python
from medicaid_utils.filters.claims import dx_and_proc

# Flag claims matching ICD-9 diagnosis codes
df_flagged = dx_and_proc.flag_diagnoses_and_procedures(
    dct_diag_codes={"asthma": {"incl": {9: ["4939", "49390"]}}},
    dct_proc_codes={},
    df_claims=ot.df,
    cms_format="MAX",
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

## Publications

Dataset generation processes developed as part of the following Medicaid research publications led to the creation of this package:

1. Liu, A., Hernandez, V., Stulberg, D., Schumm, P., Murugesan, M., McHugh, A., & Dude, A. (2025). Short-interval pregnancy following delivery in Catholic-affiliated versus non-Catholic-affiliated hospitals among patients insured through the Medicaid program. *Perspectives on Sexual and Reproductive Health*, *57*(3), 321–328. https://doi.org/10.1111/psrh.70021

2. Wan, W., Murugesan, M., Nocon, R. S., Bolton, J., Konetzka, R. T., Chin, M. H., & Huang, E. S. (2024). Comparison of two propensity score-based methods for balancing covariates: The overlap weighting and fine stratification methods in real-world claims data. *BMC Medical Research Methodology*, *24*(1), 122. https://doi.org/10.1186/s12874-024-02228-z

3. Volerman, A., Carlson, B., Wan, W., Murugesan, M., Asfour, N., Bolton, J., Chin, M. H., Sripipatana, A., & Nocon, R. S. (2024). Utilization, quality, and spending for pediatric Medicaid enrollees with primary care in health centers vs non-health centers. *BMC Pediatrics*, *24*(1), 100. https://doi.org/10.1186/s12887-024-04547-y

4. Liu, A., Hernandez, V., Dude, A., Schumm, L. P., Murugesan, M., McHugh, A., & Stulberg, D. B. (2024). Racial and ethnic disparities in short interval pregnancy following delivery in Catholic vs non-Catholic hospitals among California Medicaid enrollees. *Contraception*, *131*, 110308. https://doi.org/10.1016/j.contraception.2023.110308

5. Timtim, E., Murugesan, M., Blair, M. P., & Rodriguez, S. H. (2023). Association of health insurance status with severity and treatment among infants with retinopathy of prematurity. *Journal of Pediatric Ophthalmology and Strabismus*, *60*(6), e75–e78. https://doi.org/10.3928/01913913-20231026-01

6. Peterson, L., Murugesan, M., Nocon, R., Hoang, H., Bolton, J., Laiteerapong, N., Pollack, H., & Marsh, J. (2022). Health care use and spending for Medicaid patients diagnosed with opioid use disorder receiving primary care in Federally Qualified Health Centers and other primary care settings. *PLoS ONE*, *17*(10), e0276066. https://doi.org/10.1371/journal.pone.0276066

7. Knitter, A. C., Murugesan, M., Saulsberry, L., Wan, W., Nocon, R. S., Huang, E. S., Bolton, J., Chin, M. H., & Laiteerapong, N. (2022). Quality of care for US adults with Medicaid insurance and type 2 diabetes in Federally Qualified Health Centers compared with other primary care settings. *Medical Care*, *60*(11), 813–820. https://doi.org/10.1097/MLR.0000000000001766

8. Caldwell, A., Schumm, P., Murugesan, M., & Stulberg, D. (2022). Short-interval pregnancy in the Illinois Medicaid population following delivery in Catholic vs non-Catholic hospitals. *Contraception*, *112*, 105–110. https://doi.org/10.1016/j.contraception.2022.02.009

9. Dude, A. M., Schueler, K., Schumm, L. P., Murugesan, M., & Stulberg, D. B. (2022). Preconception care and severe maternal morbidity in the United States. *American Journal of Obstetrics & Gynecology MFM*, *4*(2), 100549. https://doi.org/10.1016/j.ajogmf.2021.100549

## Contributing

Contributions are welcome! Please open an [issue](https://github.com/uc-cms/medicaid-utils/issues) or submit a pull request.

```bash
# Set up development environment
git clone https://github.com/uc-cms/medicaid-utils.git
cd medicaid-utils
pip install -e .
pip install pylint pytest

# Run tests
pytest tests/

# Run linter
pylint medicaid_utils
```

## License

MIT License. See [LICENSE](LICENSE) for details.

## Authors

Research Computing Group, Biostatistics Laboratory, The University of Chicago

## Citation

If you use this package in your research, please cite the repository using the "Cite this repository" button on GitHub, or use:

```bibtex
@software{medicaid_utils,
  author = {Research Computing Group, Biostatistics Laboratory, University of Chicago},
  title = {medicaid-utils: Python Toolkit for Medicaid Claims Data Analysis},
  url = {https://github.com/uc-cms/medicaid-utils},
  license = {MIT}
}
```

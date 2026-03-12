CMS Data Dictionary References
===============================

For detailed documentation on the column names, coding schemes, and file layouts used in
Medicaid claims data, refer to the official CMS and ResDAC resources below.

Medicaid Claims File Documentation
------------------------------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Resource
     - URL
   * - MAX General Information
     - https://www.cms.gov/data-research/statistics-trends-and-reports/medicaid-chip-enrollment-data/medicaid-analytic-extract-max-general-information
   * - TAF Documentation (ResDAC)
     - https://resdac.org/cms-data/files/taf
   * - TAF Research Variables
     - https://resdac.org/cms-data/variables?tid_2%5B%5D=62
   * - ResDAC Data Documentation
     - https://resdac.org/cms-data

Geographic Classification
--------------------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Classification
     - URL
   * - RUCA (Rural-Urban Commuting Area)
     - https://www.ers.usda.gov/data-products/rural-urban-commuting-area-codes/
   * - RUCC (Rural-Urban Continuum Codes)
     - https://www.ers.usda.gov/data-products/rural-urban-continuum-codes/
   * - FIPS County Codes
     - https://www.census.gov/library/reference/code-lists/ansi.html
   * - SSA State/County Codes
     - https://www.nber.org/research/data/ssa-federal-information-processing-series-fips-state-and-county-crosswalk

Provider Data
-------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Dataset
     - URL
   * - NPI Registry
     - https://npiregistry.cms.hhs.gov/
   * - NPPES Data Dissemination
     - https://download.cms.gov/nppes/NPI_Files.html
   * - HCRIS (Cost Reports)
     - https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Cost-Reports
   * - UDS (Uniform Data System)
     - https://data.hrsa.gov/tools/data-reporting/program-data
   * - Provider of Services (POS)
     - https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/provider-of-services-file-hospital-non-hospital-facilities

Column Name Conventions
-----------------------

**MAX files** use column names like:

- ``DIAG_CD_1`` through ``DIAG_CD_9`` -- diagnosis codes
- ``PRCDR_CD_1`` through ``PRCDR_CD_6`` -- procedure codes
- ``PRCDR_CD_SYS_1`` through ``PRCDR_CD_SYS_6`` -- procedure coding system (1=CPT, 6=ICD-9, 7=ICD-10-PCS)
- ``SRVC_BGN_DT``, ``SRVC_END_DT`` -- service dates
- ``ADMSN_DT``, ``DSCHRG_DT`` -- admission/discharge dates (IP)
- ``RCPNT_DLVRY_CD`` -- delivery recipient code

**TAF files** use column names like:

- ``DGNS_CD_1`` through ``DGNS_CD_12`` -- diagnosis codes
- ``ADMTG_DGNS_CD`` -- admitting diagnosis
- ``PRCDR_CD_1`` through ``PRCDR_CD_6`` -- procedure codes
- ``LINE_PRCDR_CD`` -- line-level procedure code
- ``SRVC_BGN_DT``, ``SRVC_END_DT`` -- service dates
- ``NDC`` -- National Drug Code (pharmacy claims)
- ``DAYS_SUPPLY`` -- prescription days of supply

The package automatically detects MAX vs TAF column naming conventions based on the
``cms_format`` parameter (``"MAX"`` or ``"TAF"``).

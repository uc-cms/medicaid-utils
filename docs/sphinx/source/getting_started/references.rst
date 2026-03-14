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
   * - MAX General Information (ResDAC)
     - https://resdac.org/cms-data/files/max-ip
   * - TAF Documentation (ResDAC)
     - https://resdac.org/cms-data/files/taf-ip
   * - TAF Research Variables (ResDAC)
     - https://resdac.org/
   * - ResDAC Data Documentation
     - https://resdac.org/

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

For column name conventions, refer to the ResDAC documentation linked above. The package
automatically detects MAX vs TAF column naming conventions based on the ``cms_format``
parameter (``"MAX"`` or ``"TAF"``).

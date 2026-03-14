Glossary & Column Reference
===========================

CMS terminology, acronyms, and column name conventions used throughout medicaid-utils.

Acronyms
--------

========== ===================================================================
Acronym    Full Name
========== ===================================================================
**CMS**    Centers for Medicare & Medicaid Services
**MAX**    Medicaid Analytic eXtract (pre-2016 file format)
**TAF**    T-MSIS Analytic Files (2016+ file format)
**T-MSIS** Transformed Medicaid Statistical Information System
**ICD-9**  International Classification of Diseases, 9th Revision
**ICD-10** International Classification of Diseases, 10th Revision
**CPT**    Current Procedural Terminology
**HCPCS**  Healthcare Common Procedure Coding System
**NDC**    National Drug Code
**NPI**    National Provider Identifier
**FQHC**   Federally Qualified Health Center
**FFS**    Fee-For-Service
**RUCA**   Rural-Urban Commuting Area
**RUCC**   Rural-Urban Continuum Code
**PCSA**   Primary Care Service Area
**ZCTA**   ZIP Code Tabulation Area
**PQI**    Prevention Quality Indicator
**BETOS**  Berenson-Eggers Type of Service
**CDPS**   Chronic Illness and Disability Payment System
**PMCA**   Pediatric Medical Complexity Algorithm
**OUD**    Opioid Use Disorder
**MAT**    Medication-Assisted Treatment
**HCRIS**  Healthcare Cost Report Information System
**UDS**    Uniform Data System
**ResDAC** Research Data Assistance Center
========== ===================================================================

MAX Column Names
----------------

=============================== =============================================
Column                          Description
=============================== =============================================
``MSIS_ID``                     Beneficiary ID
``DIAG_CD_1`` – ``DIAG_CD_9``  Diagnosis codes
``PRCDR_CD_1`` – ``PRCDR_CD_6`` Procedure codes
``PRCDR_CD_SYS_1`` – ``PRCDR_CD_SYS_6`` Procedure coding system (1=CPT, 6=ICD-9, 7=ICD-10-PCS)
``SRVC_BGN_DT``, ``SRVC_END_DT`` Service begin/end dates
``ADMSN_DT``, ``DSCHRG_DT``    Admission/discharge dates (IP)
``RCPNT_DLVRY_CD``              Delivery recipient code
``PLC_OF_SRVC_CD``              Place of service code
``EL_RSDNC_ZIP_CD_LTST``       Beneficiary ZIP code (PS)
=============================== =============================================

TAF Column Names
----------------

=============================== =============================================
Column                          Description
=============================== =============================================
``BENE_MSIS``                   Beneficiary ID
``DGNS_CD_1`` – ``DGNS_CD_12`` Diagnosis codes
``ADMTG_DGNS_CD``               Admitting diagnosis
``PRCDR_CD_1`` – ``PRCDR_CD_6`` Procedure codes
``LINE_PRCDR_CD``               Line-level procedure code
``SRVC_BGN_DT``, ``SRVC_END_DT`` Service begin/end dates
``NDC``                         National Drug Code (pharmacy claims)
``DAYS_SUPPLY``                 Prescription days of supply
``BENE_ZIP_CD``                 Beneficiary ZIP code (DE)
=============================== =============================================

Derived Columns
---------------

Columns added by medicaid-utils during preprocessing or algorithm execution:

============================== =============================================
Column                         Description
============================== =============================================
``LST_DIAG_CD``                Comma-separated list of all diagnosis codes per beneficiary. MAX: created during preprocessing. TAF: created by calling ``gather_bene_level_diag_ndc_codes()``
``LST_NDC``                    Comma-separated list of all NDC codes per beneficiary. MAX: created during preprocessing. TAF: created by calling ``gather_bene_level_diag_ndc_codes()``
``ed_use``                     1 if claim is an ED visit (any criterion)
``ed_cpt``                     1 if ED identified via CPT codes (99281–99285)
``ed_ub92``                    1 if ED identified via UB-92 revenue codes
``ed_tos``                     1 if ED identified via Type of Service
``ed_pos``                     1 if ED identified via Place of Service
``excl_missing_dob``           1 if date of birth is missing (filter key: ``missing_dob``)
``excl_duplicated``            1 if claim is a duplicate (filter key: ``duplicated``)
``ELX_GRP_1`` – ``ELX_GRP_31`` Elixhauser comorbidity group flags
============================== =============================================

.. note::

   Filter keys (used in ``dct_filters``) omit the ``excl_`` prefix. For example,
   ``{"ip": {"missing_dob": 0}}`` filters on the ``excl_missing_dob`` column.

CMS Data Resources
------------------

- `ResDAC MAX Inpatient <https://resdac.org/cms-data/files/max-ip>`_ — MAX file documentation
- `ResDAC TAF Inpatient <https://resdac.org/cms-data/files/taf-ip>`_ — TAF file documentation
- `ResDAC <https://resdac.org/>`_ — Research Data Assistance Center
- `Census FIPS Codes <https://www.census.gov/library/reference/code-lists/ansi.html>`_
- `NBER SSA-FIPS Crosswalk <https://www.nber.org/research/data/ssa-federal-information-processing-series-fips-state-and-county-crosswalk>`_

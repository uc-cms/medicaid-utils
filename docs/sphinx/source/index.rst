medicaid-utils: Python Toolkit for Medicaid Claims Data Analysis
=================================================================

.. meta::
   :description: Open-source Python toolkit for analyzing Medicaid claims data from CMS. Preprocessing, cohort extraction, risk adjustment, and quality measures for MAX and TAF files. Built on Dask for scalable health services research.
   :keywords: medicaid, claims data, CMS, TAF, MAX, health services research, observational study, cohort extraction, risk adjustment, elixhauser, python, dask, epidemiology, healthcare analytics, medicaid analytic extract

**medicaid-utils** is an open-source Python toolkit for constructing patient-level
analytic files from Medicaid claims data. It implements validated cleaning routines,
variable construction methods, and public-domain clinical algorithms for both
MAX (Medicaid Analytic eXtract) and TAF (Transformed Medicaid Statistical Information
System) file formats published by the Centers for Medicare & Medicaid Services (CMS).

Built on `Dask <https://www.dask.org/>`_ for scalable, distributed processing of
large-scale claims datasets — from single-state analyses to multi-state observational
studies.

.. code-block:: bash

   pip install medicaid-utils

Why medicaid-utils?
-------------------

Working with Medicaid claims data involves repetitive, error-prone preprocessing
that every research team reimplements from scratch: cleaning diagnosis codes,
constructing enrollment windows, applying risk adjustment algorithms, and building
cohorts from millions of claims. **medicaid-utils** packages these validated routines
so researchers can focus on their study design rather than data plumbing.

- **Dual-format support** — seamless handling of both MAX (ICD-9 era) and TAF (ICD-10 era) Medicaid claims data
- **Validated preprocessing** — standardized cleaning, deduplication, and variable construction for inpatient, outpatient, pharmacy, long-term care, and person summary files
- **8 clinical algorithms** — Elixhauser comorbidity scoring, CDPS-Rx risk adjustment, BETOS classification, Prevention Quality Indicators, NYU/Billings ED classification, PMCA, and low-value care measures
- **Flexible cohort extraction** — filter patients by diagnosis codes, procedure codes, prescriptions, and demographic criteria across claim types
- **Scalable** — Dask-based distributed computing handles state-level and multi-state datasets on laptops, workstations, and HPC clusters

Getting Started
---------------

Load and clean inpatient claims in three lines:

.. code-block:: python

   from medicaid_utils.preprocessing import max_ip

   ip = max_ip.MAXIP(year=2012, state="WY", data_root="/path/to/data")
   df_ip = ip.df  # cleaned Dask DataFrame, ready for analysis

Extract a Type 2 diabetes cohort:

.. code-block:: python

   from medicaid_utils.filters.patients.cohort_extraction import extract_cohort

   extract_cohort(
       state="WY", lst_year=[2012],
       dct_diag_proc_codes={
           "diag_codes": {"diabetes_t2": {"incl": {9: ["250"], 10: ["E11"]}}},
           "proc_codes": {},
       },
       dct_filters={"cohort": {"ip": {"missing_dob": 0}}, "export": {}},
       lst_types_to_export=["ip", "ot", "ps"],
       dct_data_paths={"source_root": "/data", "export_folder": "/output/"},
       cms_format="MAX",
   )

Apply Elixhauser comorbidity scoring:

.. code-block:: python

   from medicaid_utils.adapted_algorithms.py_elixhauser.elixhauser_comorbidity import ElixhauserScoring

   lst_diag_cols = [col for col in ip.df.columns if col.startswith("DIAG_CD_")]
   df_scored = ElixhauserScoring.flag_comorbidities(ip.df, lst_diag_cols, cms_format="MAX")

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   getting_started/installation
   getting_started/data_layout
   getting_started/quickstart
   getting_started/references

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   user_guide/preprocessing
   user_guide/cohort_extraction
   user_guide/risk_adjustment
   user_guide/quality_measures

.. toctree::
   :maxdepth: 2
   :caption: Tutorials

   tutorials/diabetes_cohort
   tutorials/oud_cohort

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   medicaid_utils


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

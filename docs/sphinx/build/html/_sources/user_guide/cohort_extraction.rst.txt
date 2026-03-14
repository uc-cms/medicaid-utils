Cohort Extraction
=================

The cohort extraction module is the primary tool for building patient-level analytic files.
It identifies patients matching diagnosis/procedure criteria, applies inclusion/exclusion
filters, and exports the resulting claim files.

Overview
--------

A typical cohort extraction workflow:

1. **Define diagnosis/procedure codes** that identify your condition of interest
2. **Define filters** for inclusion criteria (age, gender, enrollment)
3. **Call** :func:`~medicaid_utils.filters.patients.cohort_extraction.extract_cohort`
4. **Receive** exported claim files for the matching cohort

The ``extract_cohort`` Function
-------------------------------

.. code-block:: python

   from medicaid_utils.filters.patients.cohort_extraction import extract_cohort

   extract_cohort(
       state="AL",
       lst_year=[2016, 2017, 2018],
       dct_diag_proc_codes=dct_codes,
       dct_filters=dct_filters,
       lst_types_to_export=["ip", "ot", "ps"],
       dct_data_paths=dct_paths,
       cms_format="TAF",
   )

This function:

- Loads IP, OT, PS (and optionally RX) claims for each year
- Applies cohort filters to narrow the population
- Identifies patients with the specified diagnoses/procedures
- Exports filtered claim files for the matching cohort

Defining Diagnosis and Procedure Codes
---------------------------------------

The ``dct_diag_proc_codes`` dictionary supports three types of criteria:

Diagnosis Codes
^^^^^^^^^^^^^^^

Use ICD-9 and/or ICD-10 codes with inclusion and exclusion logic:

.. code-block:: python

   dct_codes = {
       "diag_codes": {
           "diabetes_t2": {
               "incl": {
                   9: ["250"],       # ICD-9 prefix
                   10: ["E11"],      # ICD-10 prefix
               },
               "excl": {
                   9: ["25001", "25003", "25011", "25013",
                       "25021", "25023", "25031", "25033",
                       "25041", "25043", "25051", "25053",
                       "25061", "25063", "25071", "25073",
                       "25081", "25083", "25091", "25093"],
                   10: ["E10"],      # Exclude Type 1
               },
           },
       },
       "proc_codes": {},
   }

Codes are matched using **prefix matching** -- ``"250"`` matches ``"2500"``, ``"25000"``,
``"25002"``, etc. Exclusion codes take precedence over inclusion codes.

Procedure Codes
^^^^^^^^^^^^^^^

Procedure codes are keyed by procedure coding system:

.. code-block:: python

   dct_codes = {
       "diag_codes": {},
       "proc_codes": {
           "methadone": {
               7: [  # ICD-10-PCS (system code 7)
                   "HZ81ZZZ", "HZ84ZZZ", "HZ85ZZZ", "HZ86ZZZ",
                   "HZ91ZZZ", "HZ94ZZZ", "HZ95ZZZ", "HZ96ZZZ",
               ],
           },
       },
   }

Common procedure system codes:

- ``1`` -- CPT/HCPCS
- ``6`` -- ICD-9-CM procedure
- ``7`` -- ICD-10-PCS

Column Value Conditions
^^^^^^^^^^^^^^^^^^^^^^^

You can also flag patients based on specific column values:

.. code-block:: python

   dct_codes = {
       "diag_codes": {},
       "proc_codes": {},
       "column_values": {
           "diag_delivery": {
               "RCPNT_DLVRY_CD": [1],  # Delivery recipient code = 1
           },
       },
   }

Defining Filters
----------------

Filters control which claims and patients are included. The filter dictionary has two
sections: ``cohort`` (applied before cohort identification) and ``export`` (applied when
exporting claim files).

.. code-block:: python

   dct_filters = {
       "cohort": {
           "ip": {
               "missing_dob": 0,                          # Exclude missing DOB
               "range_numeric_age_prncpl_proc": (18, 64),  # Age 18-64
           },
           "ot": {
               "missing_dob": 0,
               "range_numeric_age_srvc_bgn": (18, 64),
           },
       },
       "export": {
           "ip": {
               "missing_dob": 0,
           },
       },
   }

Filter Types
^^^^^^^^^^^^

**Range filters** restrict to a numeric or date range:

.. code-block:: python

   # Numeric range (inclusive on both ends)
   "range_numeric_age_srvc_bgn": (18, 64)

   # Date range (YYYYMMDD format)
   "range_date_srvc_bgn_date": ("20160101", "20181231")

**Exclusion filters** remove patients with a positive exclusion flag:

.. code-block:: python

   # Exclude female patients (excl_female column = 1)
   "excl_female": 1

**Column value filters** restrict to rows with a specific value:

.. code-block:: python

   # Keep only claims where missing_dob = 0
   "missing_dob": 0

Data Paths
----------

.. code-block:: python

   dct_paths = {
       "source_root": "/data/cms/",          # Where raw claim files live
       "export_folder": "/output/my_cohort/", # Where to write results
       "tmp_folder": "/tmp/cohort_cache/",    # Optional intermediate cache
   }

Output Files
------------

After extraction, the export folder contains:

- ``cohort_{STATE}.csv`` -- patient-level file with condition flags, inclusion indicator,
  and date of birth
- ``cohort_{STATE}_{YEAR}.csv`` -- year-specific patient file
- ``cohort_exclusions_{TYPE}_{STATE}_{YEAR}.parquet`` -- filter statistics showing how
  many claims were removed at each step
- Exported claim files in the requested format (CSV or Parquet)

Cohort File Columns
^^^^^^^^^^^^^^^^^^^

The ``cohort_{STATE}_{YEAR}.csv`` file (indexed by ``BENE_MSIS``) contains:

================================= ====================================================
Column Pattern                    Description
================================= ====================================================
``include``                       1 if patient included in final cohort, 0 if excluded
``YEAR``                          Claim year
``STATE_CD``                      State code
``birth_date``                    Date of birth (merged from PS)
``{type}_diag_{condition}``       1 if condition found in claim type (e.g., ``ip_diag_diabetes_t2``)
``{type}_diag_{condition}_date``  Date of first occurrence
``{type}_proc_{procedure}``       1 if procedure found in claim type
``{type}_proc_{procedure}_date``  Date of first occurrence
``{type}_diag_condn``             1 if ANY diagnosis condition matched
``{type}_proc_condn``             1 if ANY procedure condition matched
================================= ====================================================

Where ``{type}`` is the claim type (``ip``, ``ot``, ``ot_line``), ``{condition}`` comes
from your ``dct_diag_codes`` keys, and ``{procedure}`` comes from your ``dct_proc_codes`` keys.

Lower-Level Functions
---------------------

For more control, you can use the building blocks directly:

Flagging Diagnoses and Procedures
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from medicaid_utils.filters.claims import dx_and_proc

   # Flag claims with specific diagnosis codes
   df_flagged = dx_and_proc.flag_diagnoses_and_procedures(
       dct_diag_codes={"diabetes_t2": {"incl": {10: ["E11"]}}},
       dct_proc_codes={},
       df_claims=ip.dct_files["base"],
       cms_format="TAF",
   )

   # Get patient IDs with conditions
   pdf_patients, dct_stats = dx_and_proc.get_patient_ids_with_conditions(
       dct_diag_codes={"diabetes_t2": {"incl": {10: ["E11"]}}},
       dct_proc_codes={},
       cms_format="TAF",
       ip=ip.dct_files["base"],
       ot=ot.dct_files["base"],
   )

Flagging Prescriptions
^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from medicaid_utils.filters.claims import rx as rx_filter

   # Flag pharmacy claims by NDC codes
   df_rx_flagged = rx_filter.flag_prescriptions(
       dct_ndc_codes={
           "metformin": ["00093727801", "00093727901"],
       },
       df_claims=rx_claims.dct_files["base"],
   )

Filtering Claim Files
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from medicaid_utils.filters.patients.cohort_extraction import filter_claim_files

   claim, filter_stats = filter_claim_files(
       claim=ip,
       dct_claim_filters={"ip": {"missing_dob": 0}},
       tmp_folder="/tmp/cache",
   )

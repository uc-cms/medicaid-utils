Tutorial: Extracting an Opioid Use Disorder (OUD) Cohort
=========================================================

This tutorial demonstrates extracting a cohort of Medicaid patients diagnosed with opioid
use disorder, including medication-assisted treatment (MAT) identification.

Scenario
--------

You want to study treatment patterns among Medicaid enrollees with OUD, including
buprenorphine prescriptions and behavioral health service utilization.

Step 1: Define OUD Diagnosis and Procedure Codes
--------------------------------------------------

OUD is identified through ICD diagnosis codes and medication-assisted treatment procedure
codes:

.. code-block:: python

   dct_codes = {
       "diag_codes": {
           "oud": {
               "incl": {
                   9: [
                       "3040",   # Opioid type dependence
                       "3047",   # Combinations of opioid with other drug dependence
                       "3055",   # Nondependent opioid abuse
                   ],
                   10: [
                       "F1110", "F1111", "F1112", "F1113", "F1114",
                       "F1115", "F1118", "F1119",   # Opioid abuse
                       "F1120", "F1121", "F1122", "F1123", "F1124",
                       "F1125", "F1128", "F1129",   # Opioid dependence
                   ],
               },
           },
       },
       "proc_codes": {
           "mat_icd10pcs": {
               7: [  # ICD-10-PCS
                   "HZ81ZZZ", "HZ84ZZZ", "HZ85ZZZ", "HZ86ZZZ",
                   "HZ91ZZZ", "HZ94ZZZ", "HZ95ZZZ", "HZ96ZZZ",
               ],
           },
       },
   }

Step 2: Define Filters
-----------------------

.. code-block:: python

   dct_filters = {
       "cohort": {
           "ip": {
               "missing_dob": 0,
               "duplicated": 0,
           },
           "ot": {
               "missing_dob": 0,
               "duplicated": 0,
           },
       },
       "export": {},
   }

Step 3: Extract the Cohort
---------------------------

.. code-block:: python

   from medicaid_utils.filters.patients.cohort_extraction import extract_cohort

   extract_cohort(
       state="IL",
       lst_year=[2016, 2017, 2018],
       dct_diag_proc_codes=dct_codes,
       dct_filters=dct_filters,
       lst_types_to_export=["ip", "ot", "ps", "rx"],
       dct_data_paths={
           "source_root": "/data/cms/",
           "export_folder": "/output/oud_cohort/",
       },
       cms_format="TAF",
       clean_exports=True,
       preprocess_exports=True,
       export_format="parquet",
   )

Step 4: Flag Buprenorphine Prescriptions
-----------------------------------------

After extraction, use the pharmacy claims to identify buprenorphine treatment:

.. code-block:: python

   from medicaid_utils.filters.claims import rx

   # Load exported RX claims as a Dask DataFrame
   import dask.dataframe as dd
   df_rx = dd.read_parquet("/output/oud_cohort/rx/")

   # Define buprenorphine NDC codes (subset shown)
   dct_ndc = {
       "buprenorphine": [
           "00378451905", "00378451993", "00378617005", "00378617077",
           # ... full NDC list from your formulary reference
       ],
   }

   df_rx_flagged = rx.flag_prescriptions(dct_ndc_codes=dct_ndc, df_claims=df_rx)

Step 5: Flag Behavioral Health Services
-----------------------------------------

Use the OUD topics module for additional variable construction:

.. code-block:: python

   from medicaid_utils.topics.oud import medication_and_behavioral_health as mbh

   # Flag behavioral health treatment in outpatient claims
   df_ot = dd.read_parquet("/output/oud_cohort/ot/")
   df_ot_bh = mbh.flag_proc_behavioral_health_trtmt(df_ot)

Step 6: Identify FQHC Providers
---------------------------------

Identify which patients received care at Federally Qualified Health Centers:

.. code-block:: python

   from medicaid_utils.other_datasets import fqhc

   # Get FQHC NPI crosswalk for identifying FQHC providers
   pdf_fqhc_crosswalk = fqhc.get_fqhc_crosswalk(start_year=2016)

Combining with Diagnosis-Only Extraction
-----------------------------------------

If you only want to identify OUD patients (without procedure codes), simplify the
diagnosis dictionary:

.. code-block:: python

   dct_codes_dx_only = {
       "diag_codes": {
           "oud": {
               "incl": {
                   10: ["F11"],  # All F11* codes (opioid-related disorders)
               },
           },
       },
       "proc_codes": {},
   }

IP-Only Diagnosis Restriction
------------------------------

To restrict diagnosis identification to inpatient claims only (more conservative definition):

.. code-block:: python

   extract_cohort(
       state="IL",
       lst_year=[2016, 2017, 2018],
       dct_diag_proc_codes=dct_codes,
       dct_filters=dct_filters,
       lst_types_to_export=["ip", "ot", "ps", "rx"],
       dct_data_paths=dct_paths,
       restrict_dx_proc_to_ip=True,   # Only look for OUD codes in IP claims
       cms_format="TAF",
   )

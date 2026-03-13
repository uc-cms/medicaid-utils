Tutorial: Extracting a Type 2 Diabetes Cohort
==============================================

This tutorial walks through extracting a cohort of adult Medicaid patients with Type 2
diabetes, including their inpatient, outpatient, and person summary files.

Scenario
--------

You want to study healthcare utilization among adult (18--64) Medicaid enrollees with
Type 2 diabetes in Alabama using TAF data from 2016--2018.

Step 1: Define Diagnosis Codes
------------------------------

Type 2 diabetes maps to ICD-10 code ``E11`` (and subcodes). We exclude Type 1 diabetes
(``E10``) to avoid misclassification.

For ICD-9 (if using MAX data or cross-era studies), Type 2 diabetes uses prefix ``250``
with even fifth digits (e.g., ``25000``, ``25002``), while Type 1 uses odd fifth digits
(``25001``, ``25003``).

.. code-block:: python

   dct_codes = {
       "diag_codes": {
           "diabetes_t2": {
               "incl": {
                   9: ["250"],
                   10: ["E11"],
               },
               "excl": {
                   9: [
                       "25001", "25003", "25011", "25013",
                       "25021", "25023", "25031", "25033",
                       "25041", "25043", "25051", "25053",
                       "25061", "25063", "25071", "25073",
                       "25081", "25083", "25091", "25093",
                   ],
                   10: ["E10"],
               },
           },
       },
       "proc_codes": {},
   }

Step 2: Define Inclusion Filters
---------------------------------

Restrict to adults aged 18--64 with valid demographics:

.. code-block:: python

   dct_filters = {
       "cohort": {
           "ip": {
               "missing_dob": 0,
               "duplicated": 0,
               "range_numeric_age_prncpl_proc": (18, 64),
           },
           "ot": {
               "missing_dob": 0,
               "duplicated": 0,
               "range_numeric_age_srvc_bgn": (18, 64),
           },
       },
       "export": {},
   }

**Filter explanations:**

- ``missing_dob: 0`` -- keep only claims where date of birth is not missing
- ``duplicated: 0`` -- exclude duplicate claims
- ``range_numeric_age_*: (18, 64)`` -- restrict to ages 18--64 inclusive, computed as of
  the principal procedure date (IP) or service begin date (OT)

Step 3: Set Up Data Paths
--------------------------

.. code-block:: python

   dct_paths = {
       "source_root": "/data/cms/",
       "export_folder": "/output/diabetes_cohort/",
   }

Step 4: Run the Extraction
--------------------------

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
       clean_exports=True,
       preprocess_exports=True,
       export_format="parquet",
   )

Step 5: What Gets Exported
---------------------------

After running, ``/output/diabetes_cohort/`` will contain:

.. code-block:: text

   diabetes_cohort/
     cohort_AL.csv              # All patients across years
     cohort_AL_2016.csv         # 2016 patients with condition flags
     cohort_AL_2017.csv
     cohort_AL_2018.csv
     cohort_exclusions_ip_AL_2016.parquet   # Filter statistics
     cohort_exclusions_ot_AL_2016.parquet
     cohort_exclusions_ps_AL_2016.parquet
     ...
     ip/                        # Exported inpatient claims (cleaned)
     ot/                        # Exported outpatient claims (cleaned)
     ps/                        # Exported person summary (cleaned)

The ``cohort_AL.csv`` file contains one row per patient-year with columns:

- Beneficiary ID (index)
- ``ip_diag_diabetes_t2`` -- 1 if the patient had a T2D diagnosis in IP claims
- ``ot_diag_diabetes_t2`` -- 1 if the patient had a T2D diagnosis in OT claims
- ``ip_diag_diabetes_t2_date`` -- earliest T2D diagnosis date in IP
- ``ot_diag_diabetes_t2_date`` -- earliest T2D diagnosis date in OT
- ``include`` -- 1 if the patient meets all cohort criteria
- ``YEAR``, ``STATE_CD`` -- year and state identifiers
- ``birth_date`` -- date of birth

Step 6: Adding Risk Adjustment
-------------------------------

After extracting the cohort, you can add Elixhauser comorbidity scores:

.. code-block:: python

   from medicaid_utils.preprocessing import taf_ip
   from medicaid_utils.adapted_algorithms.py_elixhauser.elixhauser_comorbidity import score

   # Load the exported IP claims
   ip = taf_ip.TAFIP(
       year=2016, state="AL",
       data_root="/output/diabetes_cohort",
       clean=False, preprocess=False,
   )

   # Compute Elixhauser comorbidity score
   df_ip = score(
       ip.dct_files["base"], lst_diag_col_name="LST_DIAG_CD", cms_format="TAF"
   )

Running Multiple States
-----------------------

To extract the same cohort across multiple states:

.. code-block:: python

   states = ["AL", "IL", "CA", "NY", "TX"]

   for state in states:
       extract_cohort(
           state=state,
           lst_year=[2016, 2017, 2018],
           dct_diag_proc_codes=dct_codes,
           dct_filters=dct_filters,
           lst_types_to_export=["ip", "ot", "ps"],
           dct_data_paths={
               "source_root": "/data/cms/",
               "export_folder": f"/output/diabetes_cohort/{state}/",
           },
           cms_format="TAF",
           clean_exports=True,
           preprocess_exports=True,
       )

Using MAX Data Instead
----------------------

For older data (pre-2016), switch to MAX format. The main differences:

- Use ``cms_format="MAX"`` instead of ``"TAF"``
- ICD-9 codes become primary (ICD-10 exclusions are optional)
- Age columns differ: ``age_prncpl_proc`` (IP) and ``age_srvc_bgn`` (OT)

.. code-block:: python

   extract_cohort(
       state="WY",
       lst_year=[2010, 2011, 2012],
       dct_diag_proc_codes=dct_codes,
       dct_filters=dct_filters,
       lst_types_to_export=["ip", "ot", "ps"],
       dct_data_paths=dct_paths,
       cms_format="MAX",
       clean_exports=True,
       preprocess_exports=True,
   )

Alternative: Manual Cohort Construction
-----------------------------------------

If you need more control than ``extract_cohort`` provides, you can build the cohort
step by step:

.. code-block:: python

   from medicaid_utils.preprocessing import taf_ip, taf_ot, taf_ps
   from medicaid_utils.filters.claims import dx_and_proc

   # 1. Load and preprocess claims
   ip = taf_ip.TAFIP(year=2016, state="AL", data_root="/data/cms")
   ot = taf_ot.TAFOT(year=2016, state="AL", data_root="/data/cms")
   ps = taf_ps.TAFPS(year=2016, state="AL", data_root="/data/cms")

   # 2. Flag diagnosis codes on claims
   df_ip_flagged = dx_and_proc.flag_diagnoses_and_procedures(
       dct_diag_codes={"diabetes_t2": {"incl": {10: ["E11"]}, "excl": {10: ["E10"]}}},
       dct_proc_codes={},
       df_claims=ip.dct_files["base"].rename(
           columns={"prncpl_proc_date": "service_date"}
       ),
       cms_format="TAF",
   )

   # 3. Get patient IDs with T2D
   pdf_patients, _ = dx_and_proc.get_patient_ids_with_conditions(
       dct_diag_codes={"diabetes_t2": {"incl": {10: ["E11"]}, "excl": {10: ["E10"]}}},
       dct_proc_codes={},
       cms_format="TAF",
       ip=ip.dct_files["base"].rename(
           columns={"prncpl_proc_date": "service_date"}
       ),
       ot=ot.dct_files["base"].rename(
           columns={"srvc_bgn_date": "service_date"}
       ),
   )

   # 4. Filter to your population
   t2d_patients = pdf_patients.loc[
       (pdf_patients["ip_diag_diabetes_t2"] == 1)
       | (pdf_patients["ot_diag_diabetes_t2"] == 1)
   ]

   print(f"Found {len(t2d_patients)} patients with Type 2 diabetes")

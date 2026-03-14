Common Recipes
==============

Copy-paste code examples for frequently needed operations.

Load Claims and Compute Statistics
-----------------------------------

.. code-block:: python

   from medicaid_utils.preprocessing import taf_ip, taf_ps

   ip = taf_ip.TAFIP(year=2019, state="AL", data_root="/data/cms")
   ps = taf_ps.TAFPS(year=2019, state="AL", data_root="/data/cms")

   # Number of unique beneficiaries with IP claims
   n_benes = ip.dct_files["base"].index.nunique().compute()
   print(f"Unique beneficiaries: {n_benes}")

   # Total number of claims
   n_claims = len(ip.dct_files["base"])
   print(f"Total IP claims: {n_claims}")

Flag Multiple Conditions
------------------------

.. code-block:: python

   from medicaid_utils.filters.claims import dx_and_proc

   dct_codes = {
       "diabetes_t2": {"incl": {10: ["E11"]}},
       "hypertension": {"incl": {10: ["I10"]}},
       "ckd": {"incl": {10: ["N18"]}},
       "depression": {"incl": {10: ["F32", "F33"]}},
   }

   df_flagged = dx_and_proc.flag_diagnoses_and_procedures(
       dct_diag_codes=dct_codes,
       dct_proc_codes={},
       df_claims=ip.dct_files["base"],
       cms_format="TAF",
   )

Identify Patients with Conditions
----------------------------------

.. code-block:: python

   pdf_patients, dct_stats = dx_and_proc.get_patient_ids_with_conditions(
       dct_diag_codes=dct_codes,
       dct_proc_codes={},
       cms_format="TAF",
       ip=ip.dct_files["base"],
       ot=ot.dct_files["base"],
   )

   # Patients with T2D in either IP or OT
   t2d_patients = pdf_patients.loc[
       (pdf_patients["ip_diag_diabetes_t2"] == 1) |
       (pdf_patients["ot_diag_diabetes_t2"] == 1)
   ]

Flag Pharmacy Claims by NDC
----------------------------

.. code-block:: python

   from medicaid_utils.filters.claims import rx as rx_filter

   dct_ndc = {
       "buprenorphine": [
           "00378451905", "00378451993", "00378617005",
           # ... full NDC list
       ],
   }

   df_rx_flagged = rx_filter.flag_prescriptions(
       dct_ndc_codes=dct_ndc,
       df_claims=rx.dct_files["base"],
   )

Add Elixhauser Comorbidity Scores
----------------------------------

.. code-block:: python

   from medicaid_utils.adapted_algorithms.py_elixhauser.elixhauser_comorbidity import score

   # For MAX data (LST_DIAG_CD is constructed during preprocessing)
   df_with_elix = score(ip.df, lst_diag_col_name="LST_DIAG_CD", cms_format="MAX")

   # For TAF data — first gather diagnosis codes into a list column
   ip.gather_bene_level_diag_ndc_codes()  # creates LST_DIAG_CD on dct_files["base"]
   df_with_elix = score(
       ip.dct_files["base"],
       lst_diag_col_name="LST_DIAG_CD",
       cms_format="TAF",
   )

Classify ED Visits by Severity
-------------------------------

.. code-block:: python

   from medicaid_utils.adapted_algorithms.py_nyu_billings.billings_ed import get_nyu_ed_proba

   # Filter OT claims to ED visits
   df_ed = ot.df[ot.df["ed_use"] == 1]

   # Classify using NYU/Billings algorithm
   pdf_nyu = get_nyu_ed_proba(
       df_ed, date_col="srvc_bgn_date", index_col="MSIS_ID", cms_format="MAX"
   )

Identify FQHC Providers
------------------------

.. code-block:: python

   from medicaid_utils.other_datasets import fqhc

   # Get FQHC NPI crosswalk
   pdf_fqhc = fqhc.get_fqhc_crosswalk(start_year=2016)

Export Processed Claims
-----------------------

.. code-block:: python

   # To Parquet (recommended for large datasets)
   ip.export("/output/processed/", output_format="parquet", repartition=True)

   # To CSV (single file per claim type)
   ip.export("/output/processed/", output_format="csv")

Run Multi-State Analysis
-------------------------

.. code-block:: python

   import gc
   import pandas as pd
   from medicaid_utils.preprocessing import taf_ip

   results = []
   for state in ["AL", "IL", "CA", "NY", "TX"]:
       for year in [2016, 2017, 2018]:
           ip = taf_ip.TAFIP(year=year, state=state, data_root="/data/cms")
           n_claims = len(ip.dct_files["base"])
           results.append({"state": state, "year": year, "n_claims": n_claims})
           del ip
           gc.collect()

   df_summary = pd.DataFrame(results)
   print(df_summary)

.. seealso::

   :doc:`scaling_with_dask` for performance optimization,
   :doc:`geographic_data` for geographic crosswalks,
   :doc:`max_vs_taf` for format-specific patterns.

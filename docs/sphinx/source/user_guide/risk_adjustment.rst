Risk Adjustment & Clinical Algorithms
======================================

medicaid-utils includes Python implementations of several published clinical algorithms
for risk adjustment, procedure classification, and quality measurement.

Elixhauser Comorbidity Index
----------------------------

Flags 31 comorbidity groups from diagnosis codes (Elixhauser et al., 1998; implementation
splits hypertension into uncomplicated/complicated, extending the original 30 categories).

.. code-block:: python

   from medicaid_utils.adapted_algorithms.py_elixhauser.elixhauser_comorbidity import score

   df_ip = score(ip.df, lst_diag_col_name="LST_DIAG_CD", cms_format="MAX")

CDPS-Rx Risk Adjustment
------------------------

Pharmacy-based risk adjustment using the Chronic Illness and Disability Payment System
(Kronick et al., 2000, UC San Diego).

.. code-block:: python

   from medicaid_utils.adapted_algorithms.py_cdpsmrx import cdps_rx_risk_adjustment

   df_risk = cdps_rx_risk_adjustment.cdps_rx_risk_adjust(
       df, lst_diag_col_name="LST_DIAG_CD", lst_ndc_col_name="LST_NDC"
   )

BETOS Procedure Classification
------------------------------

Assigns Berenson-Eggers Type of Service (BETOS) categories to procedure codes.

.. code-block:: python

   from medicaid_utils.adapted_algorithms.py_betos import betos_proc_codes

   df_ot = betos_proc_codes.assign_betos_cat(ot.df, year=2012)

ED Prevention Quality Indicators
---------------------------------

Flags potentially preventable emergency department visits (Davies et al., 2017).

.. code-block:: python

   from medicaid_utils.adapted_algorithms.py_ed_pqi.ed_pqi import get_ed_pqis

   df_ed = get_ed_pqis(df_ip, df_ot, df_ps, df_ed, restrict_months=False)

Inpatient PQI
-------------

AHRQ Prevention Quality Indicators for inpatient admissions.

.. code-block:: python

   from medicaid_utils.adapted_algorithms.py_ip_pqi.prevention_quality_indicators import pqirecode

   df_adult, df_children = pqirecode(ip.df)

NYU/Billings ED Classification
-------------------------------

Classifies ED visits by severity and preventability (Billings, Parikh, Mijanovich, 2000).

.. code-block:: python

   from medicaid_utils.adapted_algorithms.py_nyu_billings.billings_ed import get_nyu_ed_proba

   pdf_nyu = get_nyu_ed_proba(df_ed, date_col="srvc_bgn_date", index_col="MSIS_ID", cms_format="MAX")

Pediatric Medical Complexity Algorithm (PMCA)
---------------------------------------------

Classifies pediatric patients by medical complexity (Simon et al., 2014, Seattle Children's).

.. code-block:: python

   from medicaid_utils.adapted_algorithms.py_pmca.pmca import pmca_chronic_conditions

   df_pmca = pmca_chronic_conditions(df, diag_cd_lst_col="LST_DIAG_CD_RAW")

Low-Value Care
--------------

Identifies low-value care services (Charlesworth et al., JAMA Intern Med, 2016).

.. code-block:: python

   from medicaid_utils.adapted_algorithms.py_low_value_care.low_value_care import construct_low_value_care_measures

   pdf_lvc = construct_low_value_care_measures(
       state="AL", year=2012, lst_bene_msis_filter=[], index_col="BENE_MSIS",
       max_data_root="/data/max", out_folder="/output"
   )

Algorithm Summary
-----------------

.. list-table::
   :header-rows: 1
   :widths: 30 40 30

   * - Algorithm
     - Reference
     - Module
   * - Elixhauser Comorbidity Index
     - Elixhauser et al., 1998
     - ``py_elixhauser``
   * - CDPS-Rx Risk Adjustment
     - Kronick et al., UC San Diego
     - ``py_cdpsmrx``
   * - BETOS Classification
     - CMS Berenson-Eggers Type of Service
     - ``py_betos``
   * - ED PQI
     - Davies et al., 2017
     - ``py_ed_pqi``
   * - IP PQI
     - AHRQ Prevention Quality Indicators
     - ``py_ip_pqi``
   * - NYU/Billings ED Algorithm
     - Billings, Parikh, Mijanovich, 2000
     - ``py_nyu_billings``
   * - PMCA
     - Simon et al., Seattle Children's
     - ``py_pmca``
   * - Low-Value Care
     - Charlesworth et al., JAMA Intern Med, 2016
     - ``py_low_value_care``

Risk Adjustment & Clinical Algorithms
======================================

medicaid-utils includes Python implementations of several published clinical algorithms
for risk adjustment, procedure classification, and quality measurement.

Elixhauser Comorbidity Index
----------------------------

Flags 31 comorbidity groups from diagnosis codes (Elixhauser et al., 1998).

.. code-block:: python

   from medicaid_utils.adapted_algorithms.py_elixhauser import elixhauser_comorbidity

   df_ip = elixhauser_comorbidity.flag_comorbidities(ip.df, claim_type="max")

CDPS-Rx Risk Adjustment
------------------------

Pharmacy-based risk adjustment using the Chronic Illness and Disability Payment System
(Kronick et al., UC San Diego).

.. code-block:: python

   from medicaid_utils.adapted_algorithms.py_cdpsmrx import cdps_rx_risk_adjustment

   df_risk = cdps_rx_risk_adjustment.cdps_rx_risk_adjust(
       df_rx, year=2012, index_col="MSIS_ID"
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

   from medicaid_utils.adapted_algorithms.py_ed_pqi import ed_pqi

   df_ed = ed_pqi.flag_potentially_preventable_ed_visits(ot.df, year=2012)

Inpatient PQI
-------------

AHRQ Prevention Quality Indicators for inpatient admissions.

.. code-block:: python

   from medicaid_utils.adapted_algorithms.py_ip_pqi import ip_pqi

   df_ip_pqi = ip_pqi.flag_ip_pqi(ip.df, year=2012)

NYU/Billings ED Classification
-------------------------------

Classifies ED visits by severity and preventability (Billings, Parikh, Mijanovich, 2000).

.. code-block:: python

   from medicaid_utils.adapted_algorithms.py_nyu_billings import nyu_billings

   df_nyu = nyu_billings.classify_ed_visits(ot.df, year=2012)

Pediatric Medical Complexity Algorithm (PMCA)
---------------------------------------------

Classifies pediatric patients by medical complexity (Simon et al., Seattle Children's).

.. code-block:: python

   from medicaid_utils.adapted_algorithms.py_pmca import pmca

   df_pmca = pmca.classify_complexity(ip.df, year=2012)

Low-Value Care
--------------

Identifies low-value care services (Charlesworth et al., JAMA Intern Med, 2016).

.. code-block:: python

   from medicaid_utils.adapted_algorithms.py_low_value_care import low_value_care

   df_lvc = low_value_care.flag_low_value_care(
       pdf_dates, lvc_specs, lst_conditions
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

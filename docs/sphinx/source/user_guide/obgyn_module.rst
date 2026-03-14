OB/GYN Topic Module
===================

The :mod:`~medicaid_utils.topics.obgyn` module provides functions for maternal and
reproductive health research using Medicaid claims data. These routines were developed
for peer-reviewed publications on delivery outcomes, short-interval pregnancy, and
severe maternal morbidity.

Module Structure
----------------

================================= ================================================
Submodule                         Purpose
================================= ================================================
``hospitalization``               Delivery flagging, birth outcomes, prenatal care, maternal morbidity
``comorbities``                   Chronic condition flags via MAX CC file
``cohort_indicators``             Provider classification (religious affiliation), transfer flags
================================= ================================================

Delivery Flagging
-----------------

Detecting deliveries
^^^^^^^^^^^^^^^^^^^^

:func:`~medicaid_utils.topics.obgyn.hospitalization.flag_delivery` identifies live births
and stillbirths from inpatient claims using ICD-9 and ICD-10 diagnosis codes:

.. code-block:: python

   from medicaid_utils.topics.obgyn import hospitalization

   df_flagged = hospitalization.flag_delivery(ip.df, cms_format="MAX")

   # For TAF:
   df_flagged = hospitalization.flag_delivery(ip.dct_files["base"], cms_format="TAF")

Mode of delivery
^^^^^^^^^^^^^^^^

:func:`~medicaid_utils.topics.obgyn.hospitalization.flag_delivery_mode` classifies
deliveries as vaginal or cesarean using procedure codes:

.. code-block:: python

   df_mode = hospitalization.flag_delivery_mode(df_flagged)

Preterm birth
^^^^^^^^^^^^^

:func:`~medicaid_utils.topics.obgyn.hospitalization.flag_preterm` detects preterm
birth-related hospitalizations:

.. code-block:: python

   df_preterm = hospitalization.flag_preterm(df_flagged, cms_format="MAX")

Multiple births
^^^^^^^^^^^^^^^

:func:`~medicaid_utils.topics.obgyn.hospitalization.flag_multiple_births` identifies
multiple birth events:

.. code-block:: python

   df_multiples = hospitalization.flag_multiple_births(df_flagged)

Abnormal Pregnancy Outcomes
---------------------------

:func:`~medicaid_utils.topics.obgyn.hospitalization.flag_abnormal_pregnancy` detects
ectopic, molar, or abnormal pregnancy, as well as spontaneous or induced abortions:

.. code-block:: python

   df_abnormal = hospitalization.flag_abnormal_pregnancy(ip.df, cms_format="MAX")

Maternal Morbidity
------------------

:func:`~medicaid_utils.topics.obgyn.hospitalization.flag_smm_events` adds flags for
severe maternal morbidity (SMM) events within 90 days of delivery:

.. code-block:: python

   df_smm = hospitalization.flag_smm_events(ip.df, cms_format="MAX")

Prenatal and Preconception Care
-------------------------------

:func:`~medicaid_utils.topics.obgyn.hospitalization.flag_prenatal` flags claims with
prenatal care codes, and
:func:`~medicaid_utils.topics.obgyn.hospitalization.flag_preconception_care` flags
preconception care:

.. code-block:: python

   df_prenatal = hospitalization.flag_prenatal(ot.df, cms_format="MAX")
   df_preconception = hospitalization.flag_preconception_care(ot.df, cms_format="MAX")

Conception Date Estimation
--------------------------

:func:`~medicaid_utils.topics.obgyn.hospitalization.calculate_conception` estimates
the conception date based on delivery type and delivery date:

.. code-block:: python

   df_conception, df_details = hospitalization.calculate_conception(df_delivery)

Returns two DataFrames: one with estimated conception dates and one with supporting
detail.

Chronic Condition Comorbidities
-------------------------------

:func:`~medicaid_utils.topics.obgyn.comorbities.flag_chronic_conditions` adds boolean
columns for chronic conditions using the MAX Chronic Conditions (CC) file. Flags
include diabetes, hypertension, CKD, depression, COPD, and tobacco use:

.. code-block:: python

   from medicaid_utils.preprocessing import max_cc
   from medicaid_utils.topics.obgyn import comorbities

   cc = max_cc.MAXCC(year=2012, state="WY", data_root="/data/cms")
   df_conditions = comorbities.flag_chronic_conditions(cc)

.. note::

   This function works with the MAX CC file specifically. For TAF data, use
   diagnosis-based flagging via :mod:`~medicaid_utils.filters.claims.dx_and_proc`.

Cohort Indicators
-----------------

Provider religious affiliation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:func:`~medicaid_utils.topics.obgyn.cohort_indicators.flag_religious_npis` classifies
hospital NPIs as Catholic, other religious, or secular:

.. code-block:: python

   from medicaid_utils.topics.obgyn import cohort_indicators

   df_npi = cohort_indicators.flag_religious_npis(ip.df)

Discharge transfers
^^^^^^^^^^^^^^^^^^^

:func:`~medicaid_utils.topics.obgyn.cohort_indicators.flag_transfers` adds indicator
columns for discharge transfer status (MAX only):

.. code-block:: python

   df_transfers = cohort_indicators.flag_transfers(ip.df)

.. seealso::

   :doc:`quality_measures` for related clinical quality measures,
   :doc:`risk_adjustment` for comorbidity scoring.

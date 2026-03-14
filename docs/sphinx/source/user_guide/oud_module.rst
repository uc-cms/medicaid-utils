Opioid Use Disorder (OUD) Topic Module
=======================================

The :mod:`~medicaid_utils.topics.oud` module provides medication-assisted treatment (MAT)
detection, care setting classification, and co-occurring condition identification for
opioid use disorder research in Medicaid claims data.

Module Structure
----------------

======================================= ================================================
Submodule                               Purpose
======================================= ================================================
``medication_and_behavioral_health``    MAT flags (procedure codes and NDC), behavioral health, concurrent prescriptions
``care_settings``                       Care setting classification (FQHC, outpatient, physician office, etc.)
``cooccurring_conditions``              Mental health and substance use disorder co-occurrence
======================================= ================================================

Medication-Assisted Treatment (MAT)
------------------------------------

Each MAT medication has both a procedure-code flag (for IP/OT claims) and an NDC flag
(for RX claims). All ``flag_proc_*`` functions accept a ``cms_format`` parameter
(default ``"TAF"``).

Buprenorphine
^^^^^^^^^^^^^

.. code-block:: python

   from medicaid_utils.topics.oud import medication_and_behavioral_health as mat

   # Procedure code flagging (J0571) on IP/OT claims
   df_ip = mat.flag_proc_buprenorphine(ip.dct_files["base"], cms_format="TAF")

   # NDC flagging on RX claims
   df_rx = mat.flag_rx_buprenorphine(rx.dct_files["base"])

Buprenorphine/Naloxone
^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Procedure codes (J0572–J0575)
   df_ip = mat.flag_proc_buprenorphine_naloxone(ip.dct_files["base"], cms_format="TAF")

   # NDC codes
   df_rx = mat.flag_rx_buprenorphine_naloxone(rx.dct_files["base"])

Injectable Naltrexone
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Procedure code (J2315)
   df_ip = mat.flag_proc_injectable_naltrexone(ip.dct_files["base"], cms_format="TAF")

   # NDC codes (oral naltrexone)
   df_rx = mat.flag_rx_oral_naltrexone(rx.dct_files["base"])

Methadone
^^^^^^^^^

.. code-block:: python

   # Procedure code (H0020)
   df_ip = mat.flag_proc_methadone(ip.dct_files["base"], cms_format="TAF")

   # NDC codes (splits ≤30 mg and >30 mg dosages)
   df_rx = mat.flag_rx_methadone(rx.dct_files["base"])

Behavioral Health Services
--------------------------

:func:`~medicaid_utils.topics.oud.medication_and_behavioral_health.flag_proc_behavioral_health_trtmt`
flags claims with CPT/HCPCS behavioral health procedure codes:

.. code-block:: python

   df_bh = mat.flag_proc_behavioral_health_trtmt(ot.dct_files["base"], cms_format="TAF")

Concurrent Benzodiazepine and Opioid Prescriptions
---------------------------------------------------

:func:`~medicaid_utils.topics.oud.medication_and_behavioral_health.flag_rx_benzos_opioids`
identifies concurrent benzodiazepine and opioid prescriptions. Creates an
``rx_benzo_and_opioid`` column:

.. code-block:: python

   df_rx = mat.flag_rx_benzos_opioids(rx.dct_files["base"])

Care Setting Classification
----------------------------

:func:`~medicaid_utils.topics.oud.care_settings.flag_care_settings` creates claim-level
flags for care settings using place of service and taxonomy codes:

.. code-block:: python

   from medicaid_utils.topics.oud import care_settings

   df_ot = care_settings.flag_care_settings(ot.dct_files["base"])

Flags include:

- **FQHC** — Federally Qualified Health Centers
- **Outpatient hospital** — Hospital outpatient departments
- **Physician office** — Office-based physician visits
- **Behavioral health centers** — Specialized behavioral health facilities
- **Hospital** — Inpatient hospital settings
- **Office-based** — General office-based settings

.. note::

   TAF claims use ``POS_CD`` (Place of Service Code) and ``BLG_PRVDR_TXNMY_CD``
   (Billing Provider Taxonomy Code) for classification. MAX claims use ``TOS_CD``
   (Type of Service Code).

Co-Occurring Conditions
-----------------------

Mental health
^^^^^^^^^^^^^

:func:`~medicaid_utils.topics.oud.cooccurring_conditions.flag_cooccurring_mental_health_claims`
flags claims with mental health diagnosis codes (based on MODRN 2021 definitions):

.. code-block:: python

   from medicaid_utils.topics.oud import cooccurring_conditions

   df_mh = cooccurring_conditions.flag_cooccurring_mental_health_claims(
       ot.dct_files["base"], cms_format="TAF"
   )

Substance use disorders
^^^^^^^^^^^^^^^^^^^^^^^

:func:`~medicaid_utils.topics.oud.cooccurring_conditions.flag_cooccurring_sud_claims`
flags claims with substance use disorder diagnosis codes, excluding OUD and tobacco:

.. code-block:: python

   df_sud = cooccurring_conditions.flag_cooccurring_sud_claims(
       ot.dct_files["base"], cms_format="TAF"
   )

Example: Full OUD Analysis Pipeline
------------------------------------

.. code-block:: python

   from medicaid_utils.preprocessing import taf_ip, taf_ot, taf_rx, taf_ps
   from medicaid_utils.topics.oud import (
       medication_and_behavioral_health as mat,
       care_settings,
       cooccurring_conditions,
   )

   # Load claims
   ip = taf_ip.TAFIP(year=2019, state="AL", data_root="/data/cms")
   ot = taf_ot.TAFOT(year=2019, state="AL", data_root="/data/cms")
   rx = taf_rx.TAFRX(year=2019, state="AL", data_root="/data/cms")

   # Flag MAT in IP/OT claims
   df_ip = mat.flag_proc_buprenorphine(ip.dct_files["base"])
   df_ot = mat.flag_proc_methadone(ot.dct_files["base"])

   # Flag MAT in RX claims
   df_rx = mat.flag_rx_buprenorphine(rx.dct_files["base"])
   df_rx = mat.flag_rx_methadone(df_rx)

   # Classify care settings
   df_ot = care_settings.flag_care_settings(df_ot)

   # Flag co-occurring conditions
   df_ot = cooccurring_conditions.flag_cooccurring_mental_health_claims(df_ot)
   df_ot = cooccurring_conditions.flag_cooccurring_sud_claims(df_ot)

.. seealso::

   :doc:`../tutorials/oud_cohort` for a complete OUD cohort extraction tutorial,
   :doc:`quality_measures` for related quality measures.

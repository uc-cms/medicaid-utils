Quality Measures & Domain Modules
=================================

Beyond general-purpose algorithms, medicaid-utils includes domain-specific modules that
package covariates and outcome definitions from published Medicaid research.

Topics Modules
--------------

The ``topics`` package contains condition-specific variable construction developed for
peer-reviewed publications.

Opioid Use Disorder (OUD)
^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``topics.oud`` module provides:

- **Medication detection** -- buprenorphine treatment identification via procedure codes and NDC
- **OUD medication flags** -- medication-assisted treatment indicators
- **Behavioral health services** -- mental health and substance use service identification
- **Care setting classification** -- FQHC, outpatient hospital, physician office

.. code-block:: python

   from medicaid_utils.topics.oud import medication_and_behavioral_health as mbh

   # Flag buprenorphine claims in pharmacy data
   df_rx = mbh.flag_rx_buprenorphine(rx_claims.df)

   # Flag behavioral health treatment in outpatient claims
   df_ot = mbh.flag_proc_behavioral_health_trtmt(ot.df)

OB/GYN
^^^^^^^

The ``topics.obgyn`` module provides:

- **Delivery outcome identification** -- ICD-9 and ICD-10 based delivery detection
- **Preterm birth flags** -- gestational age classification
- **Multiple birth detection**
- **Provider classification** -- religious vs. secular hospital identification
- **Chronic condition comorbidities** -- diabetes, hypertension, CKD, depression, COPD, tobacco use

.. code-block:: python

   from medicaid_utils.topics.obgyn import hospitalization

   # Flag delivery-related hospitalizations
   df_ip = hospitalization.flag_delivery(ip.df, cms_format="TAF")

External Data Integration
-------------------------

The ``other_datasets`` module integrates external provider and geographic data:

- **NPI Registry** -- National Provider Identifier lookups
- **HCRIS** -- Healthcare Cost Report Information System (hospital cost reports)
- **UDS** -- Uniform Data System (health center data)
- **FQHC** -- Federally Qualified Health Center provider identification
- **Geographic crosswalks** -- RUCA (Rural-Urban Commuting Area), RUCC (Rural-Urban
  Continuum), PCSA (Primary Care Service Area) codes via ZIP code

.. code-block:: python

   from medicaid_utils.other_datasets import fqhc

   # Get FQHC NPI crosswalk for a given start year
   pdf_fqhc_crosswalk = fqhc.get_fqhc_crosswalk(start_year=2012)

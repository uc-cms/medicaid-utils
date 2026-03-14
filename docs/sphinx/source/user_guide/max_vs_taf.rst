MAX vs TAF: CMS File Formats
============================

medicaid-utils supports both Medicaid file formats published by CMS. Understanding
their differences is essential for working with Medicaid claims data.

Overview
--------

================= ============================================== ==============================================
Feature           MAX (Medicaid Analytic eXtract)                TAF (T-MSIS Analytic Files)
================= ============================================== ==============================================
Years available   1999–2015                                      2014–present
Diagnosis coding  Primarily ICD-9-CM                              Primarily ICD-10-CM
File structure    Single flat file per claim type                  Multiple sub-files per claim type
Beneficiary ID    ``BENE_MSIS`` (constructed; see below)          ``BENE_MSIS`` (constructed; see below)
CMS claim types   IP, OT, RX, PS, CC                              IP, OT, LT, RX, DE (person summary)
Supported types   IP, OT, PS, CC                                  IP, OT, LT, RX, PS
Diagnosis cols    ``DIAG_CD_1`` – ``DIAG_CD_9``                   ``DGNS_CD_1`` – ``DGNS_CD_12``
Procedure cols    ``PRCDR_CD_1`` – ``PRCDR_CD_6``                 ``PRCDR_CD_1`` – ``PRCDR_CD_6``, ``LINE_PRCDR_CD``
================= ============================================== ==============================================

Accessing DataFrames
--------------------

The fundamental difference in code: MAX files produce a single DataFrame, TAF files
produce a dictionary of DataFrames.

**MAX** — Single DataFrame via ``.df``:

.. code-block:: python

   from medicaid_utils.preprocessing import max_ip

   ip = max_ip.MAXIP(year=2012, state="WY", data_root="/data/cms")
   df = ip.df  # Single Dask DataFrame

**TAF** — Multiple sub-file DataFrames via ``.dct_files``:

.. code-block:: python

   from medicaid_utils.preprocessing import taf_ip

   ip = taf_ip.TAFIP(year=2019, state="AL", data_root="/data/cms")
   df_base = ip.dct_files["base"]             # Header/base records
   df_line = ip.dct_files["line"]             # Line-level detail
   df_dx   = ip.dct_files["base_diag_codes"]  # Diagnosis codes
   df_ndc  = ip.dct_files["line_ndc_codes"]   # NDC codes

TAF Dictionary Keys
-------------------

Each TAF claim type is split into sub-files, accessed by key in ``dct_files``:

**IP / OT / LT claims:**

==================== ========================================= =================
File Suffix          Description                               Dict Key
==================== ========================================= =================
``h`` (e.g., iph)    Header/base records                       ``"base"``
``l`` (e.g., ipl)    Line-level detail                         ``"line"``
``occr`` (e.g., ipoccr) Occurrence codes                       ``"occurrence_code"``
``dx`` (e.g., ipdx)  Diagnosis codes                           ``"base_diag_codes"``
``ndc`` (e.g., ipndc) NDC codes                                ``"line_ndc_codes"``
==================== ========================================= =================

**RX (Pharmacy) claims:**

==================== ========================================= =================
File Suffix          Description                               Dict Key
==================== ========================================= =================
``h`` (rxh)          Header/base records                       ``"base"``
``l`` (rxl)          Line-level detail                         ``"line"``
``ndc`` (rxndc)      NDC codes                                 ``"line_ndc_codes"``
==================== ========================================= =================

**PS (Person Summary / Demographics):**

==================== ========================================= =================
File Suffix          Description                               Dict Key
==================== ========================================= =================
``debse``            Base demographics                         ``"base"``
``dedts``            Eligibility dates                         ``"dates"``
``demc``             Managed care enrollment                   ``"managed_care"``
``dedsb``            Disability/functional status              ``"disability"``
``demfp``            Money Follows the Person                  ``"mfp"``
``dewvr``            Waiver information                        ``"waiver"``
``dehsp``            Home health/hospice                       ``"home_health"``
``dedxndc``          Diagnosis and NDC rollup                  ``"diag_and_ndc_codes"``
==================== ========================================= =================

Gathering Diagnosis Codes
-------------------------

Several algorithms (Elixhauser, CDPS-Rx, PMCA) expect a ``LST_DIAG_CD`` column — a
comma-separated string of all diagnosis codes per beneficiary. This column is **not**
created automatically by either MAX or TAF preprocessing; it must be constructed
explicitly.

**MAX** — construct from individual diagnosis columns:

.. code-block:: python

   ip = max_ip.MAXIP(year=2012, state="WY", data_root="/data/cms")
   diag_cols = [c for c in ip.df.columns if c.startswith("DIAG_CD_")]
   ip.df = ip.df.map_partitions(
       lambda pdf: pdf.assign(
           LST_DIAG_CD=pdf[diag_cols].apply(
               lambda row: ",".join(v for v in row if v and str(v).strip()), axis=1
           )
       )
   )

**TAF** — use ``gather_bene_level_diag_ndc_codes()``, which creates
``LST_DIAG_CD`` on ``dct_files["base_diag_codes"]`` (not ``"base"``):

.. code-block:: python

   ip = taf_ip.TAFIP(year=2019, state="AL", data_root="/data/cms")
   ip.gather_bene_level_diag_ndc_codes()
   df_diag = ip.dct_files["base_diag_codes"]  # has LST_DIAG_CD, LST_DIAG_CD_RAW

This step is required before using algorithms that expect ``LST_DIAG_CD``, such as
Elixhauser comorbidity scoring.

Specifying Format
-----------------

Most functions accept a ``cms_format`` parameter:

.. code-block:: python

   from medicaid_utils.adapted_algorithms.py_elixhauser.elixhauser_comorbidity import score

   # MAX (after constructing LST_DIAG_CD — see above)
   score(ip.df, lst_diag_col_name="LST_DIAG_CD", cms_format="MAX")

   # TAF (after calling gather_bene_level_diag_ndc_codes())
   score(ip.dct_files["base_diag_codes"], lst_diag_col_name="LST_DIAG_CD", cms_format="TAF")

Cohort Extraction
-----------------

The ``extract_cohort`` function handles format differences internally:

.. code-block:: python

   # Just change cms_format — the rest of the API is the same
   extract_cohort(state="WY", lst_year=[2012], cms_format="MAX", ...)
   extract_cohort(state="AL", lst_year=[2019], cms_format="TAF", ...)

Which Format Should I Use?
--------------------------

- **ICD-9 studies (pre-October 2015):** Use MAX data
- **ICD-10 studies (post-October 2015):** Use TAF data
- **Cross-era studies:** Use both, with ICD-9 and ICD-10 code mappings in your ``dct_diag_proc_codes``
- **Pharmacy studies:** TAF preferred (medicaid-utils implements TAF RX preprocessing
  via ``TAFRX``; MAX RX data exists in CMS but is not yet supported in the package)

Beneficiary ID (``BENE_MSIS``)
------------------------------

``BENE_MSIS`` is a composite identifier **constructed by medicaid-utils** (not a raw CMS
column). It applies to both MAX and TAF:

::

   BENE_MSIS = STATE_CD + "-" + HAS_BENE + "-" + (BENE_ID or MSIS_ID)

- **BENE_ID** — CMS-assigned, intended to be unique across states and years
- **MSIS_ID** — State-assigned, unique only within a state and year
- **HAS_BENE** — ``1`` if ``BENE_ID`` exists, ``0`` otherwise (falls back to ``MSIS_ID``)

Example: ``"AL-1-123456789"`` (Alabama, has BENE_ID, ID is 123456789)

The ``index_col`` parameter on all claim classes defaults to ``"BENE_MSIS"`` but can be
set to ``"MSIS_ID"`` or another column if needed.

.. seealso::

   :doc:`../getting_started/glossary` for complete column name mappings,
   :doc:`preprocessing` for loading and cleaning details.

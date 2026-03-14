Preprocessing Claims
====================

The ``preprocessing`` module is the foundation of medicaid-utils. It loads raw Parquet claim
files into Dask DataFrames and applies validated cleaning and variable construction routines.

Supported File Types
--------------------

**MAX format** (pre-2016, ICD-9 era):

========  =================================  ============================
Type      Description                        Class
========  =================================  ============================
``ip``    Inpatient claims                   :class:`~medicaid_utils.preprocessing.max_ip.MAXIP`
``ot``    Outpatient claims                  :class:`~medicaid_utils.preprocessing.max_ot.MAXOT`
``ps``    Person Summary                     :class:`~medicaid_utils.preprocessing.max_ps.MAXPS`
``cc``    Chronic Conditions                 :class:`~medicaid_utils.preprocessing.max_cc.MAXCC`
========  =================================  ============================

**TAF format** (2016+, ICD-10 era):

========  =================================  ============================
Type      Description                        Class
========  =================================  ============================
``ip``    Inpatient claims                   :class:`~medicaid_utils.preprocessing.taf_ip.TAFIP`
``ot``    Outpatient claims                  :class:`~medicaid_utils.preprocessing.taf_ot.TAFOT`
``lt``    Long-Term Care                     :class:`~medicaid_utils.preprocessing.taf_lt.TAFLT`
``rx``    Pharmacy claims                    :class:`~medicaid_utils.preprocessing.taf_rx.TAFRX`
``ps``    Person Summary (Demographics)      :class:`~medicaid_utils.preprocessing.taf_ps.TAFPS`
========  =================================  ============================

Loading Claims
--------------

All claim classes follow the same constructor pattern:

.. code-block:: python

   from medicaid_utils.preprocessing import max_ip

   ip = max_ip.MAXIP(
       year=2012,              # Claim year
       state="WY",             # Two-letter state code
       data_root="/data/cms",  # Root folder (see Data Layout)
       index_col="BENE_MSIS",  # Beneficiary ID column
       clean=True,             # Run cleaning routines
       preprocess=True,        # Compute derived variables
       tmp_folder=None,        # Cache folder (optional)
       pq_engine="pyarrow",    # Parquet engine
   )

For MAX files, the cleaned DataFrame is available at ``ip.df``.

For TAF files, sub-file DataFrames are in ``ip.dct_files`` (keyed by subtype:
``"base"``, ``"line"``, ``"occurrence_code"``, ``"base_diag_codes"``, ``"line_ndc_codes"``).

Factory Methods
---------------

You can also use factory methods to instantiate claim objects by type string:

.. code-block:: python

   from medicaid_utils.preprocessing import max_file, taf_file

   # MAX
   claim = max_file.MAXFile.get_claim_instance(
       "ip", 2012, "WY", "/data/cms"
   )

   # TAF
   claim = taf_file.TAFFile.get_claim_instance(
       "ip", 2019, "AL", "/data/cms"
   )

Default Filters
---------------

Each claim class has built-in default filters (``dct_default_filters``) that are applied
during cohort extraction. For example, inpatient claims default to excluding claims with
missing date of birth and duplicate records:

.. code-block:: python

   ip.dct_default_filters
   # {'missing_dob': 0, 'duplicated': 0}

These can be overridden when calling cohort extraction functions.

Exporting Processed Data
------------------------

After cleaning and preprocessing, you can export the resulting DataFrames:

.. code-block:: python

   ip.export(
       "/path/to/output",
       output_format="parquet",  # or "csv"
       repartition=True,
   )

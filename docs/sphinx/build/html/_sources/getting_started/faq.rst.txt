FAQ & Troubleshooting
=====================

General
-------

**What is medicaid-utils?**

An open-source Python toolkit for analyzing Medicaid claims data from CMS. It provides
preprocessing, cleaning, cohort extraction, risk adjustment algorithms, and quality
measures for both MAX (1999–2015) and TAF (2014–present) file formats.

**Who develops medicaid-utils?**

The Research Computing Group in the Biostatistics Laboratory at The University of Chicago.
The package grew out of research computing infrastructure built for peer-reviewed Medicaid
health services research publications.

**Do I need a CMS data use agreement (DUA)?**

The package itself is open-source and freely available. However, to use it with actual
Medicaid claims data, you need a data use agreement with CMS or access through
`ResDAC <https://resdac.org/>`_. The package does not include any claims data.

**What Python versions are supported?**

Python 3.11, 3.12, and 3.13 are tested in CI. Python 3.10 and earlier are not supported.

Data & Setup
------------

**What format should my data be in?**

Parquet format, organized by year and state. See :doc:`data_layout` for the required
folder structure.

**Can I use SAS or CSV data?**

Not directly — you need to convert to Parquet first. See :doc:`data_layout` for conversion
examples.

**What's the difference between MAX and TAF?**

MAX (Medicaid Analytic eXtract) covers 1999–2015 with ICD-9 coding. TAF (T-MSIS Analytic
Files) covers 2014–present with ICD-10 coding. See :doc:`../user_guide/max_vs_taf` for a
detailed comparison.

**How much RAM do I need?**

- **Small states (e.g., WY):** 16 GB is usually sufficient
- **Medium states (e.g., AL, IL):** 32–64 GB recommended
- **Large states (e.g., CA, NY, TX):** 64 GB+ recommended, or use a distributed cluster

Using ``tmp_folder`` for intermediate caching helps manage memory regardless of dataset
size. See :doc:`../user_guide/scaling_with_dask`.

Usage
-----

**How do I access the DataFrame after loading?**

- **MAX files:** ``ip.df`` (single DataFrame)
- **TAF files:** ``ip.dct_files["base"]`` (dict of sub-file DataFrames)

In both cases, the DataFrame is indexed by ``BENE_MSIS``, a composite ID constructed as
``STATE_CD-HAS_BENE-(BENE_ID or MSIS_ID)``. See :doc:`../user_guide/max_vs_taf` for details.

**Why are operations slow without** ``.compute()`` **?**

medicaid-utils uses Dask, which is lazy by default. Operations build a task graph but
don't execute until you call ``.compute()``. This allows Dask to optimize the computation
plan.

**How do I define diagnosis codes for cohort extraction?**

Use ICD-9 and ICD-10 prefixes in a dictionary. Codes are matched using prefix matching —
``"E11"`` matches ``"E110"``, ``"E1100"``, ``"E1101"``, etc. See
:doc:`../user_guide/cohort_extraction` for examples.

**Can I use both ICD-9 and ICD-10 codes?**

Yes. The ``dct_diag_proc_codes`` dictionary supports both simultaneously:

.. code-block:: python

   dct_codes = {
       "diag_codes": {
           "diabetes_t2": {
               "incl": {
                   9: ["250"],    # ICD-9
                   10: ["E11"],   # ICD-10
               },
           },
       },
       "proc_codes": {},
   }

**What risk adjustment algorithms are available?**

Eight algorithms: Elixhauser, CDPS-Rx, BETOS, ED PQI, IP PQI, NYU/Billings, PMCA, and
low-value care. See :doc:`../user_guide/risk_adjustment` for details and usage.

Troubleshooting
---------------

**ArrowInvalid or ArrowTypeError when exporting to Parquet**

This typically happens when object-type columns contain pyarrow sentinel values.
medicaid-utils handles this automatically by converting object columns to strings before
export. If you encounter this error, make sure you're using the latest version.

**Dask worker running out of memory**

- Reduce ``n_workers`` and increase ``memory_limit`` per worker
- Use ``tmp_folder`` to cache intermediate results to disk
- Consider repartitioning your data into smaller partitions
- For multi-state analyses, process states sequentially

.. code-block:: python

   # Memory-friendly multi-state loop
   import gc

   for state in ["CA", "NY", "TX"]:
       ip = taf_ip.TAFIP(year=2019, state=state, data_root="/data/cms")
       # ... process ...
       del ip
       gc.collect()

**FileNotFoundError when loading claims**

Check that your data folder structure matches the expected layout. See :doc:`data_layout`.

Contributing
------------

**How can I contribute?**

Fork the repository, make changes, run tests (``pytest tests/``), run the linter
(``pylint medicaid_utils``), and open a pull request. See the
`Contributing guide <https://github.com/uc-cms/medicaid-utils/wiki/Contributing>`_.

**How do I report a bug?**

Open an issue on `GitHub <https://github.com/uc-cms/medicaid-utils/issues>`_ with:

- Python version and medicaid-utils version
- Minimal code example that reproduces the issue
- Full error traceback
- Description of expected vs. actual behavior

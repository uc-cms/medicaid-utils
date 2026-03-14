Quick Start
===========

This page walks through the basic workflow: setting up a Dask cluster, loading claims,
cleaning them, and extracting a patient cohort.

Setting Up a Dask Cluster
--------------------------

medicaid-utils uses `Dask <https://www.dask.org/>`_ for distributed computation. All
DataFrames in the package are lazy Dask DataFrames -- operations are deferred until
``.compute()`` is called. Set up a Dask cluster before loading claims for best performance.

Local Cluster (Single Machine)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For workstations with sufficient RAM (recommended: 64 GB+ for state-level data):

.. code-block:: python

   from dask.distributed import Client, LocalCluster

   # Create a local cluster with 8 workers, 8 GB each
   cluster = LocalCluster(
       n_workers=8,
       threads_per_worker=1,    # Avoids GIL contention with pandas
       memory_limit="8GB",
   )
   client = Client(cluster)
   print(client.dashboard_link)  # Opens Dask dashboard for monitoring

.. code-block:: text

   http://127.0.0.1:8787/status

SLURM / HPC Cluster
^^^^^^^^^^^^^^^^^^^^

For high-performance computing environments, use
`dask-jobqueue <https://jobqueue.dask.org/>`_:

.. code-block:: python

   from dask_jobqueue import SLURMCluster
   from dask.distributed import Client

   cluster = SLURMCluster(
       cores=4,
       memory="32GB",
       processes=1,
       walltime="04:00:00",
       queue="standard",
   )
   cluster.scale(jobs=10)  # Request 10 SLURM jobs
   client = Client(cluster)

Without a Cluster
^^^^^^^^^^^^^^^^^

If no distributed client is created, Dask defaults to its **synchronous scheduler**, which
processes partitions sequentially. This works for small datasets or debugging:

.. code-block:: python

   import dask
   dask.config.set(scheduler="threads")  # or "synchronous" for debugging

Tips
^^^^

- **Monitor progress**: The Dask dashboard (typically at ``http://localhost:8787``) shows
  task progress, memory usage, and worker status
- **Memory management**: Use ``tmp_folder`` when loading claims to cache intermediate
  results to disk and reduce memory pressure
- **Partition size**: Aim for partitions of 50--200 MB each. The package handles
  partitioning automatically based on the input Parquet files

Loading and Cleaning Claims
---------------------------

MAX format (ICD-9 era):

.. code-block:: python

   from medicaid_utils.preprocessing import max_ip, max_ot, max_ps

   # Load and preprocess inpatient claims (cleaning + variable construction)
   ip = max_ip.MAXIP(year=2012, state="WY", data_root="/path/to/data")

   # Access the cleaned Dask DataFrame
   df_ip = ip.df
   print(df_ip.columns.tolist()[:8])

.. code-block:: text

   ['BENE_ID', 'MSIS_ID', 'STATE_CD', 'SRVC_BGN_DT', 'SRVC_END_DT',
    'DIAG_CD_1', 'DIAG_CD_2', 'PRCDR_CD_1']

.. code-block:: python

   # Load outpatient claims with IP overlap flagging
   ot = max_ot.MAXOT(year=2012, state="WY", data_root="/path/to/data")
   ot.flag_ip_overlaps_and_ed(df_ip)

   # Load person summary with rural classification
   ps = max_ps.MAXPS(year=2012, state="WY", data_root="/path/to/data")

TAF format (ICD-10 era):

.. code-block:: python

   from medicaid_utils.preprocessing import taf_ip, taf_ot, taf_ps

   ip = taf_ip.TAFIP(year=2019, state="AL", data_root="/path/to/data")
   ps = taf_ps.TAFPS(year=2019, state="AL", data_root="/path/to/data")

Verify Your Setup
^^^^^^^^^^^^^^^^^

After loading, verify everything is working:

.. code-block:: python

   print(f"Partitions: {ip.df.npartitions}")
   print(f"Columns: {len(ip.df.columns)}")
   print(f"Rows (approx): {len(ip.df):,}")

.. code-block:: text

   Partitions: 4
   Columns: 52
   Rows (approx): 12,847

If this runs without errors, your installation and data layout are correct.

What Cleaning Does
^^^^^^^^^^^^^^^^^^

Each file type has tailored cleaning routines that run automatically (``clean=True``):

- **Date standardization** -- converts date columns to consistent datetime types
- **Diagnosis code cleaning** -- strips whitespace, normalizes formatting
- **Procedure code cleaning** -- validates procedure code systems (CPT, HCPCS, ICD)
- **Demographic derivation** -- computes age, gender flags, date-of-birth validation
- **Duplicate flagging** -- identifies exact duplicate claims for exclusion

What Preprocessing Adds
^^^^^^^^^^^^^^^^^^^^^^^

Additional derived variables computed via ``preprocess=True``:

- **Payment calculation** -- standardized payment amount
- **ED use flags** -- emergency department utilization indicators
- **IP overlap detection** -- flags outpatient claims overlapping inpatient stays
- **Length of stay** -- computed from admission and discharge dates
- **Eligibility patterns** -- monthly enrollment strings and gap detection
- **Rural classification** -- RUCA or RUCC codes via ZIP code crosswalk
- **Dual eligibility** -- Medicare-Medicaid dual enrollment flags

Skipping Cleaning or Preprocessing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can load raw data without any transformations:

.. code-block:: python

   ip = max_ip.MAXIP(
       year=2012, state="WY", data_root="/path/to/data",
       clean=False, preprocess=False
   )

Caching Intermediate Results
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For large datasets, cache intermediate results to avoid recomputation:

.. code-block:: python

   ip = max_ip.MAXIP(
       year=2012, state="WY", data_root="/path/to/data",
       tmp_folder="/path/to/cache"
   )

Next Steps
----------

- :doc:`../user_guide/cohort_extraction` -- learn how to define and extract patient cohorts
- :doc:`../user_guide/risk_adjustment` -- apply comorbidity scoring and risk adjustment
- :doc:`../tutorials/diabetes_cohort` -- end-to-end example for Type 2 diabetes

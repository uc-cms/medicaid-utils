Scaling with Dask
=================

All DataFrames in medicaid-utils are `Dask <https://www.dask.org/>`_ DataFrames,
enabling lazy evaluation and distributed processing. The ``distributed`` package is
a required dependency and is installed automatically with medicaid-utils.

Local Cluster
-------------

For workstations and laptops:

.. code-block:: python

   from dask.distributed import Client, LocalCluster

   cluster = LocalCluster(
       n_workers=8,
       threads_per_worker=1,    # avoids GIL contention with pandas
       memory_limit="8GB",      # per worker
   )
   client = Client(cluster)
   print(client.dashboard_link)  # Dask dashboard for monitoring

**Sizing guidelines:**

=============== =============== =============================================
State size      Examples        Recommended configuration
=============== =============== =============================================
Small           WY, VT, ND      4 workers, 4 GB each (16 GB total)
Medium          AL, IL, OH       8 workers, 8 GB each (64 GB total)
Large           CA, NY, TX       8–16 workers, 8+ GB each, or HPC cluster
=============== =============== =============================================

SLURM / HPC Clusters
---------------------

For high-performance computing environments, use ``dask-jobqueue``:

.. code-block:: bash

   pip install dask-jobqueue

.. code-block:: python

   from dask_jobqueue import SLURMCluster
   from dask.distributed import Client

   cluster = SLURMCluster(
       cores=4,
       memory="32GB",
       processes=4,
       walltime="04:00:00",
       queue="standard",
   )
   cluster.scale(jobs=10)  # submit 10 SLURM jobs
   client = Client(cluster)

For PBS/Torque environments, substitute ``PBSCluster``:

.. code-block:: python

   from dask_jobqueue import PBSCluster

   cluster = PBSCluster(
       cores=4,
       memory="32GB",
       processes=4,
       walltime="04:00:00",
       resource_spec="nodes=1:ppn=4",
   )

Without a Cluster
-----------------

For debugging or small datasets, you can bypass the distributed scheduler:

.. code-block:: python

   import dask

   # Single-threaded (easiest to debug)
   dask.config.set(scheduler="synchronous")

   # Threaded (no cluster overhead)
   dask.config.set(scheduler="threads")

Performance Tips
----------------

Using ``tmp_folder``
^^^^^^^^^^^^^^^^^^^^

The ``tmp_folder`` parameter caches intermediate results to disk, reducing memory pressure:

.. code-block:: python

   from medicaid_utils.preprocessing import taf_ip

   ip = taf_ip.TAFIP(
       year=2019, state="CA", data_root="/data/cms",
       tmp_folder="/scratch/tmp/",  # intermediate files written here
   )

This is especially important for large states where the full dataset does not fit
in memory.

Partition sizing
^^^^^^^^^^^^^^^^

Aim for partitions of 50–200 MB. Too many small partitions create scheduling overhead;
too few large partitions cause memory spills. You can check and adjust:

.. code-block:: python

   print(ip.dct_files["base"].npartitions)

   # Repartition if needed
   ip.dct_files["base"] = ip.dct_files["base"].repartition(npartitions=20)

Multi-state analysis
^^^^^^^^^^^^^^^^^^^^

Process states sequentially and release memory between iterations:

.. code-block:: python

   import gc
   import pandas as pd
   from medicaid_utils.preprocessing import taf_ip

   results = []
   for state in ["CA", "NY", "TX", "FL", "IL"]:
       ip = taf_ip.TAFIP(year=2019, state=state, data_root="/data/cms")
       n_claims = len(ip.dct_files["base"])
       results.append({"state": state, "n_claims": n_claims})
       del ip
       gc.collect()

   df_summary = pd.DataFrame(results)

Dask dashboard
^^^^^^^^^^^^^^

The Dask dashboard (typically at ``http://localhost:8787``) provides real-time monitoring
of task progress, memory usage, and worker status. Open the URL printed by
``client.dashboard_link`` in your browser.

.. seealso::

   :doc:`../getting_started/installation` for dependency details,
   :doc:`preprocessing` for constructor parameters including ``tmp_folder``.

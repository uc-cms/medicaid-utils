Expected Data Layout
====================

The package expects Medicaid claim files stored as **Parquet** datasets, split by **year** and
**state**, and **sorted by beneficiary ID** (``BENE_MSIS`` or ``MSIS_ID``).

The folder hierarchy under your ``data_root`` must follow the structure below.

MAX Files
---------

.. code-block:: text

   data_root/
     medicaid/
       {YEAR}/
         {STATE}/
           max/
             ip/parquet/      # Inpatient claims
             ot/parquet/      # Outpatient claims
             ps/parquet/      # Person Summary
             cc/parquet/      # Chronic Conditions

Example path: ``data_root/medicaid/2012/WY/max/ip/parquet/``

TAF Files
---------

TAF claims are split into multiple subtypes per claim type:

.. code-block:: text

   data_root/
     medicaid/
       {YEAR}/
         {STATE}/
           taf/
             ip/                    # Inpatient
               iph/parquet/         #   Header (base)
               ipl/parquet/         #   Line
               ipoccr/parquet/      #   Occurrence codes
               ipdx/parquet/        #   Diagnosis codes
               ipndc/parquet/       #   NDC codes
             ot/                    # Outpatient
               oth/parquet/
               otl/parquet/
               otoccr/parquet/
               otdx/parquet/
               otndc/parquet/
             lt/                    # Long-Term Care
               lth/parquet/
               ltl/parquet/
               ltoccr/parquet/
               ltdx/parquet/
               ltndc/parquet/
             rx/                    # Pharmacy
               rxh/parquet/         #   Header (base)
               rxl/parquet/         #   Line
               rxndc/parquet/       #   NDC codes
             de/                    # Demographics/Eligibility
               debse/parquet/       #   Base demographics
               dedts/parquet/       #   Dates
               demc/parquet/        #   Managed care
               dedsb/parquet/       #   Disability
               demfp/parquet/       #   Money Follows the Person
               dewvr/parquet/       #   Waiver
               dehsp/parquet/       #   Home health/SPF
               dedxndc/parquet/     #   Diagnosis & NDC codes

Each Parquet dataset can be a single file or a directory of partitioned Parquet files.
Files must be **pre-sorted by beneficiary ID** to enable efficient partition-level operations.

Preparing Your Data
-------------------

If your raw CMS data is in SAS or CSV format, you will need to convert it to Parquet and
organize it into the folder structure above. Key points:

1. **Sort by beneficiary ID** before writing to Parquet. This enables efficient partition-level
   joins and lookups.
2. **Split by year and state** into separate directories.
3. Use ``pyarrow`` as the Parquet engine for best compatibility.

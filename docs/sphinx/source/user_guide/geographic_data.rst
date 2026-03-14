Geographic Data & Rural-Urban Classification
=============================================

medicaid-utils includes built-in geographic classification for beneficiaries and
crosswalk datasets linking ZIP codes, ZCTAs, PCSAs, and rural-urban codes.

Rural-Urban Classification via Person Summary
----------------------------------------------

Both MAX and TAF person summary classes automatically classify beneficiaries as
rural or non-rural during preprocessing. The classification is based on the
beneficiary's residence ZIP code.

.. code-block:: python

   from medicaid_utils.preprocessing import max_ps

   # RUCA-based classification (default)
   ps = max_ps.MAXPS(year=2012, state="WY", data_root="/data/cms")

   # RUCC-based classification
   ps = max_ps.MAXPS(year=2012, state="WY", data_root="/data/cms", rural_method="rucc")

After preprocessing, the DataFrame includes:

- ``rural`` — 1 if rural, 0 if non-rural, -1 if unknown
- ``ruca_code`` — the RUCA classification code (always created)
- ``rucc_code`` — the RUCC classification code (always created)

The ``rural_method`` parameter controls which code is used to compute the ``rural``
column, but both code columns are always present.

RUCA Codes
^^^^^^^^^^

Rural-Urban Commuting Area codes (version 3.1) classify census tracts by urbanization
and commuting patterns:

- **Codes 1–3:** Urban (metropolitan)
- **Codes 4–10:** Rural (micropolitan, small town, isolated)

Source: `USDA Economic Research Service <https://www.ers.usda.gov/data-products/rural-urban-commuting-area-codes/>`_

RUCC Codes
^^^^^^^^^^

Rural-Urban Continuum Codes classify counties by population size and adjacency
to metro areas:

- **Codes 1–3:** Metro counties
- **Codes 4–9:** Non-metro counties (4–7 are urban, 8–9 are rural)

Source: `USDA Economic Research Service <https://www.ers.usda.gov/data-products/rural-urban-continuum-codes/>`_

Primary Care Service Areas (PCSA)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

PCSAs are geographic units defined by the Dartmouth Atlas Project based on
Medicare patient flow to primary care providers. medicaid-utils includes a
ZIP-to-PCSA crosswalk from the Dartmouth Atlas.

ZIP Crosswalk Module
--------------------

The :mod:`~medicaid_utils.other_datasets.zip` module builds a comprehensive
crosswalk linking ZIP codes to ZCTAs, PCSAs, and RUCA codes by combining
multiple public data sources.

Pooling ZIP/PCSA datasets
^^^^^^^^^^^^^^^^^^^^^^^^^

:func:`~medicaid_utils.other_datasets.zip.pool_zipcode_pcsa_datasets` combines
four data sources into a unified crosswalk:

1. **Dartmouth Atlas** — ZIP-to-PCSA crosswalk
2. **SimpleMaps** — ZIP code centroids and state codes
3. **GeoNames** — ZIP code geographic coordinates
4. **UDS Mapper** — ZIP-to-ZCTA crosswalk

.. code-block:: python

   from medicaid_utils.other_datasets import zip as zip_data

   df_crosswalk = zip_data.pool_zipcode_pcsa_datasets()

Generating the full crosswalk
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:func:`~medicaid_utils.other_datasets.zip.generate_zip_pcsa_ruca_crosswalk` extends
the pooled dataset by merging RUCA codes:

.. code-block:: python

   df_full = zip_data.generate_zip_pcsa_ruca_crosswalk()
   # Columns: zip, state_cd, pcsa, zcta, ruca_code, ...

Data Sources
^^^^^^^^^^^^

================================= ================================================
Dataset                           Source
================================= ================================================
RUCA 3.1                          USDA Economic Research Service
RUCC                              USDA Economic Research Service
PCSA crosswalk                    Dartmouth Atlas Project (via data.world)
ZIP-ZCTA crosswalk                UDS Mapper
ZIP centroids                     SimpleMaps, GeoNames
================================= ================================================

.. seealso::

   :doc:`../getting_started/glossary` for acronym definitions,
   :doc:`preprocessing` for person summary loading.

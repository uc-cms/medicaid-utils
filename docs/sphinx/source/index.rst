medicaid-utils Documentation
============================

**medicaid-utils** is a Python toolkit for constructing patient-level analytic files from
Medicaid claims data. It implements validated cleaning routines, variable construction methods,
and public-domain clinical measure algorithms for both MAX and TAF CMS file formats.

Built on `Dask <https://www.dask.org/>`_ for scalable, distributed processing of large-scale
claims datasets.

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   getting_started/installation
   getting_started/data_layout
   getting_started/quickstart
   getting_started/references

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   user_guide/preprocessing
   user_guide/cohort_extraction
   user_guide/risk_adjustment
   user_guide/quality_measures

.. toctree::
   :maxdepth: 2
   :caption: Tutorials

   tutorials/diabetes_cohort
   tutorials/oud_cohort

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   medicaid_utils


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

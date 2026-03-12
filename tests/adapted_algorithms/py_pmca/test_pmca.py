"""Tests for the Pediatric Medical Complexity Algorithm (PMCA)."""

import pytest
import pandas as pd
import dask.dataframe as dd
import numpy as np

from medicaid_utils.adapted_algorithms.py_pmca.pmca import (
    PediatricMedicalComplexity,
    pmca_chronic_conditions,
)


def _make_dask_df(data, npartitions=1):
    """Helper to create a dask DataFrame from a dict."""
    pdf = pd.DataFrame(data)
    return dd.from_pandas(pdf, npartitions=npartitions)


# ---------------------------------------------------------------------------
# Tests for PediatricMedicalComplexity.create_pmca_condition_counts
# ---------------------------------------------------------------------------


class TestCreatePmcaConditionCounts:
    """Tests for PediatricMedicalComplexity.create_pmca_condition_counts."""

    def test_adds_condition_columns(self):
        """Verify condition count, any_*, and *_2h columns are created."""
        df = _make_dask_df(
            {
                "MSIS_ID": ["P001", "P002"],
                "LST_DIAG_CD_RAW": [
                    "25000,49300",  # diabetes + asthma-related prefixes
                    "00000",
                ],
            }
        )
        result = PediatricMedicalComplexity.create_pmca_condition_counts(
            df, "LST_DIAG_CD_RAW"
        )
        result_pdf = result.compute()

        # Should have any_progressive column
        assert "any_progressive" in result_pdf.columns

        # Should have any_ and _2h variants for each condition
        conditions_csv = pd.read_csv(
            PediatricMedicalComplexity.data_folder + "/pmca_condition_codes.csv"
        )
        conditions = conditions_csv["condition"].unique().tolist()
        for cond in conditions:
            assert (
                f"any_{cond}" in result_pdf.columns
            ), f"Missing any_{cond}"
            assert (
                f"{cond}_2h" in result_pdf.columns
            ), f"Missing {cond}_2h"

    def test_no_matching_codes_yields_zeros(self):
        """When no codes match, all condition counts should be 0."""
        df = _make_dask_df(
            {
                "MSIS_ID": ["P001"],
                "LST_DIAG_CD_RAW": ["ZZZZZ"],
            }
        )
        result = PediatricMedicalComplexity.create_pmca_condition_counts(
            df, "LST_DIAG_CD_RAW"
        )
        result_pdf = result.compute()

        assert result_pdf["any_progressive"].iloc[0] == 0
        # All any_ columns should be 0
        any_cols = [c for c in result_pdf.columns if c.startswith("any_")]
        for col in any_cols:
            assert result_pdf[col].iloc[0] == 0, f"{col} should be 0"


# ---------------------------------------------------------------------------
# Tests for PediatricMedicalComplexity.get_pmca_chronic_condition_categories
# ---------------------------------------------------------------------------


class TestGetPmcaCategories:
    """Tests for get_pmca_chronic_condition_categories."""

    def test_categories_added(self):
        """Verify cond_less and cond_more columns are created."""
        # First create condition counts, then categorize
        df = _make_dask_df(
            {
                "MSIS_ID": ["P001"],
                "LST_DIAG_CD_RAW": ["ZZZZZ"],
            }
        )
        df = PediatricMedicalComplexity.create_pmca_condition_counts(
            df, "LST_DIAG_CD_RAW"
        )
        result = PediatricMedicalComplexity.get_pmca_chronic_condition_categories(
            df
        )
        result_pdf = result.compute()

        assert "cond_less" in result_pdf.columns
        assert "cond_more" in result_pdf.columns

    def test_healthy_patient_gets_non_chronic(self):
        """A patient with no matching codes should be classified as Non-Chronic (1)."""
        df = _make_dask_df(
            {
                "MSIS_ID": ["P001"],
                "LST_DIAG_CD_RAW": ["ZZZZZ"],
            }
        )
        df = PediatricMedicalComplexity.create_pmca_condition_counts(
            df, "LST_DIAG_CD_RAW"
        )
        result = PediatricMedicalComplexity.get_pmca_chronic_condition_categories(
            df
        )
        result_pdf = result.compute()

        assert result_pdf["cond_less"].iloc[0] == 1  # Non-Chronic
        assert result_pdf["cond_more"].iloc[0] == 1  # Non-Chronic


# ---------------------------------------------------------------------------
# Tests for the convenience pmca_chronic_conditions() function
# ---------------------------------------------------------------------------


class TestPmcaChronicConditions:
    """Tests for the top-level pmca_chronic_conditions() function."""

    def test_end_to_end(self):
        """End-to-end test: should produce pmca_cond_less and pmca_cond_more."""
        df = _make_dask_df(
            {
                "MSIS_ID": ["P001", "P002"],
                "LST_DIAG_CD_RAW": ["25000,49300", "ZZZZZ"],
            }
        )
        result = pmca_chronic_conditions(df, diag_cd_lst_col="LST_DIAG_CD_RAW")
        result_pdf = result.compute()

        assert "pmca_cond_less" in result_pdf.columns
        assert "pmca_cond_more" in result_pdf.columns

        # Original columns should be preserved
        assert "MSIS_ID" in result_pdf.columns
        assert "LST_DIAG_CD_RAW" in result_pdf.columns

        # Values should be 1, 2, or 3
        assert set(result_pdf["pmca_cond_less"].unique()).issubset({1, 2, 3})
        assert set(result_pdf["pmca_cond_more"].unique()).issubset({1, 2, 3})

    def test_intermediate_columns_removed(self):
        """Intermediate condition count columns should be removed from final output."""
        df = _make_dask_df(
            {
                "MSIS_ID": ["P001"],
                "LST_DIAG_CD_RAW": ["ZZZZZ"],
            }
        )
        result = pmca_chronic_conditions(df, diag_cd_lst_col="LST_DIAG_CD_RAW")
        result_pdf = result.compute()

        # The final output should only have original columns + pmca_cond_less/more
        assert len(result_pdf.columns) == 4  # MSIS_ID, LST_DIAG_CD_RAW, pmca_cond_less, pmca_cond_more

    def test_single_row(self):
        """Should work with a single-row dataframe."""
        df = _make_dask_df(
            {
                "MSIS_ID": ["P001"],
                "LST_DIAG_CD_RAW": ["25000"],
            }
        )
        result = pmca_chronic_conditions(df)
        result_pdf = result.compute()
        assert len(result_pdf) == 1
        assert "pmca_cond_less" in result_pdf.columns

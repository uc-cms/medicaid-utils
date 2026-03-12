"""Tests for the Elixhauser comorbidity scoring algorithm."""

import pytest
import pandas as pd
import dask.dataframe as dd
import numpy as np

from medicaid_utils.adapted_algorithms.py_elixhauser.elixhauser_comorbidity import (
    ElixhauserScoring,
    score,
)


def _make_dask_df(data, npartitions=1):
    """Helper to create a dask DataFrame from a dict."""
    pdf = pd.DataFrame(data)
    return dd.from_pandas(pdf, npartitions=npartitions)


# ---------------------------------------------------------------------------
# Tests for ElixhauserScoring.flag_comorbidities
# ---------------------------------------------------------------------------


class TestFlagComorbidities:
    """Tests for ElixhauserScoring.flag_comorbidities."""

    def test_basic_icd9_max_format(self):
        """Verify that flag_comorbidities adds ELX_GRP_1..31 columns using ICD-9 (MAX format)."""
        df = _make_dask_df(
            {
                "BENE_MSIS": ["B001", "B002", "B003"],
                "LST_DIAG_CD": [
                    "39891,40201",  # codes that may match CHF / hypertension groups
                    "25000",  # diabetes-related prefix
                    "00000",  # unlikely to match anything
                ],
            }
        )
        result = ElixhauserScoring.flag_comorbidities(
            df, "LST_DIAG_CD", cms_format="MAX"
        )
        result_pdf = result.compute()

        # All 31 ELX_GRP columns should be present
        for i in range(1, 32):
            col = f"ELX_GRP_{i}"
            assert col in result_pdf.columns, f"Missing column {col}"
            assert result_pdf[col].dtype in (
                np.int64,
                np.int32,
                int,
            ), f"{col} should be integer type"

        # Values should be 0 or 1
        for i in range(1, 32):
            assert set(result_pdf[f"ELX_GRP_{i}"].unique()).issubset(
                {0, 1}
            ), f"ELX_GRP_{i} should only contain 0 or 1"

    def test_icd10_taf_format(self):
        """Verify flag_comorbidities works with ICD-10 codes (TAF format)."""
        df = _make_dask_df(
            {
                "BENE_MSIS": ["B001"],
                "LST_DIAG_CD": ["I110,E119"],
            }
        )
        result = ElixhauserScoring.flag_comorbidities(
            df, "LST_DIAG_CD", cms_format="TAF"
        )
        result_pdf = result.compute()

        for i in range(1, 32):
            assert f"ELX_GRP_{i}" in result_pdf.columns

    def test_original_columns_preserved(self):
        """Verify that original columns are not dropped."""
        df = _make_dask_df(
            {
                "BENE_MSIS": ["B001"],
                "LST_DIAG_CD": ["25000"],
                "extra_col": [42],
            }
        )
        result = ElixhauserScoring.flag_comorbidities(
            df, "LST_DIAG_CD", cms_format="MAX"
        )
        result_pdf = result.compute()
        assert "BENE_MSIS" in result_pdf.columns
        assert "extra_col" in result_pdf.columns
        assert result_pdf["extra_col"].iloc[0] == 42


# ---------------------------------------------------------------------------
# Tests for ElixhauserScoring.calculate_final_score
# ---------------------------------------------------------------------------


class TestCalculateFinalScore:
    """Tests for ElixhauserScoring.calculate_final_score."""

    def test_score_sums_groups(self):
        """Verify that the final score is the sum of group flags."""
        data = {"BENE_MSIS": ["B001"]}
        for i in range(1, 32):
            data[f"ELX_GRP_{i}"] = [1 if i <= 5 else 0]
        df = _make_dask_df(data)

        result = ElixhauserScoring.calculate_final_score(df)
        result_pdf = result.compute()
        assert result_pdf["elixhauser_score"].iloc[0] == 5

    def test_custom_output_column_name(self):
        """Verify that a custom output column name works."""
        data = {"BENE_MSIS": ["B001"]}
        for i in range(1, 32):
            data[f"ELX_GRP_{i}"] = [0]
        df = _make_dask_df(data)

        result = ElixhauserScoring.calculate_final_score(
            df, output_column_name="my_score"
        )
        result_pdf = result.compute()
        assert "my_score" in result_pdf.columns
        assert result_pdf["my_score"].iloc[0] == 0


# ---------------------------------------------------------------------------
# Tests for the convenience score() function
# ---------------------------------------------------------------------------


class TestScoreFunction:
    """Tests for the top-level score() convenience function."""

    def test_end_to_end_max(self):
        """End-to-end test: score() should produce ELX_GRP_* columns and a final score."""
        df = _make_dask_df(
            {
                "BENE_MSIS": ["B001", "B002"],
                "LST_DIAG_CD": ["25000,39891", "00000"],
            }
        )
        result = score(df, "LST_DIAG_CD", cms_format="MAX")
        result_pdf = result.compute()

        assert "elixhauser_score" in result_pdf.columns
        for i in range(1, 32):
            assert f"ELX_GRP_{i}" in result_pdf.columns

        # Score should be non-negative integer
        assert (result_pdf["elixhauser_score"] >= 0).all()

    def test_single_row(self):
        """Score function should work with a single-row dataframe."""
        df = _make_dask_df(
            {
                "BENE_MSIS": ["B001"],
                "LST_DIAG_CD": ["25000"],
            }
        )
        result = score(df, "LST_DIAG_CD", cms_format="MAX")
        result_pdf = result.compute()
        assert len(result_pdf) == 1
        assert "elixhauser_score" in result_pdf.columns

    def test_no_matching_codes(self):
        """When no diagnosis codes match any group, score should be 0."""
        df = _make_dask_df(
            {
                "BENE_MSIS": ["B001"],
                "LST_DIAG_CD": ["ZZZZZ"],
            }
        )
        result = score(df, "LST_DIAG_CD", cms_format="MAX")
        result_pdf = result.compute()
        assert result_pdf["elixhauser_score"].iloc[0] == 0

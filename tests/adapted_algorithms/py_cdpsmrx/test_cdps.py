"""Tests for the CDPS+Rx risk adjustment algorithm."""

import pytest
import pandas as pd
import dask.dataframe as dd
import numpy as np

from medicaid_utils.adapted_algorithms.py_cdpsmrx.cdps_rx_risk_adjustment import (
    CdpsRxRiskAdjustment,
    cdps_rx_risk_adjust,
)


def _make_dask_df(data, npartitions=1):
    """Helper to create a dask DataFrame from a dict."""
    pdf = pd.DataFrame(data)
    return dd.from_pandas(pdf, npartitions=npartitions)


def _make_base_df(
    n=3,
    ages=None,
    genders=None,
    aids=None,
    diag_codes=None,
    ndc_codes=None,
):
    """Create a synthetic beneficiary-level dask DataFrame for CDPS testing.

    Parameters
    ----------
    n : int
        Number of rows (ignored if explicit lists are provided).
    ages : list, optional
        Ages for each row.
    genders : list, optional
        Female indicator (1=female, 0=male) for each row.
    aids : list, optional
        Aid category codes (AA, DA, AC, DC) for each row.
    diag_codes : list, optional
        Comma-separated diagnosis code strings for each row.
    ndc_codes : list, optional
        Comma-separated NDC code strings for each row.
    """
    if ages is None:
        ages = [30, 10, 55]
    if genders is None:
        genders = [1, 0, 0]
    if aids is None:
        aids = ["AA", "DC", "DA"]
    if diag_codes is None:
        diag_codes = ["25000,49300", "49300", "25000"]
    if ndc_codes is None:
        ndc_codes = ["00000000000", "00000000000", "00000000000"]
    n = len(ages)
    return _make_dask_df(
        {
            "BENE_MSIS": [f"B{i:03d}" for i in range(n)],
            "age": ages,
            "Female": genders,
            "aid": aids,
            "LST_DIAG_CD": diag_codes,
            "LST_NDC": ndc_codes,
        }
    )


# ---------------------------------------------------------------------------
# Tests for CdpsRxRiskAdjustment.add_age_and_gender_cols
# ---------------------------------------------------------------------------


class TestAddAgeAndGenderCols:
    """Tests for CdpsRxRiskAdjustment.add_age_and_gender_cols."""

    def test_adds_all_age_gender_columns(self):
        """Should add 11 age/gender indicator columns."""
        df = _make_base_df()
        result_df, lst_cols = CdpsRxRiskAdjustment.add_age_and_gender_cols(df)
        result_pdf = result_df.compute()

        expected_cols = [
            "a_under1",
            "a_1_4",
            "a_5_14m",
            "a_5_14f",
            "a_15_24m",
            "a_15_24f",
            "a_25_44m",
            "a_25_44f",
            "a_45_64m",
            "a_45_64f",
            "a_65",
        ]
        for col in expected_cols:
            assert col in result_pdf.columns, f"Missing column {col}"
            assert col in lst_cols, f"Missing from lst_cols: {col}"

    def test_age_gender_values_are_binary(self):
        """All age/gender columns should contain only 0 or 1."""
        df = _make_base_df()
        result_df, lst_cols = CdpsRxRiskAdjustment.add_age_and_gender_cols(df)
        result_pdf = result_df.compute()

        for col in lst_cols:
            assert set(result_pdf[col].unique()).issubset(
                {0, 1}
            ), f"{col} should only contain 0 or 1"

    def test_mutually_exclusive_age_groups(self):
        """Each row should have exactly one age/gender indicator set to 1."""
        df = _make_base_df(ages=[0, 3, 10, 10, 20, 20, 35, 35, 50, 50, 70],
                           genders=[0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0],
                           aids=["AA"] * 11,
                           diag_codes=["00000"] * 11,
                           ndc_codes=["00000000000"] * 11)
        result_df, lst_cols = CdpsRxRiskAdjustment.add_age_and_gender_cols(df)
        result_pdf = result_df.compute()

        row_sums = result_pdf[lst_cols].sum(axis=1)
        assert (row_sums == 1).all(), "Each row should have exactly one age/gender flag"

    def test_female_30_maps_to_a_25_44f(self):
        """A 30-year-old female should map to a_25_44f."""
        df = _make_base_df(ages=[30], genders=[1], aids=["AA"],
                           diag_codes=["00000"], ndc_codes=["00000000000"])
        result_df, _ = CdpsRxRiskAdjustment.add_age_and_gender_cols(df)
        result_pdf = result_df.compute()
        assert result_pdf["a_25_44f"].iloc[0] == 1

    def test_male_50_maps_to_a_45_64m(self):
        """A 50-year-old male should map to a_45_64m."""
        df = _make_base_df(ages=[50], genders=[0], aids=["AA"],
                           diag_codes=["00000"], ndc_codes=["00000000000"])
        result_df, _ = CdpsRxRiskAdjustment.add_age_and_gender_cols(df)
        result_pdf = result_df.compute()
        assert result_pdf["a_45_64m"].iloc[0] == 1


# ---------------------------------------------------------------------------
# Tests for CdpsRxRiskAdjustment.add_cdps_category_cols
# ---------------------------------------------------------------------------


class TestAddCdpsCategoryCols:
    """Tests for CdpsRxRiskAdjustment.add_cdps_category_cols."""

    def test_adds_cdps_columns(self):
        """Should add CDPS category columns including NONE, OTHER, NOCDPS."""
        df = _make_base_df()
        result_df, lst_cols = CdpsRxRiskAdjustment.add_cdps_category_cols(
            df, "LST_DIAG_CD"
        )
        result_pdf = result_df.compute()

        assert "NONE" in result_pdf.columns
        assert "OTHER" in result_pdf.columns
        assert "NOCDPS" in result_pdf.columns
        assert len(lst_cols) > 0

    def test_no_diag_codes_flags_none(self):
        """Beneficiary with empty diagnosis codes should have NONE=1."""
        df = _make_base_df(
            ages=[30], genders=[1], aids=["AA"],
            diag_codes=[""], ndc_codes=["00000000000"]
        )
        result_df, _ = CdpsRxRiskAdjustment.add_cdps_category_cols(
            df, "LST_DIAG_CD"
        )
        result_pdf = result_df.compute()
        assert result_pdf["NONE"].iloc[0] == 1


# ---------------------------------------------------------------------------
# Tests for CdpsRxRiskAdjustment.add_mrx_cat_cols
# ---------------------------------------------------------------------------


class TestAddMrxCatCols:
    """Tests for CdpsRxRiskAdjustment.add_mrx_cat_cols."""

    def test_adds_mrx_columns(self):
        """Should add MRx category columns."""
        df = _make_base_df()
        result_df, lst_cols = CdpsRxRiskAdjustment.add_mrx_cat_cols(
            df, lst_ndc_col_name="LST_NDC"
        )
        result_pdf = result_df.compute()

        assert len(lst_cols) > 0
        assert "NONE" in lst_cols
        assert "OTHER" in lst_cols

    def test_unknown_ndc_maps_to_other(self):
        """An NDC code not in the mapping should map to OTHER."""
        df = _make_base_df(
            ages=[30], genders=[1], aids=["AA"],
            diag_codes=["25000"], ndc_codes=["99999999999"]
        )
        result_df, _ = CdpsRxRiskAdjustment.add_mrx_cat_cols(
            df, lst_ndc_col_name="LST_NDC"
        )
        result_pdf = result_df.compute()
        assert result_pdf["OTHER"].iloc[0] == 1

    def test_empty_ndc_flags_none(self):
        """Empty NDC column should flag NONE=1."""
        df = _make_base_df(
            ages=[30], genders=[1], aids=["AA"],
            diag_codes=["25000"], ndc_codes=[""]
        )
        result_df, _ = CdpsRxRiskAdjustment.add_mrx_cat_cols(
            df, lst_ndc_col_name="LST_NDC"
        )
        result_pdf = result_df.compute()
        assert result_pdf["NONE"].iloc[0] == 1


# ---------------------------------------------------------------------------
# Tests for the convenience cdps_rx_risk_adjust() function
# ---------------------------------------------------------------------------


class TestCdpsRxRiskAdjust:
    """Tests for the top-level cdps_rx_risk_adjust() function."""

    def test_end_to_end_produces_risk_column(self):
        """End-to-end test: should produce a 'risk' score column."""
        df = _make_base_df()
        result = cdps_rx_risk_adjust(
            df,
            lst_diag_col_name="LST_DIAG_CD",
            lst_ndc_col_name="LST_NDC",
            score_col_name="risk",
        )
        result_pdf = result.compute()

        assert "risk" in result_pdf.columns
        # Risk scores should be numeric
        assert result_pdf["risk"].dtype in (np.float64, np.float32, float)

    def test_custom_score_column_name(self):
        """Should work with a custom score column name."""
        df = _make_base_df()
        result = cdps_rx_risk_adjust(
            df,
            lst_diag_col_name="LST_DIAG_CD",
            lst_ndc_col_name="LST_NDC",
            score_col_name="my_risk",
        )
        result_pdf = result.compute()
        assert "my_risk" in result_pdf.columns

    def test_original_id_column_preserved(self):
        """The BENE_MSIS column should be preserved in the output."""
        df = _make_base_df()
        result = cdps_rx_risk_adjust(df)
        result_pdf = result.compute()
        assert "BENE_MSIS" in result_pdf.columns

    def test_all_aid_categories(self):
        """Should handle all four aid categories: AA, AC, DA, DC."""
        df = _make_base_df(
            ages=[30, 10, 30, 10],
            genders=[0, 1, 1, 0],
            aids=["AA", "AC", "DA", "DC"],
            diag_codes=["25000", "49300", "25000", "49300"],
            ndc_codes=["00000000000"] * 4,
        )
        result = cdps_rx_risk_adjust(df)
        result_pdf = result.compute()
        assert len(result_pdf) == 4
        assert "risk" in result_pdf.columns
        # All rows should have a numeric risk score
        assert result_pdf["risk"].notna().all()

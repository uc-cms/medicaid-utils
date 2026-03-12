"""Tests for medicaid_utils.common_utils.dataframe_utils."""
import pandas as pd
import numpy as np
import dask.dataframe as dd
import pytest

from medicaid_utils.common_utils import dataframe_utils


@pytest.fixture
def sample_dask_df():
    """Create a small dask DataFrame for testing."""
    pdf = pd.DataFrame(
        {
            "BENE_MSIS": ["A", "B", "C", "A", "B"],
            "date_col": pd.to_datetime(
                ["20200101", "20200215", "20200301", "20200401", "20200515"],
                format="%Y%m%d",
            ),
            "value": [1.0, 2.0, 3.0, 4.0, 5.0],
            "code": ["100", "200", "300", "400", "500"],
        }
    )
    return dd.from_pandas(pdf, npartitions=2)


@pytest.fixture
def indexed_dask_df():
    """Create a dask DataFrame with BENE_MSIS as index."""
    pdf = pd.DataFrame(
        {
            "BENE_MSIS": ["A", "B", "C"],
            "value": [10, 20, 30],
        }
    ).set_index("BENE_MSIS")
    pdf.index.name = "BENE_MSIS"
    return dd.from_pandas(pdf, npartitions=1)


class TestToggleDatetimeString:
    def test_datetime_to_string(self, sample_dask_df):
        result = dataframe_utils.toggle_datetime_string(
            sample_dask_df, ["date_col"], to_string=True
        )
        computed = result.compute()
        # Why: pyarrow-backed DataFrames use string[pyarrow] instead of object
        assert not pd.api.types.is_datetime64_any_dtype(computed["date_col"])
        assert computed["date_col"].iloc[0] == "20200101"

    def test_returns_dataframe(self, sample_dask_df):
        """Verify the function returns a DataFrame (bug fix test)."""
        result = dataframe_utils.toggle_datetime_string(
            sample_dask_df, ["date_col"], to_string=True
        )
        assert result is not None
        assert isinstance(result, dd.DataFrame)

    def test_string_to_datetime(self, sample_dask_df):
        # First convert to string
        df_str = dataframe_utils.toggle_datetime_string(
            sample_dask_df, ["date_col"], to_string=True
        )
        # Then convert back
        df_dt = dataframe_utils.toggle_datetime_string(
            df_str, ["date_col"], to_string=False
        )
        computed = df_dt.compute()
        assert pd.api.types.is_datetime64_any_dtype(computed["date_col"])

    def test_no_matching_columns(self, sample_dask_df):
        result = dataframe_utils.toggle_datetime_string(
            sample_dask_df, ["nonexistent_col"], to_string=True
        )
        # Should return unchanged DataFrame
        pd.testing.assert_frame_equal(
            result.compute(), sample_dask_df.compute()
        )


class TestConvertDdcolsToDatetime:
    def test_converts_string_to_datetime(self):
        pdf = pd.DataFrame({"dt": ["20200101", "20200215", "20200301"]})
        ddf = dd.from_pandas(pdf, npartitions=1)
        result = dataframe_utils.convert_ddcols_to_datetime(ddf, ["dt"])
        computed = result.compute()
        assert pd.api.types.is_datetime64_any_dtype(computed["dt"])
        assert computed["dt"].iloc[0] == pd.Timestamp("2020-01-01")

    def test_invalid_dates_become_nat(self):
        pdf = pd.DataFrame({"dt": ["20200101", "INVALID", "20200301"]})
        ddf = dd.from_pandas(pdf, npartitions=1)
        result = dataframe_utils.convert_ddcols_to_datetime(ddf, ["dt"])
        computed = result.compute()
        assert pd.isna(computed["dt"].iloc[1])


class TestCopyDdcols:
    def test_copies_columns(self):
        pdf = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        ddf = dd.from_pandas(pdf, npartitions=1)
        result = dataframe_utils.copy_ddcols(ddf, ["a", "b"], ["a_copy", "b_copy"])
        computed = result.compute()
        pd.testing.assert_series_equal(
            computed["a"], computed["a_copy"], check_names=False
        )
        pd.testing.assert_series_equal(
            computed["b"], computed["b_copy"], check_names=False
        )


class TestGetReducedColumnNames:
    def test_without_combine(self):
        cols = [("sum", "value"), ("mean", "score"), ("count", "")]
        result = dataframe_utils.get_reduced_column_names(cols)
        assert result == ["value", "score", "count"]

    def test_with_combine(self):
        cols = [("sum", "value"), ("mean", "score")]
        result = dataframe_utils.get_reduced_column_names(
            cols, combine_levels=True
        )
        assert result == ["value_sum", "score_mean"]


class TestFixIndex:
    def test_sets_index_from_column(self):
        pdf = pd.DataFrame(
            {"BENE_MSIS": ["A", "B", "C"], "value": [1, 2, 3]}
        )
        ddf = dd.from_pandas(pdf, npartitions=1)
        result = dataframe_utils.fix_index(ddf, "BENE_MSIS")
        computed = result.compute()
        assert computed.index.name == "BENE_MSIS"
        assert "BENE_MSIS" not in computed.columns

    def test_keeps_column_when_drop_false(self):
        pdf = pd.DataFrame(
            {"BENE_MSIS": ["A", "B", "C"], "value": [1, 2, 3]}
        )
        ddf = dd.from_pandas(pdf, npartitions=1)
        result = dataframe_utils.fix_index(ddf, "BENE_MSIS", drop_column=False)
        computed = result.compute()
        assert computed.index.name == "BENE_MSIS"

    def test_raises_for_missing_column(self):
        pdf = pd.DataFrame({"value": [1, 2, 3]})
        ddf = dd.from_pandas(pdf, npartitions=1)
        with pytest.raises(ValueError, match="not in dataframe"):
            dataframe_utils.fix_index(ddf, "MISSING_COL")


class TestSafeConvertIntToStr:
    def test_converts_float_strings(self):
        pdf = pd.DataFrame({"code": ["100.0", "200.0", "abc", ""]})
        ddf = dd.from_pandas(pdf, npartitions=1)
        result = dataframe_utils.safe_convert_int_to_str(ddf, ["code"])
        computed = result.compute()
        assert computed["code"].iloc[0] == "100"
        assert computed["code"].iloc[1] == "200"
        assert computed["code"].iloc[2] == "abc"

    def test_handles_nan(self):
        pdf = pd.DataFrame({"code": [np.nan, "5.0", None]})
        ddf = dd.from_pandas(pdf, npartitions=1)
        result = dataframe_utils.safe_convert_int_to_str(ddf, ["code"])
        computed = result.compute()
        assert computed["code"].iloc[0] == ""
        assert computed["code"].iloc[1] == "5"


class TestGetFirstDayGap:
    def test_finds_gap(self):
        pdf = pd.DataFrame(
            {
                "patient": ["A", "A", "A", "A"],
                "visit_date": pd.to_datetime(
                    ["2020-01-01", "2020-01-15", "2020-03-01", "2020-03-15"]
                ),
                "start_date": pd.to_datetime(["2020-01-01"] * 4),
            }
        )
        result = dataframe_utils.get_first_day_gap(
            pdf, "patient", "visit_date", "start_date", threshold=30
        )
        assert result.shape[0] == 1
        assert result["patient"].iloc[0] == "A"

    def test_no_gap(self):
        pdf = pd.DataFrame(
            {
                "patient": ["A", "A"],
                "visit_date": pd.to_datetime(["2020-01-01", "2020-01-10"]),
                "start_date": pd.to_datetime(["2020-01-01"] * 2),
            }
        )
        result = dataframe_utils.get_first_day_gap(
            pdf, "patient", "visit_date", "start_date", threshold=30
        )
        assert result.shape[0] == 0

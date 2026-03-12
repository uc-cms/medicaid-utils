"""Tests for the NYU/Billings ED visit classification algorithm."""

import pytest
import pandas as pd
import dask.dataframe as dd
import numpy as np

from medicaid_utils.adapted_algorithms.py_nyu_billings.billings_ed import (
    BillingsED,
    get_nyu_ed_proba,
)


def _make_dask_df(data, npartitions=1):
    """Helper to create a dask DataFrame from a dict."""
    pdf = pd.DataFrame(data)
    return dd.from_pandas(pdf, npartitions=npartitions)


# ---------------------------------------------------------------------------
# Tests for BillingsED.recode_diag_code
# ---------------------------------------------------------------------------


class TestRecodeDiagCode:
    """Tests for BillingsED.recode_diag_code."""

    def test_unknown_code_returns_itself(self):
        """A code with no recode rule should return unchanged."""
        result = BillingsED.recode_diag_code("ZZZZZ")
        assert result == "ZZZZZ"

    def test_returns_string(self):
        """Recode should always return a string."""
        result = BillingsED.recode_diag_code("25000")
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# Tests for BillingsED.get_special_categories
# ---------------------------------------------------------------------------


class TestGetSpecialCategories:
    """Tests for BillingsED.get_special_categories."""

    def test_returns_dict_with_expected_keys(self):
        """Should return a dict with acs, psych, drug, alcohol, injury keys."""
        result = BillingsED.get_special_categories("25000")
        expected_keys = {"acs", "psych", "drug", "alcohol", "injury"}
        assert set(result.keys()) == expected_keys

    def test_values_are_binary(self):
        """All category values should be 0 or 1."""
        result = BillingsED.get_special_categories("25000")
        for key, val in result.items():
            assert val in (0, 1), f"Category {key} should be 0 or 1"

    def test_injury_code_flagged(self):
        """An injury-like ICD-9 code should flag injury category."""
        # E-codes (E800-E999) are injury codes in ICD-9
        result = BillingsED.get_special_categories("E8000")
        assert result["injury"] == 1

    def test_non_special_code(self):
        """A code matching no special category should have all zeros."""
        result = BillingsED.get_special_categories("ZZZZZ")
        assert all(v == 0 for v in result.values())


# ---------------------------------------------------------------------------
# Tests for BillingsED.get_nyu_ed_proba_for_dx_code
# ---------------------------------------------------------------------------


class TestGetNyuEdProbaForDxCode:
    """Tests for BillingsED.get_nyu_ed_proba_for_dx_code."""

    def test_unknown_code_returns_empty_dict(self):
        """A code not in the eddxs lookup should return an empty dict."""
        result = BillingsED.get_nyu_ed_proba_for_dx_code("ZZZZZ")
        assert result == {}

    def test_known_code_returns_probabilities(self):
        """A code present in the lookup should return probability keys."""
        # Use a code we know is in the eddxs table
        known_codes = BillingsED.df_eddxs["prindx"].tolist()
        if known_codes:
            result = BillingsED.get_nyu_ed_proba_for_dx_code(known_codes[0])
            assert isinstance(result, dict)
            assert len(result) > 0


# ---------------------------------------------------------------------------
# Tests for BillingsED.get_nyu_ed_categories
# ---------------------------------------------------------------------------


class TestGetNyuEdCategories:
    """Tests for BillingsED.get_nyu_ed_categories."""

    def test_returns_tuple_of_10(self):
        """Should return a tuple with 10 elements."""
        result = BillingsED.get_nyu_ed_categories("25000")
        assert isinstance(result, tuple)
        assert len(result) == 10

    def test_elements_are_numeric(self):
        """All elements should be int or float."""
        result = BillingsED.get_nyu_ed_categories("25000")
        for i, val in enumerate(result):
            assert isinstance(
                val, (int, float, np.integer, np.floating)
            ), f"Element {i} should be numeric, got {type(val)}"

    def test_unclassified_code(self):
        """A code matching no category and no probability lookup should be unclassified=1."""
        result = BillingsED.get_nyu_ed_categories("ZZZZZ")
        unclassified = result[0]
        assert unclassified == 1

    def test_injury_code_categories(self):
        """An injury code should have injury=1 and zero non-emergent/epct probabilities."""
        result = BillingsED.get_nyu_ed_categories("E8000")
        injury = result[1]
        ne = result[6]
        epct = result[7]
        assert injury == 1
        assert ne == 0
        assert epct == 0


# ---------------------------------------------------------------------------
# Tests for BillingsED.is_peds_acsed
# ---------------------------------------------------------------------------


class TestIsPedsAcsed:
    """Tests for BillingsED.is_peds_acsed."""

    def test_asthma_code(self):
        """ICD-9 493xx (asthma) should qualify as pediatric ACS ED."""
        assert BillingsED.is_peds_acsed("49300") == 1

    def test_non_peds_code(self):
        """A code not in the peds ACS list should return 0."""
        assert BillingsED.is_peds_acsed("ZZZZZ") == 0

    def test_returns_int(self):
        """Return value should always be int."""
        assert isinstance(BillingsED.is_peds_acsed("49300"), int)


# ---------------------------------------------------------------------------
# Tests for get_nyu_ed_proba (the main DataFrame-level function)
# ---------------------------------------------------------------------------


class TestGetNyuEdProba:
    """Tests for the top-level get_nyu_ed_proba() function."""

    def test_end_to_end(self):
        """End-to-end test: should produce ED classification columns."""
        df = _make_dask_df(
            {
                "BENE_MSIS": ["B001", "B001", "B002"],
                "SRVC_BGN_DT": pd.to_datetime(
                    ["2014-01-01", "2014-01-01", "2014-02-15"]
                ),
                "DIAG_CD_1": ["25000", "49300", "ZZZZZ"],
                "adult": [1, 0, 1],
            }
        )
        result = get_nyu_ed_proba(
            df,
            date_col="SRVC_BGN_DT",
            index_col="BENE_MSIS",
            cms_format="MAX",
        )

        assert isinstance(result, pd.DataFrame)
        expected_cols = [
            "injury",
            "drug",
            "psych",
            "alcohol",
            "peds_acs_ed_visit",
            "non_emergent_visit",
            "emergent_visit",
            "non_emergent_or_pct_visit",
            "indeterminate_ed_visit",
        ]
        for col in expected_cols:
            assert col in result.columns, f"Missing column {col}"

    def test_taf_format_uses_dgns_cd(self):
        """TAF format should use DGNS_CD_1 column name."""
        df = _make_dask_df(
            {
                "BENE_MSIS": ["B001"],
                "SRVC_BGN_DT": pd.to_datetime(["2020-01-01"]),
                "DGNS_CD_1": ["25000"],
                "adult": [1],
            }
        )
        result = get_nyu_ed_proba(
            df,
            date_col="SRVC_BGN_DT",
            index_col="BENE_MSIS",
            cms_format="TAF",
        )
        assert isinstance(result, pd.DataFrame)

    def test_output_values_non_negative(self):
        """All output visit counts should be non-negative."""
        df = _make_dask_df(
            {
                "BENE_MSIS": ["B001"],
                "SRVC_BGN_DT": pd.to_datetime(["2014-01-01"]),
                "DIAG_CD_1": ["25000"],
                "adult": [1],
            }
        )
        result = get_nyu_ed_proba(
            df,
            date_col="SRVC_BGN_DT",
            index_col="BENE_MSIS",
            cms_format="MAX",
        )
        for col in result.columns:
            assert (result[col] >= 0).all(), f"{col} has negative values"

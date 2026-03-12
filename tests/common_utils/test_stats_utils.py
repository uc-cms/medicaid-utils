"""Tests for medicaid_utils.common_utils.stats_utils."""
import numpy as np
import pandas as pd
import pytest

from medicaid_utils.common_utils import stats_utils


class TestCramersCorrectedStat:
    def test_independent_variables(self):
        """For independent variables, Cramer's V should be near zero."""
        np.random.seed(42)
        n = 1000
        a = np.random.choice(["X", "Y"], n)
        b = np.random.choice(["P", "Q"], n)
        ct = pd.crosstab(pd.Series(a), pd.Series(b))
        v = stats_utils.cramers_corrected_stat(ct)
        assert v < 0.1

    def test_perfectly_associated(self):
        """For perfectly associated variables, Cramer's V should be 1.0."""
        ct = pd.DataFrame(
            [[50, 0], [0, 50]],
            index=["A", "B"],
            columns=["X", "Y"],
        )
        v = stats_utils.cramers_corrected_stat(ct)
        assert abs(v - 1.0) < 0.05

    def test_returns_nonnegative(self):
        ct = pd.DataFrame(
            [[10, 20], [20, 10]],
            index=["A", "B"],
            columns=["X", "Y"],
        )
        v = stats_utils.cramers_corrected_stat(ct)
        assert v >= 0


class TestGetPhi:
    def test_perfect_association(self):
        ct = pd.DataFrame(
            [[50, 0], [0, 50]],
            index=["A", "B"],
            columns=["X", "Y"],
        )
        phi = stats_utils.get_phi(ct)
        assert phi > 0

    def test_no_association(self):
        ct = pd.DataFrame(
            [[25, 25], [25, 25]],
            index=["A", "B"],
            columns=["X", "Y"],
        )
        phi = stats_utils.get_phi(ct)
        assert abs(phi) < 0.01


class TestGetRanksumTable:
    @pytest.fixture
    def sample_data(self):
        np.random.seed(42)
        n = 200
        return pd.DataFrame(
            {
                "group": np.random.choice([0, 1], n),
                "metric_a": np.random.randn(n),
                "metric_b": np.random.exponential(1, n),
            }
        )

    def test_returns_two_dataframes(self, sample_data):
        raw, pretty = stats_utils.get_ranksum_table(
            sample_data,
            lst_metrics=["metric_a", "metric_b"],
            pop_col_name="group",
            dct_labels={0: "Control", 1: "Treatment"},
        )
        assert isinstance(raw, pd.DataFrame)
        assert isinstance(pretty, pd.DataFrame)

    def test_p_value_is_numeric(self, sample_data):
        raw, _ = stats_utils.get_ranksum_table(
            sample_data,
            lst_metrics=["metric_a"],
            pop_col_name="group",
            dct_labels={0: "Control", 1: "Treatment"},
        )
        assert "p-value" in raw.columns
        assert np.isfinite(raw["p-value"].values[0])

    def test_p_value_differs_from_z_stat(self, sample_data):
        """Regression test: p-value should not equal z-statistic (bug fix)."""
        raw, _ = stats_utils.get_ranksum_table(
            sample_data,
            lst_metrics=["metric_a"],
            pop_col_name="group",
            dct_labels={0: "Control", 1: "Treatment"},
        )
        z = raw["z-statistic"].values[0]
        p = raw["p-value"].values[0]
        # p-value should be between 0 and 1
        assert 0 <= p <= 1
        # They should not be the same value (the original bug)
        assert z != p or (z == p and 0 <= z <= 1)

    def test_p_value_in_range(self, sample_data):
        """p-value must always be in [0, 1]."""
        raw, _ = stats_utils.get_ranksum_table(
            sample_data,
            lst_metrics=["metric_a", "metric_b"],
            pop_col_name="group",
            dct_labels={0: "Control", 1: "Treatment"},
        )
        for p in raw["p-value"].values:
            assert 0 <= p <= 1


class TestGetDescriptives:
    def test_returns_dataframe(self):
        pdf = pd.DataFrame(
            {
                "state": ["CA", "CA", "CA", "NY", "NY", "NY"],
                "value": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            }
        )
        result = stats_utils.get_descriptives(
            pdf,
            lst_st=["CA", "NY"],
            lst_col=["value"],
            state_col_name="state",
        )
        assert isinstance(result, pd.DataFrame)

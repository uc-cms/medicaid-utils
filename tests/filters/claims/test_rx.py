"""Tests for medicaid_utils.filters.claims.rx."""
import pandas as pd
import dask.dataframe as dd
import pytest

from medicaid_utils.filters.claims import rx


@pytest.fixture
def sample_rx_claims():
    """Create sample prescription claims data."""
    pdf = pd.DataFrame(
        {
            "BENE_MSIS": ["A", "A", "B", "C", "C"],
            "NDC": [
                "000378451905",
                "999999999999",
                "000378451905",
                "000378617005",
                "000000000000",
            ],
            # Why: C's matching NDC (000378617005) has -5 days supply to test
            # that negative supply is excluded; the non-matching NDC has 60
            "DAYS_SUPPLY": ["30", "15", "0", "-5", "60"],
        }
    )
    pdf = pdf.set_index("BENE_MSIS")
    return dd.from_pandas(pdf, npartitions=1)


class TestFlagPrescriptions:
    def test_flags_matching_ndc(self, sample_rx_claims):
        dct_ndc = {"buprenorphine": ["00378451905", "00378617005"]}
        result = rx.flag_prescriptions(dct_ndc, sample_rx_claims)
        computed = result.compute()
        assert "rx_buprenorphine" in computed.columns

    def test_excludes_zero_days_supply(self, sample_rx_claims):
        """Claims with 0 days_supply should not be flagged."""
        dct_ndc = {"buprenorphine": ["00378451905"]}
        result = rx.flag_prescriptions(dct_ndc, sample_rx_claims)
        computed = result.compute()
        # B has matching NDC but 0 days_supply
        b_claims = computed.loc["B"]
        if isinstance(b_claims, pd.DataFrame):
            assert b_claims["rx_buprenorphine"].max() == 0
        else:
            assert b_claims["rx_buprenorphine"] == 0

    def test_excludes_negative_days_supply(self, sample_rx_claims):
        """Claims with negative days_supply should not be flagged."""
        dct_ndc = {"buprenorphine": ["00378617005"]}
        result = rx.flag_prescriptions(dct_ndc, sample_rx_claims)
        computed = result.compute()
        # C has one matching NDC with -5 days_supply - none should be flagged
        c_claims = computed.loc["C"]
        if isinstance(c_claims, pd.DataFrame):
            assert c_claims["rx_buprenorphine"].max() == 0
        else:
            assert c_claims["rx_buprenorphine"] == 0

    def test_ignore_missing_days_supply(self, sample_rx_claims):
        """When ignore_missing_days_supply=True, flag regardless of days_supply."""
        dct_ndc = {"buprenorphine": ["00378451905"]}
        result = rx.flag_prescriptions(
            dct_ndc, sample_rx_claims, ignore_missing_days_supply=True
        )
        computed = result.compute()
        # B has matching NDC with 0 days_supply - should still be flagged
        b_claims = computed.loc["B"]
        if isinstance(b_claims, pd.DataFrame):
            assert b_claims["rx_buprenorphine"].max() == 1
        else:
            assert b_claims["rx_buprenorphine"] == 1

    def test_ndc_zero_padding(self):
        """NDC codes should be zero-padded to 12 characters."""
        pdf = pd.DataFrame(
            {
                "BENE_MSIS": ["A"],
                "NDC": ["000000012345"],
                "DAYS_SUPPLY": ["30"],
            }
        ).set_index("BENE_MSIS")
        ddf = dd.from_pandas(pdf, npartitions=1)

        # Pass an NDC without leading zeros - should still match
        dct_ndc = {"test_drug": ["12345"]}
        result = rx.flag_prescriptions(dct_ndc, ddf)
        computed = result.compute()
        assert computed["rx_test_drug"].iloc[0] == 1

    def test_no_matching_ndc(self, sample_rx_claims):
        dct_ndc = {"unknown_drug": ["111111111111"]}
        result = rx.flag_prescriptions(dct_ndc, sample_rx_claims)
        computed = result.compute()
        assert computed["rx_unknown_drug"].sum() == 0

    def test_multiple_conditions(self, sample_rx_claims):
        dct_ndc = {
            "drug_a": ["00378451905"],
            "drug_b": ["00378617005"],
        }
        result = rx.flag_prescriptions(
            dct_ndc, sample_rx_claims, ignore_missing_days_supply=True
        )
        computed = result.compute()
        assert "rx_drug_a" in computed.columns
        assert "rx_drug_b" in computed.columns

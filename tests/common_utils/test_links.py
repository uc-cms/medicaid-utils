"""Tests for medicaid_utils.common_utils.links."""
import os
import pytest

from medicaid_utils.common_utils import links


class TestGetMaxParquetLoc:
    def test_basic_path(self):
        result = links.get_max_parquet_loc("/data", "ip", "CA", 2019)
        expected = os.path.join(
            "/data", "medicaid", "2019", "CA", "max", "ip", "parquet"
        )
        assert result == expected

    def test_state_uppercased(self):
        result = links.get_max_parquet_loc("/data", "ot", "ca", 2020)
        assert "/CA/" in result

    def test_claim_type_lowercased(self):
        result = links.get_max_parquet_loc("/data", "IP", "NY", 2018)
        assert "/ip/" in result


class TestGetTafParquetLoc:
    def test_ip_has_expected_keys(self):
        result = links.get_taf_parquet_loc("/data", "ip", "CA", 2019)
        expected_keys = {
            "base", "line", "occurrence_code",
            "base_diag_codes", "line_ndc_codes",
        }
        assert expected_keys.issubset(result.keys())

    def test_ps_has_expected_keys(self):
        result = links.get_taf_parquet_loc("/data", "ps", "NY", 2020)
        expected_keys = {
            "dates", "base", "managed_care", "disability",
            "mfp", "waiver", "home_health", "diag_and_ndc_codes",
        }
        assert expected_keys.issubset(result.keys())

    def test_rx_has_expected_keys(self):
        result = links.get_taf_parquet_loc("/data", "rx", "IL", 2019)
        expected_keys = {"base", "line", "line_ndc_codes"}
        assert expected_keys.issubset(result.keys())

    def test_ot_has_expected_keys(self):
        result = links.get_taf_parquet_loc("/data", "ot", "TX", 2021)
        expected_keys = {
            "base", "line", "occurrence_code",
            "base_diag_codes", "line_ndc_codes",
        }
        assert expected_keys.issubset(result.keys())

    def test_no_duplicate_occurrence_code(self):
        """Verify the duplicate occurrence_code key bug was fixed."""
        result = links.get_taf_parquet_loc("/data", "ip", "CA", 2019)
        # If there were a duplicate, the dict would just have one key anyway,
        # but we verify the path is correct
        assert "ipoccr" in result["occurrence_code"]

    def test_state_uppercased(self):
        result = links.get_taf_parquet_loc("/data", "ip", "ca", 2019)
        assert "/CA/" in result["base"]

    def test_lt_has_expected_keys(self):
        result = links.get_taf_parquet_loc("/data", "lt", "OH", 2019)
        expected_keys = {
            "base", "line", "occurrence_code",
            "base_diag_codes", "line_ndc_codes",
        }
        assert expected_keys.issubset(result.keys())

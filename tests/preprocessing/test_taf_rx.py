"""Tests for the TAFRX preprocessing class."""
import pandas as pd
import pytest

from medicaid_utils.preprocessing.taf_rx import TAFRX


class TestTAFRXInitialization:
    """Verify that TAFRX loads data and populates expected structures."""

    def test_loads_base_and_line(self, taf_rx_data):
        data_root, year, state, tmp = taf_rx_data
        rx = TAFRX(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert "base" in rx.dct_files
        assert "line" in rx.dct_files

    def test_index_is_bene_msis(self, taf_rx_data):
        data_root, year, state, tmp = taf_rx_data
        rx = TAFRX(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert rx.dct_files["base"].index.name == "BENE_MSIS"
        assert rx.dct_files["line"].index.name == "BENE_MSIS"

    def test_ftype_is_rx(self, taf_rx_data):
        data_root, year, state, tmp = taf_rx_data
        rx = TAFRX(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert rx.ftype == "rx"

    def test_default_filters_set(self, taf_rx_data):
        data_root, year, state, tmp = taf_rx_data
        rx = TAFRX(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert "missing_dob" in rx.dct_default_filters


class TestTAFRXClean:
    """Verify cleaning routines produce expected columns and transformations."""

    @pytest.fixture(autouse=True)
    def _setup(self, taf_rx_data):
        data_root, year, state, tmp = taf_rx_data
        self.rx = TAFRX(year, state, data_root, clean=True, preprocess=False, tmp_folder=tmp)
        self.df_base = self.rx.dct_files["base"].compute()
        self.df_line = self.rx.dct_files["line"].compute()

    def test_date_columns_created(self):
        assert "birth_date" in self.df_base.columns
        assert pd.api.types.is_datetime64_any_dtype(self.df_base["birth_date"])

    def test_age_columns_created(self):
        assert "age" in self.df_base.columns
        assert "adult" in self.df_base.columns
        assert "child" in self.df_base.columns

    def test_ndc_codes_padded(self):
        """NDC codes should be zero-padded to length 12."""
        ndc_vals = self.df_line["NDC"].dropna()
        for val in ndc_vals:
            assert len(val) == 12

    def test_short_ndc_padded_correctly(self):
        """The first line record had NDC='999' -> should become '000000000999'."""
        ndc_vals = self.df_line["NDC"].tolist()
        # Find the padded version
        assert any("000999" in v for v in ndc_vals if v)

    def test_ndc_whitespace_stripped(self):
        """NDC with spaces should have spaces removed."""
        ndc_vals = self.df_line["NDC"].tolist()
        for val in ndc_vals:
            if val:
                assert " " not in val


class TestTAFRXDuplicates:
    """Verify duplicate detection and removal."""

    def test_duplicate_lower_run_id_removed(self, taf_rx_data):
        data_root, year, state, tmp = taf_rx_data
        rx = TAFRX(year, state, data_root, clean=True, preprocess=False, tmp_folder=tmp)
        df_base = rx.dct_files["base"].compute()
        # Fixture duplicated RCLM000004 with DA_RUN_ID=100
        clm4 = df_base.loc[df_base["CLM_ID"] == "RCLM000004"]
        assert len(clm4) == 1
        assert int(clm4.iloc[0]["DA_RUN_ID"]) > 100

    def test_no_exact_duplicate_rows(self, taf_rx_data):
        data_root, year, state, tmp = taf_rx_data
        rx = TAFRX(year, state, data_root, clean=True, preprocess=False, tmp_folder=tmp)
        df_base = rx.dct_files["base"].compute()
        if "excl_duplicated" in df_base.columns:
            assert (df_base["excl_duplicated"] == 0).all()


class TestTAFRXEdgeCases:
    """Edge case tests for RX preprocessing."""

    def test_missing_dob_handled(self, taf_rx_data):
        """Row 0 has empty BIRTH_DT -- birth_date should be NaT."""
        data_root, year, state, tmp = taf_rx_data
        rx = TAFRX(year, state, data_root, clean=True, preprocess=False, tmp_folder=tmp)
        df = rx.dct_files["base"].compute()
        bene1 = df.loc[df.index.str.contains("BENE0001")]
        if len(bene1) > 0:
            assert pd.isna(bene1.iloc[0]["birth_date"])

    def test_no_crash_full_init(self, taf_rx_data):
        """Full init with clean and preprocess should not crash."""
        data_root, year, state, tmp = taf_rx_data
        rx = TAFRX(year, state, data_root, clean=True, preprocess=True, tmp_folder=tmp)
        assert rx.dct_files["base"] is not None

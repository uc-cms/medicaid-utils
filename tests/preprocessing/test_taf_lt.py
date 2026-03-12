"""Tests for the TAFLT preprocessing class."""
import pandas as pd
import pytest

from medicaid_utils.preprocessing.taf_lt import TAFLT


class TestTAFLTInitialization:
    """Verify that TAFLT loads data and populates expected structures."""

    def test_loads_base_and_line(self, taf_lt_data):
        data_root, year, state, tmp = taf_lt_data
        lt = TAFLT(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert "base" in lt.dct_files
        assert "line" in lt.dct_files

    def test_index_is_bene_msis(self, taf_lt_data):
        data_root, year, state, tmp = taf_lt_data
        lt = TAFLT(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert lt.dct_files["base"].index.name == "BENE_MSIS"
        assert lt.dct_files["line"].index.name == "BENE_MSIS"

    def test_occurrence_loaded(self, taf_lt_data):
        data_root, year, state, tmp = taf_lt_data
        lt = TAFLT(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert "occurrence_code" in lt.dct_files

    def test_ftype_is_lt(self, taf_lt_data):
        data_root, year, state, tmp = taf_lt_data
        lt = TAFLT(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert lt.ftype == "lt"

    def test_default_filters_set(self, taf_lt_data):
        data_root, year, state, tmp = taf_lt_data
        lt = TAFLT(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert "missing_dob" in lt.dct_default_filters


class TestTAFLTClean:
    """Verify cleaning routines produce expected columns and transformations."""

    @pytest.fixture(autouse=True)
    def _setup(self, taf_lt_data):
        data_root, year, state, tmp = taf_lt_data
        self.lt = TAFLT(year, state, data_root, clean=True, preprocess=False, tmp_folder=tmp)
        self.df_base = self.lt.dct_files["base"].compute()

    def test_date_columns_created(self):
        for col in ["birth_date", "srvc_bgn_date", "srvc_end_date"]:
            assert col in self.df_base.columns, f"Missing date column: {col}"

    def test_birth_date_parsed(self):
        valid = self.df_base.loc[self.df_base["birth_date"].notna()]
        assert len(valid) > 0
        assert pd.api.types.is_datetime64_any_dtype(self.df_base["birth_date"])

    def test_age_columns_created(self):
        assert "age" in self.df_base.columns
        assert "adult" in self.df_base.columns
        assert "child" in self.df_base.columns
        assert "elderly" in self.df_base.columns

    def test_age_values_reasonable(self):
        valid_age = self.df_base.loc[self.df_base["age"].notna(), "age"]
        assert (valid_age >= 0).all()
        assert (valid_age <= 120).all()


class TestTAFLTDuplicates:
    """Verify duplicate detection and removal."""

    def test_duplicate_lower_run_id_removed(self, taf_lt_data):
        data_root, year, state, tmp = taf_lt_data
        lt = TAFLT(year, state, data_root, clean=True, preprocess=False, tmp_folder=tmp)
        df_base = lt.dct_files["base"].compute()
        # Fixture duplicated LCLM000004 with DA_RUN_ID=100
        clm4 = df_base.loc[df_base["CLM_ID"] == "LCLM000004"]
        assert len(clm4) == 1
        assert int(clm4.iloc[0]["DA_RUN_ID"]) > 100

    def test_no_exact_duplicate_rows(self, taf_lt_data):
        data_root, year, state, tmp = taf_lt_data
        lt = TAFLT(year, state, data_root, clean=True, preprocess=False, tmp_folder=tmp)
        df_base = lt.dct_files["base"].compute()
        if "excl_duplicated" in df_base.columns:
            assert (df_base["excl_duplicated"] == 0).all()


class TestTAFLTEdgeCases:
    """Edge case tests for LT preprocessing."""

    def test_missing_dob_handled(self, taf_lt_data):
        """Row 0 has empty BIRTH_DT -- birth_date should be NaT."""
        data_root, year, state, tmp = taf_lt_data
        lt = TAFLT(year, state, data_root, clean=True, preprocess=False, tmp_folder=tmp)
        df = lt.dct_files["base"].compute()
        bene1 = df.loc[df.index.str.contains("BENE0001")]
        if len(bene1) > 0:
            assert pd.isna(bene1.iloc[0]["birth_date"])

    def test_no_crash_full_init(self, taf_lt_data):
        """Full init with clean and preprocess should not crash."""
        data_root, year, state, tmp = taf_lt_data
        lt = TAFLT(year, state, data_root, clean=True, preprocess=True, tmp_folder=tmp)
        assert lt.dct_files["base"] is not None

    def test_service_dates_are_datetime(self, taf_lt_data):
        data_root, year, state, tmp = taf_lt_data
        lt = TAFLT(year, state, data_root, clean=True, preprocess=False, tmp_folder=tmp)
        df = lt.dct_files["base"].compute()
        for col in ["srvc_bgn_date", "srvc_end_date"]:
            assert pd.api.types.is_datetime64_any_dtype(df[col])

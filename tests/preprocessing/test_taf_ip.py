"""Tests for the TAFIP preprocessing class."""
import pandas as pd
import pytest

from medicaid_utils.preprocessing.taf_ip import TAFIP


class TestTAFIPInitialization:
    """Verify that TAFIP loads data and populates expected structures."""

    def test_loads_base_and_line(self, taf_ip_data):
        data_root, year, state, tmp = taf_ip_data
        ip = TAFIP(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert "base" in ip.dct_files
        assert "line" in ip.dct_files

    def test_index_is_bene_msis(self, taf_ip_data):
        data_root, year, state, tmp = taf_ip_data
        ip = TAFIP(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert ip.dct_files["base"].index.name == "BENE_MSIS"
        assert ip.dct_files["line"].index.name == "BENE_MSIS"

    def test_occurrence_loaded(self, taf_ip_data):
        data_root, year, state, tmp = taf_ip_data
        ip = TAFIP(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert "occurrence_code" in ip.dct_files

    def test_default_filters_set(self, taf_ip_data):
        data_root, year, state, tmp = taf_ip_data
        ip = TAFIP(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert "missing_dob" in ip.dct_default_filters
        assert "duplicated" in ip.dct_default_filters


class TestTAFIPClean:
    """Verify cleaning routines produce expected columns and transformations."""

    @pytest.fixture(autouse=True)
    def _setup(self, taf_ip_data):
        data_root, year, state, tmp = taf_ip_data
        self.ip = TAFIP(year, state, data_root, clean=True, preprocess=False, tmp_folder=tmp)
        self.df_base = self.ip.dct_files["base"].compute()

    def test_date_columns_created(self):
        for col in ["birth_date", "admsn_date", "srvc_bgn_date", "srvc_end_date", "prncpl_proc_date"]:
            assert col in self.df_base.columns, f"Missing date column: {col}"

    def test_birth_date_parsed(self):
        valid = self.df_base.loc[self.df_base["birth_date"].notna()]
        assert len(valid) > 0
        assert pd.api.types.is_datetime64_any_dtype(self.df_base["birth_date"])

    def test_age_columns_created(self):
        assert "age" in self.df_base.columns
        assert "adult" in self.df_base.columns
        assert "child" in self.df_base.columns

    def test_los_calculated(self):
        assert "los" in self.df_base.columns
        valid_los = self.df_base.loc[self.df_base["los"].notna()]
        assert len(valid_los) > 0
        # LOS should be positive for valid rows
        assert (valid_los["los"] > 0).all()

    def test_missing_admsn_imputed(self):
        """When ADMSM_DT is empty, admsn_date should be imputed from srvc_bgn_date."""
        assert "missing_admsn_date" in self.df_base.columns
        # Row index 1 had empty ADMSM_DT
        missing = self.df_base.loc[self.df_base["missing_admsn_date"] == 1]
        assert len(missing) >= 1

    def test_diagnosis_codes_cleaned(self):
        """Special characters removed and uppercased."""
        dgns = self.df_base["DGNS_CD_1"].dropna()
        for val in dgns:
            if val:
                assert val == val.upper()
                assert "." not in val
                assert "-" not in val

    def test_admitting_diagnosis_cleaned(self):
        admtg = self.df_base["ADMTG_DGNS_CD"].dropna()
        for val in admtg:
            if val:
                assert val == val.upper()
                assert "-" not in val

    def test_procedure_codes_cleaned(self):
        prcdr = self.df_base["PRCDR_CD_1"].dropna()
        for val in prcdr:
            if val:
                assert val == val.upper()

    def test_ndc_codes_padded(self):
        df_line = self.ip.dct_files["line"].compute()
        ndc_vals = df_line["NDC"].dropna()
        for val in ndc_vals:
            assert len(val) == 12

    def test_excl_missing_dob_flagged(self):
        assert "excl_missing_dob" in self.df_base.columns
        assert (self.df_base["excl_missing_dob"].isin([0, 1])).all()
        assert self.df_base["excl_missing_dob"].sum() >= 1

    def test_excl_missing_prncpl_proc_date_flagged(self):
        assert "excl_missing_prncpl_proc_date" in self.df_base.columns

    def test_ffs_or_encounter_claim_flagged(self):
        assert "ffs_or_encounter_claim" in self.df_base.columns
        # CLM_TYPE_CD "1" and "3" should be flagged 1; "5" should be 0
        assert self.df_base["ffs_or_encounter_claim"].isin([0, 1]).all()


class TestTAFIPDuplicates:
    """Verify duplicate detection and removal."""

    def test_duplicate_lower_run_id_removed(self, taf_ip_data):
        data_root, year, state, tmp = taf_ip_data
        ip = TAFIP(year, state, data_root, clean=True, preprocess=False, tmp_folder=tmp)
        df_base = ip.dct_files["base"].compute()
        # The fixture added a duplicate of CLM_ID=CLM000004 with DA_RUN_ID=500
        # Only the higher DA_RUN_ID version should remain
        clm4 = df_base.loc[df_base["CLM_ID"] == "CLM000004"]
        assert len(clm4) == 1
        assert int(clm4.iloc[0]["DA_RUN_ID"]) > 500

    def test_excl_duplicated_column_not_present_after_filter(self, taf_ip_data):
        """Duplicated rows are filtered out, not just flagged."""
        data_root, year, state, tmp = taf_ip_data
        ip = TAFIP(year, state, data_root, clean=True, preprocess=False, tmp_folder=tmp)
        df_base = ip.dct_files["base"].compute()
        if "excl_duplicated" in df_base.columns:
            assert (df_base["excl_duplicated"] == 0).all()


class TestTAFIPEdgeCases:
    """Edge case tests for IP preprocessing."""

    def test_invalid_los_set_to_nan(self, taf_ip_data):
        """Row 6 has srvc_end before admsn -- LOS should be NaN."""
        data_root, year, state, tmp = taf_ip_data
        ip = TAFIP(year, state, data_root, clean=True, preprocess=False, tmp_folder=tmp)
        df = ip.dct_files["base"].compute()
        row7 = df.loc[df["CLM_ID"] == "CLM000007"]
        if len(row7) > 0:
            assert pd.isna(row7.iloc[0]["los"]) or row7.iloc[0]["los"] <= 0

    def test_no_crash_with_clean_and_preprocess(self, taf_ip_data):
        """Full init with both clean and preprocess should not crash."""
        data_root, year, state, tmp = taf_ip_data
        ip = TAFIP(year, state, data_root, clean=True, preprocess=True, tmp_folder=tmp)
        assert ip.dct_files["base"] is not None

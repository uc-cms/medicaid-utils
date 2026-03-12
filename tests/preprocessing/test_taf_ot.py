"""Tests for the TAFOT preprocessing class."""
import pandas as pd
import pytest

from medicaid_utils.preprocessing.taf_ot import TAFOT


class TestTAFOTInitialization:
    """Verify that TAFOT loads data and populates expected structures."""

    def test_loads_base_and_line(self, taf_ot_data):
        data_root, year, state, tmp = taf_ot_data
        ot = TAFOT(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert "base" in ot.dct_files
        assert "line" in ot.dct_files

    def test_index_is_bene_msis(self, taf_ot_data):
        data_root, year, state, tmp = taf_ot_data
        ot = TAFOT(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert ot.dct_files["base"].index.name == "BENE_MSIS"

    def test_occurrence_loaded(self, taf_ot_data):
        data_root, year, state, tmp = taf_ot_data
        ot = TAFOT(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert "occurrence_code" in ot.dct_files

    def test_ftype_is_ot(self, taf_ot_data):
        data_root, year, state, tmp = taf_ot_data
        ot = TAFOT(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert ot.ftype == "ot"


class TestTAFOTClean:
    """Verify cleaning routines produce expected columns and transformations."""

    @pytest.fixture(autouse=True)
    def _setup(self, taf_ot_data):
        data_root, year, state, tmp = taf_ot_data
        self.ot = TAFOT(year, state, data_root, clean=True, preprocess=False, tmp_folder=tmp)
        self.df_base = self.ot.dct_files["base"].compute()

    def test_date_columns_created(self):
        for col in ["birth_date", "srvc_bgn_date", "srvc_end_date"]:
            assert col in self.df_base.columns, f"Missing date column: {col}"

    def test_birth_date_parsed(self):
        valid = self.df_base.loc[self.df_base["birth_date"].notna()]
        assert len(valid) > 0
        assert pd.api.types.is_datetime64_any_dtype(self.df_base["birth_date"])

    def test_duration_calculated(self):
        assert "duration" in self.df_base.columns
        valid = self.df_base.loc[self.df_base["duration"].notna()]
        assert len(valid) > 0

    def test_age_columns_created(self):
        assert "age" in self.df_base.columns
        assert "adult" in self.df_base.columns
        assert "child" in self.df_base.columns

    def test_diagnosis_codes_cleaned(self):
        dgns = self.df_base["DGNS_CD_1"].dropna()
        for val in dgns:
            if val:
                assert val == val.upper()
                assert "." not in val

    def test_procedure_codes_cleaned(self):
        df_line = self.ot.dct_files["line"].compute()
        prcdr = df_line["LINE_PRCDR_CD"].dropna()
        for val in prcdr:
            if val:
                assert "." not in val
                assert val == val.upper()

    def test_ndc_codes_padded(self):
        df_line = self.ot.dct_files["line"].compute()
        ndc_vals = df_line["NDC"].dropna()
        for val in ndc_vals:
            assert len(val) == 12

    def test_excl_missing_dob_flagged(self):
        assert "excl_missing_dob" in self.df_base.columns
        assert self.df_base["excl_missing_dob"].sum() >= 1

    def test_excl_missing_srvc_bgn_date_flagged(self):
        assert "excl_missing_srvc_bgn_date" in self.df_base.columns
        assert self.df_base["excl_missing_srvc_bgn_date"].sum() >= 1

    def test_ffs_or_encounter_claim_flagged(self):
        assert "ffs_or_encounter_claim" in self.df_base.columns
        assert self.df_base["ffs_or_encounter_claim"].isin([0, 1]).all()


class TestTAFOTDuplicates:
    """Verify duplicate detection and removal."""

    def test_duplicate_lower_run_id_removed(self, taf_ot_data):
        data_root, year, state, tmp = taf_ot_data
        ot = TAFOT(year, state, data_root, clean=True, preprocess=False, tmp_folder=tmp)
        df_base = ot.dct_files["base"].compute()
        # Fixture duplicated OCLM000006 with DA_RUN_ID=100
        clm6 = df_base.loc[df_base["CLM_ID"] == "OCLM000006"]
        assert len(clm6) == 1
        assert int(clm6.iloc[0]["DA_RUN_ID"]) > 100


class TestTAFOTEdgeCases:
    """Edge case tests for OT preprocessing."""

    def test_invalid_duration_set_to_nan(self, taf_ot_data):
        """Row 4 has srvc_bgn after srvc_end -- duration should be NaN."""
        data_root, year, state, tmp = taf_ot_data
        ot = TAFOT(year, state, data_root, clean=True, preprocess=False, tmp_folder=tmp)
        df = ot.dct_files["base"].compute()
        row5 = df.loc[df["CLM_ID"] == "OCLM000005"]
        if len(row5) > 0:
            assert pd.isna(row5.iloc[0]["duration"])

    def test_non_ffs_claim_flagged_zero(self, taf_ot_data):
        """CLM_TYPE_CD='5' should not be flagged as ffs_or_encounter_claim."""
        data_root, year, state, tmp = taf_ot_data
        ot = TAFOT(year, state, data_root, clean=True, preprocess=False, tmp_folder=tmp)
        df = ot.dct_files["base"].compute()
        row3 = df.loc[df["CLM_ID"] == "OCLM000003"]
        if len(row3) > 0:
            assert row3.iloc[0]["ffs_or_encounter_claim"] == 0

    def test_no_crash_with_clean_and_preprocess(self, taf_ot_data):
        data_root, year, state, tmp = taf_ot_data
        ot = TAFOT(year, state, data_root, clean=True, preprocess=True, tmp_folder=tmp)
        assert ot.dct_files["base"] is not None

"""Tests for the TAFPS preprocessing class."""
import pandas as pd
import pytest

from medicaid_utils.preprocessing.taf_ps import TAFPS


class TestTAFPSInitialization:
    """Verify that TAFPS loads data and populates expected structures."""

    def test_loads_dates_and_base(self, taf_ps_data):
        data_root, year, state, tmp = taf_ps_data
        ps = TAFPS(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert "dates" in ps.dct_files
        assert "base" in ps.dct_files

    def test_index_is_bene_msis(self, taf_ps_data):
        data_root, year, state, tmp = taf_ps_data
        ps = TAFPS(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert ps.dct_files["base"].index.name == "BENE_MSIS"
        assert ps.dct_files["dates"].index.name == "BENE_MSIS"

    def test_ftype_is_ps(self, taf_ps_data):
        data_root, year, state, tmp = taf_ps_data
        ps = TAFPS(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert ps.ftype == "ps"

    def test_default_filters_set(self, taf_ps_data):
        data_root, year, state, tmp = taf_ps_data
        ps = TAFPS(year, state, data_root, clean=False, preprocess=False, tmp_folder=tmp)
        assert "duplicated_bene_id" in ps.dct_default_filters


class TestTAFPSClean:
    """Verify cleaning routines produce expected columns and transformations."""

    @pytest.fixture(autouse=True)
    def _setup(self, taf_ps_data):
        data_root, year, state, tmp = taf_ps_data
        self.ps = TAFPS(
            year, state, data_root,
            clean=True, preprocess=False, tmp_folder=tmp,
        )
        self.df_base = self.ps.dct_files["base"].compute()
        self.df_dates = self.ps.dct_files["dates"].compute()

    def test_birth_date_parsed_in_dates(self):
        assert "birth_date" in self.df_dates.columns
        valid = self.df_dates.loc[self.df_dates["birth_date"].notna()]
        assert len(valid) > 0
        assert pd.api.types.is_datetime64_any_dtype(self.df_dates["birth_date"])

    def test_death_date_parsed_in_dates(self):
        assert "death_date" in self.df_dates.columns

    def test_death_flag_created_in_dates(self):
        assert "death" in self.df_dates.columns
        # Row 0 has death in claim year -- should be flagged
        bene1 = self.df_dates.loc[
            self.df_dates.index.str.contains("BENE0001")
        ]
        if len(bene1) > 0:
            assert bene1["death"].max() == 1

    def test_gender_column_added(self):
        assert "female" in self.df_base.columns
        assert set(self.df_base["female"].unique()).issubset({-1, 0, 1})

    def test_gender_female_correct(self):
        # i%3==0 -> F -> female=1; i%3==1 -> M -> female=0; else -> U -> -1
        females = self.df_base.loc[self.df_base["SEX_CD"].str.strip() == "F"]
        if len(females) > 0:
            assert (females["female"] == 1).all()

    def test_gender_male_correct(self):
        males = self.df_base.loc[self.df_base["SEX_CD"].str.strip() == "M"]
        if len(males) > 0:
            assert (males["female"] == 0).all()

    def test_gender_unknown_correct(self):
        unknowns = self.df_base.loc[self.df_base["SEX_CD"].str.strip() == "U"]
        if len(unknowns) > 0:
            assert (unknowns["female"] == -1).all()

    def test_excl_duplicated_bene_id_flagged(self):
        assert "excl_duplicated_bene_id" in self.df_base.columns
        # Row index 4 was duplicated
        assert self.df_base["excl_duplicated_bene_id"].sum() >= 2

    def test_excl_missing_dob_flagged(self):
        assert "excl_missing_dob" in self.df_base.columns


class TestTAFPSPreprocess:
    """Verify preprocessing adds expected constructed variables."""

    @pytest.fixture(autouse=True)
    def _setup(self, taf_ps_data):
        data_root, year, state, tmp = taf_ps_data
        self.ps = TAFPS(
            year, state, data_root,
            clean=True, preprocess=True, tmp_folder=tmp,
        )
        self.df_base = self.ps.dct_files["base"].compute()

    def test_dual_flags_created(self):
        assert "any_dual_month" in self.df_base.columns
        assert "total_dual_months" in self.df_base.columns
        assert "dual_months" in self.df_base.columns

    def test_dual_correctly_flagged(self):
        # Benes at i%5==0 have DUAL_ELGBL_CD=1 for months 1-6
        # so any_dual_month should be 1 for them
        duals = self.df_base.loc[self.df_base["any_dual_month"] == 1]
        assert len(duals) >= 1

    def test_restricted_benefits_flags(self):
        assert "any_restricted_benefit_month" in self.df_base.columns
        assert "total_restricted_benefit_months" in self.df_base.columns

    def test_enrollment_gap_columns(self):
        assert "n_enrollment_gaps" in self.df_base.columns
        assert "max_enrollment_gap" in self.df_base.columns

    def test_mas_boe_columns(self):
        # Check at least some MAS/BOE month columns exist
        mas_cols = [c for c in self.df_base.columns if c.startswith("mas_") and c.endswith("_months")]
        boe_cols = [c for c in self.df_base.columns if c.startswith("boe_") and c.endswith("_months")]
        assert len(mas_cols) > 0
        assert len(boe_cols) > 0
        assert "max_mas_type" in self.df_base.columns
        assert "max_boe_type" in self.df_base.columns

    def test_tanf_flag(self):
        assert "tanf" in self.df_base.columns
        # i%4==0 have TANF_CASH_CD=2 -> tanf=1
        tanf_benes = self.df_base.loc[self.df_base["tanf"] == 1]
        assert len(tanf_benes) >= 1

    def test_enrollment_months_columns(self):
        assert "total_enrolled_months" in self.df_base.columns
        assert "enrolled_months" in self.df_base.columns
        assert "max_continuous_enrolment" in self.df_base.columns

    def test_no_enrollment_bene_has_zero_months(self):
        """Row 6 had zero enrollment days and missing enrollment indicator=1."""
        bene7 = self.df_base.loc[
            self.df_base.index.str.contains("BENE0007")
        ]
        if len(bene7) > 0:
            assert bene7.iloc[0]["total_enrolled_months"] == 0

    def test_managed_care_columns(self):
        # managed_care file is not provided so defaults should be set
        for mc_type in ["comp", "behav_health", "pccm", "comp_or_pccm"]:
            assert f"mc_{mc_type}_months" in self.df_base.columns
            assert f"total_mc_{mc_type}_months" in self.df_base.columns

    def test_ffs_months_columns(self):
        assert "ffs_months" in self.df_base.columns
        assert "total_ffs_months" in self.df_base.columns

    def test_rural_column_exists(self):
        assert "rural" in self.df_base.columns

    def test_ruca_code_exists(self):
        assert "ruca_code" in self.df_base.columns


class TestTAFPSDuplicates:
    """Verify duplicate detection."""

    def test_duplicated_bene_flagged(self, taf_ps_data):
        data_root, year, state, tmp = taf_ps_data
        ps = TAFPS(year, state, data_root, clean=True, preprocess=False, tmp_folder=tmp)
        df_base = ps.dct_files["base"].compute()
        dup_rows = df_base.loc[df_base["excl_duplicated_bene_id"] == 1]
        assert len(dup_rows) >= 2

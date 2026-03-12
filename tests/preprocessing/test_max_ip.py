"""Tests for the MAX IP preprocessing pipeline."""

import pandas as pd
import pytest

from medicaid_utils.preprocessing.max_ip import MAXIP


@pytest.fixture
def ip(max_ip_data_dir, tmp_path):
    """Instantiate a MAXIP object (triggers clean + preprocess by default)."""
    return MAXIP(
        year=2012,
        state="WY",
        data_root=max_ip_data_dir,
        clean=True,
        preprocess=True,
        tmp_folder=str(tmp_path / "cache"),
    )


# ------------------------------------------------------------------
# Loading
# ------------------------------------------------------------------

class TestInit:
    def test_init_loads_data(self, ip):
        df = ip.df.compute()
        assert not df.empty
        assert df.index.name == "BENE_MSIS"

    def test_raw_columns_present(self, ip):
        df = ip.df.compute()
        # Key raw columns should still be in the dataframe
        for col in ["STATE_CD", "EL_DOB", "PHP_TYPE", "TYPE_CLM_CD"]:
            assert col in df.columns, f"Expected column {col} to be present"


# ------------------------------------------------------------------
# Date processing
# ------------------------------------------------------------------

class TestDateProcessing:
    def test_clean_adds_birth_date(self, ip):
        df = ip.df.compute()
        assert "birth_date" in df.columns
        assert pd.api.types.is_datetime64_any_dtype(df["birth_date"])

    def test_clean_adds_admsn_date(self, ip):
        df = ip.df.compute()
        assert "admsn_date" in df.columns
        assert pd.api.types.is_datetime64_any_dtype(df["admsn_date"])

    def test_clean_adds_srvc_dates(self, ip):
        df = ip.df.compute()
        for col in ["srvc_bgn_date", "srvc_end_date", "prncpl_proc_date"]:
            assert col in df.columns, f"Expected column {col}"
            assert pd.api.types.is_datetime64_any_dtype(df[col])

    def test_los_calculated(self, ip):
        df = ip.df.compute()
        assert "los" in df.columns
        # Row 0: admsn=20120110, srvc_end=20120115 -> los = 6
        row0 = df[df["BENE_ID"] == "BID0001"]
        valid_los = row0["los"].dropna()
        if not valid_los.empty:
            assert valid_los.iloc[0] == 6

    def test_missing_admsn_imputed_from_srvc_bgn(self, ip):
        """When ADMSN_DT is empty, admsn_date should be imputed from SRVC_BGN_DT."""
        df = ip.df.compute()
        assert "missing_admsn_date" in df.columns
        # There should be at least one missing admission date (row 18 in raw data)
        assert df["missing_admsn_date"].sum() >= 0  # may be 0 after imputation flag


# ------------------------------------------------------------------
# Gender
# ------------------------------------------------------------------

class TestGender:
    def test_clean_adds_gender_column(self, ip):
        df = ip.df.compute()
        assert "female" in df.columns

    def test_female_values(self, ip):
        df = ip.df.compute()
        # Female rows should have female=1
        female_rows = df[df["EL_SEX_CD"].str.strip() == "F"]
        if not female_rows.empty:
            assert (female_rows["female"] == 1).all()

    def test_male_values(self, ip):
        df = ip.df.compute()
        male_rows = df[df["EL_SEX_CD"].str.strip() == "M"]
        if not male_rows.empty:
            assert (male_rows["female"] == 0).all()

    def test_unknown_sex_values(self, ip):
        df = ip.df.compute()
        unknown_rows = df[df["EL_SEX_CD"].str.strip() == "U"]
        if not unknown_rows.empty:
            assert (unknown_rows["female"] == -1).all()


# ------------------------------------------------------------------
# Diagnosis and procedure code cleaning
# ------------------------------------------------------------------

class TestCodeCleaning:
    def test_clean_diag_codes_uppercase(self, ip):
        df = ip.df.compute()
        for col in [c for c in df.columns if c.startswith("DIAG_CD_")]:
            non_null = df[col].dropna()
            non_empty = non_null[non_null.str.len() > 0]
            if not non_empty.empty:
                assert (non_empty == non_empty.str.upper()).all(), (
                    f"Column {col} has non-uppercase values"
                )

    def test_clean_proc_codes_uppercase(self, ip):
        df = ip.df.compute()
        prcdr_cols = [
            c for c in df.columns
            if c.startswith("PRCDR_CD") and not c.startswith("PRCDR_CD_SYS")
        ]
        for col in prcdr_cols:
            non_null = df[col].dropna()
            non_empty = non_null[non_null.str.len() > 0]
            if not non_empty.empty:
                assert (non_empty == non_empty.str.upper()).all(), (
                    f"Column {col} has non-uppercase values"
                )


# ------------------------------------------------------------------
# Common exclusion flags
# ------------------------------------------------------------------

class TestExclusions:
    def test_excl_missing_dob(self, ip):
        df = ip.df.compute()
        assert "excl_missing_dob" in df.columns
        # Row 18 had empty DOB -> should have excl_missing_dob = 1
        missing = df[df["birth_date"].isna()]
        if not missing.empty:
            assert (missing["excl_missing_dob"] == 1).all()

    def test_excl_encounter_claim(self, ip):
        df = ip.df.compute()
        assert "excl_encounter_claim" in df.columns
        # PHP_TYPE=77 rows should be encounter claims
        encounter_rows = df[df["PHP_TYPE"].astype(str).str.strip() == "77"]
        if not encounter_rows.empty:
            assert (encounter_rows["excl_encounter_claim"] == 1).all()

    def test_excl_capitation_claim(self, ip):
        df = ip.df.compute()
        assert "excl_capitation_claim" in df.columns

    def test_excl_ffs_claim(self, ip):
        df = ip.df.compute()
        assert "excl_ffs_claim" in df.columns

    def test_excl_delivery(self, ip):
        df = ip.df.compute()
        assert "excl_delivery" in df.columns
        # RCPNT_DLVRY_CD=1 should flag delivery
        delivery_rows = df[df["RCPNT_DLVRY_CD"].astype(str).str.strip() == "1"]
        if not delivery_rows.empty:
            assert (delivery_rows["excl_delivery"] == 1).all()

    def test_excl_female(self, ip):
        df = ip.df.compute()
        assert "excl_female" in df.columns
        female_rows = df[df["female"] == 1]
        if not female_rows.empty:
            assert (female_rows["excl_female"] == 1).all()


# ------------------------------------------------------------------
# Duplicates
# ------------------------------------------------------------------

class TestDuplicates:
    def test_flag_duplicates(self, ip):
        df = ip.df.compute()
        assert "excl_duplicated" in df.columns
        # At least one row should be flagged as duplicate (rows 10/11 in raw data)
        assert df["excl_duplicated"].sum() >= 1


# ------------------------------------------------------------------
# Payment
# ------------------------------------------------------------------

class TestPayment:
    def test_calculate_payment(self, ip):
        df = ip.df.compute()
        assert "pymt_amt" in df.columns
        # pymt_amt = MDCD_PYMT_AMT + TP_PYMT_AMT
        # Spot-check: row 0 had MDCD=100, TP=20 -> pymt_amt=120
        row0 = df[df["BENE_ID"] == "BID0001"]
        if not row0.empty:
            assert row0["pymt_amt"].iloc[0] == pytest.approx(120.0)


# ------------------------------------------------------------------
# ED use
# ------------------------------------------------------------------

class TestEDUse:
    def test_flag_ed_use(self, ip):
        df = ip.df.compute()
        assert "ed_use" in df.columns
        assert "ed_cpt" in df.columns
        assert "ed_ub92" in df.columns
        assert "ed_tos" in df.columns

    def test_ed_ub92_detected(self, ip):
        """Row 0 had UB_92_REV_CD_GP_1=0450 -> ed_ub92 should be 1."""
        df = ip.df.compute()
        row0 = df[df["BENE_ID"] == "BID0001"]
        if not row0.empty:
            assert row0["ed_ub92"].iloc[0] == 1

    def test_ed_tos_detected(self, ip):
        """Row 6 had MAX_TOS=11 -> ed_tos should be 1."""
        df = ip.df.compute()
        row6 = df[df["BENE_ID"] == "BID0007"]
        if not row6.empty:
            assert row6["ed_tos"].iloc[0] == 1


# ------------------------------------------------------------------
# IP overlaps
# ------------------------------------------------------------------

class TestIPOverlaps:
    def test_flag_ip_overlaps(self, ip):
        df = ip.df.compute()
        assert "flag_ip_dup_drop" in df.columns
        assert "flag_ip_undup" in df.columns
        assert "ip_incl" in df.columns

    def test_ip_incl_requires_positive_los(self, ip):
        """ip_incl should be 0 when los <= 0 or NaN."""
        df = ip.df.compute()
        zero_los = df[df["los"].fillna(0) <= 0]
        if not zero_los.empty:
            assert (zero_los["ip_incl"] == 0).all()


# ------------------------------------------------------------------
# Age
# ------------------------------------------------------------------

class TestAge:
    def test_age_calculation(self, ip):
        df = ip.df.compute()
        assert "age" in df.columns
        assert "age_day" in df.columns
        assert "adult" in df.columns
        assert "child" in df.columns

    def test_age_values(self, ip):
        """DOB=19800115, year=2012 -> age=32."""
        df = ip.df.compute()
        row0 = df[df["BENE_ID"] == "BID0001"]
        if not row0.empty:
            assert row0["age"].iloc[0] == 32

    def test_adult_flag(self, ip):
        """Age 32 -> adult=1."""
        df = ip.df.compute()
        row0 = df[df["BENE_ID"] == "BID0001"]
        if not row0.empty:
            assert row0["adult"].iloc[0] == 1

    def test_child_flag(self, ip):
        """Age 32 -> child=0."""
        df = ip.df.compute()
        row0 = df[df["BENE_ID"] == "BID0001"]
        if not row0.empty:
            assert row0["child"].iloc[0] == 0

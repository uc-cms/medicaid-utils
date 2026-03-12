"""Tests for the MAX OT preprocessing pipeline."""

import pandas as pd
import pytest

from medicaid_utils.preprocessing.max_ot import MAXOT


@pytest.fixture
def ot(max_ot_data_dir, tmp_path):
    """Instantiate a MAXOT object (triggers clean + preprocess by default)."""
    return MAXOT(
        year=2012,
        state="WY",
        data_root=max_ot_data_dir,
        clean=True,
        preprocess=True,
        tmp_folder=str(tmp_path / "cache"),
    )


# ------------------------------------------------------------------
# Loading
# ------------------------------------------------------------------

class TestInit:
    def test_init_loads_data(self, ot):
        df = ot.df.compute()
        assert not df.empty
        assert df.index.name == "BENE_MSIS"

    def test_raw_columns_present(self, ot):
        df = ot.df.compute()
        for col in ["STATE_CD", "EL_DOB", "PHP_TYPE", "TYPE_CLM_CD"]:
            assert col in df.columns


# ------------------------------------------------------------------
# Date processing
# ------------------------------------------------------------------

class TestDateProcessing:
    def test_clean_adds_date_columns(self, ot):
        df = ot.df.compute()
        for col in ["birth_date", "srvc_bgn_date", "srvc_end_date"]:
            assert col in df.columns
            assert pd.api.types.is_datetime64_any_dtype(df[col])

    def test_birth_year_month_day(self, ot):
        df = ot.df.compute()
        for col in ["birth_year", "birth_month", "birth_day"]:
            assert col in df.columns


# ------------------------------------------------------------------
# Common exclusion flags
# ------------------------------------------------------------------

class TestExclusions:
    def test_excl_missing_dob(self, ot):
        df = ot.df.compute()
        assert "excl_missing_dob" in df.columns
        missing = df[df["birth_date"].isna()]
        if not missing.empty:
            assert (missing["excl_missing_dob"] == 1).all()

    def test_excl_missing_srvc_bgn_date(self, ot):
        df = ot.df.compute()
        assert "excl_missing_srvc_bgn_date" in df.columns

    def test_excl_encounter_claim(self, ot):
        df = ot.df.compute()
        assert "excl_encounter_claim" in df.columns
        encounter_rows = df[df["PHP_TYPE"].astype(str).str.strip() == "77"]
        if not encounter_rows.empty:
            assert (encounter_rows["excl_encounter_claim"] == 1).all()

    def test_excl_capitation_claim(self, ot):
        df = ot.df.compute()
        assert "excl_capitation_claim" in df.columns

    def test_excl_ffs_claim(self, ot):
        df = ot.df.compute()
        assert "excl_ffs_claim" in df.columns

    def test_excl_female(self, ot):
        df = ot.df.compute()
        assert "excl_female" in df.columns


# ------------------------------------------------------------------
# Duplicates
# ------------------------------------------------------------------

class TestDuplicates:
    def test_flag_duplicates(self, ot):
        df = ot.df.compute()
        assert "excl_duplicated" in df.columns
        # Row 10/11 are duplicate -> at least one flagged
        assert df["excl_duplicated"].sum() >= 1


# ------------------------------------------------------------------
# ED use
# ------------------------------------------------------------------

class TestEDUse:
    def test_flag_ed_use(self, ot):
        df = ot.df.compute()
        assert "ed_use" in df.columns
        assert "ed_cpt" in df.columns
        assert "ed_ub92" in df.columns
        assert "ed_pos" in df.columns
        assert "any_ed" in df.columns

    def test_ed_pos_detected(self, ot):
        """Row 8 had PLC_OF_SRVC_CD=23 -> ed_pos should be 1."""
        df = ot.df.compute()
        row8 = df[df["BENE_ID"] == "BID0009"]
        if not row8.empty:
            assert row8["ed_pos"].iloc[0] == 1

    def test_ed_ub92_rev_code(self, ot):
        """Row 0 had UB_92_REV_CD=0450 -> ed_ub92 should be 1."""
        df = ot.df.compute()
        row0 = df[df["BENE_ID"] == "BID0001"]
        if not row0.empty:
            assert row0["ed_ub92"].iloc[0] == 1


# ------------------------------------------------------------------
# Transport
# ------------------------------------------------------------------

class TestTransport:
    def test_flag_transport(self, ot):
        df = ot.df.compute()
        assert "transport" in df.columns
        assert "transport_TOS" in df.columns
        assert "transport_POS" in df.columns

    def test_transport_tos_detected(self, ot):
        """Row 6 had MAX_TOS=26 -> transport_TOS should be 1."""
        df = ot.df.compute()
        row6 = df[df["BENE_ID"] == "BID0007"]
        if not row6.empty:
            assert row6["transport_TOS"].iloc[0] == 1

    def test_transport_pos_ambulance(self, ot):
        """Row 9 had PLC_OF_SRVC_CD=41 -> transport_POS should be 1."""
        df = ot.df.compute()
        row9 = df[df["BENE_ID"] == "BID0010"]
        if not row9.empty:
            assert row9["transport_POS"].iloc[0] == 1

    def test_transport_combined(self, ot):
        """transport should be 1 when either transport_TOS or transport_POS is 1."""
        df = ot.df.compute()
        transport_rows = df[(df["transport_TOS"] == 1) | (df["transport_POS"] == 1)]
        if not transport_rows.empty:
            assert (transport_rows["transport"] == 1).all()


# ------------------------------------------------------------------
# Dental
# ------------------------------------------------------------------

class TestDental:
    def test_flag_dental(self, ot):
        df = ot.df.compute()
        assert "dental" in df.columns
        assert "dental_TOS" in df.columns
        assert "dental_PROC" in df.columns

    def test_dental_tos_detected(self, ot):
        """Row 5 had MAX_TOS=9 -> dental_TOS should be 1."""
        df = ot.df.compute()
        row5 = df[df["BENE_ID"] == "BID0006"]
        if not row5.empty:
            assert row5["dental_TOS"].iloc[0] == 1

    def test_dental_proc_detected(self, ot):
        """Row 7 had PRCDR_CD starting with 'D' -> dental_PROC should be 1."""
        df = ot.df.compute()
        row7 = df[df["BENE_ID"] == "BID0008"]
        if not row7.empty:
            assert row7["dental_PROC"].iloc[0] == 1

    def test_dental_combined(self, ot):
        """dental should be 1 when either dental_TOS or dental_PROC is 1."""
        df = ot.df.compute()
        dental_rows = df[(df["dental_TOS"] == 1) | (df["dental_PROC"] == 1)]
        if not dental_rows.empty:
            assert (dental_rows["dental"] == 1).all()


# ------------------------------------------------------------------
# E/M flag
# ------------------------------------------------------------------

class TestEM:
    def test_flag_em(self, ot):
        df = ot.df.compute()
        assert "EM" in df.columns

    def test_em_cpt_range(self, ot):
        """Rows with PRCDR_CD=99213 and PRCDR_CD_SYS=1 -> EM should be 1."""
        df = ot.df.compute()
        # 99213 is in [99211, 99215] range -> EM=1
        em_rows = df[
            (df["PRCDR_CD"].astype(str).str.strip() == "99213")
            & (df["EM"] == 1)
        ]
        assert not em_rows.empty, "Expected E/M flag for CPT 99213"


# ------------------------------------------------------------------
# Payment
# ------------------------------------------------------------------

class TestPayment:
    def test_calculate_payment(self, ot):
        df = ot.df.compute()
        assert "pymt_amt" in df.columns
        # Row 0: MDCD=50, TP=10 -> pymt_amt=60
        row0 = df[df["BENE_ID"] == "BID0001"]
        if not row0.empty:
            assert row0["pymt_amt"].iloc[0] == pytest.approx(60.0)


# ------------------------------------------------------------------
# Duration
# ------------------------------------------------------------------

class TestDuration:
    def test_duration_calculation(self, ot):
        df = ot.df.compute()
        assert "duration" in df.columns

    def test_duration_values(self, ot):
        """Row 0: srvc_bgn=20120110, srvc_end=20120112 -> duration=2 days."""
        df = ot.df.compute()
        row0 = df[df["BENE_ID"] == "BID0001"]
        if not row0.empty:
            dur = row0["duration"].iloc[0]
            if not pd.isna(dur):
                assert dur == 2

    def test_negative_duration_set_nan(self, ot):
        """Duration should be NaN when srvc_bgn > srvc_end."""
        df = ot.df.compute()
        invalid = df[df["srvc_bgn_date"] > df["srvc_end_date"]]
        if not invalid.empty:
            assert invalid["duration"].isna().all()

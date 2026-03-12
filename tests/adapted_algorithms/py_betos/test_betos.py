"""Tests for the BETOS procedure code categorization algorithm."""

import pandas as pd
import dask.dataframe as dd

from medicaid_utils.adapted_algorithms.py_betos.betos_proc_codes import (
    BetosProcCodes,
    assign_betos_cat,
)


def _make_dask_df(data, npartitions=1):
    """Helper to create a dask DataFrame from a dict."""
    pdf = pd.DataFrame(data)
    return dd.from_pandas(pdf, npartitions=npartitions)


# ---------------------------------------------------------------------------
# Tests for BetosProcCodes.get_betos_cpt_crosswalk
# ---------------------------------------------------------------------------


class TestGetBetosCptCrosswalk:
    """Tests for BetosProcCodes.get_betos_cpt_crosswalk."""

    def test_crosswalk_pre2020(self):
        """Verify crosswalk loads correctly for a pre-2020 year."""
        pdf = BetosProcCodes.get_betos_cpt_crosswalk(2014)
        assert isinstance(pdf, pd.DataFrame)
        assert "cpt_code" in pdf.columns
        assert "betos_code" in pdf.columns
        assert "betos_cat" in pdf.columns
        assert "betos_code_name" in pdf.columns
        assert len(pdf) > 0

    def test_crosswalk_2020(self):
        """Verify crosswalk loads correctly for year 2020 (BETOS v2)."""
        pdf = BetosProcCodes.get_betos_cpt_crosswalk(2020)
        assert isinstance(pdf, pd.DataFrame)
        assert "cpt_code" in pdf.columns
        assert "betos_code" in pdf.columns
        assert "betos_cat" in pdf.columns
        # v2 has additional columns
        assert "betos_cat_lvl2" in pdf.columns
        assert "betos_fam" in pdf.columns
        assert len(pdf) > 0

    def test_crosswalk_cpt_codes_are_strings(self):
        """CPT codes should be strings."""
        pdf = BetosProcCodes.get_betos_cpt_crosswalk(2014)
        assert pdf["cpt_code"].dtype == object  # string dtype in pandas


# ---------------------------------------------------------------------------
# Tests for BetosProcCodes.get_betos_cat
# ---------------------------------------------------------------------------


class TestGetBetosCat:
    """Tests for BetosProcCodes.get_betos_cat."""

    def test_medicaid_claim_adds_betos_columns(self):
        """Verify lst_betos_code and lst_betos_cat columns are added for medicaid claims."""
        pdf_crosswalk = BetosProcCodes.get_betos_cpt_crosswalk(2014)
        # Pick a real CPT code from the crosswalk
        sample_cpt = pdf_crosswalk["cpt_code"].iloc[0]

        df = _make_dask_df(
            {
                "BENE_MSIS": ["C001"],
                "PRCDR_CD_1": [sample_cpt],
                "PRCDR_CD_SYS_1": ["1"],  # CPT system code
            }
        )
        result = BetosProcCodes.get_betos_cat(
            df, pdf_crosswalk, claim_type="medicaid", proc_code_prefix="PRCDR_CD"
        )
        result_pdf = result.compute()

        assert "lst_betos_code" in result_pdf.columns
        assert "lst_betos_cat" in result_pdf.columns

    def test_medicare_claim_adds_betos_columns(self):
        """Verify betos columns are added for medicare claims (no SYS filtering)."""
        pdf_crosswalk = BetosProcCodes.get_betos_cpt_crosswalk(2014)
        sample_cpt = pdf_crosswalk["cpt_code"].iloc[0]

        df = _make_dask_df(
            {
                "BENE_ID": ["M001"],
                "PRCDR_CD_1": [sample_cpt],
            }
        )
        result = BetosProcCodes.get_betos_cat(
            df, pdf_crosswalk, claim_type="medicare", proc_code_prefix="PRCDR_CD"
        )
        result_pdf = result.compute()

        assert "lst_betos_code" in result_pdf.columns
        assert "lst_betos_cat" in result_pdf.columns

    def test_no_matching_procedure_codes(self):
        """When procedure codes do not match any CPT, betos lists should be empty strings."""
        pdf_crosswalk = BetosProcCodes.get_betos_cpt_crosswalk(2014)

        df = _make_dask_df(
            {
                "BENE_MSIS": ["C001"],
                "PRCDR_CD_1": ["ZZZZZ"],
                "PRCDR_CD_SYS_1": ["1"],
            }
        )
        result = BetosProcCodes.get_betos_cat(
            df, pdf_crosswalk, claim_type="medicaid"
        )
        result_pdf = result.compute()

        assert result_pdf["lst_betos_code"].iloc[0] == ""
        assert result_pdf["lst_betos_cat"].iloc[0] == ""


# ---------------------------------------------------------------------------
# Tests for the convenience assign_betos_cat() function
# ---------------------------------------------------------------------------


class TestAssignBetosCat:
    """Tests for the top-level assign_betos_cat() function."""

    def test_end_to_end_medicaid(self):
        """End-to-end: assign_betos_cat should add betos columns for medicaid."""
        pdf_crosswalk = BetosProcCodes.get_betos_cpt_crosswalk(2014)
        sample_cpt = pdf_crosswalk["cpt_code"].iloc[0]

        df = _make_dask_df(
            {
                "BENE_MSIS": ["C001", "C002"],
                "PRCDR_CD_1": [sample_cpt, "ZZZZZ"],
                "PRCDR_CD_SYS_1": ["1", "1"],
            }
        )
        result = assign_betos_cat(
            df, year=2014, claim_type="medicaid", proc_code_prefix="PRCDR_CD"
        )
        result_pdf = result.compute()

        assert "lst_betos_code" in result_pdf.columns
        assert "lst_betos_cat" in result_pdf.columns
        assert len(result_pdf) == 2

    def test_original_columns_preserved(self):
        """Original columns should still be present in the output."""
        pdf_crosswalk = BetosProcCodes.get_betos_cpt_crosswalk(2014)
        sample_cpt = pdf_crosswalk["cpt_code"].iloc[0]

        df = _make_dask_df(
            {
                "BENE_MSIS": ["C001"],
                "PRCDR_CD_1": [sample_cpt],
                "PRCDR_CD_SYS_1": ["1"],
                "extra": [99],
            }
        )
        result = assign_betos_cat(df, year=2014, claim_type="medicaid")
        result_pdf = result.compute()

        assert "BENE_MSIS" in result_pdf.columns
        assert "extra" in result_pdf.columns

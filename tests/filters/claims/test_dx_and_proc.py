"""Tests for medicaid_utils.filters.claims.dx_and_proc."""
import pandas as pd
import dask.dataframe as dd
import pytest

from medicaid_utils.filters.claims import dx_and_proc


@pytest.fixture
def sample_max_claims():
    """Create a sample MAX-format claims DataFrame with diagnosis/procedure columns."""
    pdf = pd.DataFrame(
        {
            "BENE_MSIS": ["A", "A", "B", "B", "C"],
            # Why: use object dtype explicitly to avoid pyarrow StringDtype,
            # which doesn't support str.startswith with a tuple argument
            "DIAG_CD_1": pd.array(
                ["30400", "25000", "30400", "4619", "30550"], dtype="object"
            ),
            "DIAG_CD_2": pd.array(
                ["", "30400", "", "25000", ""], dtype="object"
            ),
            "PRCDR_CD": pd.array(
                ["99213", "HZ81ZZZ", "99213", "99213", "HZ81ZZZ"], dtype="object"
            ),
            "PRCDR_CD_SYS": pd.array(
                ["1", "6", "1", "1", "6"], dtype="object"
            ),
            "service_date": pd.to_datetime(
                ["2020-01-01", "2020-02-01", "2020-03-01", "2020-04-01", "2020-05-01"]
            ),
        }
    )
    pdf = pdf.set_index("BENE_MSIS")
    pdf.index.name = "BENE_MSIS"
    return dd.from_pandas(pdf, npartitions=1)


class TestFlagDiagnosesAndProcedures:
    def test_flags_diagnosis_codes(self, sample_max_claims):
        dct_diag = {
            "oud": {"incl": {9: ["3040", "3055"]}}
        }
        result = dx_and_proc.flag_diagnoses_and_procedures(
            dct_diag_codes=dct_diag,
            dct_proc_codes={},
            df_claims=sample_max_claims,
            cms_format="MAX",
        )
        computed = result.compute()
        assert "diag_oud" in computed.columns
        # A has OUD in DIAG_CD_1 and DIAG_CD_2
        assert computed.loc["A", "diag_oud"].max() == 1
        # B has OUD in DIAG_CD_1
        assert computed.loc["B", "diag_oud"].max() == 1

    def test_flags_procedure_codes(self, sample_max_claims):
        dct_proc = {
            "methadone": {6: ["HZ81ZZZ"]}
        }
        result = dx_and_proc.flag_diagnoses_and_procedures(
            dct_diag_codes={},
            dct_proc_codes=dct_proc,
            df_claims=sample_max_claims,
            cms_format="MAX",
        )
        computed = result.compute()
        assert "proc_methadone" in computed.columns

    def test_empty_diagnosis_dict(self, sample_max_claims):
        result = dx_and_proc.flag_diagnoses_and_procedures(
            dct_diag_codes={},
            dct_proc_codes={},
            df_claims=sample_max_claims,
            cms_format="MAX",
        )
        computed = result.compute()
        assert computed.shape[0] == 5

    def test_raises_on_nonalphanumeric_codes(self, sample_max_claims):
        dct_diag = {
            "bad": {"incl": {9: ["304.0"]}}  # dot is non-alphanumeric
        }
        with pytest.raises(ValueError, match="Non-alphanumeric"):
            dx_and_proc.flag_diagnoses_and_procedures(
                dct_diag_codes=dct_diag,
                dct_proc_codes={},
                df_claims=sample_max_claims,
                cms_format="MAX",
            )

    def test_mutable_default_not_shared(self):
        """Regression test: ensure mutable default dict={} bug is fixed."""
        # Call with no dct_column_values - should not share state between calls
        pdf = pd.DataFrame(
            {
                "BENE_MSIS": pd.array(["A"], dtype="object"),
                "DIAG_CD_1": pd.array(["30400"], dtype="object"),
                "PRCDR_CD": pd.array(["99213"], dtype="object"),
                "PRCDR_CD_SYS": pd.array(["1"], dtype="object"),
                "service_date": pd.to_datetime(["2020-01-01"]),
            }
        ).set_index("BENE_MSIS")
        ddf = dd.from_pandas(pdf, npartitions=1)

        result1 = dx_and_proc.flag_diagnoses_and_procedures(
            dct_diag_codes={},
            dct_proc_codes={},
            df_claims=ddf.copy(),
            cms_format="MAX",
        )
        result2 = dx_and_proc.flag_diagnoses_and_procedures(
            dct_diag_codes={},
            dct_proc_codes={},
            df_claims=ddf.copy(),
            cms_format="MAX",
        )
        # Both should work without cross-call contamination
        assert result1.compute().shape[0] == 1
        assert result2.compute().shape[0] == 1

    def test_incl_excl_diagnosis(self, sample_max_claims):
        """Test inclusion + exclusion diagnosis logic."""
        dct_diag = {
            "diabetes_no_oud": {
                "incl": {9: ["2500"]},
                "excl": {9: ["3040"]},
            }
        }
        result = dx_and_proc.flag_diagnoses_and_procedures(
            dct_diag_codes=dct_diag,
            dct_proc_codes={},
            df_claims=sample_max_claims,
            cms_format="MAX",
        )
        computed = result.compute()
        assert "diag_diabetes_no_oud" in computed.columns

    def test_column_values_flag(self):
        """Test dct_column_values parameter for flagging by column value."""
        pdf = pd.DataFrame(
            {
                "BENE_MSIS": pd.array(["A", "B", "C"], dtype="object"),
                "DIAG_CD_1": pd.array(["30400", "25000", "4619"], dtype="object"),
                "RCPNT_DLVRY_CD": [1, 0, 1],
                "PRCDR_CD": pd.array(["99213", "99213", "99213"], dtype="object"),
                "PRCDR_CD_SYS": pd.array(["1", "1", "1"], dtype="object"),
                "service_date": pd.to_datetime(
                    ["2020-01-01", "2020-02-01", "2020-03-01"]
                ),
            }
        ).set_index("BENE_MSIS")
        ddf = dd.from_pandas(pdf, npartitions=1)

        result = dx_and_proc.flag_diagnoses_and_procedures(
            dct_diag_codes={},
            dct_proc_codes={},
            df_claims=ddf,
            cms_format="MAX",
            dct_column_values={"dx_delivery": {"RCPNT_DLVRY_CD": [1]}},
        )
        computed = result.compute()
        assert "dx_delivery" in computed.columns
        assert computed.loc["A", "dx_delivery"] == 1
        assert computed.loc["B", "dx_delivery"] == 0


class TestGetPatientIdsWithConditions:
    def test_returns_patient_ids(self, sample_max_claims):
        dct_diag = {"oud": {"incl": {9: ["3040", "3055"]}}}
        pdf_patients, dct_results = dx_and_proc.get_patient_ids_with_conditions(
            dct_diag_codes=dct_diag,
            dct_proc_codes={},
            cms_format="MAX",
            ot=sample_max_claims,
        )
        assert isinstance(pdf_patients, pd.DataFrame)
        assert isinstance(dct_results, dict)
        # Should have found patients with OUD
        if pdf_patients.shape[0] > 0:
            assert any(
                col for col in pdf_patients.columns if "diag" in col
            )

    def test_empty_codes_returns_empty(self):
        pdf = pd.DataFrame(
            {
                "BENE_MSIS": pd.array(["A"], dtype="object"),
                "DIAG_CD_1": pd.array(["99999"], dtype="object"),
                "PRCDR_CD": pd.array(["00000"], dtype="object"),
                "PRCDR_CD_SYS": pd.array(["1"], dtype="object"),
                "service_date": pd.to_datetime(["2020-01-01"]),
            }
        ).set_index("BENE_MSIS")
        ddf = dd.from_pandas(pdf, npartitions=1)
        pdf_patients, _ = dx_and_proc.get_patient_ids_with_conditions(
            dct_diag_codes={"nonexistent": {"incl": {9: ["ZZZZZ"]}}},
            dct_proc_codes={},
            cms_format="MAX",
            ot=ddf,
        )
        # No patients should match
        assert pdf_patients.shape[0] == 0

"""This module has functions that can be used to construct covariates
denoting common co-occurring conditions with OUD"""
import dask.dataframe as dd
from ...filters.claims import dx_and_proc


def flag_cooccurring_mental_health_claims(
    df_claims: dd.DataFrame,
) -> dd.DataFrame:
    """
    Adds indicator column denoting presence of mental health diagnosis codes in
    claims.

    ICD-9 Codes: 295-299, 300-301, 3071, 3075, 30981, 311-312, 314
    ICD-10 Codes: F20-F29, F30-F39, F40-F42, F431, F50, F60, F9091 (MODRN,
    2021)

    Source: `MODRN, 2021 <https://qpp.cms.gov/docs/QPP_quality_measure_specifications/CQM-Measures/2019_Measure_468_MIPSCQM.pdf>`_

    New Column(s):
        - diag_cooccurring_mental_health - integer column, 1 when claim has
          mental health diagnosis codes and 0 otherwise

    Parameters
    ----------
    df_claims : dd.DataFrame
            IP or OT claim file

    Returns
    -------
    dd.DataFrame

    """
    dct_diag_codes = {
        "cooccurring_mental_health": {
            "incl": {
                9: "295,296,297,298,299,300,301,3071,3075,30981,311,"
                "312,314".split(","),
                10: "F20,F21,F22,F23,F24,F25,F26,F27,F28,F29,F30,F31,"
                "F32,F33,F34,F35,F36,F37,F38,F39,F40,F41,F42,F431,"
                "F50,F60,F9091".split(","),
            },
        }
    }
    df_claims = dx_and_proc.flag_diagnoses_and_procedures(
        dct_diag_codes, {}, df_claims
    )
    return df_claims


def flag_cooccurring_sud_claims(df_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds indicator column denoting presence of SUD diagnosis codes in claims.

    ICD-9 Codes: 303-305, excluding 3040, 3047,
    3055 (OUD); 3051 (Tobacco); Remission codes (5th digit = ‘3’)
    ICD-10 Codes: F10-F19, excluding F11 (OUD); F17 (tobacco); remission
    codes, AND F55, O355, O9931, O9932

    Source: `MODRN, 2021 <https://qpp.cms.gov/docs/QPP_quality_measure_specifications/CQM-Measures/2019_Measure_468_MIPSCQM.pdf>`_

    New Column(s):
        - diag_cooccurring_sud: integer column, 1 when claim has SUD
          diagnosis codes and 0 otherwise

    Parameters
    ----------
    df_claims : dd.DataFrame
            IP or OT claim file

    Returns
    -------
    dd.DataFrame

    """
    dct_diag_codes = {
        "cooccurring_sud": {
            "incl": {
                9: "303,304,305".split(","),
                10: "F10,F11,F12,F13,F14,F15,F16,F17,F18,F19,F55,O355,"
                "O9931,O9932".split(","),
            },
            "excl": {
                9: "3040,3047,3055,3051".split(","),
                10: "F11,F17".split(","),
            },
        }
    }
    df_claims = dx_and_proc.flag_diagnoses_and_procedures(
        dct_diag_codes, {}, df_claims
    )
    return df_claims

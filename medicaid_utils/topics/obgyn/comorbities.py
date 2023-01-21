"""This module has functions used to flag presence of chronic conditions"""
import dask.dataframe as dd

from ...preprocessing import max_cc


def flag_chronic_conditions(cc_file: max_cc.MAXCC) -> dd.DataFrame:
    """
    Adds boolean columns that denote presence of chronic conditions.

    New Columns:
        - ckd_combined: any cardiac comorbidity
        - diab_combined: diabetes
        - hypten_combined: hypertension
        - ckd_combined: chronic kidney disease
        - depr_combined: depression
        - copd_combined: COPD
        - toba_combined: tobacco use

    Parameters
    ----------
    cc_file: max_cc.MAXCC

    Returns
    -------
    dd.DataFrame

    """

    lst_conditions = [
        "diab",
        "hypten",
        "depr",
        "depsn",
        "ckd",
        "copd",
        "obesity",
        "toba",
    ]
    df_cc = cc_file.get_chronic_conditions(lst_conditions)
    return df_cc

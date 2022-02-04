import sys
import dask.dataframe as dd

sys.path.append("../../../")
from medicaid_utils.preprocessing import cc


def flag_chronic_conditions(cc_file: cc.MAXCC) -> dd.DataFrame:
    """
    Adds boolean columns that denote presence of chronic conditions.
     New Columns:
        - any cardiac comorbidity
            diab_combined- diabetes
            hypten_combined- hypertension
            ckd_combined - chronic kidney disease
            depr_combined- depression
            copd_combined - COPD
            toba_combined - tobacco use

    :param cc_file:
    :return: dataframe
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

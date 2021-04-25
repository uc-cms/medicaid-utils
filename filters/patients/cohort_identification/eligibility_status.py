import pandas as pd
import dask.dataframe as dd
import numpy as np
from statistics import mode

def add_eligibility_status_columns(df: dd.DataFrame) -> dd.DataFrame:
    """
    Add eligibility columns based on MAX_ELG_CD_MO_{month} values for each month.
    MAX_ELG_CD_MO:00 = NOT ELIGIBLE, 99 = UNKNOWN ELIGIBILITY  => codes to denote ineligibility
    New Column(s):
        elg_mon_{month} - 0 or 1 value column, denoting eligibility for each month
        total_elg_mon - No. of eligible months
        elg_full_year - 0 or 1 value column, 1 if TotElMo = 12
        elg_over_9mon - 0 or 1 value column, 1 if TotElMo >= 9
        elg_over_6mon - 0 or 1 value column, 1 if TotElMo >= 6
        elg_cont_6mon - 0 or 1 value column, 1 if patient has 6 continuous eligible months
        mas_elg_change - 0 or 1 value column, 1 if patient had multiple mas group memberships during claim year
        mas_assignments -
        boe_assignments -
        dominant_boe_group -
        boe_elg_change - 0 or 1 value column, 1 if patient had multiple boe group memberships during claim year
        child_boe_elg_change - 0 or 1 value column, 1 if patient had multiple boe group memberships during claim year
        elg_change - 0 or 1 value column, 1 if patient had multiple eligibility group memberships during claim year
        eligibility_aged
        eligibility_child
        max_gap
        max_cont_enrollment
    :param dataframe df:
    :rtype: dd.DataFrame
    """
    df = df.map_partitions(lambda pdf: pdf
                    .assign(**dict([('MAS_ELG_MON' + str(mon),
                                     pdf['MAX_ELG_CD_MO_' + str(mon)]
                                        .where(~(pdf['MAX_ELG_CD_MO_' + str(mon)].str.strip()
                                                 .isin(['00', '99', '', '.']) |
                                     pdf['MAX_ELG_CD_MO_' + str(mon)].isna()),
                                               '99').astype(str).str.strip().str[:1]) for mon in range(1, 13)])))
    df = df.map_partitions(lambda pdf:
                    pdf.assign(mas_elg_change=(pdf[['MAS_ELG_MON' + str(mon) for mon in range(1, 13)]].replace('9', np.nan)\
                                               .nunique(axis=1, dropna=True) > 1).astype(int),
                               mas_assignments=pdf[['MAS_ELG_MON' + str(mon) for mon in range(1, 13)]].replace('9', '')\
                                               .apply(lambda x: ",".join(set(','.join(x).split(","))), axis=1)))
    df = df.map_partitions(lambda pdf: pdf.assign(**dict([('BOE_ELG_MON' + str(mon),
                                                           np.select(
                                                               [pdf['MAX_ELG_CD_MO_' + str(mon)].astype(str).isin(
                                                                   ["11", "21", "31", "41"]),  # aged
                                                                pdf['MAX_ELG_CD_MO_' + str(mon)].astype(str).isin(
                                                                    ["12", "22", "32", "42"]),  # blind / disabled
                                                                pdf['MAX_ELG_CD_MO_' + str(mon)].astype(str).isin(
                                                                    ["14", "16", "24", "34", "44", "48"]),  # child
                                                                pdf['MAX_ELG_CD_MO_' + str(mon)].astype(str).isin(
                                                                    ["15", "17", "25", "35", "3A", "45"]),  # adult
                                                                pdf['MAX_ELG_CD_MO_' + str(mon)].astype(str).isin(
                                                                    ["51", "52", "54", "55"])],
                                                               # state demonstration EXPANSION
                                                               [1, 2, 3, 4, 5],
                                                               default=6))
                                                          for mon in range(1, 13)] +
                                                         [('CHILD_BOE_ELG_MON' + str(mon),
                                                           np.select(
                                                               [pdf['MAX_ELG_CD_MO_' + str(mon)].astype(str).isin(
                                                                   ["11", "21", "31", "41"]),  # aged
                                                                   pdf['MAX_ELG_CD_MO_' + str(mon)].astype(str).isin(
                                                                       ["12", "22", "32", "42", "14", "16", "24", "34",
                                                                        "44", "48"]),  # blind / disabled OR CHILD
                                                                   pdf['MAX_ELG_CD_MO_' + str(mon)].astype(str).isin(
                                                                       ["15", "17", "25", "35", "3A", "45"]),  # adult
                                                                   pdf['MAX_ELG_CD_MO_' + str(mon)].astype(str).isin(
                                                                       ["51", "52", "54", "55"])],
                                                               # state demonstration EXPANSION
                                                               [1, 2, 3, 4],
                                                               default=5))
                                                          for mon in range(1, 13)]
                                                         )))
    df = df.map_partitions(lambda pdf:
                           pdf.assign(
                               boe_elg_change=(pdf[['BOE_ELG_MON' + str(mon) for mon in range(1, 13)]]\
                                               .replace(6, np.nan).nunique(axis=1, dropna=True) > 1).astype(int),
                               child_boe_elg_change=(pdf[['CHILD_BOE_ELG_MON' + str(mon) for mon in range(1, 13)]]\
                                               .replace(5, np.nan).nunique(axis=1, dropna=True) > 1).astype(int),
                               boe_assignments=pdf[['BOE_ELG_MON' + str(mon) for mon in range(1, 13)]].astype(str)\
                                               .apply(lambda x: ",".join(set(','.join(x).split(","))), axis=1),
                               dominant_boe_grp=pdf[['BOE_ELG_MON' + str(mon) for mon in range(1, 13)]
                               ].apply(lambda x: mode([y for y in x if y != 6] or [6]), axis=1).astype(int)
                           ))
    df['elg_change'] = (df[['boe_elg_change', 'mas_elg_change']].sum(axis=1) > 0).astype(int)
    df['eligibility_aged'] = (df[['BOE_ELG_MON' + str(mon) for mon in range(1, 13)]] == 1).any(axis='columns')
    df['eligibility_child'] = (df[['BOE_ELG_MON' + str(mon) for mon in range(1, 13)]] == 3).any(axis='columns')
    df = df.map_partitions(lambda pdf: pdf.assign(**dict([('elg_mon_' + str(mon), (
        ~(pdf['MAX_ELG_CD_MO_' + str(mon)].str.strip().isin(['00', '99', '', '.']) | pdf[
            'MAX_ELG_CD_MO_' + str(mon)].isna())).astype(int))
                                                          for mon in range(1, 13)])))
    df['total_elg_mon'] = df[['elg_mon_' + str(i) for i in range(1, 13)]].sum(axis=1)
    df['elg_full_year'] = (df['total_elg_mon'] == 12).astype(int)
    df['elg_over_9mon'] = (df['total_elg_mon'] >= 9).astype(int)
    df['elg_over_6mon'] = (df['total_elg_mon'] >= 6).astype(int)
    df = df.map_partitions(lambda pdf:
                           pdf.assign(max_gap=pdf[['elg_mon_' + str(mon) for mon in range(1,13)]]\
                                              .astype(int).astype(str).apply(lambda x: max(map(len,"".join(x).split('1'))),
                                                                 axis=1),
                                      max_cont_enrollment=pdf[['elg_mon_' + str(mon) for mon in range(1,13)]]\
                                              .astype(int).astype(str).apply(lambda x: max(map(len,"".join(x).split('0'))),
                                                                 axis=1)))
    df['elg_cont_6mon'] = (df['max_cont_enrollment'] >= 6).astype(int)

    lst_cols_to_delete = ['MAS_ELG_MON' + str(mon) for mon in range(1,13)] + \
                         ['BOE_ELG_MON' + str(mon) for mon in range(1,13)]
    df = df[[col for col in df.columns if col not in lst_cols_to_delete]]
    return df

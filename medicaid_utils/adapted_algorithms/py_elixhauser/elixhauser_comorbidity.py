import sys
import os

import numpy as np
import pandas as pd
import dask.dataframe as dd
from typing import List

class ElixhauserScoring():
    package_folder, filename = os.path.split(__file__)
    data_folder = os.path.join(package_folder, 'data')


    @classmethod
    def flag_comorbidities_icd9(cls, df: dd.DataFrame, lst_diag_col_name) -> dd.DataFrame:
        df_icd9_mapping = pd.read_csv(os.path.join(cls.data_folder, 'icd9_mapping.csv'))
        df_icd9_mapping['ICD9'] = df_icd9_mapping['ICD9'].str.split(",")
        df = df.map_partitions(lambda pdf: \
                               pdf.assign(**dict([('ELX_GRP_' + str(i),
                                                   pdf[lst_diag_col_name].str.split(",", expand=True)\
                                                        .apply(lambda x: \
                                                                x.str.replace(".", "") \
                                                                .str.strip()\
                                                                .str.upper().str.startswith(tuple(df_icd9_mapping\
                                                                                    .loc[df_icd9_mapping['ELX_GRP'] == i,
                                                                                         'ICD9'].values[0])))\
                                                   .any(axis='columns').astype(int)) for i in range(1, 32)])))
        return df


    @classmethod
    def calculate_final_score(cls, df: dd.DataFrame, output_column_name='elixhauser_score') -> dd.DataFrame:
        df[output_column_name] = df[['ELX_GRP_' + str(i) for i in range(1,32)]].sum(axis=1).fillna(0).astype(int)
        return df


def score(df: dd.DataFrame, lst_diag_col_name,  output_column_name='elixhauser_score') -> dd.DataFrame:
    df = ElixhauserScoring.flag_comorbidities_icd9(df, lst_diag_col_name)
    df = ElixhauserScoring.calculate_final_score(df, output_column_name)
    return df
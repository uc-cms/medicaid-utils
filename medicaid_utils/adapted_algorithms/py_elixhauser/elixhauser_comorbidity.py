"""This a python package for computing Elixhauser comorbidity score"""
import sys
import os

import numpy as np
import pandas as pd
import dask.dataframe as dd
from typing import List


class ElixhauserScoring:
    package_folder, filename = os.path.split(__file__)
    data_folder = os.path.join(package_folder, "data")

    @classmethod
    def flag_comorbidities(
        cls, df: dd.DataFrame, lst_diag_col_name, cms_format="MAX"
    ) -> dd.DataFrame:
        df_icd_mapping = pd.read_csv(
            os.path.join(
                cls.data_folder,
                f"icd{9 if cms_format == 'MAX' else 10}_mapping.csv",
            )
        )
        df_icd_mapping = df_icd_mapping.assign(
            ICD=df_icd_mapping[
                f"ICD{9 if cms_format == 'MAX' else 10}"
            ].str.split(",")
        )

        df = df.map_partitions(
            lambda pdf: pdf.assign(
                **dict(
                    [
                        (
                            "ELX_GRP_" + str(i),
                            pdf[lst_diag_col_name]
                            .str.split(",", expand=True)
                            .apply(
                                lambda x: x.str.replace(".", "")
                                .str.strip()
                                .str.upper()
                                .str.startswith(
                                    tuple(
                                        df_icd_mapping.loc[
                                            df_icd_mapping["ELX_GRP"] == i,
                                            "ICD",
                                        ].values[0]
                                    )
                                )
                            )
                            .any(axis="columns")
                            .astype(int),
                        )
                        for i in range(1, 32)
                    ]
                )
            )
        )
        return df

    @classmethod
    def calculate_final_score(
        cls, df: dd.DataFrame, output_column_name="elixhauser_score"
    ) -> dd.DataFrame:
        df[output_column_name] = (
            df[["ELX_GRP_" + str(i) for i in range(1, 32)]]
            .sum(axis=1)
            .fillna(0)
            .astype(int)
        )
        return df


def score(
    df: dd.DataFrame,
    lst_diag_col_name,
    cms_format="MAX",
    output_column_name="elixhauser_score",
) -> dd.DataFrame:
    """
    Computes Elixhauser score for the benes in the input dataframe. The input dataframe should be at bene level, with
    a column containing each bene’s comma separated list of diagnosis codes from the observed period.

    Parameters
    ----------
    df : dask.DataFrame
        Bene level dataframe
    lst_diag_col_name : str
        Column name containing the list of diagnosis codes
    cms_format : {'MAX', 'TAF'}
        CMS file format.
    output_column_name : str, default='elixhauser_score'
        Output column name. Defaults to elixhauser score

    Returns
    -------
    dask.DataFrame

    """
    df = ElixhauserScoring.flag_comorbidities(
        df, lst_diag_col_name, cms_format=cms_format
    )
    df = ElixhauserScoring.calculate_final_score(df, output_column_name)
    return df

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Python implementation of The New York University (NYU) Emergency Department (ED) visit algorithm.
    This algorithm is the most widely used tool for retrospectively assessing the probability
    that ED visits are urgent, preventable, or optimally treated in an ED,
    using administrative data (Billings, Parikh, and Mijanovich 2000b; Feldman 2010).
"""
__author__ = "Manoradhan Murugesan"
__email__ = "manorathan@uchicago.edu"

import sys
import os
from ast import literal_eval
import pandas as pd
import numpy as np
import dask.dataframe as dd
import math
import logging

sys.path.append("../../")
from medicaid_utils.common_utils import recipes


class BillingsED:
    logger_name = __name__
    package_folder, filename = os.path.split(__file__)
    data_folder = os.path.join(package_folder, "data")

    @classmethod
    def add_category_check_columns(cls, df, dx_col_name="dxgroup"):
        """
        Add additional boolean columns corresponding to ED categories
        to a dataframe with dx code. The indlicator columns added are,

        ===============   ==============================================
        column name       description
        ===============   ==============================================
          injury          Injury
          psych           Mental Health Related
          alcohol         Alcohol Related
          drug            Drug Related (excluding alcohol)
          unclassified    Not in a Special Category, and Not Classified
        ===============   ==============================================

        Parameters
        ----------
        df : dask.DataFrame
             dataframe with dx column
        dx_col_name : str, default='dxgroup'

        Returns
        -------
        dask.DataFrame
        """

        lst_ed_categories = ["acs", "psych", "drug", "alcohol", "injury"]
        # Load recode csv file that has information for
        # recoding principal diagnosis
        # Load category data files to a dictionary
        dct_category_lists = dict()
        for cat in lst_ed_categories:
            df_cat_check = pd.read_csv(
                os.path.join(cls.data_folder, cat + "_check.csv"),
                dtype={
                    "start_index": "int",
                    "end_index": "float",
                    "code": "str",
                    "comments": "str",
                },
            )
            recipes.log_assert(
                "end_index" in df_cat_check.columns,
                cat + "_check csv file has no end_index column",
                logger_name=cls.logger_name,
            )
            recipes.log_assert(
                "code" in df_cat_check.columns,
                cat + "_check csv file has no code column",
                logger_name=cls.logger_name,
            )
            lst_end_variants = df_cat_check.end_index.unique()
            dct_category_lists[cat] = dict()
            for end_index in lst_end_variants:
                dct_category_lists[cat][end_index] = (
                    df_cat_check.loc[
                        pd.isnull(df_cat_check.end_index), "code"
                    ].values.tolist()
                    if math.isnan(end_index)
                    else df_cat_check.loc[
                        df_cat_check.end_index == end_index, "code"
                    ].values.tolist()
                )

        def _apply_category_check(dx_code, cat, dct_category_lists):
            """Check if a dx code matches with codes corresponding to an ED category=
            :param str dx_codex: dx code
            :param str cat: ED category
            :rtype int
            """
            if isinstance(dx_code, float) or isinstance(dx_code, int):
                dx_code = str(int(dx_code))
            return int(
                any(
                    [
                        dx_code[
                            slice(
                                0,
                                None
                                if math.isnan(end_index)
                                else int(end_index + 1),
                            )
                        ]
                        in map(str, dct_category_lists[cat][end_index])
                        for end_index in dct_category_lists[cat]
                    ]
                )
            )

        df = df.map_partitions(
            lambda pdf: pdf.assign(
                **dict(
                    [
                        (
                            cat,
                            pdf[dx_col_name].apply(
                                lambda x: _apply_category_check(
                                    dx_code=x,
                                    cat=cat,
                                    dct_category_lists=dct_category_lists,
                                )
                            ),
                        )
                        for cat in lst_ed_categories
                    ]
                )
            )
        )
        df["unclassified"] = (
            ~(df[["injury", "drug", "psych", "alcohol"]].any(axis="columns"))
        ).astype(int)
        return df

    @classmethod
    def recode_dataframe(cls, df, dx_col_name="dx", output_col_name="dxgroup"):
        """
        Recode principal diagnosis codes in a dataframe

        Parameters
        ----------
        df : dask.DataFrame
            dataframe with principal diagnosis codes
        dx_col_name : str, default='dx'
            name of dx column to be recoded
        output_col_name : str, default='dxgroup'

        Returns
        -------

        """
        index_name = df.index.name
        df_recode = pd.read_csv(
            os.path.join(cls.data_folder, "recode.csv"),
            dtype={
                "origin": "str",
                "target": "str",
                "comments": "str",
                "updates": "str",
                "deleted": "int",
                "starts_with": "int",
            },
        )

        recipes.log_assert(
            "deleted" in df_recode.columns,
            "recode csv file has no deleted column",
            logger_name=cls.logger_name,
        )
        recipes.log_assert(
            "origin" in df_recode.columns,
            "recode csv file has no origin column",
            logger_name=cls.logger_name,
        )
        recipes.log_assert(
            "target" in df_recode.columns,
            "recode csv file has no target column",
            logger_name=cls.logger_name,
        )

        df_recode = df_recode.loc[
            df_recode["deleted"] != 1,
        ]
        df_recode["origin"] = df_recode.origin.apply(literal_eval)

        df_startswith_recode = df_recode.loc[df_recode["starts_with"] == 1]

        dct_recode = pd.Series(
            df_startswith_recode.origin.values,
            index=df_startswith_recode.target,
        ).to_dict()

        _dct_new = dict()

        for target, lst_origin in dct_recode.items():
            for origin in lst_origin:
                _dct_new[origin] = target

        dct_recode = dict(_dct_new)
        df_nonstartswith_recode = df_recode.loc[df_recode["starts_with"] == 0][
            ["origin", "target"]
        ].explode("origin")
        df_nonstartswith_recode = df_nonstartswith_recode.drop_duplicates()
        recipes.log_assert(
            dx_col_name in df.columns,
            "input dataframe has no {0} column".format(dx_col_name),
            logger_name=cls.logger_name,
        )
        df = df.merge(
            df_nonstartswith_recode,
            left_on=dx_col_name,
            right_on="origin",
            how="left",
        ).drop_duplicates()
        df["target"] = (
            df["target"]
            .fillna(
                df[dx_col_name].apply(
                    lambda x: next(
                        (
                            val
                            for key, val in dct_recode.items()
                            if x.startswith(key)
                        ),
                        x,
                    )
                )
            )
            .astype(str)
            .str.strip()
        )
        df = df.drop(["origin"], axis=1)
        df = df.rename(columns={"target": output_col_name})
        if df.index.name != index_name:
            df = df.set_index(index_name, drop=False)
        return df

    @classmethod
    def get_ed_probas(cls, df, output_col_name):
        """
        Merge with EDDXs and adds the following columns,

        ============   =====================================================
        column name    description
        ============   =====================================================
        ne             Non-Emergent
        epct           Emergent, Primary Care Treatable
        edcnpa         Emergent, ED Care Needed, Preventable/Avoidable
        edcnnpa        Emergent, ED Care Needed, Not Preventable/Avoidable
        ============   =====================================================

        Parameters
        ----------
        df : dask.DataFrame
        output_col_name : str

        Returns
        -------
        dask.DataFrame
        """
        df_eddxs = pd.read_sas(os.path.join(cls.data_folder, "eddxs.sas7bdat"))
        df_eddxs["prindx"] = df_eddxs.prindx.str.decode("utf-8").str.strip()
        df_eddxs = df_eddxs.drop_duplicates()
        index_name = df.index.name
        df = df.merge(
            df_eddxs, how="left", left_on=output_col_name, right_on="prindx"
        ).drop_duplicates()  # .set_index(index_name, drop=False)
        # classified by our docs and/or case file review
        df["ne"] = df["nonemerg"].where(
            ~(
                df[["drug", "injury", "psych", "alcohol"]].any(axis=1)
                | df["nonemerg"].isna()
            ),
            0,
        )
        df["epct"] = df["emergpc"].where(
            ~(
                df[["drug", "injury", "psych", "alcohol"]].any(axis=1)
                | df["emergpc"].isna()
            ),
            0,
        )
        df["edcnpa"] = df["acs"] * df[["emedpa", "emednpa"]].sum(axis=1)
        df["edcnpa"] = df["edcnpa"].where(
            ~(
                df[["drug", "injury", "psych", "alcohol"]].any(axis=1)
                | df["edcnpa"].isna()
            ),
            0,
        )
        df["edcnnpa"] = (df["acs"] != 1).astype(int) * df[
            ["emedpa", "emednpa"]
        ].sum(axis=1).fillna(0)
        df["edcnnpa"] = df["edcnnpa"].where(
            ~(
                df[["drug", "injury", "psych", "alcohol"]].any(axis=1)
                | df["edcnnpa"].isna()
            ),
            0,
        )
        df["unclassified"] = df["unclassified"].where(
            df["unclassified"] == 0, (df["prindx"].isna()).astype(int)
        )
        df = df.drop(
            ["prindx", "acs", "emednpa", "emedpa", "emergpc", "nonemerg"],
            axis=1,
        )
        logging.warning(
            "Setting index to {0}, current index is {1}".format(
                index_name, df.index.name
            )
        )
        if df.index.name != index_name:
            df = df.set_index(index_name, drop=False)
        return df


def get_nyu_ed_proba(
    df, dx_col_name, logger_name=None
):
    """
    This functions returns probabilities for dxgroup codes in the input dataframe being urgent, preventable, or
    optimally treated ED visits. This function recodes the passed dx codes to a set of dx codes used in (Billings,
    Parikh, and Mijanovich 2000b; Feldman 2010) study, adds columns (‘injury’, ‘psych’, ‘alcohol’ and ‘drug’),
    indicating the type of ED service.

    Parameters
    ----------
    df : dask.DataFrame
        dataframe with dx codes
    dx_col_name : str
        name of dx code column
    logger_name : str, optional
        logger name

    Returns
    -------
    dask.DataFrame
    """
    if logger_name is not None:
        BillingsED.logger_name = logger_name
    output_col_name = 'dxgroup'
    df = BillingsED.recode_dataframe(df, dx_col_name, output_col_name)
    df = BillingsED.add_category_check_columns(df, output_col_name)
    df = BillingsED.get_ed_probas(df, output_col_name)
    return df

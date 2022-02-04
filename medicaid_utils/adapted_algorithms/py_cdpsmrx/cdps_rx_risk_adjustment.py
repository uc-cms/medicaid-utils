import sys
import os

import numpy as np
import pandas as pd
import dask.dataframe as dd
from typing import List


class CdpsRxRiskAdjustment:
    package_folder, filename = os.path.split(__file__)
    data_folder = os.path.join(package_folder, "data")

    @classmethod
    def add_age_and_gender_cols(
        cls, df: dd.DataFrame
    ) -> (dd.DataFrame, List[str]):
        """
        Adds age & gender related covariates used in CDPS Rx Risk Adjustment model. Returns a dataframe with the
        below columns:

        * a_under1 - 0 or 1, age<=1
        * a_1_4 - 0 or 1, 1<age<5
        * a_5_14m - 0 or 1, 5<=age<15 male
        * a_5_14f - 0 or 1, 5<=age<15 female
        * a_15_24m - 0 or 1, 15<=age<25 male
        * a_15_24f - 0 or 1, 15<=age<25 female
        * a_25_44m - 0 or 1, 25<=age<45 male
        * a_25_44f - 0 or 1, 25<=age<45 female
        * a_45_64m - 0 or 1, 45<=age<65 male
        * a_45_64f - 0 or 1, 45<=age<65 female
        * a_65 - 0 or 1, 65<=age

        Parameters
        ----------
        df - dask.DataFrame

        Returns
        -------
        dask.DataFrame

        """
        lst_age_and_gender_cols = [
            "a_under1",
            "a_1_4",
            "a_5_14m",
            "a_5_14f",
            "a_15_24m",
            "a_15_24f",
            "a_25_44m",
            "a_25_44f",
            "a_45_64m",
            "a_45_64f",
            "a_65",
        ]
        df = df.assign(
            male=df["Female"].where(
                df["Female"].isna(), (df["Female"] == 0).astype(int)
            )
        )
        df = df.assign(
            a_under1=((df["age"] <= 1) | df["age"].isna()).astype(int),
            a_1_4=(df["age"].between(1, 5, inclusive=False)).astype(int),
            a_5_14m=(
                (df["age"] >= 5) & (df["age"] < 15) & (df["male"] == 1)
            ).astype(int),
            a_5_14f=(
                (df["age"] >= 5) & (df["age"] < 15) & (df["male"] == 0)
            ).astype(int),
            a_15_24m=(
                (df["age"] >= 15) & (df["age"] < 25) & (df["male"] == 1)
            ).astype(int),
            a_15_24f=(
                (df["age"] >= 15) & (df["age"] < 25) & (df["male"] == 0)
            ).astype(int),
            a_25_44m=(
                (df["age"] >= 25) & (df["age"] < 45) & (df["male"] == 1)
            ).astype(int),
            a_25_44f=(
                (df["age"] >= 25) & (df["age"] < 45) & (df["male"] == 0)
            ).astype(int),
            a_45_64m=(
                (df["age"] >= 45) & (df["age"] < 65) & (df["male"] == 1)
            ).astype(int),
            a_45_64f=(
                (df["age"] >= 45) & (df["age"] < 65) & (df["male"] == 0)
            ).astype(int),
            a_65=(df["age"] >= 65).astype(int),
        )
        return df, lst_age_and_gender_cols

    @classmethod
    def add_cdps_category_cols(
        cls, df: dd.DataFrame, lst_diag_cd_col_name
    ) -> (dd.DataFrame, List[str]):
        """
        Adds CDPS category columns

        Parameters
        ----------
        df : dask.DataFrame
            PS Dataframe with LST_DIAG_CD column
        lst_diag_cd_col_name : str

        Returns
        -------
        dask.DataFrame
        """

        # Load var lists and maps
        data_folder = os.path.join(cls.data_folder, "cdps")
        pdf_var_map = pd.read_csv(
            os.path.join(data_folder, "cdps_hierarchical_vars.csv")
        )
        pdf_adult_var_map = pdf_var_map.loc[pdf_var_map["adult"] == 1]
        dct_adult_diag_var = (
            pdf_adult_var_map[["val", "label"]]
            .set_index("val")
            .to_dict()["label"]
        )
        pdf_child_var_map = pdf_var_map.loc[pdf_var_map["adult"] == 0]
        dct_child_diag_var = (
            pdf_child_var_map[["val", "label"]]
            .set_index("val")
            .to_dict()["label"]
        )
        lst_interaction_vars = [
            lbl.strip()
            for lbl in pd.read_csv(
                os.path.join(data_folder, "lst_interaction_vars.csv"),
                header=None,
            )[0].tolist()
        ]
        lst_regression_vars = [
            lbl.strip()
            for lbl in pd.read_csv(
                os.path.join(data_folder, "lst_regr_var_cdps.csv"), header=None
            )[0].tolist()
        ]
        lst_adult_vars = [
            lbl.strip()
            for lbl in pd.read_csv(
                os.path.join(data_folder, "lst_cdps_categories_adult.csv"),
                header=None,
            )[0].tolist()
        ]
        lst_child_vars = [
            lbl.strip()
            for lbl in pd.read_csv(
                os.path.join(data_folder, "lst_cdps_categories_child.csv"),
                header=None,
            )[0].tolist()
        ]
        lst_non_hierarchical_diags_adult = (
            pdf_adult_var_map.loc[pdf_adult_var_map["level"] == 0]["label"]
            .unique()
            .tolist()
        )
        lst_non_hierarchical_diags_child = (
            pdf_child_var_map.loc[pdf_child_var_map["level"] == 0]["label"]
            .unique()
            .tolist()
        )
        pdf_adult_hierarchical_labels = (
            pdf_adult_var_map.loc[pdf_adult_var_map["level"] != 0][
                ["label", "level", "level_order"]
            ]
            .drop_duplicates()
            .sort_values(by=["level", "level_order"], ascending=True)
        )
        pdf_child_hierarchical_labels = (
            pdf_child_var_map.loc[pdf_child_var_map["level"] != 0][
                ["label", "level", "level_order"]
            ]
            .drop_duplicates()
            .sort_values(by=["level", "level_order"], ascending=True)
        )
        lst_common_hierarchical_labels = list(
            set(pdf_adult_hierarchical_labels.label.tolist()).intersection(
                set(pdf_child_hierarchical_labels.label.tolist())
            )
        )
        dct_diag_hierarchies_adult = (
            pdf_adult_hierarchical_labels.set_index(["level", "level_order"])
            .groupby(level=0)
            .apply(lambda pdf: pdf.xs(pdf.name)["label"].to_dict())
            .to_dict()
        )
        dct_diag_hierarchies_child = (
            pdf_child_hierarchical_labels.set_index(["level", "level_order"])
            .groupby(level=0)
            .apply(lambda pdf: pdf.xs(pdf.name)["label"].to_dict())
            .to_dict()
        )

        # Fix missing levels in child diag hierarchies
        for level in list(dct_diag_hierarchies_child.keys()):
            if len(list(dct_diag_hierarchies_child[level].keys())) != max(
                list(dct_diag_hierarchies_child[level].keys())
            ):
                dct_level = dict(
                    [
                        (idx + 1, item[1])
                        for idx, item in enumerate(
                            list(
                                sorted(
                                    dct_diag_hierarchies_child[level].items()
                                )
                            )
                        )
                    ]
                )
                dct_diag_hierarchies_child[level] = dct_level

        # Store list of new columns
        lst_cdps_cat_cols = list(
            set(
                lst_adult_vars
                + lst_child_vars
                + lst_interaction_vars
                + ["NONE", "OTHER", "NOCDPS"]
            )
        )
        df = df.assign(
            NONE=(
                df[lst_diag_cd_col_name].isna()
                | (df[lst_diag_cd_col_name].str.strip() == "")
            ).astype(int)
        )
        df["LST_ADULT_DIAG_CD"] = ""
        df = df.assign(
            LST_ADULT_DIAG_CD=df["LST_ADULT_DIAG_CD"].str.split(",")
        )
        df = df.assign(
            LST_CHILD_DIAG_CD=df["LST_ADULT_DIAG_CD"],
            LST_ADULT_DIAG_CD=df["LST_ADULT_DIAG_CD"].where(
                ~df["aid"].isin(["DA", "AA"]),
                df[lst_diag_cd_col_name].apply(
                    lambda lst_raw_diag: list(
                        set(
                            [
                                dct_adult_diag_var.get(
                                    diag_cd.strip().upper().replace(".", ""),
                                    "OTHER",
                                )
                                for diag_cd in lst_raw_diag.split(",")
                            ]
                        )
                    )
                ),
            ),
        )
        df = df.assign(
            LST_CHILD_DIAG_CD=df["LST_CHILD_DIAG_CD"].where(
                ~df["aid"].isin(["DC", "AC"]),
                df[lst_diag_cd_col_name].apply(
                    lambda lst_raw_diag: list(
                        set(
                            [
                                dct_child_diag_var.get(
                                    diag_cd.strip().upper().replace(".", ""),
                                    "OTHER",
                                )
                                for diag_cd in lst_raw_diag.split(",")
                            ]
                        )
                    )
                ),
            )
        )
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                OTHER=(
                    (
                        pdf["aid"].isin(["DA", "AA"])
                        & pd.DataFrame(
                            pdf["LST_ADULT_DIAG_CD"].tolist(), index=pdf.index
                        )
                        .isin(["OTHER"])
                        .any(1)
                        | (
                            pdf["aid"].isin(["DC", "AC"])
                            & pd.DataFrame(
                                pdf["LST_CHILD_DIAG_CD"].tolist(),
                                index=pdf.index,
                            )
                            .isin(["OTHER"])
                            .any(1)
                        )
                    ).astype(int)
                )
            )
        )
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                **dict(
                    [
                        (
                            diag,
                            (
                                pdf["aid"].isin(["DA", "AA"])
                                & pd.DataFrame(
                                    pdf["LST_ADULT_DIAG_CD"].tolist(),
                                    index=pdf.index,
                                )
                                .isin([diag])
                                .any(1)
                            ).astype(int),
                        )
                        for diag in lst_non_hierarchical_diags_adult
                    ]
                )
            )
        )
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                **dict(
                    [
                        (
                            diag,
                            (
                                pdf["aid"].isin(["DC", "AC"])
                                & pd.DataFrame(
                                    pdf["LST_CHILD_DIAG_CD"].tolist(),
                                    index=pdf.index,
                                )
                                .isin([diag])
                                .any(1)
                            ).astype(int),
                        )
                        for diag in lst_non_hierarchical_diags_child
                        if diag not in lst_non_hierarchical_diags_adult
                    ]
                    + [
                        (
                            diag,
                            pdf[diag].where(
                                ~pdf["aid"].isin(["DC", "AC"]),
                                pd.DataFrame(
                                    pdf["LST_CHILD_DIAG_CD"].tolist(),
                                    index=pdf.index,
                                )
                                .isin([diag])
                                .any(1)
                                .astype(int),
                            ),
                        )
                        for diag in lst_non_hierarchical_diags_child
                        if diag in lst_non_hierarchical_diags_adult
                    ]
                )
            )
        )
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                **dict(
                    [
                        item
                        for sublist in [
                            [
                                (
                                    diag,
                                    (
                                        pdf["aid"].isin(["DA", "AA"])
                                        & pd.DataFrame(
                                            pdf["LST_ADULT_DIAG_CD"].tolist(),
                                            index=pdf.index,
                                        )
                                        .isin([diag])
                                        .any(1)
                                        & (
                                            (level_order == 1)
                                            | (
                                                ~pd.DataFrame(
                                                    pdf[
                                                        "LST_ADULT_DIAG_CD"
                                                    ].tolist(),
                                                    index=pdf.index,
                                                )
                                                .isin(
                                                    [
                                                        diag
                                                        for level_order, diag in sorted(
                                                            dct_level.items()
                                                        )[
                                                            : level_order - 1
                                                        ]
                                                    ]
                                                )
                                                .any(1)
                                            )
                                        )
                                    ).astype(int),
                                )
                                for level_order, diag in sorted(
                                    dct_level.items()
                                )
                            ]
                            for level, dct_level in dct_diag_hierarchies_adult.items()
                        ]
                        for item in sublist
                    ]
                )
            )
        )
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                **dict(
                    [
                        item
                        for sublist in [
                            [
                                (
                                    diag,
                                    (
                                        pdf["aid"].isin(["DC", "AC"])
                                        & pd.DataFrame(
                                            pdf["LST_CHILD_DIAG_CD"].tolist(),
                                            index=pdf.index,
                                        )
                                        .isin([diag])
                                        .any(1)
                                        & (
                                            (level_order == 1)
                                            | (
                                                ~pd.DataFrame(
                                                    pdf[
                                                        "LST_CHILD_DIAG_CD"
                                                    ].tolist(),
                                                    index=pdf.index,
                                                )
                                                .isin(
                                                    [
                                                        diag
                                                        for level_order, diag in sorted(
                                                            dct_level.items()
                                                        )[
                                                            : level_order - 1
                                                        ]
                                                    ]
                                                )
                                                .any(1)
                                            )
                                        )
                                    ).astype(int),
                                )
                                for level_order, diag in sorted(
                                    dct_level.items()
                                )
                                if diag not in lst_common_hierarchical_labels
                            ]
                            + [
                                (
                                    diag,
                                    pdf[diag].where(
                                        ~pdf["aid"].isin(["DC", "AC"]),
                                        (
                                            pd.DataFrame(
                                                pdf[
                                                    "LST_CHILD_DIAG_CD"
                                                ].tolist(),
                                                index=pdf.index,
                                            )
                                            .isin([diag])
                                            .any(1)
                                            & (
                                                (level_order == 1)
                                                | (
                                                    ~pd.DataFrame(
                                                        pdf[
                                                            "LST_CHILD_DIAG_CD"
                                                        ].tolist(),
                                                        index=pdf.index,
                                                    )
                                                    .isin(
                                                        [
                                                            diag
                                                            for level_order, diag in sorted(
                                                                dct_level.items()
                                                            )[
                                                                : level_order
                                                                - 1
                                                            ]
                                                        ]
                                                    )
                                                    .any(1)
                                                )
                                            )
                                        ).astype(int),
                                    ),
                                )
                                for level_order, diag in sorted(
                                    dct_level.items()
                                )
                                if diag in lst_common_hierarchical_labels
                            ]
                            for level, dct_level in dct_diag_hierarchies_child.items()
                        ]
                        for item in sublist
                    ]
                )
            )
        )
        df = df.assign(
            **dict(
                [
                    (diag, df[diag].where(~df["aid"].isin(["DC", "AC"]), 0))
                    for diag in ["CANB", "DIA1H", "DIA1M", "DIA2M", "EYEL"]
                ]
            )
        )
        df = df.assign(
            PULH=df["PULH"].where(
                ~df["aid"].isin(["AA", "AC", "AG"]),
                df[["PULH", "PULVH"]].max(axis=1),
            ),
            PULVH=df["PULVH"].where(~df["aid"].isin(["AA", "AC", "AG"]), 0),
            DDL=df["DDL"].where(
                ~df["aid"].isin(["AA", "AG"]), df[["DDL", "DDM"]].max(axis=1)
            ),
            DDM=df["DDM"].where(~df["aid"].isin(["AA", "AG"]), 0),
        )
        df = df.assign(
            NOCDPS=(df[lst_regression_vars].sum(axis=1) == 0).astype(int)
        )
        df = df.assign(
            **dict(
                [
                    (idiag, df[diag].where(df["aid"].isin(["DC"]), 0))
                    for idiag, diag in zip(
                        lst_interaction_vars,
                        [diag[1:] for diag in lst_interaction_vars],
                    )
                ]
            )
        )
        df = df.assign(
            **dict(
                [
                    (
                        diag,
                        df[diag].where(df["aid"].isin(["DC", "DA"]), np.nan),
                    )
                    for diag in lst_interaction_vars
                ]
            )
        )
        return (
            df[
                [
                    col
                    for col in df.columns
                    if col not in ["LST_ADULT_DIAG_CD", "LST_CHILD_DIAG_CD"]
                ]
            ],
            lst_cdps_cat_cols,
        )

    @classmethod
    def add_mrx_cat_cols(
        cls, df: dd.DataFrame, lst_ndc_col_name="LST_NDC"
    ) -> (dd.DataFrame, List[str]):
        """
        Adds MRX category columns

        Parameters
        ----------
        df : dask.DataFrame
        lst_ndc_col_name : str, default='LST_NDC'

        Returns
        -------
        dask.DataFrame

        """
        # Load var lists and maps
        data_folder = os.path.join(cls.data_folder, "mrx")
        pdf_ndc_mrx_cat_map = pd.read_csv(
            os.path.join(data_folder, "ndc_mrx_cat_map.csv"), dtype=object
        )
        dct_ndc_mrx_cat = pdf_ndc_mrx_cat_map.set_index("NDC").to_dict()[
            "MRX_CAT"
        ]
        pdf_mrx_cat_hierarchies = pd.read_csv(
            os.path.join(data_folder, "mrx_cat_hierarchies.csv"), dtype=object
        )
        pdf_mrx_cat_hierarchies[
            ["level", "level_order"]
        ] = pdf_mrx_cat_hierarchies[["level", "level_order"]].astype(int)
        lst_non_hierarchical_mrx_cat = (
            pdf_mrx_cat_hierarchies.loc[pdf_mrx_cat_hierarchies["level"] == 0][
                "mrx_cat"
            ]
            .unique()
            .tolist()
        )
        pdf_hierarchical_mrx_cats = (
            pdf_mrx_cat_hierarchies.loc[pdf_mrx_cat_hierarchies["level"] != 0][
                ["mrx_cat", "level", "level_order"]
            ]
            .drop_duplicates()
            .sort_values(by=["level", "level_order"], ascending=True)
        )
        dct_mrx_hierarchies = (
            pdf_hierarchical_mrx_cats.set_index(["level", "level_order"])
            .groupby(level=0)
            .apply(lambda df: df.xs(df.name)["mrx_cat"].to_dict())
            .to_dict()
        )
        # Store list of columns in output file
        lst_mrx_cat_cols = list(
            set(
                pdf_mrx_cat_hierarchies["mrx_cat"].unique().tolist()
                + ["NONE", "OTHER"]
            )
        )
        df = df.assign(
            NONE=(
                df[lst_ndc_col_name].isna()
                | (df[lst_ndc_col_name].str.strip() == "")
            ).astype(int),
            LST_NDC_MRX=df[lst_ndc_col_name].apply(
                lambda lst_ndc: list(
                    set(
                        [
                            dct_ndc_mrx_cat.get(
                                ndc_cd.strip()
                                .upper()
                                .replace(".", "")
                                .zfill(11),
                                "OTHER",
                            )
                            for ndc_cd in lst_ndc.split(",")
                        ]
                    )
                )
            ),
        )
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                OTHER=pd.DataFrame(
                    pdf["LST_NDC_MRX"].tolist(), index=pdf.index
                )
                .isin(["OTHER"])
                .any(1)
                .astype(int)
            )
        )
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                **dict(
                    [
                        (
                            mrx,
                            pd.DataFrame(
                                pdf["LST_NDC_MRX"].tolist(), index=pdf.index
                            )
                            .isin([mrx])
                            .any(1)
                            .astype(int),
                        )
                        for mrx in lst_non_hierarchical_mrx_cat
                    ]
                )
            )
        )
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                **dict(
                    [
                        item
                        for sublist in [
                            [
                                (
                                    mrx,
                                    (
                                        pd.DataFrame(
                                            pdf["LST_NDC_MRX"].tolist(),
                                            index=pdf.index,
                                        )
                                        .isin([mrx])
                                        .any(1)
                                        & (
                                            (level_order == 1)
                                            | (
                                                ~pd.DataFrame(
                                                    pdf[
                                                        "LST_NDC_MRX"
                                                    ].tolist(),
                                                    index=pdf.index,
                                                )
                                                .isin(
                                                    [
                                                        mrx
                                                        for level_order, mrx in sorted(
                                                            dct_level.items()
                                                        )[
                                                            : level_order - 1
                                                        ]
                                                    ]
                                                )
                                                .any(1)
                                            )
                                        )
                                    ).astype(int),
                                )
                                for level_order, mrx in sorted(
                                    dct_level.items()
                                )
                            ]
                            for level, dct_level in dct_mrx_hierarchies.items()
                        ]
                        for item in sublist
                    ]
                )
            )
        )
        return (
            df[[col for col in df.columns if col not in ["LST_NDC_MRX"]]],
            lst_mrx_cat_cols,
        )

    @classmethod
    def combine_cdps_mrx_hierarchies(cls, df: dd.DataFrame):
        """
        Apply CDPS MRX hierarchical rollups

        Parameters
        ----------
        df : dask.DataFrame

        Returns
        -------
        dask.DataFrame

        """
        df = df.assign(
            MRX1=df["MRX1"].where(
                ~(df[["CARVH", "CARM"]].sum(axis="columns") > 0), 0
            )
        )
        df = df.assign(CARL=df["CARL"].where(~(df["MRX1"] == 1), 0))
        df = df.assign(CAREL=df["CAREL"].where(~(df["MRX1"] == 1), 0))
        df = df.assign(
            MRX2=df["MRX2"].where(
                ~(
                    (df["MRX1"] == 1)
                    | (df[["CARVH", "CARM"]].sum(axis="columns") > 0)
                    | (df[["CARL", "CAREL"]].sum(axis="columns") > 0)
                ),
                0,
            )
        )
        df = df.assign(
            MRX3=df["MRX3"].where(
                ~(
                    df[["PSYH", "PSYM", "PSYML", "PSYL"]].sum(axis="columns")
                    > 0
                ),
                0,
            )
        )
        df = df.assign(
            MRX4=df["MRX4"].where(
                ~(
                    df[["DIA1H", "DIA1M", "DIA2M", "DIA2L"]].sum(
                        axis="columns"
                    )
                    > 0
                ),
                0,
            )
        )
        df = df.assign(
            MRX5=df["MRX5"].where(
                ~(df[["RENEH", "RENVH"]].sum(axis="columns") > 0), 0
            )
        )
        df = df.assign(RENM=df["RENM"].where(~(df["MRX5"] == 1), 0))
        df = df.assign(RENL=df["RENL"].where(~(df["MRX5"] == 1), 0))
        df = df.assign(MRX6=df["MRX6"].where(~(df["HEMEH"] == 1), 0))
        df = df.assign(HEMVH=df["HEMVH"].where(~(df["MRX6"] == 1), 0))
        df = df.assign(HEMM=df["HEMM"].where(~(df["MRX6"] == 1), 0))
        df = df.assign(HEML=df["HEML"].where(~(df["MRX6"] == 1), 0))
        df = df.assign(
            MRX9=df["MRX9"].where(
                ~(df[["AIDSH", "INFH"]].sum(axis="columns") > 0), 0
            )
        )
        df = df.assign(HIVM=df["HIVM"].where(~(df["MRX9"] == 1), 0))
        df = df.assign(
            MRX7=df["MRX7"].where(
                ~(
                    (df[["AIDSH", "INFH"]].sum(axis="columns") > 0)
                    | (df["HIVM"] == 1)
                ),
                0,
            )
        )
        df = df.assign(
            MRX8=df["MRX8"].where(
                ~(
                    (df[["AIDSH", "INFH"]].sum(axis="columns") > 0)
                    | (df["HIVM"] == 1)
                ),
                0,
            )
        )
        df = df.assign(
            INFM=df["INFM"].where(
                ~(df[["MRX7", "MRX8", "MRX9"]].sum(axis="columns") > 0), 0
            )
        )
        df = df.assign(
            INFL=df["INFL"].where(
                ~(df[["MRX7", "MRX8", "MRX9"]].sum(axis="columns") > 0), 0
            )
        )
        df = df.assign(
            MRX10=df["MRX10"].where(
                ~(df[["SKCM", "SKCL", "SKCVL"]].sum(axis="columns") > 0), 0
            )
        )
        df = df.assign(
            MRX11=df["MRX11"].where(
                ~(df[["CANVH", "CANH", "CANM"]].sum(axis="columns") > 0), 0
            )
        )
        df = df.assign(CANL=df["CANL"].where(~(df["MRX11"] == 1), 0))
        df = df.assign(
            MRX12=df["MRX12"].where(
                ~(df[["CNSH", "CNSM"]].sum(axis="columns") > 0), 0
            )
        )
        df = df.assign(CNSL=df["CNSL"].where(~(df["MRX12"] == 1), 0))
        df = df.assign(
            MRX14=df["MRX14"].where(
                ~(
                    (df[["CNSH", "CNSM"]].sum(axis="columns") > 0)
                    | (df["MRX12"] == 1)
                    | (df["CNSL"] == 1)
                ),
                0,
            )
        )
        df = df.assign(
            MRX13=df["MRX13"].where(
                ~(
                    (df[["CNSH", "CNSM"]].sum(axis="columns") > 0)
                    | (df["MRX12"] == 1)
                    | (df["CNSL"] == 1)
                    | (df["MRX14"] == 1)
                ),
                0,
            )
        )
        df = df.assign(
            MRX15=df["MRX15"].where(
                ~(
                    df[["PULVH", "PULH", "PULM", "PULL"]].sum(axis="columns")
                    > 0
                ),
                0,
            )
        )
        df = df.assign(
            CCARM=df["CCARM"].where(
                ~((df["MRX1"] == 1) & (df["aid"] == "DC")), 1
            )
        )
        df = df.assign(
            CHEMEH=df["CHEMEH"].where(
                ~((df["MRX6"] == 1) & (df["aid"] == "DC")), 1
            )
        )
        df = df.assign(
            CHIVM=df["CHIVM"].where(
                ~((df["MRX9"] == 1) & (df["aid"] == "DC")), 0
            )
        )
        df = df.assign(
            CINFM=df["CINFM"].where(
                ~((df["MRX9"] == 1) & (df["aid"] == "DC")), 0
            )
        )
        df = df.assign(
            CHIVM=df["CHIVM"].where(
                ~(
                    (df[["MRX7", "MRX8"]].sum(axis="columns") > 0)
                    & (df["aid"] == "DC")
                ),
                1,
            )
        )
        df = df.assign(
            CINFM=df["CINFM"].where(
                ~(
                    (df[["MRX7", "MRX8"]].sum(axis="columns") > 0)
                    & (df["aid"] == "DC")
                ),
                0,
            )
        )
        return df

    @classmethod
    def calculate_risk(
        cls, df: dd.DataFrame, lst_cov: List[str], score_col_name="risk"
    ) -> dd.DataFrame:
        """
        Calculates CDPS risk adjustment score. The returned dataframe has the following new columns:

            * risk - CDPS risk score, dtype: float64

        Parameters
        ----------
        df : dask.DataFrame
        lst_cov : list of str
            List of covariates that CDPS MRX model uses
        score_col_name : str, default='risk'

        Returns
        -------
        dask.DataFrame
        """
        dct_weights = (
            pd.read_excel(
                os.path.join(cls.data_folder, "cdps_mrx_con_weighting.xlsx"),
                sheet_name="CDPSMRX2",
            )
            .fillna(0)
            .to_dict("records")[0]
        )
        df[score_col_name] = np.nan
        np_aa_coef = np.array(
            [dct_weights.get("AA_" + col, 0) for col in lst_cov]
        )
        np_ac_coef = np.array(
            [dct_weights.get("AC_" + col, 0) for col in lst_cov]
        )
        np_dadc_coef = np.array(
            [dct_weights.get("DADC_" + col, 0) for col in lst_cov]
        )
        df = df.assign(
            **dict(
                [
                    (
                        score_col_name,
                        df[score_col_name].where(
                            ~(df["aid"] == "AA"),
                            dct_weights["AA_Intercept"]
                            + (df[[col for col in lst_cov]] * np_aa_coef).sum(
                                axis=1
                            ),
                        ),
                    )
                ]
            )
        )
        df = df.assign(
            **dict(
                [
                    (
                        score_col_name,
                        df[score_col_name].where(
                            ~(df["aid"] == "AC"),
                            dct_weights["AC_Intercept"]
                            + (df[[col for col in lst_cov]] * np_ac_coef).sum(
                                axis=1
                            ),
                        ),
                    )
                ]
            )
        )
        df = df.assign(
            **dict(
                [
                    (
                        score_col_name,
                        df[score_col_name].where(
                            ~((df["aid"] == "DC") | (df["aid"] == "DA")),
                            dct_weights["DADC_Intercept"]
                            + (
                                df[[col for col in lst_cov]] * np_dadc_coef
                            ).sum(axis=1),
                        ),
                    )
                ]
            )
        )
        return df[[col for col in df.columns if col not in lst_cov]]


def cdps_rx_risk_adjust(
    df: dd.DataFrame,
    lst_diag_col_name="LST_DIAG_CD",
    lst_ndc_col_name="LST_NDC",
    score_col_name="risk",
) -> dd.DataFrame:
    """
    Calculate CDPS MRX risk adjustment score. This function expects the input dataframe to be aggregated to patient
    level, with columns containing comma separated lists of diagnosis codes & NDC codes during the observed time period.

    Parameters
    ----------
    df : dask.DataFrame
        BENE level Dataframe with diagnosis code & ndc_code list columns
    lst_diag_col_name : str, default='LST_DIAG_CD'
        Name of diagnosis code list column.
    lst_ndc_col_name : str, default='LST_NDC'
        Name of NDC code list column
    score_col_name : str, default='risk'
        CDPS MRX score output column name

    Returns
    -------
    dask.DataFrame
    """
    df, lst_age_and_gender_cols = CdpsRxRiskAdjustment.add_age_and_gender_cols(
        df
    )
    df, lst_cdps_category_cols = CdpsRxRiskAdjustment.add_cdps_category_cols(
        df, lst_diag_col_name
    )
    df, lst_mrx_category_cols = CdpsRxRiskAdjustment.add_mrx_cat_cols(
        df, lst_ndc_col_name
    )
    df = CdpsRxRiskAdjustment.combine_cdps_mrx_hierarchies(df)
    lst_cov = (
        lst_age_and_gender_cols
        + lst_cdps_category_cols
        + lst_mrx_category_cols
    )
    return CdpsRxRiskAdjustment.calculate_risk(df, lst_cov, score_col_name)

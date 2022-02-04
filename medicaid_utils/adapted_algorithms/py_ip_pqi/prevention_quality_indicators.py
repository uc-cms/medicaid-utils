#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
from ast import literal_eval
import pandas as pd
import numpy as np
import dask.dataframe as dd
from copy import copy
from itertools import product

sys.path.append("../../")
from medicaid_utils.common_utils import recipes, dataframe_utils


class PreventionQualityIndicators:
    package_folder, filename = os.path.split(__file__)
    data_folder = os.path.join(package_folder, "data")
    logger_name = __name__

    @classmethod
    def prepare_cols(cls, df, dx_col_suffix, pr_col_suffix, prsys_col_suffix):
        """
        New Column(s):

            DRG - Inpatient DIAGNOSIS RELATED GROUP (DRG) CODE (if from CMS system)
            DRGVER - GROUPING ALGORITHM USED TO ASSIGN DIAGNOSIS RELATED GROUP (DRG) VALUES (if from CMS system)
            DQTR - Claim quarter of the year
            YEAR - Claim year
            PR{1-6} - ICD9 procedure code columns
            DX{1-9} - ICD9 diagnosis code columns

        :param df:
        :return:
        """
        # IDENTIFIES THE GROUPING ALGORITHM USED TO ASSIGN DIAGNOSIS RELATED GROUP (DRG) VALUES,
        # HG = IF THE DRG VALUES ARE FROM THE CMS SYSTEM
        df["DRGVER"] = (
            df["DRG_REL_GROUP"].astype(str).str.slice(start=2, stop=4)
        )
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                DRG=pd.to_numeric(
                    pdf["DRG_REL_GROUP"].where(
                        (
                            pdf["DRG_REL_GROUP_IND"]
                            .astype(str)
                            .str.slice(stop=2)
                            == "HG"
                        ),
                        np.nan,
                    ),
                    errors="coerce",
                ),
                DRGVER=pd.to_numeric(
                    pdf["DRGVER"].where(
                        (
                            pdf["DRG_REL_GROUP_IND"]
                            .astype(str)
                            .str.slice(stop=2)
                            == "HG"
                        ),
                        np.nan,
                    ),
                    errors="coerce",
                ),
            )
        )
        df["DQTR"] = df["admsn_date"].dt.quarter
        df["YEAR"] = df["admsn_date"].dt.year
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                **dict(
                    [
                        (
                            "DX" + str(i),
                            pdf[dx_col_suffix + str(i)].str.slice(stop=5),
                        )
                        for i in range(1, 10)
                    ]
                    + [
                        (
                            "PR" + str(i),
                            pdf[pr_col_suffix + str(i)]
                            .where(
                                (
                                    pd.to_numeric(
                                        pdf[
                                            prsys_col_suffix + str(i)
                                        ],  # THE PROCEDURE CODING SYSTEM USED FOR THE PRINCIPAL PROCEDURE:
                                        # 02 = ICD-9-CM
                                        errors="coerce",
                                    )
                                    == 2
                                ),
                                "",
                            )
                            .str.slice(stop=4),
                        )
                        for i in range(1, 7)
                    ]
                )
            )
        )
        return df

    @classmethod
    def add_drg_cols(cls, df):
        """
        New Column(s):

            IMMUNDR
            MDC24
            MDC25
            MDCNEW
            ICDVER

        :param df:
        :return:
        """
        # VER=version,FOR EXAMPLE "HG15" WOULD REPRESENT CMS DRG, VERSION 15
        # df['DRGVER'] = df['DRGVER'].where(df['DRG_REL_GROUP_IND'].astype(str).str.slice(stop=2) != 'HG',
        #                                   df['DRG_REL_GROUP'].astype(str).str.slice(start=2, stop=4))
        df_mdc24 = pd.read_excel(
            open(
                os.path.join(cls.data_folder, "DRG_MDC_Conversion.xlsx"), "rb"
            ),
            sheet_name="MDC24",
        )
        df_mdc25 = pd.read_excel(
            open(
                os.path.join(cls.data_folder, "DRG_MDC_Conversion.xlsx"), "rb"
            ),
            sheet_name="MDC25",
        )
        df_mdc = df_mdc24.merge(df_mdc25, on="DRG", how="outer")
        df = df.merge(df_mdc, on="DRG", how="left")
        df = dataframe_utils.fix_index(df, "MSIS_ID", drop_column=False)
        df = df.map_partitions(lambda pdf: pdf.drop_duplicates())
        df["IMMUNDR"] = df["DRG"].isin([488, 489, 490]).astype(int)
        df["MDC24"] = df["MDC24"].fillna(99).astype(int)
        df["MDC25"] = df["MDC25"].fillna(99).astype(int)
        df["MDCNEW"] = np.nan
        if "MDC" not in df.columns:
            df["MDC"] = np.nan
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                MDC=pd.to_numeric(pdf["MDC"], errors="coerce")
            )
        )
        # df['MDCNEW'] = df['MDC']
        mask_mdc = ~df["MDC"].isin(
            [
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
                18,
                19,
                20,
                21,
                22,
                23,
                24,
                25,
            ]
        )
        df["MDCNEW"] = df["MDCNEW"].where(
            ~(mask_mdc & (df["DRGVER"] <= 24)), df["MDC24"]
        )
        df["MDCNEW"] = df["MDCNEW"].where(
            ~(mask_mdc & (df["DRGVER"] >= 25)), df["MDC25"]
        )
        mask_mdcnew = df["MDCNEW"].isin(
            [
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
                18,
                19,
                20,
                21,
                22,
                23,
                24,
                25,
            ]
        )
        df["MDC"] = df["MDC"].where(~(mask_mdc & mask_mdcnew), df["MDCNEW"])
        mask_mdc_else = mask_mdc & (~mask_mdcnew)
        mask_mdc0 = ((df["DRGVER"] <= 24) & (df["DRG"] == 470)) | (
            (df["DRGVER"] >= 25) & (df["DRG"] == 999)
        )
        df["MDC"] = df["MDC"].where(~(mask_mdc_else & mask_mdc0), 0)
        # TODO : verify logic : ELSE PUT "INVALID MDC KEY: " KEY " MDC " MDC " DRG " DRG DRGVER;
        # df['MDC'] = df['MDC'].where(~(mask_mdc & ~mask_mdc0), np.nan)
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                ICDVER=np.select(
                    [
                        (pdf["YEAR"] == 2006) & pdf["DQTR"].isin([1, 2, 3]),
                        ((pdf["YEAR"] == 2006) & pdf["DQTR"].isin([4]))
                        | (
                            (pdf["YEAR"] == 2007) & pdf["DQTR"].isin([1, 2, 3])
                        ),
                        ((pdf["YEAR"] == 2007) & pdf["DQTR"].isin([4]))
                        | (
                            (pdf["YEAR"] == 2008) & pdf["DQTR"].isin([1, 2, 3])
                        ),
                        ((pdf["YEAR"] == 2008) & pdf["DQTR"].isin([4]))
                        | (
                            (pdf["YEAR"] == 2009) & pdf["DQTR"].isin([1, 2, 3])
                        ),
                        ((pdf["YEAR"] == 2009) & pdf["DQTR"].isin([4]))
                        | (
                            (pdf["YEAR"] == 2010) & pdf["DQTR"].isin([1, 2, 3])
                        ),
                        ((pdf["YEAR"] == 2010) & pdf["DQTR"].isin([4]))
                        | (
                            (pdf["YEAR"] == 2011) & pdf["DQTR"].isin([1, 2, 3])
                        ),
                        ((pdf["YEAR"] == 2011) & pdf["DQTR"].isin([4]))
                        | ((pdf["YEAR"] >= 2012)),
                    ],
                    [23, 24, 25, 26, 27, 28, 29],
                    default=0,
                )
            )
        )
        return df

    @classmethod
    def add_pr_dxgrp_cols(cls, df):
        """
        New Column(s):

            DX{1-9}
            PR{1-6}
            ACDIASD{1-9}
            ACPGASD{1-9}
            ACSAPPD{1-9}
            ACSAP2D{1-9}
            ACDIALD{1-9}
            ACSASTD{1-9}
            RESPAN{1-9}
            ACCOPDD{1-9}
            ACCPD2D{1-9}
            ACSHYPD{1-9}
            ACSHY2D{1-9}
            CRENLFD{1-9}
            PHYSIDB{1-9}
            ACSCHFD{1-9}
            ACSCH2D{1-9}
            LIVEBND{1-9}
            V29D{1-9}
            LIVEB2D{1-9}
            ACSDEHD{1-9}
            ACSBACD{1-9}
            ACSBA2D{1-9}
            HYPERID{1-9}
            ACSUTID{1-9}
            IMMUNID{1-9}
            KIDNEY{1-9}
            ACSANGD{1-9}
            ACDIAUD{1-9}
            ACSLEAD{1-9}
            ACLEA2D{1-9}
            TOEAMIP{1-9}
            ACSCYFD{1-9}
            ACGDISD{1-9}
            ACBACGD{1-9}
            IMMUNHD{1-9}
            IMMUITD{1-9}
            HEPFA2D{1-9}
            HEPFA3D{1-9}
            ACSLBWD{1-9}
            ACSHYPP{1-6}
            ACSCARP{1-6}
            IMMUNIP{1-6}
            ACSLEAP{1-6}
            TRANSPP{1-6}
            DIABT2D{1-9}

        :param DataFrame df:
        :param str dx_col_suffix:
        :param str pr_col_suffix:
        :param str prsys_col_suffix:
        :return:
        """
        df_dxgrp = pd.read_csv(os.path.join(cls.data_folder, "dxgrp.csv"))[
            ["var_name", "lst_dx"]
        ]
        df_dxgrp["lst_dx"] = df_dxgrp.lst_dx.str.split(",").apply(tuple)
        df_prgrp = pd.read_csv(os.path.join(cls.data_folder, "prgrp.csv"))[
            ["var_name", "lst_pr"]
        ]
        df_prgrp["lst_pr"] = df_prgrp.lst_pr.str.split(",").apply(tuple)
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                **dict(
                    [
                        (
                            var + str(i),
                            pdf["DX" + str(i)]
                            .str.startswith(
                                df_dxgrp.loc[
                                    df_dxgrp.var_name == var, "lst_dx"
                                ].values[0]
                            )
                            .astype(int),
                        )
                        for var, i in product(
                            df_dxgrp.var_name.unique(), range(1, 10)
                        )
                    ]
                    + [
                        (
                            var + str(i),
                            pdf["PR" + str(i)]
                            .str.startswith(
                                df_prgrp.loc[
                                    df_prgrp.var_name == var, "lst_pr"
                                ].values[0]
                            )
                            .astype(int),
                        )
                        for var, i in product(
                            df_prgrp.var_name.unique(), range(1, 7)
                        )
                    ]
                )
            )
        )

        return df

    @classmethod
    def add_neonate_newborn_cols(cls, df):
        """
        New Column(s):

            NEONATE
            NEWBORN

        :param df:
        :return:
        """
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                NEONATE=(
                    pd.to_numeric(
                        pdf["ageday_admsn"], errors="coerce"
                    ).between(0, 28, inclusive=True)
                    | (
                        pdf["ageday_admsn"].isna()
                        & (pdf["age_admsn"] == 0)
                        & (
                            (
                                pd.to_numeric(
                                    pdf["RCPNT_DLVRY_CD"], errors="coerce"
                                )
                                == 2
                            )
                            | pdf[["V29D" + str(i) for i in range(1, 10)]].any(
                                axis=1
                            )
                            | pdf[
                                ["LIVEBND" + str(i) for i in range(1, 10)]
                            ].any(axis=1)
                        )
                    )
                ).astype(int)
            )
        )
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                NEWBORN=(
                    (pdf["NEONATE"] == 1)
                    & (
                        (
                            (
                                ~(
                                    pd.to_numeric(
                                        pdf["ageday_admsn"], errors="coerce"
                                    )
                                    > 0
                                )
                            )
                            & pdf[
                                ["LIVEBND" + str(i) for i in range(1, 10)]
                            ].any(axis=1)
                        )
                        | (
                            pdf[
                                ["LIVEB2D" + str(i) for i in range(1, 10)]
                            ].any(axis=1)
                            & (
                                pd.to_numeric(
                                    pdf["RCPNT_DLVRY_CD"], errors="coerce"
                                )
                                == 2
                            )
                            & (
                                pd.to_numeric(
                                    pdf["ageday_admsn"], errors="coerce"
                                )
                                == 0
                            )
                        )
                    )
                ).astype(int)
            )
        )
        return df

    @classmethod
    def add_area_level_indicator_cols(cls, df):
        """
        New Column(s):

            TAPD14: PEDIATRIC ASTHMA
            TAPD15 : DIABETES SHORT TERM COMPLICATION
            TAPD16: PEDIATRIC GASTROENTERITIS
            TAPD17: PERFORATED APPENDIX
            TAPD18: URINARY INFECTION
            TAPQ09:  LOW BIRTH WEIGHT
            TAPD90: OVERALL
            TAPD91: ACUTE
            TAPD92: CHRONIC

        :param df:
        :return:
        """
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                TAPD16=(
                    (pdf["ACPGASD1"] == 1)
                    | (  # PEDIATRIC GASTROENTERITIS
                        pdf[["ACPGASD" + str(i) for i in range(1, 10)]].any(
                            axis=1
                        )
                        & (pdf["ACSDEHD1"] == 1)
                    )
                ).astype(int),
                # PERFORATED APPENDIX
                TAPD17=pdf[["ACSAPPD" + str(i) for i in range(1, 10)]]
                .any(axis=1)
                .astype(int),
                # LOW BIRTH WEIGHT
                TAPQ09=pdf[["ACSLBWD" + str(i) for i in range(1, 10)]]
                .any(axis=1)
                .astype(int),
            )
        )
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                # PEDIATRIC ASTHMA
                # Exclude Cystic Fibrosis and Anomalies of the Respiratory System &
                # age < 2 years
                TAPD14=pdf["ACSASTD1"].where(  # PEDIATRIC ASTHMA
                    ~(
                        (pdf["age_admsn"].astype(float) < 2)
                        | pdf[["RESPAN" + str(i) for i in range(1, 10)]].any(
                            axis=1
                        )
                    ),
                    np.nan,
                ),
                # DIABETES SHORT TERM COMPLICATION
                # Exclude age < 6 years
                TAPD15=pdf["ACDIASD1"].where(
                    ~((pdf["age_admsn"].astype(float) < 6)), np.nan
                ),
                # PEDIATRIC GASTROENTERITIS
                # Exclude age <= 90 days & Gastrointestinal Abnormalities and Bacterial Gastroenteritis
                TAPD16=pdf["TAPD16"].where(
                    ~(
                        pdf["ageday_admsn"]
                        .astype(float)
                        .between(0, 90, inclusive=True)
                        | pdf[["ACGDISD" + str(i) for i in range(1, 10)]].any(
                            axis=1
                        )
                        | pdf[["ACBACGD" + str(i) for i in range(1, 10)]].any(
                            axis=1
                        )
                    ),
                    np.nan,
                ),
                TAPD17=pdf["TAPD17"].where(
                    ~(
                        pdf[["ACSAP2D" + str(i) for i in range(1, 10)]].any(
                            axis=1
                        )
                        | pdf["age_admsn"].astype(float)
                        < 1
                    ),
                    np.nan,
                ),
                # URINARY INFECTION
                # Exclude Kidney/Urinary Tract Disorder, High and Intermediate Risk Immunocompromised state, age <= 90 days
                TAPD18=pdf["ACSUTID1"].where(
                    ~(
                        pdf[["KIDNEY" + str(i) for i in range(1, 10)]].any(
                            axis=1
                        )
                        | (
                            pdf[
                                ["IMMUNHD" + str(i) for i in range(1, 10)]
                            ].any(axis=1)
                            | pdf[
                                ["TRANSPP" + str(i) for i in range(1, 7)]
                            ].any(axis=1)
                        )
                        | (
                            pdf[
                                ["IMMUITD" + str(i) for i in range(1, 10)]
                            ].any(axis=1)
                            | (
                                pdf[
                                    ["HEPFA2D" + str(i) for i in range(1, 10)]
                                ].any(axis=1)
                                & pdf[
                                    ["HEPFA3D" + str(i) for i in range(1, 10)]
                                ].any(axis=1)
                            )
                        )
                        | (
                            pdf["ageday_admsn"]
                            .astype(float)
                            .between(0, 90, inclusive=True)
                            | (
                                (pdf["ageday_admsn"].astype(float) < 0)
                                & (pdf["NEONATE"] == 1)
                            )
                        )
                    ),
                    np.nan,
                ),
                TAPQ09=pdf["TAPQ09"].where(~(pdf["NEWBORN"] == 1), np.nan),
            )
        )

        # CONSTRUCT AREA LEVEL COMPOSITE INDICATORS
        # OVERALL
        df["TAPD90"] = df[["TAPD14", "TAPD15", "TAPD16", "TAPD18"]].max(axis=1)
        df["TAPD90"] = df["TAPD90"].where(
            ~(df["age_admsn"].astype(float) >= 6), np.nan
        )
        # ACUTE
        df["TAPD91"] = df[["TAPD16", "TAPD18"]].max(axis=1)
        df["TAPD91"] = df["TAPD91"].where(
            ~(df["age_admsn"].astype(float) >= 6), np.nan
        )
        # CHRONIC
        df["TAPD92"] = df[["TAPD14", "TAPD15"]].max(axis=1)
        df["TAPD92"] = df["TAPD92"].where(
            ~(df["age_admsn"].astype(float) >= 6), np.nan
        )
        return df

    @classmethod
    def add_acsc_cat_cols(cls, df):
        """
        New Column(s):

            TAPQ01: DIABETES SHORT TERM COMPLICATION
            TAPQ02: PERFORATED APPENDIX
            TAPQ03: DIABETES LONG TERM COMPLICATION
            TAPQ05: COPD
            TAPQ07: HYPERTENSION
            TAPQ08: CONGESTIVE HEART FAILURE
            TAPQ10: DEHYDRATION
            TAPQ11: BACTERIAL PNEUMONIA
            TAPQ12: URINARY INFECTION
            TAPQ13: ANGINA
            TAPQ14: DIABETES UNCONTROLLED
            TAPQ15: ADULT ASTHMA
            TAPQ16: LOWER EXTREMITY AMPUTATION
            TAPQ90: OVERALL
            TAPQ91: ACUTE
            TAPQ92: CHRONIC

        :param df:
        :return:
        """
        ################################################################################################################
        #########################################   CONSTRUCT ACSC CATEGORIES   ########################################
        ################################################################################################################
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                TAPQ01=(pdf["ACDIASD1"] == 1).astype(
                    int
                ),  # DIABETES SHORT TERM COMPLICATION
                # PERFORATED APPENDIX
                TAPQ02=(
                    (
                        pdf[["ACSAP2D" + str(i) for i in range(1, 10)]].any(
                            axis=1
                        )
                        == 1
                    )
                    & (
                        pdf[["ACSAPPD" + str(i) for i in range(1, 10)]].any(
                            axis=1
                        )
                        == 1
                    )
                ).astype(int),
                TAPQ03=(pdf["ACDIALD1"] == 1).astype(
                    int
                ),  # DIABETES LONG TERM COMPLICATION
                TAPQ05=(
                    (pdf["ACCOPDD1"] == 1)
                    | (  # COPD
                        (pdf["ACCPD2D1"] == 1)
                        & (
                            pdf[
                                ["ACCOPDD" + str(i) for i in range(2, 10)]
                            ].any(axis=1)
                            == 1
                        )
                        | (pdf["ACSASTD1"] == 1)
                    )
                ).astype(int),
                TAPQ07=(pdf["ACSHYPD1"] == 1).astype(int),  # HYPERTENSION
                # CONGESTIVE HEART FAILURE
                TAPQ08=(
                    ((pdf["ICDVER"] <= 19) & (pdf["ACSCHFD1"] == 1))
                    | ((pdf["ICDVER"] >= 20) & (pdf["ACSCH2D1"] == 1))
                ).astype(int),
                TAPQ10=(
                    (pdf["ACSDEHD1"] == 1)
                    | (  # DEHYDRATION
                        (
                            pdf[
                                ["ACSDEHD" + str(i) for i in range(2, 10)]
                            ].any(axis=1)
                            == 1
                        )
                        & (
                            (pdf["HYPERID1"] == 1)
                            | (pdf["ACPGASD1"] == 1)
                            | (pdf["PHYSIDB1"] == 1)
                        )
                    )
                ).astype(int),
                TAPQ11=(pdf["ACSBACD1"] == 1).astype(
                    int
                ),  # BACTERIAL PNEUMONIA
                TAPQ12=(pdf["ACSUTID1"] == 1).astype(int),  # URINARY INFECTION
                TAPQ13=(pdf["ACSANGD1"] == 1).astype(int),  # ANGINA
                TAPQ14=(pdf["ACDIAUD1"] == 1).astype(
                    int
                ),  # DIABETES UNCONTROLLED
                TAPQ15=(pdf["ACSASTD1"] == 1).astype(int),  # ADULT ASTHMA
                # LOWER EXTREMITY AMPUTATION
                TAPQ16=(
                    (
                        pdf[["ACSLEAP" + str(i) for i in range(1, 7)]].any(
                            axis=1
                        )
                        == 1
                    )
                    & (
                        pdf[["ACSLEAD" + str(i) for i in range(1, 10)]].any(
                            axis=1
                        )
                        == 1
                    )
                ).astype(int),
                TAPQ16_T2D=(
                    (
                        pdf[["ACSLEAP" + str(i) for i in range(1, 7)]].any(
                            axis=1
                        )
                        == 1
                    )  # &
                    # (pdf[['DIABT2D' + str(i) for i in range(1, 10)]].any(axis=1) == 1)
                ).astype(int),
                TAPQ_T2D=(
                    pdf[["DIABT2D" + str(i) for i in range(1, 10)]].any(axis=1)
                    == 1
                ).astype(int),
            )
        )

        # Exclusions
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                TAPQ02=pdf["TAPQ02"].where(
                    ~(
                        (
                            pdf[
                                ["ACSAP2D" + str(i) for i in range(1, 10)]
                            ].any(axis=1)
                            != 1
                        )
                        | (pdf["MDC"] == 14)
                    ),
                    np.nan,
                ),
                TAPQ05=pdf["TAPQ05"].where(pdf["age_admsn"] >= 40, np.nan),
                # Exclude Stage I-IV Kidney Disease with dialysis access procedures & Cardiac Procedures
                TAPQ07=pdf["TAPQ07"].where(
                    ~(
                        (
                            (
                                pdf[
                                    ["ACSHY2D" + str(i) for i in range(1, 10)]
                                ].any(axis=1)
                                == 1
                            )
                            & (
                                pdf[
                                    ["ACSHYPP" + str(i) for i in range(1, 7)]
                                ].any(axis=1)
                                == 1
                            )
                        )
                        | (
                            pdf[["ACSCARP" + str(i) for i in range(1, 7)]].any(
                                axis=1
                            )
                            == 1
                        )
                    ),
                    np.nan,
                ),
                TAPQ08=pdf["TAPQ08"].where(  # Exclude Cardiac Procedures
                    pdf[["ACSCARP" + str(i) for i in range(1, 7)]].any(axis=1)
                    != 1,
                    np.nan,
                ),
                TAPQ10=pdf["TAPQ10"].where(  # Exclude chronic renal failure
                    (
                        pdf[["CRENLFD" + str(i) for i in range(1, 10)]].any(
                            axis=1
                        )
                        != 1
                    ),
                    np.nan,
                ),
                TAPQ11=pdf[
                    "TAPQ11"
                ].where(  # Exclude: Sickle Cell & Immunocompromised state
                    ~(
                        (
                            pdf[
                                ["ACSBA2D" + str(i) for i in range(1, 10)]
                            ].any(axis=1)
                            == 1
                        )
                        | (
                            pdf[
                                ["IMMUNID" + str(i) for i in range(1, 10)]
                            ].any(axis=1)
                            == 1
                        )
                        | (
                            pdf[["IMMUNIP" + str(i) for i in range(1, 7)]].any(
                                axis=1
                            )
                            == 1
                        )
                    ),
                    np.nan,
                ),
                TAPQ12=pdf[
                    "TAPQ12"
                ].where(  # Exclude Immunocompromised state & Kidney/Urinary Tract Disorder
                    ~(
                        (
                            pdf[
                                ["IMMUNID" + str(i) for i in range(1, 10)]
                            ].any(axis=1)
                            == 1
                        )
                        | (
                            pdf[["IMMUNIP" + str(i) for i in range(1, 7)]].any(
                                axis=1
                            )
                            == 1
                        )
                        | (
                            pdf[["KIDNEY" + str(i) for i in range(1, 10)]].any(
                                axis=1
                            )
                            == 1
                        )
                    ),
                    np.nan,
                ),
                TAPQ13=pdf["TAPQ13"].where(  # Exclude Cardiac Procedures
                    (
                        pdf[["ACSCARP" + str(i) for i in range(1, 7)]].any(
                            axis=1
                        )
                        != 1
                    ),
                    np.nan,
                ),
                TAPQ15=pdf[
                    "TAPQ15"
                ].where(  # Exclude Cystic Fibrosis and Anomalies of the Respiratory System
                    ~(
                        (
                            pdf[["RESPAN" + str(i) for i in range(1, 10)]].any(
                                axis=1
                            )
                            == 1
                        )
                        | (pdf["age_admsn"] >= 40)
                    ),
                    np.nan,
                ),
                TAPQ16=pdf["TAPQ16"].where(
                    ~(
                        (pdf["MDC"] == 14)
                        | (  # Exclude: MDC 14
                            pdf[
                                ["ACLEA2D" + str(i) for i in range(1, 10)]
                            ].any(axis=1)
                            == 1
                        )
                        | (  # Trauma
                            pdf[["TOEAMIP" + str(i) for i in range(1, 7)]].any(
                                axis=1
                            )
                            == 1
                        )
                    ),
                    np.nan,
                ),
                TAPQ16_T2D=pdf["TAPQ16_T2D"].where(
                    ~(
                        (pdf["MDC"] == 14)
                        | (  # Exclude: MDC 14
                            pdf[
                                ["ACLEA2D" + str(i) for i in range(1, 10)]
                            ].any(axis=1)
                            == 1
                        )
                        | (  # Trauma
                            pdf[["TOEAMIP" + str(i) for i in range(1, 7)]].any(
                                axis=1
                            )
                            == 1
                        )
                    ),
                    np.nan,
                ),
            )
        )

        ################################################################################################################
        #############################   CONSTRUCT AREA LEVEL COMPOSITE INDICATORS   ####################################
        ################################################################################################################
        # OVERALL
        df["TAPQ90"] = df[
            [
                "TAPQ01",
                "TAPQ03",
                "TAPQ05",
                "TAPQ07",
                "TAPQ08",
                "TAPQ10",
                "TAPQ11",
                "TAPQ12",
                "TAPQ13",
                "TAPQ14",
                "TAPQ15",
                "TAPQ16",
            ]
        ].max(axis=1)
        # ACUTE
        df["TAPQ91"] = df[["TAPQ10", "TAPQ11", "TAPQ12"]].max(axis=1)
        # CHRONIC
        df["TAPQ92"] = df[
            [
                "TAPQ01",
                "TAPQ03",
                "TAPQ05",
                "TAPQ07",
                "TAPQ08",
                "TAPQ13",
                "TAPQ14",
                "TAPQ15",
                "TAPQ16",
            ]
        ].max(axis=1)
        return df


def pqirecode(
    df,
    dx_col_suffix="DIAG_CD_",
    pr_col_suffix="PRCDR_CD_",
    prsys_col_suffix="PRCDR_CD_SYS_",
    logger_name="pqi",
    lst_keep_cols=None,
):
    """
    Adds PQI measures to IP claims. Columns added:

    * Adults

    ====================    =======================================
    PQI indicator column    Description
    ====================    =======================================
    TAPQ01                  DIABETES SHORT TERM COMPLICATION
    TAPQ02                  PERFORATED APPENDIX
    TAPQ03                  DIABETES LONG TERM COMPLICATION
    TAPQ05                  COPD
    TAPQ07                  HYPERTENSION
    TAPQ08                  CONGESTIVE HEART FAILURE
    TAPQ10                  DEHYDRATION
    TAPQ11                  BACTERIAL PNEUMONIA
    TAPQ12                  URINARY INFECTION
    TAPQ13                  ANGINA
    TAPQ14                  DIABETES UNCONTROLLED
    TAPQ15                  ADULT ASTHMA
    TAPQ16                  LOWER EXTREMITY AMPUTATION
    TAPQ90                  OVERALL
    TAPQ91                  ACUTE
    TAPQ92                  CHRONIC
    ====================    =======================================

    * Children

    ====================    =======================================
    PQI indicator column    Description
    ====================    =======================================
    TAPD14                  PEDIATRIC ASTHMA
    TAPD15                  DIABETES SHORT TERM COMPLICATION
    TAPD16                  PEDIATRIC GASTROENTERITIS
    TAPD17                  PERFORATED APPENDIX
    TAPD18                  URINARY INFECTION
    TAPQ09                  LOW BIRTH WEIGHT
    TAPD90                  OVERALL
    TAPD91                  ACUTE
    TAPD92                  CHRONIC
    ====================    =======================================

    Parameters
    ----------
    df : dask.DataFrame
        IP claims dataframe
    dx_col_suffix : str
        Diagnosis column names suffix
    pr_col_suffix : str
        Procedure code column names suffix
    prsys_col_suffix : str
         Procedure code system column names suffix
    logger_name : str
        Logger name
    lst_keep_cols : str
        List of columns in the claim that should be kept in the output dataframe

    Returns
    -------
    dask.DataFrame

    """
    PreventionQualityIndicators.logger_name = logger_name
    for col in (
        [
            "MSIS_ID",
            "admsn_date",
            "age_admsn",
            "RCPNT_DLVRY_CD",
            "DRG_REL_GROUP",
            "DRG_REL_GROUP_IND",
        ]
        + [dx_col_suffix + str(i) for i in range(1, 10)]
        + [prsys_col_suffix + str(i) for i in range(1, 7)]
        + [pr_col_suffix + str(i) for i in range(1, 7)]
    ):
        recipes.log_assert(
            col in df.columns,
            "Dataframe passed as input to pqirecode function must have {0}"
            " column".format(col),
            logger_name=PreventionQualityIndicators.logger_name,
        )
    df = PreventionQualityIndicators.prepare_cols(
        df, dx_col_suffix, pr_col_suffix, prsys_col_suffix
    ).persist()
    df = PreventionQualityIndicators.add_pr_dxgrp_cols(df).persist()
    df = PreventionQualityIndicators.add_drg_cols(df).persist()
    df = PreventionQualityIndicators.add_acsc_cat_cols(df).persist()
    df_children = copy(df)
    # df = df.loc[df['adult'].astype(float)==1]
    df_children = PreventionQualityIndicators.add_neonate_newborn_cols(
        df_children
    ).persist()
    df_children = PreventionQualityIndicators.add_area_level_indicator_cols(
        df_children
    ).persist()
    df = df.set_index("MSIS_ID", drop=False)
    df_children = df_children.set_index("MSIS_ID", drop=False)
    df = df[
        [
            "MSIS_ID",
            "admsn_date",
            "DRG",
            "DRGVER",
            "MDC",
            "YEAR",
            "DQTR",
            "age_admsn",
            "RCPNT_DLVRY_CD",
        ]
        + lst_keep_cols
        + ["DX" + str(i) for i in range(1, 10)]
        + ["PR" + str(i) for i in range(1, 7)]
        + [
            "TAPQ" + str(i).zfill(2)
            for i in range(1, 17)
            if i not in [4, 6, 9]
        ]
        + ["TAPQ9" + str(i) for i in range(3)]
        + ["TAPQ_T2D", "TAPQ16_T2D"]
    ].rename(
        columns={
            "MSIS_ID": "KEY",
            "age_admsn": "AGE",
            "admsn_date": "DATE",
            "year_of_admission": "YEAR",
        }
    )
    df_children = df_children[
        [
            "MSIS_ID",
            "admsn_date",
            "DRG",
            "DRGVER",
            "MDC",
            "YEAR",
            "DQTR",
            "age_admsn",
            "ageday_admsn",
            "RCPNT_DLVRY_CD",
        ]
        + lst_keep_cols
        + ["DX" + str(i) for i in range(1, 10)]
        + ["PR" + str(i) for i in range(1, 7)]
        + ["TAPQ09"]
        + ["TAPD1" + str(i) for i in range(4, 9)]
        + ["TAPD9" + str(i) for i in range(3)]
    ].rename(
        columns={
            "MSIS_ID": "KEY",
            "age_admsn": "AGE",
            "ageday_admsn": "AGEDAY",
            "admsn_date": "DATE",
            "year_of_admission": "YEAR",
        }
    )
    # https://www.cms.gov/icd10manual/fullcode_cms/P0372.html
    # 	MDC 14,Logic for ICD-9
    # 	Cesarean Section	765-766
    # 	Vaginal Delivery with O.R. Procedure	767-768
    # 	Postpartum and Post Abortion Diagnoses with O.R. Procedure
    # 	Abortion with D&C, Aspiration Curettage or Hysterotomy
    df = df.loc[
        ~(
            (df["AGE"] < 18)
            & ((df["MDC"] != 14) | (df["RCPNT_DLVRY_CD"].astype(float) == 0))
        ),
        df.columns,
    ].map_partitions(
        lambda pdf: pdf.reset_index(drop=False)
        .sort_values(by=["KEY"] + [col for col in pdf.columns if col != "KEY"])
        .drop_duplicates()
        .set_index("MSIS_ID", drop=False)
    )
    df = dataframe_utils.fix_index(df, "MSIS_ID", drop_column=False)
    df_children = df_children.loc[
        ~(
            (df_children["AGE"].astype(float) < 0)
            | (df_children["AGE"].astype(float) > 17)
            | (df_children["MDC"] == 14)
        )
    ].map_partitions(
        lambda pdf: pdf.reset_index(drop=False)
        .sort_values(by=["KEY"] + [col for col in pdf.columns if col != "KEY"])
        .drop_duplicates()
        .set_index("MSIS_ID", drop=False)
    )
    df_children = dataframe_utils.fix_index(
        df_children, "MSIS_ID", drop_column=False
    )
    return df, df_children

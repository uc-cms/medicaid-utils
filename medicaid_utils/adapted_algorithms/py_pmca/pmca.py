#!/usr/bin/env python

"""This program implements the Pediatric Medical Complexity Algorithm (Tamara D. Simon, Dorothy Lyons, Peter Woodcox et
 al 2014) to identify children with complex and non-complex chronic conditions using Medicaid claims data and to
 distinguish them from children with neither chronic nor chronic complex conditions (healthy children). ‘Complex’ and
 ‘Non-Complex’ designations are assigned based on whether a child’s condition(s) identified by ICD-9 code(s) can be
 considered chronic, malignant, or progressive, and whether multiple body systems are involved.
"""
__author__ = "Manoradhan Murugesan"
__email__ = "manorathan@uchicago.edu"

import pandas as pd
import os
import numpy as np


class PediatricMedicalComplexity:
    """This class packages functions to create indicator variables for conditions of interest based on their chronic
    nature identified by Seattle Children's Research Institute subset from conditions defined by diagnostic codes
    outlined in the Chronic Illness and Disability Payment System (CDPS) version 5.3"""

    package_folder, filename = os.path.split(__file__)
    data_folder = os.path.join(package_folder, "data")

    @classmethod
    def create_pmca_condition_counts(cls, df, diag_cd_lst_col):
        pdf_conditions = pd.read_csv(
            os.path.join(cls.data_folder, "pmca_condition_codes.csv")
        )
        pdf_progressives = pd.read_csv(
            os.path.join(cls.data_folder, "progressive_condition_codes.csv")
        )

        dct_conditions = pdf_conditions.set_index("condition").to_dict("index")
        dct_progressives = pdf_progressives.set_index("condition").to_dict(
            "index"
        )

        for condn in dct_conditions.keys():
            dct_condn_codes = dct_conditions[condn]
            dct_condn_codes["exclude_icd9"] = (
                ()
                if pd.isnull(dct_condn_codes["exclude_icd9"])
                else tuple(dct_condn_codes["exclude_icd9"].split(","))
            )
            dct_condn_codes["include_icd9"] = tuple(
                dct_condn_codes["include_icd9"].split(",")
            )
            dct_conditions[condn] = dct_condn_codes

        tpl_progressive_include_codes = tuple(
            ",".join(
                [
                    cond["include_icd9"]
                    for key, cond in dct_progressives.items()
                    if pd.notnull(cond["include_icd9"])
                ]
            ).split(",")
        )
        tpl_progressive_exclude_codes = tuple(
            ",".join(
                [
                    cond["exclude_icd9"]
                    for key, cond in dct_progressives.items()
                    if pd.notnull(cond["exclude_icd9"])
                ]
            ).split(",")
        )

        lst_conditions_with_exclude_codes = [
            condn
            for condn in dct_conditions.keys()
            if len(dct_conditions[condn]["exclude_icd9"]) > 0
        ]
        lst_conditions_without_exclude_codes = [
            condn
            for condn in dct_conditions.keys()
            if condn not in lst_conditions_with_exclude_codes
        ]
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                **dict(
                    [
                        (
                            condn,
                            pdf[diag_cd_lst_col].apply(
                                lambda lst_cd: sum(
                                    [
                                        int(
                                            cd.startswith(
                                                dct_conditions[condn][
                                                    "include_icd9"
                                                ]
                                            )
                                        )
                                        for cd in lst_cd.split(",")
                                    ]
                                )
                            ),
                        )
                        for condn in lst_conditions_without_exclude_codes
                    ]
                    + [
                        (
                            condn,
                            pdf[diag_cd_lst_col].apply(
                                lambda lst_cd: sum(
                                    [
                                        int(
                                            cd.startswith(
                                                dct_conditions[condn][
                                                    "include_icd9"
                                                ]
                                            )
                                            & (
                                                not cd.startswith(
                                                    dct_conditions[condn][
                                                        "exclude_icd9"
                                                    ]
                                                )
                                            )
                                        )
                                        for cd in lst_cd.split(",")
                                    ]
                                )
                            ),
                        )
                        for condn in lst_conditions_with_exclude_codes
                    ]
                    + [
                        (
                            "progressive",
                            pdf[diag_cd_lst_col].apply(
                                lambda lst_cd: sum(
                                    [
                                        int(
                                            cd.startswith(
                                                tpl_progressive_include_codes
                                            )
                                            & (
                                                not cd.startswith(
                                                    tpl_progressive_exclude_codes
                                                )
                                            )
                                        )
                                        for cd in lst_cd.split(",")
                                    ]
                                )
                            ),
                        )
                    ]
                )
            )
        )

        df = df.map_partitions(
            lambda pdf: pdf.assign(
                **dict(
                    [
                        ("any_" + condn, (pdf[condn] > 0).astype(int))
                        for condn in list(dct_conditions.keys())
                        + ["progressive"]
                    ]
                    + [
                        (condn + "_2h", (pdf[condn] >= 2).astype(int))
                        for condn in dct_conditions.keys()
                    ]
                )
            )
        )

        return df

    @classmethod
    def get_pmca_chronic_condition_categories(cls, df):
        pdf_conditions = pd.read_csv(
            os.path.join(cls.data_folder, "pmca_condition_codes.csv")
        )
        lst_conditions = [
            condn
            for condn in set(pdf_conditions["condition"].tolist())
            if condn not in ["malign"]
        ]

        df = df.map_partitions(
            lambda pdf: pdf.assign(
                scount_less=pdf[
                    ["any_" + condn for condn in lst_conditions]
                ].sum(axis="columns"),
                scount_more=pdf[
                    [condn + "_2h" for condn in lst_conditions]
                ].sum(axis="columns"),
            )
        )

        df = df.map_partitions(
            lambda pdf: pdf.assign(
                cond_less=np.select(
                    [
                        (
                            (pdf["scount_less"] >= 2)
                            | (pdf["any_progressive"] == 1)
                            | (pdf["any_malign"] == 1)
                        ),  # Complex Chronic
                        (pdf["scount_less"] == 1),
                    ],  # Non-complex Chronic
                    [3, 2],
                    default=1,
                ),
                cond_more=np.select(
                    [
                        (
                            (pdf["scount_more"] >= 2)
                            | (pdf["any_progressive"] == 1)
                            | (pdf["any_malign"] == 1)
                        ),  # Complex Chronic
                        (pdf["scount_less"] == 1),
                    ],  # Non-complex Chronic
                    [3, 2],
                    default=1,
                ),
            )
        )

        return df


def pmca_chronic_conditions(df, diag_cd_lst_col="LST_DIAG_CD_RAW"):
    """
    This function implements the Pediatric Medical Complexity Algorithm to identify children with complex and
    non-complex chronic conditions using Medicaid claims data and to distinguish them from children with neither
    chronic nor chronic complex conditions (healthy children).

    'Complex' and 'Non-Complex' designations are assigned based on whether a child's condition(s) identified by ICD-9
    code(s) can be considered chronic, malignant, or progressive, and whether multiple body systems are involved.

    Definitions of the categories assigned by the Algorithm:

    * The less conservative version (cond_less) calculates values as
        * 'Complex Chronic':
            1) more than one body system is involved, or
            2) one or more conditions are progressive, or
            3) one or more conditions are malignant
        * 'Non-complex Chronic':
            1) only one body system is involved, and
            2) the condition is not progressive or malignant
        * 'Non-Chronic':
            1) no body system indicators are present, and
            2) the condition is not progressive or malignant
    * The more conservative version (cond_more) calculates values as
        * 'Complex Chronic':
            1) more than one body system is involved, and each must be indicated in more than one claim, or
            2) one or more conditions are progressive, or
            3) one or more conditions are malignant
        * 'Non-complex Chronic':
            1) only one body system is indicated in more than one claim, and
            2) the condition is not progressive or malignant
        * 'Non-Chronic':
            1) no body system indicators are present in more than one claim, and
            2) the condition is not progressive or malignant

    Body Systems of interest and the variables used to indicate them:

        =======================   =========
        Body system                variable
        =======================   =========
        cardiac                    cardiac
        craniofacial               cranio
        dermatological             derm
        endocrinological           endo
        gastrointestinal           gastro
        genetic                    genetic
        genitourinary              genito
        hematological              hemato
        immunological              immuno
        malignancy                 malign
        mental health              mh
        metabolic                  metab
        musculoskeletal            musculo
        neurological               neuro
        pulmonary-respiratory      pulresp
        renal                      renal
        ophthalmological           opthal
        otologic                   otol
        otolaryngological          otolar
        =======================   =========

    Parameters
    ----------
    df : dask.DataFrame
        Patient level dask dataframe
    diag_cd_lst_col : str, default=LST_DIAG_CD_RAW
        Column name that has comma separated list of diagnosis codes in the observation period

    Returns
    -------
    dask.DataFrame

    """
    lst_columns = list(df.columns)
    df = PediatricMedicalComplexity.create_pmca_condition_counts(
        df, diag_cd_lst_col
    )
    df = PediatricMedicalComplexity.get_pmca_chronic_condition_categories(df)
    df = df.rename(
        columns=dict(
            [(col, "pmca_" + col) for col in ["cond_less", "cond_more"]]
        )
    )
    df = df[
        lst_columns
        + ["pmca_" + metric for metric in ["cond_less", "cond_more"]]
    ]

    return df

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Python implementation of The New York University (NYU) Emergency Department (ED) visit algorithm.
    This algorithm is the most widely used tool for retrospectively assessing the probability
    that ED visits are urgent, preventable, or optimally treated in an ED,
    using administrative data (Billings, Parikh, and Mijanovich 2000b; Feldman 2010).
"""
__author__ = "Manoradhan Murugesan"
__email__ = "manorathan@uchicago.edu"

import re
import os
from ast import literal_eval
import pandas as pd
import dask.dataframe as dd


class BillingsED:
    """This class packages functions to perform NYU/ Billings algorithm based classification of ED visits"""

    logger_name = __name__
    package_folder, filename = os.path.split(__file__)
    data_folder = os.path.join(package_folder, "data")

    # Recoding diagnosis code lookups
    pdf_recode = pd.read_csv(
        os.path.join(data_folder, "recode.csv"),
        dtype={
            "origin": "str",
            "target": "str",
            "comments": "str",
            "updates": "str",
            "deleted": "int",
            "starts_with": "int",
        },
    )

    pdf_recode = pdf_recode.loc[
        pdf_recode["deleted"] != 1,
    ]
    pdf_recode = pdf_recode.assign(
        origin=pdf_recode.origin.apply(literal_eval)
    )

    pdf_startswith_recode = pdf_recode.loc[pdf_recode["starts_with"] == 1]
    pdf_startswith_recode = (
        pdf_startswith_recode[["origin", "target"]]
        .explode("origin")
        .drop_duplicates(["origin"], keep="first")
    )

    pdf_nonstartswith_recode = pdf_recode.loc[pdf_recode["starts_with"] == 0][
        ["origin", "target"]
    ].explode("origin")
    pdf_nonstartswith_recode = pdf_nonstartswith_recode.drop_duplicates(
        ["origin"], keep="first"
    )
    dct_recode_non_startswith = pdf_nonstartswith_recode.set_index(
        "origin"
    ).to_dict()["target"]

    # Special categories lookups
    lst_special_ed_categories = ["acs", "psych", "drug", "alcohol", "injury"]
    # Load recode csv file that has information for
    # recoding principal diagnosis
    # Load category data files to a dictionary
    dct_category_dx_codes = {}
    for cat in lst_special_ed_categories:
        dct_category_dx_codes[cat] = (
            pd.read_csv(
                os.path.join(data_folder, cat + "_check.csv"),
                dtype={
                    "start_index": "int",
                    "end_index": "float",
                    "code": "str",
                    "comments": "str",
                },
            )
            .apply(
                lambda row: re.sub(
                    r"[^a-zA-Z0-9]+",
                    "",
                    row.code[
                        : (int(row.end_index) + 1)
                        if pd.notnull(row.end_index)
                        else len(row.code)
                    ].upper(),
                ),
                axis=1,
            )
            .tolist()
        )

    # Probabilities lookup
    df_eddxs = pd.read_sas(os.path.join(data_folder, "eddxs.sas7bdat"))
    df_eddxs = df_eddxs.assign(
        prindx=df_eddxs.prindx.str.decode("utf-8")
        .str.strip()
        .str.upper()
        .str.replace("[^a-zA-Z0-9]+", "", regex=True)
    )
    df_eddxs = df_eddxs.drop_duplicates(["prindx"], keep="first")

    @classmethod
    def recode_diag_code(cls, dx_code: str) -> str:
        """
        Recodes diagnosis code

        Parameters
        ----------
        dx_code : str
            Diagnosis code

        Returns
        -------
        str

        """
        return cls.dct_recode_non_startswith.get(
            dx_code,
            next(
                (
                    row.target
                    for idx, row in cls.pdf_startswith_recode.iterrows()
                    if dx_code.startswith(row.origin)
                ),
                dx_code,
            ),
        )

    @classmethod
    def get_special_categories(cls, dx_code: str) -> dict:
        """
        Categorizes the diagnosis code into below categories

        ===============   ==============================================
        category_code     description
        ===============   ==============================================
          injury          Injury
          psych           Mental Health Related
          alcohol         Alcohol Related
          drug            Drug Related (excluding alcohol)
          acs
        ===============   ==============================================

        Parameters
        ----------
        dx_code : str
            Diagnosis code

        Returns
        -------
        dict

        """
        return {
            cat: int(dx_code.startswith(tuple(cls.dct_category_dx_codes[cat])))
            for cat in cls.lst_special_ed_categories
        }

    @classmethod
    def get_nyu_ed_proba_for_dx_code(cls, dx_code) -> dict:
        """
        Merge with EDDXs gets the probbolities attached with the code

        ============   =====================================================
        ed_category    description
        ============   =====================================================
        nonemerg       Non-Emergent
        emergpc        Emergent, Primary Care Treatable
        emedpa         Emergent, ED Care Needed, Preventable/Avoidable
        emednpa        Emergent, ED Care Needed, Not Preventable/Avoidable
        ============   =====================================================

        Parameters
        ----------
        dx_code : str
            Diagnosis code

        Returns
        -------
        dict

        """
        return (
            cls.df_eddxs.set_index("prindx")
            .to_dict(orient="index")
            .get(dx_code, {})
        )

    @classmethod
    def get_nyu_ed_categories(
        cls, dx_code: str
    ) -> (int, int, int, int, int, int, float, float, float, float):
        """
        Returns probabilities for each of the NYU ED categories, based on the input diagnosis code

        Parameters
        ----------
        dx_code : str
            Diagnosis code

        Returns
        -------
        unclassified : int
            The code did not meet any NYU/Billings category
        injury : int
            Injury
        drug : int
            Drug Related (excluding alcohol)
        psych : int
            Mental Health Related
        alcohol : int
            Alcohol Related
        peds_acs_ed : int
            Pediatric ambuilatory care sensitive ED visit
        ne : float
            Non-Emergent probability
        epct : float
            Emergent, Primary Care Treatable probability
        edcnpa : float
            Emergent, ED Care Needed, Preventable/Avoidable
        edcnnpa : float
            Emergent, ED Care Needed, Not Preventable/Avoidable

        """
        peds_acsed = cls.is_peds_acsed(dx_code)
        dx_code = cls.recode_diag_code(dx_code)
        dct_special_cat = cls.get_special_categories(dx_code)
        dct_nyu_ed_proba = cls.get_nyu_ed_proba_for_dx_code(dx_code)
        unclassified = int(
            not (
                any(
                    dct_special_cat[cat]
                    for cat in ["injury", "drug", "psych", "alcohol"]
                )
                or bool(dct_nyu_ed_proba)
            )
        )
        # ne = dct_nyu_ed_proba.get("nonemerg", 0)
        # epct = dct_nyu_ed_proba.get("emergpc", 0)
        ne = int(
            not any(
                dct_special_cat[cat]
                for cat in ["injury", "drug", "psych", "alcohol"]
            )
        ) * dct_nyu_ed_proba.get("nonemerg", 0)
        epct = int(
            not any(
                dct_special_cat[cat]
                for cat in ["injury", "drug", "psych", "alcohol"]
            )
        ) * dct_nyu_ed_proba.get("emergpc", 0)
        edcnpa = dct_special_cat["acs"] * sum(
            [
                dct_nyu_ed_proba.get("emedpa", 0),
                dct_nyu_ed_proba.get("emednpa", 0),
            ]
        )
        edcnnpa = int(dct_special_cat["acs"] != 1) * sum(
            [
                dct_nyu_ed_proba.get("emedpa", 0),
                dct_nyu_ed_proba.get("emednpa", 0),
            ]
        )

        edcnpa = (
            int(
                not any(
                    dct_special_cat[cat]
                    for cat in ["injury", "drug", "psych", "alcohol"]
                )
            )
            * edcnpa
        )
        edcnnpa = (
            int(
                not any(
                    dct_special_cat[cat]
                    for cat in ["injury", "drug", "psych", "alcohol"]
                )
            )
            * edcnnpa
        )

        peds_acsed = max(peds_acsed, sum([ne, epct, edcnpa]))

        return (
            unclassified,
            dct_special_cat["injury"],
            dct_special_cat["drug"],
            dct_special_cat["psych"],
            dct_special_cat["alcohol"],
            peds_acsed,
            ne,
            epct,
            edcnpa,
            edcnnpa,
        )

    @classmethod
    def is_peds_acsed(cls, dx_code: str) -> int:
        """
        Checks if the diagnosis code meets pediatric ACS ED visit criteria

        Parameters
        ----------
        dx_code : str
            Diagnosis code

        Returns
        -------
        int

        """
        return int(
            dx_code.startswith(
                (
                    "493",
                    "079",
                    "480",
                    "487",
                    "780",
                    "381",
                    "382",
                    "384",
                    "385",
                    "471",
                    "472",
                    "477",
                    "690",
                    "691",
                    "692",
                    "693",
                    "695",
                    "V03",
                    "V04",
                    "V05",
                    "V06",
                    "V07",
                    "V20",
                    "V67",
                    "V68",
                    "V69",
                    "V70",
                    "840",
                    "841",
                    "842",
                    "843",
                    "844",
                    "845",
                    "846",
                    "847",
                    "848",
                    "910",
                    "911",
                    "913",
                    "914",
                    "915",
                    "916",
                    "917",
                    "918",
                    "919",
                    "923",
                    "924",
                    "955",
                    "956",
                )
            )
        )


def get_nyu_ed_proba(
    df: dd.DataFrame, date_col: str, index_col: str, cms_format: str = "MAX"
) -> pd.DataFrame:
    # pylint: disable=missing-param-doc
    """
    This functions returns probabilities for dxgroup codes in the input dataframe being urgent, preventable, or
    optimally treated ED visits. This function recodes the passed dx codes to a set of dx codes used in (Billings,
    Parikh, and Mijanovich 2000b; Feldman 2010) study, adds columns (‘injury’, ‘psych’, ‘alcohol’ and ‘drug’),
    indicating the type of ED service.

    Parameters
    ----------
    df : dask.DataFrame
        dataframe with dx codes
    date_col : str
        Date column name
    index_col : str
        Index column name
    cms_format : {'MAX', 'TAF'}
        CMS file format

    Returns
    -------
    pd.DataFrame

    """
    principal_diag_col_name = (
        "DIAG_CD_1" if (cms_format == "MAX") else "DGNS_CD_1"
    )
    df = df.map_partitions(
        lambda pdf: pd.concat(
            [
                pdf,
                pd.DataFrame(
                    pdf[principal_diag_col_name]
                    .apply(BillingsED.get_nyu_ed_categories)
                    .tolist(),
                    columns=[
                        "unclassified",
                        "injury",
                        "drug",
                        "psych",
                        "alcohol",
                        "peds_acs_ed",
                        "ne",
                        "epct",
                        "edcnpa",
                        "edcnnpa",
                    ],
                    index=pdf.index,
                ),
            ],
            axis=1,
        )
    )
    df = df.map_partitions(
        lambda pdf: pdf.groupby([index_col, date_col]).agg(
            dict(
                [
                    (col, "max")
                    for col in ["injury", "drug", "psych", "alcohol", "adult"]
                ]
                + [
                    (col, "mean")
                    for col in ["ne", "epct", "edcnpa", "edcnnpa"]
                ]
                + [(col, "min") for col in ["peds_acs_ed"]]
            )
        )
    )

    df = df.assign(
        peds_acs_ed_visit=df["adult"]
        .where(df["adult"] == 0, (df["peds_acs_ed"] > 0.75).astype(int))
        .astype(int),
        non_emergent_visit=(df[["ne", "epct"]].sum(axis=1) > 0.5).astype(int),
        emergent_visit=(df[["edcnpa", "edcnnpa"]].sum(axis=1) > 0.5).astype(
            int
        ),
        non_emergent_or_pct_visit=(
            df[["ne", "epct", "edcnpa"]].sum(axis=1) > 0.5
        ).astype(int),
        indeterminate_ed_visit=(
            (df[["ne", "epct"]].sum(axis=1) == 0.5)
            | (df[["edcnpa", "edcnnpa"]].sum(axis=1) == 0.5)
        ).astype(int),
    )
    df = (
        df.groupby(index_col)
        .agg(
            {
                col: "sum"
                for col in [
                    "injury",
                    "drug",
                    "psych",
                    "alcohol",
                    "peds_acs_ed_visit",
                    "non_emergent_visit",
                    "emergent_visit",
                    "non_emergent_or_pct_visit",
                    "indeterminate_ed_visit",
                ]
            }
        )
        .compute()
    )
    return df

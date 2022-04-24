import os
from typing import List
import pandas as pd
import dask.dataframe as dd
from itertools import product
import numpy as np
import logging
import itertools

data_folder = os.path.join(os.path.dirname(__file__), "data")


def get_patient_ids_with_conditions(
    dct_diag_codes: dict,
    dct_procedure_codes: dict,
    logger_name=__file__,
    **dct_claims,
) -> pd.DataFrame():
    """
    Gets patient ids with conditions denoted by provided diagnosis codes or procedure codes
    :param dct_diag_codes:
    :param dct_procedure_codes:
    :param logger_name:
    :param dct_claims:
    :return:
    """
    logger = logging.getLogger(logger_name)
    pdf_patient_ids = pd.DataFrame()
    index_col = None
    for claim_type in dct_claims:
        lst_col = ["proc_condn", "diag_condn"]
        df = flag_diagnoses_and_procedures(
            dct_diag_codes, dct_procedure_codes, dct_claims[claim_type].copy()
        )
        if df is not None:
            df["diag_condn"] = 0
            df["proc_condn"] = 0
            if (index_col is not None) and (index_col != df.index.name):
                raise Exception(
                    "Passed claims files do not have the same index"
                )
            index_col = df.index.name
            if bool(dct_diag_codes):
                df = df.assign(
                    diag_condn=df[
                        [f"diag_{condn}" for condn in dct_diag_codes]
                    ]
                    .any(axis=1)
                    .astype(int)
                )
                lst_col.extend([f"diag_{condn}" for condn in dct_diag_codes])
            if bool(dct_procedure_codes):
                df = df.assign(
                    proc_condn=df[
                        [f"proc_{proc}" for proc in dct_procedure_codes]
                    ]
                    .any(axis=1)
                    .astype(int)
                )
                lst_col.extend(
                    [f"proc_{proc}" for proc in dct_procedure_codes]
                )
            df = df.loc[df[lst_col].any(axis=1)][lst_col + ["service_date"]]
            logger.info(
                f"Restricting {claim_type} to condition diagnoses/ procedures"
                f" reduces the claim count to {df.shape[0].compute()}"
            )
            df = df.assign(
                **dict(
                    [
                        (
                            f"{col}_date",
                            df["service_date"].where(df[col] == 1, np.nan),
                        )
                        for col in lst_col
                    ]
                )
            )
            df = df.drop(["service_date"], axis=1)
            df = df.map_partitions(
                lambda pdf: pdf.assign(
                    **dict(
                        [
                            (
                                f"{col}_date",
                                pdf.groupby(pdf.index)[
                                    f"{col}_date"
                                ].transform("min"),
                            )
                            for col in lst_col
                        ]
                    )
                )
            )
            df = df.groupby(index_col).max().compute().reset_index(drop=False)
            df = df.rename(
                columns=dict(
                    [
                        (col, f"{claim_type}_{col}")
                        for col in df.columns
                        if col != index_col
                    ]
                )
            )
            pdf_patient_ids = pd.concat(
                [pdf_patient_ids, df.copy()], ignore_index=True
            )
            logger.info(f"Finished processing {claim_type} claims")
    if pdf_patient_ids.shape[0] > 0:
        pdf_patient_ids = pdf_patient_ids.groupby(index_col).max()
        pdf_patient_ids = pdf_patient_ids.assign(
            **dict(
                [
                    (col, pdf_patient_ids[col].fillna(0).astype(int))
                    for col in pdf_patient_ids.columns
                    if not col.endswith("_date")
                ]
            )
        )
    return pdf_patient_ids


def flag_diagnoses_and_procedures(
    dct_diag_codes: dict,
    dct_proc_codes: dict,
    df_claims: dd.DataFrame,
    cms_format: str = "MAX",
    lst_claim_diag_col: List[str] = None,
) -> dd.DataFrame:
    """


    Parameters
    ----------
    dct_diag_codes
    dct_proc_codes
    df_claims
    cms_format
    lst_claim_diag_col

    Returns
    -------

    """
    # Validate procedure codes
    dct_invalid_proc_codes = dict(
        filter(
            lambda elem: len(
                [
                    code
                    for code in itertools.chain(*elem[1].values())
                    if not code.isalnum()
                ]
            ),
            dct_proc_codes.items(),
        )
    )
    dct_invalid_diag_codes = dict(
        filter(
            lambda elem: len(
                [
                    code
                    for code in itertools.chain(
                        *[
                            list(itertools.chain(*x.values()))
                            for x in elem[1].values()
                        ]
                    )
                    if not code.isalnum()
                ]
            ),
            dct_diag_codes.items(),
        )
    )
    if bool(dct_invalid_proc_codes) or bool(dct_invalid_diag_codes):
        raise ValueError(
            f"{','.join(list(dct_invalid_proc_codes.keys()) + list(dct_invalid_diag_codes.keys()))}"
            f"have codes with non-alphanumeric values"
        )
    if df_claims is not None:
        lst_diag_col = (
            lst_claim_diag_col
            if bool(lst_claim_diag_col)
            else (
                [
                    col
                    for col in df_claims.columns
                    if col.startswith("DIAG_CD_")
                ]
                if (cms_format == "MAX")
                else [
                    col
                    for col in df_claims.columns
                    if col.startswith("DGNS_CD_") or (col == "ADMTG_DGNS_CD")
                ]
            )
        )
        lst_proc_col = (
            [
                col
                for col in df_claims.columns
                if col.startswith("PRCDR_CD")
                and (not col.startswith("PRCDR_CD_SYS"))
            ]
            if (cms_format == "MAX")
            else [
                col
                for col in df_claims.columns
                if (
                    col.startswith("PRCDR_CD")
                    or col.startswith("LINE_PRCDR_CD")
                )
                and (
                    not (
                        col.startswith("PRCDR_CD_SYS")
                        or col.startswith("PRCDR_CD_DT")
                        or col.startswith("LINE_PRCDR_CD_SYS")
                        or col.startswith("LINE_PRCDR_CD_DT")
                    )
                )
            ]
        )
        if bool(dct_diag_codes) and bool(lst_diag_col):
            if any("DGNS_VRSN_" in colname for colname in df_claims.columns):
                df_claims = df_claims.map_partitions(
                    lambda pdf: pdf.assign(
                        **dict(
                            [
                                (
                                    f"valid_icd_{ver + 8}_{col}",
                                    pdf[col].where(
                                        pd.isnull(
                                            pd.to_numeric(
                                                pdf[
                                                    col.replace(
                                                        "DGNS_CD",
                                                        "DGNS_VRSN_CD",
                                                    )
                                                ],
                                                errors="coerce",
                                            )
                                        )
                                        | pd.to_numeric(
                                            pdf[
                                                col.replace(
                                                    "DGNS_CD", "DGNS_VRSN_CD"
                                                )
                                            ],
                                            errors="coerce",
                                        ).isin([ver, 3]),
                                        "",
                                    ),
                                )
                                for ver, col in product([1, 2], lst_diag_col)
                            ]
                        )
                    )
                )
            else:
                df_claims = df_claims.map_partitions(
                    lambda pdf: pdf.assign(
                        **dict(
                            [
                                (f"valid_icd_9_{col}", pdf[col])
                                for col in lst_diag_col
                            ]
                            + [
                                (f"valid_icd_10_{col}", "")
                                for col in lst_diag_col
                            ]
                        )
                    )
                )
            lst_icd9_diag_col = [f"valid_icd_9_{col}" for col in lst_diag_col]
            lst_icd10_diag_col = [
                f"valid_icd_10_{col}" for col in lst_diag_col
            ]
            lst_incl_excl_condn = [
                condn
                for condn in dct_diag_codes
                if (
                    ("excl" in dct_diag_codes[condn])
                    and (
                        bool(dct_diag_codes[condn]["excl"][9])
                        or bool(dct_diag_codes[condn]["excl"][10])
                    )
                )
                and (
                    ("incl" in dct_diag_codes[condn])
                    and (
                        bool(dct_diag_codes[condn]["incl"][9])
                        or bool(dct_diag_codes[condn]["incl"][10])
                    )
                )
            ]
            lst_incl_condn = [
                condn
                for condn in dct_diag_codes
                if (
                    ("excl" not in dct_diag_codes[condn])
                    or (
                        not (
                            bool(dct_diag_codes[condn]["excl"][9])
                            or bool(dct_diag_codes[condn]["excl"][10])
                        )
                    )
                )
                and (
                    ("incl" in dct_diag_codes[condn])
                    and (
                        bool(dct_diag_codes[condn]["incl"][9])
                        or bool(dct_diag_codes[condn]["incl"][10])
                    )
                )
            ]
            lst_excl_condn = [
                condn
                for condn in dct_diag_codes
                if (
                    ("excl" in dct_diag_codes[condn])
                    and (
                        bool(dct_diag_codes[condn]["excl"][9])
                        or bool(dct_diag_codes[condn]["excl"][10])
                    )
                )
                and (
                    ("incl" not in dct_diag_codes[condn])
                    or (
                        not (
                            bool(dct_diag_codes[condn]["incl"][9])
                            or bool(dct_diag_codes[condn]["incl"][10])
                        )
                    )
                )
            ]

            df_claims = df_claims.map_partitions(
                lambda pdf: pdf.assign(
                    **dict(
                        [
                            (
                                f"diag_{condn}",
                                np.column_stack(
                                    [
                                        pdf[col].str.startswith(
                                            tuple(
                                                [
                                                    str(dx_code)
                                                    for dx_code in dct_diag_codes[
                                                        condn
                                                    ][
                                                        "incl"
                                                    ][
                                                        9
                                                    ]
                                                ]
                                            ),
                                            na=False,
                                        )
                                        & (
                                            ~pdf[col].str.startswith(
                                                tuple(
                                                    [
                                                        str(dx_code)
                                                        for dx_code in dct_diag_codes[
                                                            condn
                                                        ][
                                                            "excl"
                                                        ][
                                                            9
                                                        ]
                                                    ]
                                                ),
                                                na=False,
                                            )
                                        )
                                        for col in lst_icd9_diag_col
                                    ]
                                    + [
                                        pdf[col].str.startswith(
                                            tuple(
                                                [
                                                    str(dx_code)
                                                    for dx_code in dct_diag_codes[
                                                        condn
                                                    ][
                                                        "incl"
                                                    ][
                                                        10
                                                    ]
                                                ]
                                            ),
                                            na=False,
                                        )
                                        & (
                                            ~pdf[col].str.startswith(
                                                tuple(
                                                    [
                                                        str(dx_code)
                                                        for dx_code in dct_diag_codes[
                                                            condn
                                                        ][
                                                            "excl"
                                                        ][
                                                            10
                                                        ]
                                                    ]
                                                ),
                                                na=False,
                                            )
                                        )
                                        for col in lst_icd10_diag_col
                                    ]
                                )
                                .any(axis=1)
                                .astype(int),
                            )
                            for condn in lst_incl_excl_condn
                        ]
                        + [
                            (
                                f"diag_{condn}",
                                np.column_stack(
                                    [
                                        pdf[col].str.startswith(
                                            tuple(
                                                [
                                                    str(dx_code)
                                                    for dx_code in dct_diag_codes[
                                                        condn
                                                    ][
                                                        "incl"
                                                    ][
                                                        9
                                                    ]
                                                ]
                                            ),
                                            na=False,
                                        )
                                        for col in lst_icd9_diag_col
                                    ]
                                    + [
                                        pdf[col].str.startswith(
                                            tuple(
                                                [
                                                    str(dx_code)
                                                    for dx_code in dct_diag_codes[
                                                        condn
                                                    ][
                                                        "incl"
                                                    ][
                                                        10
                                                    ]
                                                ]
                                            ),
                                            na=False,
                                        )
                                        for col in lst_icd10_diag_col
                                    ]
                                )
                                .any(axis=1)
                                .astype(int),
                            )
                            for condn in lst_incl_condn
                        ]
                        + [
                            (
                                f"diag_{condn}",
                                np.column_stack(
                                    [
                                        (
                                            ~pdf[col].str.startswith(
                                                tuple(
                                                    [
                                                        str(dx_code)
                                                        for dx_code in dct_diag_codes[
                                                            condn
                                                        ][
                                                            "excl"
                                                        ][
                                                            9
                                                        ]
                                                    ]
                                                ),
                                                na=False,
                                            )
                                        )
                                        for col in lst_icd9_diag_col
                                    ]
                                    + [
                                        (
                                            ~pdf[col].str.startswith(
                                                tuple(
                                                    [
                                                        str(dx_code)
                                                        for dx_code in dct_diag_codes[
                                                            condn
                                                        ][
                                                            "excl"
                                                        ][
                                                            10
                                                        ]
                                                    ]
                                                ),
                                                na=False,
                                            )
                                        )
                                        for col in lst_icd10_diag_col
                                    ]
                                )
                                .any(axis=1)
                                .astype(int),
                            )
                            for condn in lst_excl_condn
                        ]
                    )
                )
            )

        if bool(dct_proc_codes) and bool(lst_proc_col):
            lst_sys_code = list(
                set(
                    [
                        int(sys_code)
                        for lst_sys_code in [
                            list(dct_proc_codes[proc].keys())
                            for proc in dct_proc_codes.keys()
                        ]
                        for sys_code in lst_sys_code
                        if int(sys_code) != 1
                    ]
                )
            )
            df_claims = df_claims.map_partitions(
                lambda pdf: pdf.assign(
                    **dict(
                        [
                            (f"VALID_PRCDR_SYS_1_{proc_col}", pdf[proc_col])
                            for proc_col in lst_proc_col
                        ]
                        + [
                            (
                                f"VALID_PRCDR_SYS_{sys_code}_{proc_col}",
                                pdf[proc_col].where(
                                    pd.isnull(
                                        pd.to_numeric(
                                            pdf[
                                                f"{proc_col.replace('PRCDR_CD', 'PRCDR_CD_SYS')}"
                                            ],
                                            errors="coerce",
                                        )
                                    )
                                    | pd.to_numeric(
                                        pdf[
                                            f"{proc_col.replace('PRCDR_CD', 'PRCDR_CD_SYS')}"
                                        ],
                                        errors="coerce",
                                    ).isin([sys_code, 99, 88]),
                                    "",
                                ),
                            )
                            for sys_code, proc_col in product(
                                lst_sys_code, lst_proc_col
                            )
                        ]
                    )
                )
            )
            df_claims = df_claims.map_partitions(
                lambda pdf: pdf.assign(
                    **dict(
                        [
                            (
                                f"proc_{proc}_{sys_code}",
                                np.column_stack(
                                    [
                                        pdf[col].str.startswith(
                                            tuple(
                                                dct_proc_codes[proc][sys_code]
                                            ),
                                            na=False,
                                        )
                                        for col in pdf.columns
                                        if col.startswith(
                                            (f"VALID_PRCDR_SYS_{sys_code}_",)
                                        )
                                    ]
                                )
                                .any(axis=1)
                                .astype(int),
                            )
                            for sublist in [
                                product(
                                    [proc], list(dct_proc_codes[proc].keys())
                                )
                                for proc in dct_proc_codes
                            ]
                            for proc, sys_code in sublist
                        ]
                    )
                )
            )

            df_claims = df_claims.assign(
                **dict(
                    [
                        (
                            f"proc_{proc}",
                            df_claims[
                                [
                                    col
                                    for col in df_claims.columns
                                    if col.startswith(f"proc_{proc}_")
                                ]
                            ]
                            .any(axis=1)
                            .astype(int),
                        )
                        for proc in dct_proc_codes.keys()
                    ]
                )
            )
            df_claims = df_claims[
                [
                    col
                    for col in df_claims.columns
                    if col
                    not in [
                        item
                        for subitem in [
                            [
                                col
                                for col in df_claims.columns
                                if col.startswith(f"proc_{proc}_")
                            ]
                            for proc in dct_proc_codes.keys()
                        ]
                        for item in subitem
                    ]
                    + [
                        col
                        for col in df_claims.columns
                        if col.startswith("VALID_PRCDR_SYS_")
                    ]
                    + [
                        f"valid_icd_{ver}_{col}"
                        for ver, col in product([9, 10], lst_diag_col)
                    ]
                ]
            ]
    return df_claims

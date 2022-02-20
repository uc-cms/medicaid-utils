#!/usr/bin/env python

"""This module generates low value care measures as in
Charlesworth CJ, Meath THA, Schwartz AL, McConnell KJ. Low-value care in Medicaid and
commercially insured populations: a comprehensive single-state analysis. JAMA Intern Med.
Published online May 31, 2016. doi:10.1001/jamainternmed.2016.2086.
"""

__author__ = "Manoradhan Murugesan"
__email__ = "manorathan@uchicago.edu"

import pandas as pd
import os
import numpy as np
from itertools import product

from medicaid_utils.filters.claims import dx_and_proc
from medicaid_utils.preprocessing import max_ot, max_ip, max_file


class LowValueCare:
    """This class packages functions to create indicator variables for low value care services"""

    package_folder, filename = os.path.split(__file__)
    data_folder = os.path.join(package_folder, "data")

    @classmethod
    def normalize_condition_names(cls, pdf):
        pdf = pdf.assign(
            **dict(
                [
                    (
                        col,
                        pdf[col]
                        .str.replace("\W", " ", regex=True)
                        .str.replace("\s+", " ", regex=True)
                        .str.lower()
                        .str.replace(" ", "_"),
                    )
                    for col in ["description", "measure"]
                ]
            )
        )
        return pdf

    @classmethod
    def get_diag_proc_specs(cls, pdf, prefix):

        dct_diag_codes = dict(
            [
                (
                    f"{prefix}_{condn}",
                    {
                        "incl": {
                            9: pdf.loc[
                                (pdf["description"] == condn)
                                & (pdf["include"].astype(int) == 1)
                                & (pdf["except"].astype(int) == 0)
                                & pdf["icd_code"].notna()
                            ]["icd_code"].tolist(),
                            10: [],
                        },
                        "excl": {
                            9: pdf.loc[
                                (pdf["description"] == condn)
                                & (pdf["include"].astype(int) == 1)
                                & (pdf["except"].astype(int) == 1)
                                & pdf["icd_code"].notna()
                            ]["icd_code"].tolist(),
                            10: [],
                        },
                    },
                )
                for condn in pdf.loc[
                    pdf["icd_code"].notna() & (pdf["include"].astype(int) == 1)
                ].description.unique()
            ]
        )
        dct_excl_diag_codes = dict(
            [
                (
                    f"{prefix}_excl_{condn}",
                    {
                        "incl": {
                            9: pdf.loc[
                                (pdf["description"] == condn)
                                & (pdf["include"].astype(int) == 0)
                                & (pdf["except"].astype(int) == 0)
                                & pdf["icd_code"].notna()
                            ]["icd_code"].tolist(),
                            10: [],
                        },
                        "excl": {
                            9: pdf.loc[
                                (pdf["description"] == condn)
                                & (pdf["include"].astype(int) == 0)
                                & (pdf["except"].astype(int) == 1)
                                & pdf["icd_code"].notna()
                            ]["icd_code"].tolist(),
                            10: [],
                        },
                    },
                )
                for condn in pdf.loc[
                    pdf["icd_code"].notna() & (pdf["include"].astype(int) == 0)
                ].description.unique()
            ]
        )

        pdf_proc = pdf.loc[pdf.proc_sys.notna()]
        pdf_proc = pdf_proc.assign(proc_sys=pdf_proc["proc_sys"].astype(int))

        dct_proc_codes = dict(
            [
                (
                    f"{prefix}_{proc}",
                    pdf_proc.loc[pdf_proc["description"] == proc][
                        ["proc_sys", "proc_code"]
                    ]
                    .groupby("proc_sys")
                    .agg(list)
                    .to_dict()["proc_code"],
                )
                for proc in pdf_proc.loc[
                    pdf_proc["proc_code"].notna()
                    & (pdf_proc["include"].astype(int) == 1)
                ].description.unique()
            ]
        )

        dct_excl_proc_codes = dict(
            [
                (
                    f"{prefix}_excl_{proc}",
                    pdf_proc.loc[pdf_proc["description"] == proc][
                        ["proc_sys", "proc_code"]
                    ]
                    .groupby("proc_sys")
                    .agg(list)
                    .to_dict()["proc_code"],
                )
                for proc in pdf_proc.loc[
                    pdf_proc["proc_code"].notna()
                    & (pdf_proc["include"].astype(int) == 0)
                ].description.unique()
            ]
        )

        return (
            dct_diag_codes,
            dct_excl_diag_codes,
            dct_proc_codes,
            dct_excl_proc_codes,
        )

    @classmethod
    def construct_low_value_care_measures(
        cls, dct_msr_spec, dct_denom_spec, pdf_dates
    ):
        for idx, row in pdf_dates.iterrows():
            for service, measure in dct_msr_spec:
                dct_service = dct_msr_spec[(service, measure)]
                lst_denom_condns = []
                lst_denom_with_msr = []
                lst_msr_dates = (
                    row[f"msr_{service}_all_dates"]
                    if (
                        (dct_service["exclude_ip"] != 1)
                        & (dct_service["exclude_ed"] != 1)
                    )
                    else (
                        row[f"msr_{service}_ip_dates"]
                        + row[f"msr_{service}_ot_dates"]
                        if (dct_service["exclude_ip"] != 1)
                        else (row[f"msr_{service}_ot_dates"])
                    )
                )
                for denom_condn in dct_service["denom_description"]:
                    lst_denom_dates = (
                        row[f"denom_{denom_condn}_all_dates"]
                        if (
                            (
                                (
                                    dct_denom_spec[(measure, denom_condn)][
                                        "exclude_ip"
                                    ]
                                    != 1
                                )
                                & (
                                    dct_denom_spec[(measure, denom_condn)][
                                        "exclude_ed"
                                    ]
                                    != 1
                                )
                            )
                        )
                        else (
                            (
                                row[f"denom_{denom_condn}_ip_dates"]
                                + row[f"denom_{denom_condn}_ot_dates"]
                            )
                            if (
                                dct_denom_spec[(measure, denom_condn)][
                                    "exclude_ip"
                                ]
                                != 1
                            )
                            else (row[f"denom_{denom_condn}_ot_dates"])
                        )
                    )
                    if bool(lst_denom_dates):
                        lst_denom_condns.append(denom_condn)
                    prior_gap = (
                        dct_denom_spec[(measure, denom_condn)]["prior_days"]
                        if pd.notnull(
                            dct_denom_spec[(measure, denom_condn)][
                                "prior_days"
                            ]
                        )
                        else dct_denom_spec[(measure, denom_condn)][
                            "prior_months"
                        ]
                        * 30
                    )
                    if pd.isnull(prior_gap):
                        prior_gap = dct_service["prior_months"] * 30
                    followup_gap = (
                        dct_denom_spec[(measure, denom_condn)]["followup_days"]
                        if pd.notnull(
                            dct_denom_spec[(measure, denom_condn)][
                                "followup_days"
                            ]
                        )
                        else dct_denom_spec[(measure, denom_condn)][
                            "followup_months"
                        ]
                        * 30
                    )
                    if pd.isnull(followup_gap):
                        followup_gap = dct_service["followup_months"] * 30

                    gap = (
                        prior_gap
                        if not (pd.isnull(prior_gap))
                        else (-1 * followup_gap)
                    )

                    offset = (
                        dct_denom_spec[(measure, denom_condn)]["offset_days"]
                        if pd.notnull(
                            dct_denom_spec[(measure, denom_condn)][
                                "offset_days"
                            ]
                        )
                        else dct_denom_spec[(measure, denom_condn)][
                            "offset_months"
                        ]
                        * 30
                    )
                    if pd.isnull(offset):
                        offset = 0
                    if pd.notnull(gap) & (gap < 0):
                        offset = -1 * offset

                    lst_denom_gaps = [
                        ((y - x).days, x, y)
                        for x, y in product(lst_denom_dates, lst_msr_dates)
                    ]

                    if pd.notnull(gap):
                        if gap < 0:
                            lst_matches = [
                                x
                                for x in lst_denom_gaps
                                if (offset >= x[0] >= gap)
                            ]
                            if bool(lst_matches):
                                lst_denom_with_msr.append(
                                    (denom_condn, lst_matches[0])
                                )
                        if gap > 0:
                            lst_matches = [
                                x
                                for x in lst_denom_gaps
                                if (offset >= x[0] >= gap)
                            ]
                            if bool(lst_matches):
                                lst_denom_with_msr.append(
                                    (denom_condn, lst_matches[0])
                                )
                    else:
                        if bool(lst_denom_dates) & bool(lst_msr_dates):
                            lst_denom_with_msr.append(
                                (denom_condn, lst_denom_gaps[-1])
                            )
                if bool(lst_denom_condns) & (
                    not any(
                        condn
                        for condn in lst_denom_condns
                        if condn.startswith("excl_")
                    )
                ):
                    pdf_dates.loc[idx, f"pop_denom_{measure}"] = 1
                if bool(lst_denom_with_msr) & (
                    not any(
                        condn
                        for condn in lst_denom_with_msr
                        if condn[0].startswith("excl_")
                    )
                ):
                    pdf_dates.at[
                        idx, f"service_with_denom_{service}_{measure}"
                    ] = 1
                    pdf_dates.at[
                        idx, f"service_with_denom_dates_{service}_{measure}"
                    ] = lst_denom_with_msr[0][1][1]
        return pdf_dates

    @classmethod
    def get_diag_proc_code_spec(cls, pdf_denom_spec, pdf_measure_spec):
        (
            dct_denom_diag_codes,
            dct_denom_excl_diag_codes,
            dct_denom_proc_codes,
            dct_denom_excl_proc_codes,
        ) = LowValueCare.get_diag_proc_specs(pdf_denom_spec, "denom")

        (
            dct_msr_diag_codes,
            dct_msr_excl_diag_codes,
            dct_msr_proc_codes,
            dct_msr_excl_proc_codes,
        ) = LowValueCare.get_diag_proc_specs(pdf_measure_spec, "msr")

        dct_diag_codes = {
            **dct_denom_diag_codes,
            **dct_denom_excl_diag_codes,
            **dct_msr_diag_codes,
            **dct_msr_excl_diag_codes,
        }
        dct_proc_codes = {
            **dct_denom_proc_codes,
            **dct_denom_excl_proc_codes,
            **dct_msr_proc_codes,
            **dct_msr_excl_proc_codes,
        }
        return dct_diag_codes, dct_proc_codes

    @classmethod
    def generate_condn_indicators(
        cls,
        st,
        year,
        pdf_denom_spec,
        pdf_measure_spec,
        max_data_root,
        lst_bene_id_filter,
        tmp_folder,
    ):
        dct_diag_codes, dct_proc_codes = cls.get_diag_proc_code_spec(
            pdf_denom_spec, pdf_measure_spec
        )
        for year in range(year - 1, year + 1):
            ip_claim = max_ip.MAXIP(
                year, st, max_data_root, clean=False, preprocess=False
            )
            if bool(lst_bene_id_filter):
                ip_claim.df = ip_claim.df.map_partitions(
                    lambda pdf: pdf.loc[pdf.index.isin(lst_bene_id_filter)]
                )
            ip_claim.clean()
            ip_claim.preprocess()
            df_ip = ip_claim.df.compute()
            ip_claim.df = dx_and_proc.flag_diagnoses_and_procedures(
                dct_diag_codes, dct_proc_codes, ip_claim.df
            )

            ip_claim.df = ip_claim.df.loc[
                ip_claim.df[
                    ["diag_" + diag for diag in dct_diag_codes]
                    + ["proc_" + diag for diag in dct_proc_codes]
                ].any(axis=1)
            ]

            ip_claim.df.compute().to_parquet(
                os.path.join(tmp_folder, "ip", f"{st}_{year}.parquet"),
                engine="fastparquet",
                compression="snappy",
                index=True,
            )
            del ip_claim
            ot_claim = max_ot.MAXOT(
                year,
                st,
                max_data_root,
                clean=False,
                preprocess=False,
                tmp_folder=os.path.join(tmp_folder, "ot"),
            )
            ot_claim.df = ot_claim.df.map_partitions(
                lambda pdf: pdf.loc[pdf.index.isin(lst_bene_id_filter)]
            )
            ot_claim.clean()
            ot_claim.preprocess()
            ot_claim.flag_ip_overlaps_and_ed(df_ip)
            ot_claim.df = dx_and_proc.flag_diagnoses_and_procedures(
                dct_diag_codes, dct_proc_codes, ot_claim.df
            )
            ot_claim.df = ot_claim.df.loc[
                ot_claim.df[
                    ["diag_" + diag for diag in dct_diag_codes]
                    + ["proc_" + diag for diag in dct_proc_codes]
                ].any(axis=1)
            ]
            df_ot = ot_claim.df.compute()
            df_ot.to_parquet(
                os.path.join(tmp_folder, "ot", f"{st}_{year}.parquet"),
                engine="fastparquet",
                compression="snappy",
                write_index=True,
            )

    @classmethod
    def get_dates(cls, st, year, lst_condn, tmp_folder):
        pdf_dates = None
        for ftype in ["ip", "ot"]:
            pdf = pd.read_parquet(
                os.path.join(tmp_folder, ftype, f"{st}_{year}.parquet"),
                engine="fastparquet",
            )
            pdf = pdf.rename(
                columns=dict(
                    [
                        (col, col.replace("diag_", "").replace("proc_", ""))
                        for col in pdf.columns
                        if col.startswith(
                            (
                                "diag_",
                                "proc_",
                            )
                        )
                    ]
                )
            )
            pdf = pdf.assign(
                **dict(
                    [
                        (col, pdf[col.replace("_excl_chronic_", "_")])
                        for col in [
                            "denom_excl_chronic_sinusitis",
                            "denom_excl_chronic_acquired_hypothyroidism",
                            "denom_excl_chronic_low_back_pain",
                            "denom_excl_chronic_plantar_fasciitis",
                        ]
                    ]
                )
            )
            if "msr_total_or_free_t4" in pdf.columns:
                pdf = pdf.assign(
                    msr_total_or_free_t3=pdf[
                        ["msr_total_or_free_t4", "msr_total_or_free_t3"]
                    ].any(axis=1)
                )
            pdf = pdf.assign(
                **dict(
                    [
                        (
                            f"{col}_{ftype}_dates",
                            pdf[
                                "prncpl_proc_date"
                                if ftype == "ip"
                                else "srvc_bgn_date"
                            ].where(pdf[col] == 1, np.nan),
                        )
                        for col in lst_condn
                    ]
                )
            )
            if ftype == "ot":
                pdf = pdf.assign(
                    **dict(
                        [
                            (
                                f"{col}_ed_dates",
                                pdf["srvc_bgn_date"].where(
                                    (pdf[col] == 1) & (pdf["any_ed"] == 1),
                                    np.nan,
                                ),
                            )
                            for col in lst_condn
                        ]
                    )
                )
            pdf = pdf.groupby(pdf.index, dropna=True).agg(
                dict(
                    [
                        (
                            f"{col}_{ftype}_dates",
                            lambda x: [y for y in x if pd.notna(y)],
                        )
                        for col in lst_condn
                    ]
                    + [
                        (
                            f"{col}_ed_dates",
                            lambda x: [y for y in x if pd.notna(y)],
                        )
                        for col in lst_condn
                        if f"{col}_ed_dates" in pdf.columns
                    ]
                )
            )
            pdf_dates = (
                pdf
                if pdf_dates is None
                else pdf.merge(
                    pdf_dates, left_index=True, right_index=True, how="outer"
                )
            )
        pdf_dates = pdf_dates.assign(
            **dict(
                [
                    (
                        col + "_all_dates",
                        pdf_dates[f"{col}_ip_dates"].apply(
                            lambda x: x if not (x is np.nan) else []
                        )
                        + pdf_dates[f"{col}_ot_dates"].apply(
                            lambda x: x if not (x is np.nan) else []
                        ),
                    )
                    for col in lst_condn
                ]
            )
        )
        return pdf_dates

    @classmethod
    def combine_dates_in_claims(cls, st, year, lst_condn):
        pdf_dates = cls.get_dates(st, year, lst_condn)
        pdf_prior_dates = cls.get_dates(
            st,
            year - 1,
            [condn for condn in lst_condn if not condn.startswith("msr_")],
        )
        pdf_dates = pd.concat([pdf_dates, pdf_prior_dates])
        pdf_dates = pdf_dates.groupby(pdf_dates.index).agg(
            dict(
                [
                    (
                        col,
                        lambda x: sum([y for y in x if (not y is np.nan)], []),
                    )
                    for col in pdf_dates.columns
                ]
            )
        )
        return pdf_dates


def construct_low_value_care_measures(
    st, year, lst_bene_id_filter, max_data_root, tmp_folder
):
    pdf_denom_spec = pd.read_excel(
        os.path.join(LowValueCare.data_folder, "low_value_care.xlsx"),
        sheet_name="denominator",
        dtype=str,
        engine="openpyxl",
    )
    pdf_measure_spec = pd.read_excel(
        os.path.join(LowValueCare.data_folder, "low_value_care.xlsx"),
        sheet_name="measures",
        dtype=str,
        engine="openpyxl",
    )

    pdf_denom_spec = LowValueCare.normalize_condition_names(pdf_denom_spec)
    pdf_measure_spec = LowValueCare.normalize_condition_names(pdf_measure_spec)

    LowValueCare.generate_condn_indicators(
        st,
        year,
        pdf_denom_spec,
        pdf_measure_spec,
        max_data_root,
        lst_bene_id_filter,
        tmp_folder,
    )

    pdf_denom_spec = pdf_denom_spec.assign(
        description=pdf_denom_spec["description"].where(
            pdf_denom_spec["include"].astype(int) == 1,
            "excl_" + pdf_denom_spec["description"],
        )
    )

    pdf_denom = pdf_denom_spec.drop(
        ["icd_code", "proc_code", "proc_sys", "include", "except"], axis=1
    ).drop_duplicates(keep="first")
    pdf_measure = pdf_measure_spec.drop(
        ["icd_code", "proc_code", "proc_sys", "include", "except"], axis=1
    ).drop_duplicates(keep="first")
    pdf_measure = pdf_measure.assign(
        **dict(
            [
                (col, pd.to_numeric(pdf_measure[col], errors="coerce"))
                for col in [
                    "exclude_ip",
                    "exclude_ed",
                    "prior_months",
                    "followup_months",
                    "min_enrolled_months",
                ]
            ]
        )
    )
    pdf_denom = pdf_denom.assign(
        **dict(
            [
                (col, pd.to_numeric(pdf_denom[col], errors="coerce"))
                for col in [
                    "exclude_ip",
                    "exclude_ed",
                    "prior_months",
                    "offset_months",
                    "followup_months",
                    "followup_days",
                    "prior_days",
                    "offset_days",
                ]
            ]
        )
    )

    pdf_combined_spec = pdf_measure.merge(
        pdf_denom.rename(
            columns=dict(
                [
                    (col, "denom_" + col)
                    for col in pdf_denom.columns
                    if col != "measure"
                ]
            )
        ),
        on="measure",
        how="outer",
    )

    lst_condn = list(
        set(
            [
                condn
                for condn in [
                    "msr_" + condn
                    for condn in pdf_combined_spec["description"]
                ]
                + [
                    "denom_" + condn
                    for condn in pdf_combined_spec["denom_description"]
                ]
            ]
        )
    )

    pdf_dates = LowValueCare.combine_dates_in_claims(st, year, lst_condn)
    dct_measures = (
        pdf_combined_spec.groupby("description")
        .agg(
            dict(
                [
                    ("measure", lambda x: x.values[0]),
                    ("exclude_ip", lambda x: x.values[0]),
                    ("exclude_ed", lambda x: x.values[0]),
                    ("followup_months", lambda x: x.values[0]),
                    ("prior_months", lambda x: x.values[0]),
                    ("min_enrolled_months", lambda x: x.values[0]),
                    ("denom_description", list),
                ]
            )
        )
        .to_dict(orient="index")
    )
    dct_denom = pdf_denom.set_index(["measure", "description"]).to_dict(
        orient="index"
    )

    pdf_dates = LowValueCare.construct_low_value_care_measures(
        dct_measures, dct_denom, pdf_dates
    )

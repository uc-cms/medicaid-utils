#!/usr/bin/env python

"""This module generates low value care measures as in
Charlesworth CJ, Meath THA, Schwartz AL, McConnell KJ. Low-value care in Medicaid and
commercially insured populations: a comprehensive single-state analysis. JAMA Intern Med.
Published online May 31, 2016. doi:10.1001/jamainternmed.2016.2086.
"""

__author__ = "Manoradhan Murugesan"
__email__ = "manorathan@uchicago.edu"

import os
from typing import List, Tuple
import math
import json
from itertools import product

import pandas as pd
import numpy as np


from medicaid_utils.filters.claims import dx_and_proc
from medicaid_utils.preprocessing import max_ot, max_ip, max_ps


class LowValueCare:
    """This class packages functions to create indicator variables for low value care services"""

    package_folder, filename = os.path.split(__file__)
    data_folder = os.path.join(package_folder, "data")

    @classmethod
    def normalize_condition_names(cls, pdf_spec: pd.DataFrame) -> pd.DataFrame:
        """
        Normalizers condition names so they can be used as column names

        Parameters
        ----------
        pdf_spec : pd.DataFrame
            Diagnostic/ procedure code specifications dataframe

        Returns
        -------
        pd.DataFrame
            Dataframe with conditions names normalized

        """
        pdf_spec = pdf_spec.assign(
            **{
                col: pdf_spec[col]
                .str.replace(r"\W", " ", regex=True)
                .str.replace(r"\s+", " ", regex=True)
                .str.lower()
                .str.replace(" ", "_")
                for col in ["description", "measure"]
            }
        )
        return pdf_spec

    @classmethod
    def get_diag_proc_specs(
        cls, pdf_spec: pd.DataFrame, prefix: str
    ) -> (dict, dict, dict, dict):
        # pylint: disable=missing-param-doc
        """
        Returns DX & procedure code specs as dictionary

        Parameters
        ----------
        pdf_spec : pd.DataFrame
            Diagnostic/ procedure code specifications dataframe
        prefix : {'denom', 'msr'}
            Prefix to denote condition type

        Returns
        -------
        dct_diag_codes : dict
            Diagnosis codes dictionary
        dct_excl_diag_codes : dict
            Excluded Diagnosis codes dictionary
        dct_proc_codes : dict
            Procedure codes dictionary
        dct_excl_proc_codes : dict
            Excluded procedure codes dictionary

        """
        dct_diag_codes = {
            f"{prefix}_{condn}": {
                "incl": {
                    9: pdf_spec.loc[
                        (pdf_spec["description"] == condn)
                        & (pdf_spec["include"].astype(int) == 1)
                        & (pdf_spec["except"].astype(int) == 0)
                        & pdf_spec["icd_code"].notna()
                    ]["icd_code"].tolist(),
                    10: [],
                },
                "excl": {
                    9: pdf_spec.loc[
                        (pdf_spec["description"] == condn)
                        & (pdf_spec["include"].astype(int) == 1)
                        & (pdf_spec["except"].astype(int) == 1)
                        & pdf_spec["icd_code"].notna()
                    ]["icd_code"].tolist(),
                    10: [],
                },
            }
            for condn in pdf_spec.loc[
                pdf_spec["icd_code"].notna()
                & (pdf_spec["include"].astype(int) == 1)
            ].description.unique()
        }

        dct_excl_diag_codes = {
            f"{prefix}_{condn}": {
                "incl": {
                    9: pdf_spec.loc[
                        (pdf_spec["description"] == condn)
                        & (pdf_spec["include"].astype(int) == 0)
                        & (pdf_spec["except"].astype(int) == 0)
                        & pdf_spec["icd_code"].notna()
                    ]["icd_code"].tolist(),
                    10: [],
                },
                "excl": {
                    9: pdf_spec.loc[
                        (pdf_spec["description"] == condn)
                        & (pdf_spec["include"].astype(int) == 0)
                        & (pdf_spec["except"].astype(int) == 1)
                        & pdf_spec["icd_code"].notna()
                    ]["icd_code"].tolist(),
                    10: [],
                },
            }
            for condn in pdf_spec.loc[
                pdf_spec["icd_code"].notna()
                & (pdf_spec["include"].astype(int) == 0)
            ].description.unique()
        }

        pdf_proc = pdf_spec.loc[pdf_spec.proc_sys.notna()]
        pdf_proc = pdf_proc.assign(proc_sys=pdf_proc["proc_sys"].astype(int))

        dct_proc_codes = {
            f"{prefix}_{proc}": pdf_proc.loc[pdf_proc["description"] == proc][
                ["proc_sys", "proc_code"]
            ]
            .groupby("proc_sys")
            .agg(list)
            .to_dict()["proc_code"]
            for proc in pdf_proc.loc[
                pdf_proc["proc_code"].notna()
                & (pdf_proc["include"].astype(int) == 1)
            ].description.unique()
        }

        dct_excl_proc_codes = {
            f"{prefix}_{proc}": pdf_proc.loc[pdf_proc["description"] == proc][
                ["proc_sys", "proc_code"]
            ]
            .groupby("proc_sys")
            .agg(list)
            .to_dict()["proc_code"]
            for proc in pdf_proc.loc[
                pdf_proc["proc_code"].notna()
                & (pdf_proc["include"].astype(int) == 0)
            ].description.unique()
        }

        return (
            dct_diag_codes,
            dct_excl_diag_codes,
            dct_proc_codes,
            dct_excl_proc_codes,
        )

    @classmethod
    def construct_low_value_care_measures(
        cls,
        pdf_dates: pd.DataFrame,
        year: int,
        dct_msr_spec: dict,
        dct_denom_spec: dict,
    ) -> pd.DataFrame:
        """
        Constructs low value care measures

        Parameters
        ----------
        pdf_dates : pd.DataFrame
            Dataframe with condition/ measure date lists and eligibility info
        year : int
            Year
        dct_msr_spec
            Measure spec dictionary
        dct_denom_spec
            Denomination spec dictionary

        Returns
        -------
        pd.DataFrame

        """
        for (  # pylint: disable=too-many-nested-blocks
            idx,
            row,
        ) in pdf_dates.iterrows():
            for service, measure in dct_msr_spec:
                dct_service = dct_msr_spec[(service, measure)]
                dct_denom_pop = {}
                dct_denom_pop_msr = {}
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
                # srvc_prior_gap = dct_service["prior_months"] * 30
                # srvc_followup_gap = dct_service["followup_months"] * 30
                # min_enrolled_months = dct_service["min_enrolled_months"]
                for denom_condn in dct_service["denom_description"]:
                    index_date = pd.to_datetime("", errors="coerce")
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
                        if (
                            f"excl_chronic_{denom_condn}"
                            in dct_service["denom_description"]
                        ):
                            lst_denom_dates = [
                                min(
                                    (
                                        x
                                        for x in lst_denom_dates
                                        if x.year == year
                                    ),
                                    default=pd.to_datetime(
                                        "", errors="coerce"
                                    ),
                                )
                            ]
                        elif denom_condn.startswith("excl_chronic") and (
                            f"{denom_condn.replace('excl_chronic_', '')}"
                            in dct_service["denom_description"]
                        ):
                            index_date = min(
                                (x for x in lst_denom_dates if x.year == year),
                                default=pd.to_datetime("", errors="coerce"),
                            )
                            lst_denom_dates = [
                                x for x in lst_denom_dates if x < index_date
                            ]
                        lst_denom_dates = [
                            x for x in lst_denom_dates if pd.notnull(x)
                        ]
                        if bool(lst_denom_dates):
                            denom_prior_gap = (
                                dct_denom_spec[(measure, denom_condn)][
                                    "prior_days"
                                ]
                                if pd.notnull(
                                    dct_denom_spec[(measure, denom_condn)][
                                        "prior_days"
                                    ]
                                )
                                else (
                                    dct_denom_spec[(measure, denom_condn)][
                                        "prior_months"
                                    ]
                                    * 30
                                    if pd.notnull(
                                        dct_denom_spec[(measure, denom_condn)][
                                            "prior_months"
                                        ]
                                    )
                                    else dct_service["followup_months"] * 30
                                )
                            )
                            denom_followup_gap = (
                                dct_denom_spec[(measure, denom_condn)][
                                    "followup_days"
                                ]
                                if pd.notnull(
                                    dct_denom_spec[(measure, denom_condn)][
                                        "followup_days"
                                    ]
                                )
                                else (
                                    dct_denom_spec[(measure, denom_condn)][
                                        "followup_months"
                                    ]
                                    * 30
                                    if pd.notnull(
                                        dct_denom_spec[(measure, denom_condn)][
                                            "followup_months"
                                        ]
                                    )
                                    else dct_service["prior_months"] * 30
                                )
                            )
                            if pd.notnull(denom_prior_gap):
                                lst_denom_dates = [
                                    denom_date
                                    for denom_date in lst_denom_dates
                                    if (
                                        len(
                                            row["eligibility_pattern"][
                                                : (
                                                    denom_date.month
                                                    + 12
                                                    * (
                                                        denom_date.year
                                                        - (year - 1)
                                                    )
                                                    - 1
                                                )
                                            ].replace("0", "")
                                        )
                                        >= (
                                            math.ceil(denom_prior_gap / 30)
                                            if pd.isnull(
                                                dct_service[
                                                    "min_enrolled_months"
                                                ]
                                            )
                                            else dct_service[
                                                "min_enrolled_months"
                                            ]
                                        )
                                    )
                                ]

                            if pd.notnull(denom_followup_gap):
                                lst_denom_dates = [
                                    denom_date
                                    for denom_date in lst_denom_dates
                                    if (
                                        len(
                                            row["eligibility_pattern"][
                                                (
                                                    denom_date.month
                                                    + 12
                                                    * (
                                                        denom_date.year
                                                        - (year - 1)
                                                    )
                                                    - 1
                                                ) :
                                            ].replace("0", "")
                                        )
                                        >= (
                                            math.ceil(denom_followup_gap / 30)
                                            if pd.isnull(
                                                dct_service[
                                                    "min_enrolled_months"
                                                ]
                                            )
                                            else dct_service[
                                                "min_enrolled_months"
                                            ]
                                        )
                                    )
                                ]

                            offset = (
                                dct_denom_spec[(measure, denom_condn)][
                                    "offset_days"
                                ]
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
                            if pd.notnull(index_date):
                                lst_denom_dates = [
                                    denom_date
                                    for denom_date in lst_denom_dates
                                    if (index_date - denom_date).days
                                    <= denom_followup_gap - offset
                                ]

                            if bool(lst_denom_dates):
                                dct_denom_pop[denom_condn] = lst_denom_dates[
                                    -1
                                ]

                            gap = (
                                denom_followup_gap
                                if not (pd.isnull(denom_followup_gap))
                                else (-1 * denom_prior_gap)
                            )

                            if pd.notnull(gap) & (gap < 0):
                                offset = -1 * offset

                            lst_denom_gaps = [
                                ((y - x).days, str(x), str(y))
                                for x, y in product(
                                    lst_denom_dates, lst_msr_dates
                                )
                            ]

                            if pd.notnull(gap):
                                if gap < 0:
                                    lst_matches = [
                                        x
                                        for x in lst_denom_gaps
                                        if (offset >= x[0] >= gap)
                                    ]
                                    if bool(lst_matches):
                                        dct_denom_pop_msr[
                                            denom_condn
                                        ] = lst_matches[0]
                                if gap > 0:
                                    lst_matches = [
                                        x
                                        for x in lst_denom_gaps
                                        if (offset <= x[0] <= gap)
                                    ]
                                    if bool(lst_matches):
                                        dct_denom_pop_msr[
                                            denom_condn
                                        ] = lst_matches[0]
                            elif bool(lst_denom_dates) & bool(lst_msr_dates):
                                dct_denom_pop_msr[
                                    denom_condn
                                ] = lst_denom_gaps[-1]
                pdf_dates.loc[idx, f"pop_{service}_denom_{measure}"] = (
                    1
                    if (
                        bool(dct_denom_pop)
                        & (
                            not any(
                                condn
                                for condn in dct_denom_pop
                                if condn.startswith("excl_")
                            )
                        )
                    )
                    else 0
                )

                pdf_dates.at[
                    idx, f"service_{service}_with_denom_{measure}"
                ] = (
                    1
                    if (
                        bool(dct_denom_pop_msr)
                        & (
                            not any(
                                condn
                                for condn in dct_denom_pop
                                if condn.startswith("excl_")
                            )
                        )
                    )
                    else 0
                )
                pdf_dates.at[
                    idx, f"dates_service_{service}_with_denom_{measure}"
                ] = (
                    json.dumps(dct_denom_pop_msr)
                    if (
                        bool(dct_denom_pop_msr)
                        & (
                            not any(
                                condn
                                for condn in dct_denom_pop
                                if condn.startswith("excl_")
                            )
                        )
                    )
                    else ""
                )
        return pdf_dates

    @classmethod
    def get_diag_proc_codes(
        cls, pdf_denom_spec: pd.DataFrame, pdf_measure_spec: pd.DataFrame
    ) -> (dict, dict):
        """
        Returns dictionaries of diagnosis & procedure codes

        Parameters
        ----------
        pdf_denom_spec : pd.DataFrame
            Denom spec dataframe
        pdf_measure_spec : pd.DataFrame
            Measure spec dataframe

        Returns
        -------
        dct_diag_codes : dict
            Dictionary of diagnostic codes
        dct_proc_codes : dict
            Dictionary of procedure codes

        """
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
    def generate_condn_and_eligibility_indicators(
        cls,
        state: str,
        year: int,
        pdf_denom_spec: pd.DataFrame,
        pdf_measure_spec: pd.DataFrame,
        max_data_root: str,
        lst_bene_id_filter: List[str],
        out_folder: str,
        index_col="BENE_MSIS",
    ) -> None:
        """
        Creates condition & eligibility pattern indicators, and saves them as parquet files

        Parameters
        ----------
        state : str
            State
        year : int
            Year
        pdf_denom_spec : pd.DataFrame
            Denomination spec dataframe
        pdf_measure_spec : pd.DataFrame
            Measure spec dataframe
        max_data_root : str
            Max files folder
        lst_bene_id_filter : List[str]
            List of bene ids to filter to
        out_folder : str
            Folder where temporary files can be saved
        index_col : str
            Index column name

        Returns
        -------

        """
        cache_folder = os.path.join(out_folder, "cache")
        os.makedirs(cache_folder, exist_ok=True)
        dct_diag_codes, dct_proc_codes = cls.get_diag_proc_codes(
            pdf_denom_spec, pdf_measure_spec
        )

        for curr_year in range(year - 1, year + 1):
            ip_claim = max_ip.MAXIP(
                curr_year,
                state,
                max_data_root,
                index_col=index_col,
                clean=False,
                preprocess=False,
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
                os.path.join(out_folder, "ip", f"{state}_{curr_year}.parquet"),
                engine="fastparquet",
                compression="snappy",
                index=True,
            )
            del ip_claim
            ot_claim = max_ot.MAXOT(
                curr_year,
                state,
                max_data_root,
                clean=False,
                preprocess=False,
                index_col=index_col,
                tmp_folder=os.path.join(cache_folder, "ot"),
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
                os.path.join(out_folder, "ot", f"{state}_{curr_year}.parquet"),
                engine="fastparquet",
                compression="snappy",
                index=True,
            )
            del df_ot

            ps_file = max_ps.MAXPS(
                curr_year,
                state,
                max_data_root,
                index_col=index_col,
                clean=False,
                preprocess=False,
            )
            ps_file.df = ps_file.df.map_partitions(
                lambda pdf: pdf.loc[pdf.index.isin(lst_bene_id_filter)]
            )
            ps_file.add_eligibility_status_columns()
            ps_file.df = ps_file.df.map_partitions(
                lambda pdf: pdf.assign(
                    eligibility_pattern=pdf[
                        ["elg_mon_" + str(mon) for mon in range(1, 13)]
                    ]
                    .astype(int)
                    .astype(str)
                    .apply("".join, axis=1)
                )
            )
            ps_file.df = ps_file.df[
                ["eligibility_pattern"]
                + [
                    col
                    for col in ["BENE_ID", "MSIS_ID", "BENE_MSIS"]
                    if col != index_col
                ]
            ]

            df_ps = ps_file.df.compute()
            df_ps.to_parquet(
                os.path.join(out_folder, "ps", f"{state}_{curr_year}.parquet"),
                engine="fastparquet",
                compression="snappy",
                index=True,
            )

    @classmethod
    def get_dates(
        cls,
        state: str,
        year: int,
        lst_condn: List[str],
        index_col: str,
        claims_folder: str,
    ) -> pd.DataFrame:
        """
        Aggregates dates at bene level for each denom/ measure condition. Three versions of date lists are returned
        based on the claim type the condition was present: ip, ed, ot, and all

        Parameters
        ----------
        state : str
            State
        year : int
            Year
        lst_condn : list of str
            List of conditions
        index_col : str
            Index column name
        claims_folder : str
            Folder where claim files with condition flags are present

        Returns
        -------
        pdf_dates : pd.DataFrame
            Dataframe with denom/ measure condition date lists at bene level

        """
        pdf_dates = None
        for ftype in ["ip", "ot"]:
            pdf = pd.read_parquet(
                os.path.join(claims_folder, ftype, f"{state}_{year}.parquet"),
                engine="pyarrow",
                # index=False,
            ).set_index(index_col)
            pdf = pdf.rename(
                columns={
                    col: col.replace("diag_", "").replace("proc_", "")
                    for col in pdf.columns
                    if col.startswith(
                        (
                            "diag_",
                            "proc_",
                        )
                    )
                }
            )
            pdf = pdf.assign(
                **{
                    col: pdf[col.replace("_excl_chronic_", "_")]
                    for col in [
                        "denom_excl_chronic_sinusitis",
                        "denom_excl_chronic_acquired_hypothyroidism",
                        "denom_excl_chronic_low_back_pain",
                        "denom_excl_chronic_plantar_fasciitis",
                    ]
                }
            )
            if "msr_total_or_free_t4" in pdf.columns:
                pdf = pdf.assign(
                    msr_total_or_free_t3=pdf[
                        ["msr_total_or_free_t4", "msr_total_or_free_t3"]
                    ].any(axis=1)
                )
            pdf = pdf.assign(
                **{
                    f"{col}_{ftype}_dates": pdf[
                        "prncpl_proc_date"
                        if ftype == "ip"
                        else "srvc_bgn_date"
                    ].where(pdf[col] == 1, np.nan)
                    for col in lst_condn
                }
            )
            if ftype == "ot":
                pdf = pdf.assign(
                    **{
                        f"{col}_ed_dates": pdf["srvc_bgn_date"].where(
                            (pdf[col] == 1) & (pdf["any_ed"] == 1), np.nan
                        )
                        for col in lst_condn
                    }
                )
            pdf = pdf.groupby(pdf.index, dropna=True).agg(
                {
                    **{
                        f"{col}_{ftype}_dates": lambda x: [
                            y for y in x if pd.notna(y)
                        ]
                        for col in lst_condn
                    },
                    **{
                        f"{col}_ed_dates": lambda x: [
                            y for y in x if pd.notna(y)
                        ]
                        for col in lst_condn
                        if f"{col}_ed_dates" in pdf.columns
                    },
                }
            )
            pdf_dates = (
                pdf
                if pdf_dates is None
                else pdf.merge(
                    pdf_dates, left_index=True, right_index=True, how="outer"
                )
            )
        pdf_dates = pdf_dates.assign(
            **{
                col
                + "_all_dates": pdf_dates[f"{col}_ip_dates"].apply(
                    lambda x: x if not (x is np.nan) else []
                )
                + pdf_dates[f"{col}_ot_dates"].apply(
                    lambda x: x if not (x is np.nan) else []
                )
                for col in lst_condn
            }
        )
        return pdf_dates

    @classmethod
    def get_dates_with_eligibility(
        cls,
        state: str,
        year: int,
        lst_condn: List[str],
        index_col: str,
        claims_folder: str,
    ) -> pd.DataFrame:
        """
        Returns eligibility info and dates for all conditions at bene level

        Parameters
        ----------
        state : str
            State
        year : int
            Year
        lst_condn : List[str]
            List of conditions
        index_col : str
            Index column name
        claims_folder : str
            Location of claims folder that contains flags for conditions

        Returns
        -------
        pd.DataFrame

        """
        pdf_dates = cls.get_dates(
            state, year, lst_condn, index_col, claims_folder
        )
        if os.path.exists(
            os.path.join(claims_folder, "ot", f"{state}_{year - 1}.parquet")
        ) & os.path.exists(
            os.path.join(claims_folder, "ip", f"{state}_{year - 1}.parquet")
        ):
            pdf_prior_dates = cls.get_dates(
                state,
                year - 1,
                [condn for condn in lst_condn if not condn.startswith("msr_")],
                index_col,
                claims_folder,
            )
            pdf_dates = pd.concat([pdf_dates, pdf_prior_dates])
        pdf_dates = pdf_dates.groupby(pdf_dates.index).agg(
            {
                col: lambda x: sum([y for y in x if (not (y is np.nan))], [])
                for col in pdf_dates.columns
            }
        )
        pdf_ps = pd.read_parquet(
            os.path.join(claims_folder, "ps", f"{state}_{year}.parquet"),
            engine="pyarrow",
            # index=False,
        ).set_index(index_col)[["eligibility_pattern"]]
        if os.path.exists(
            os.path.join(claims_folder, "ps", f"{state}_{year}.parquet")
        ):
            pdf_ps = pdf_ps.merge(
                pd.read_parquet(
                    os.path.join(
                        claims_folder, "ps", f"{state}_{year - 1}.parquet"
                    ),
                    engine="pyarrow",
                    # index=False,
                )
                .set_index(index_col)[["eligibility_pattern"]]
                .rename(
                    columns={
                        "eligibility_pattern": "lookback_eligibility_pattern"
                    }
                ),
                on=index_col,
                how="left",
            )
        else:
            pdf_ps = pdf_ps.assign(lookback_eligibility_pattern="0" * 12)
        pdf_ps = pdf_ps.assign(
            eligibility_pattern=pdf_ps["lookback_eligibility_pattern"].fillna(
                "0" * 12
            )
            + pdf_ps["eligibility_pattern"]
        )
        pdf_dates = pdf_dates.merge(pdf_ps, on=index_col, how="inner")
        return pdf_dates

    @classmethod
    def get_denom_measure_spec(
        cls,
    ) -> Tuple[dict, dict, List[str], pd.DataFrame, pd.DataFrame]:
        """
        Returns denom measure specs

        Returns
        -------
        dct_measures : dict
            Measures dictionary
        dct_denom : dict
            Denominator dictionary
        lst_condn : list of str
            List of conditions
        pdf_denom_spec_codes : pd.DataFrame
            Denominator spec codes dataframe
        pdf_measure_spec_codes : pd.DataFrame
            Measure spec codes dataframe

        """
        pdf_denom_spec = pd.read_excel(
            os.path.join(cls.data_folder, "low_value_care.xlsx"),
            sheet_name="denominator",
            dtype=str,
            engine="openpyxl",
        )
        pdf_measure_spec = pd.read_excel(
            os.path.join(cls.data_folder, "low_value_care.xlsx"),
            sheet_name="measures",
            dtype=str,
            engine="openpyxl",
        )

        pdf_denom_spec = cls.normalize_condition_names(pdf_denom_spec)
        pdf_measure_spec = cls.normalize_condition_names(pdf_measure_spec)

        pdf_denom_spec = pdf_denom_spec.assign(
            description=pdf_denom_spec["description"].where(
                pdf_denom_spec["include"].astype(int) == 1,
                "excl_" + pdf_denom_spec["description"],
            )
        )

        pdf_measure_spec = pdf_measure_spec.assign(
            **{
                col: pd.to_numeric(pdf_measure_spec[col], errors="coerce")
                for col in [
                    "exclude_ip",
                    "exclude_ed",
                    "prior_months",
                    "followup_months",
                    "min_enrolled_months",
                ]
            }
        )
        pdf_denom_spec = pdf_denom_spec.assign(
            **{
                col: pd.to_numeric(pdf_denom_spec[col], errors="coerce")
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
            }
        )
        pdf_denom_spec_codes = pdf_denom_spec.copy()
        pdf_measure_spec_codes = pdf_measure_spec.copy()
        pdf_denom_spec = pdf_denom_spec.drop(
            ["icd_code", "proc_code", "proc_sys", "include", "except"], axis=1
        ).drop_duplicates(keep="first")
        pdf_measure_spec = pdf_measure_spec.drop(
            ["icd_code", "proc_code", "proc_sys", "include", "except"], axis=1
        ).drop_duplicates(keep="first")

        pdf_combined_spec = pdf_measure_spec.merge(
            pdf_denom_spec.rename(
                columns={
                    col: "denom_" + col
                    for col in pdf_denom_spec.columns
                    if col != "measure"
                }
            ),
            on="measure",
            how="outer",
        )

        lst_condn = list(
            {  # pylint: disable=unnecessary-comprehension
                condn
                for condn in [
                    "msr_" + condn
                    for condn in pdf_combined_spec["description"]
                ]
                + [
                    "denom_" + condn
                    for condn in pdf_combined_spec["denom_description"]
                ]
            }
        )
        dct_measures = (
            pdf_combined_spec.groupby(["description", "measure"])
            .agg(
                {
                    "measure": lambda x: x.values[0],
                    "exclude_ip": lambda x: x.values[0],
                    "exclude_ed": lambda x: x.values[0],
                    "followup_months": lambda x: x.values[0],
                    "prior_months": lambda x: x.values[0],
                    "min_enrolled_months": lambda x: x.values[0],
                    "denom_description": list,
                }
            )
            .to_dict(orient="index")
        )

        dct_denom = pdf_denom_spec.set_index(
            ["measure", "description"]
        ).to_dict(orient="index")
        return (
            dct_measures,
            dct_denom,
            lst_condn,
            pdf_denom_spec_codes,
            pdf_measure_spec_codes,
        )


def construct_low_value_care_measures(
    state: str,
    year: int,
    lst_bene_msis_filter: List[str],
    index_col: str,
    max_data_root: str,
    out_folder: str,
) -> pd.DataFrame:
    """
    Constructs low value care measures for the given year and state

    Parameters
    ----------
    state : str
        State
    year : str
        Year
    lst_bene_msis_filter : List[str]
        Bene id filder
    index_col : str
        Index column name
    max_data_root : str
        Location of claims files
    out_folder : str
        Location where temporary files will be saved

    Returns
    -------
    pdf_dates : pd.DataFrame
        Dataframe with low value care measures

    """
    (
        dct_measures,
        dct_denom,
        lst_condn,
        pdf_denom_spec_codes,
        pdf_measure_spec_codes,
    ) = LowValueCare.get_denom_measure_spec()
    LowValueCare.generate_condn_and_eligibility_indicators(
        state,
        year,
        pdf_denom_spec_codes,
        pdf_measure_spec_codes,
        max_data_root,
        lst_bene_msis_filter,
        out_folder,
        index_col=index_col,
    )

    pdf_dates = LowValueCare.get_dates_with_eligibility(
        state, year, lst_condn, index_col, out_folder
    )

    pdf_dates = LowValueCare.construct_low_value_care_measures(
        pdf_dates, year, dct_measures, dct_denom
    )
    return pdf_dates

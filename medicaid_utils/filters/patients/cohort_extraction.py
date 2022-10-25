"""This module has functions that extract cohorts for studies based on multiple filters"""
import gc
import os
import shutil
import logging
from typing import List, Union, Tuple

import pandas as pd

from medicaid_utils.preprocessing import (
    max_file,
    max_ip,
    max_ps,
    max_cc,
    max_ot,  # pylint: disable=unused-import
    taf_file,
    taf_ip,
    taf_ot,
    taf_ps,  # pylint: disable=unused-import
)
from medicaid_utils.filters.claims import dx_and_proc

data_folder = os.path.join(os.path.dirname(__file__), "data")


def apply_range_filter(  # pylint: disable=missing-param-doc
    tpl_range: tuple,
    df: pd.DataFrame,
    filter_name: str,
    col_name: str,
    data_type: str,
    logger_name: str = __file__,
) -> pd.DataFrame:
    """
    Applies data/ numeric range based filter on a dataframe

    Parameters
    ----------
    tpl_range : tuple
        Upper and lower bound tuple
    df : dd.DataFrame
        Dataframe to be filtered
    filter_name : str
        Name of filter. Should be of the format range_[datatype]_[col_name]. date and numeric range type filters are
        currently supported
    col_name : str
        Name of column
    data_type : str
        Datatype of column. Eg. date, int
    logger_name : str, default=__file__
        Logger name

    Returns
    -------
    dd.DataFrame

    """
    logger = logging.getLogger(logger_name)
    start = (
        pd.to_datetime(tpl_range[0], format="%Y%m%d", errors="coerce")
        if (data_type == "date")
        else pd.to_numeric(tpl_range[0], errors="coerce")
    )
    end = (
        pd.to_datetime(tpl_range[1], format="%Y%m%d", errors="coerce")
        if (data_type == "date")
        else pd.to_numeric(tpl_range[1], errors="coerce")
    )
    if pd.notnull(start) | pd.notnull(end):
        if ~(pd.isnull(start) | pd.isnull(end)):
            df = df.loc[df[col_name].between(start, end, inclusive="both")]
        elif pd.isnull(start):
            df = df.loc[df[col_name] <= end]
        else:
            df = df.loc[df[col_name] >= start]
    else:
        logger.info(
            "%s range (%s) is  invalid.",
            " ".join(filter_name.split("_")[2:]),
            ",".join([str(val) for val in tpl_range]),
        )
    return df


def filter_claim_files(  # pylint: disable=missing-param-doc
    claim: Union[max_file.MAXFile, taf_file.TAFFile],
    dct_claim_filters: dict,
    tmp_folder: str,
    subtype: str = None,
    logger_name: str = __file__,
) -> Tuple[Union[max_file.MAXFile, taf_file.TAFFile], pd.DataFrame]:
    """
    Filters claim files

    Parameters
    ----------
    claim : Union[max_file.MAXFile, taf_file.TAFFile]
        Claim object
    dct_claim_filters : dict
        Filters to apply. Filter dictionary should be of the format:
        {claim_type_1: {range_[datatype]_[col_name]: (start, end),
                        excl_[col_name]: [0/1],
                        [col_name]: value,
                        ..}
        claim_type_2: ...}} date and numeric range type filters are currently supported. Filter names beginning with
        `excl_` with values set to 1 will exclude benes that have a positive value for that exclusion flag. Filter
        names that are just column names will restrict the result to benes with the filter value for the corresponding
        column.
        Eg: {'ip': {'range_numeric_age_prncpl_proc': (0, 18),
                                      'missing_dob': 0,
                                      'excl_female': 1}}
                              'ot': {'range_numeric_age_srvc_bgn': (0, 18),
                                      'missing_dob': 0,
                                      'excl_female': 1}}
                              }
        The example filter will exclude all IP claims of female benes and also claims with missing DOB. The resulting
        set will also be restricted to those of benes whose age is between 0-18 (inclusive of both 0 and 18) as of
        prinicipal procedure data/ service begin date.
    tmp_folder : str
        Temporary folder to cache results mid-processing. This is useful for large datasets, as the dask cluster can
        crash if the task graph is too large for large datasets. This is handled by caching results at intermediate
        stages.
    subtype : str, default=None
        Claim subtype (required for TAF datasets)
    logger_name : str, default=None
        Logger name

    Returns
    -------
    Tuple[Union[max_file.MAXFile, taf_file.TAFFile], pd.DataFrame]

    Raises
    ------
    ValueError
        When subtype is parameter is missing for a function with TAFFile claim type input

    """
    logger = logging.getLogger(logger_name)
    if isinstance(claim, taf_file.TAFFile) and (subtype is None):
        raise ValueError(
            f"Parameter {subtype} is required to filter TAF {claim.ftype} file"
        )

    df_claim = (
        claim.df.copy()
        if isinstance(claim, max_file.MAXFile)
        else claim.dct_files[subtype].copy()
    )
    df_filter_counts = pd.DataFrame(
        {"N": df_claim.shape[0].compute()}, index=[0]
    )
    logger.info(
        "%s (%d) has %d %s %sclaims",
        claim.state,
        claim.year,
        df_filter_counts.N.values[0],
        claim.ftype,
        "(" + subtype + ") " if (subtype is not None) else "",
    )
    dct_filter = claim.dct_default_filters.copy()
    if claim.ftype in dct_claim_filters:
        dct_filter.update(dct_claim_filters[claim.ftype])
    for filter_name in dct_filter:
        filtered = 1
        if filter_name.startswith("range_") and (
            "_".join(filter_name.split("_")[2:]) in df_claim.columns
        ):
            df_claim = apply_range_filter(
                dct_filter[filter_name],
                df_claim,
                filter_name,
                "_".join(filter_name.split("_")[2:]),
                filter_name.split("_")[1],
                logger_name=logger_name,
            )

        elif f"excl_{filter_name}" in df_claim.columns:
            df_claim = df_claim.loc[
                ~(
                    df_claim[f"excl_{filter_name}"]
                    == int(dct_filter[filter_name] == 0)
                )
            ]
        elif filter_name.startswith("excl_") and (
            filter_name in df_claim.columns
        ):
            df_claim = df_claim.loc[
                df_claim[filter_name] == int(dct_filter[filter_name] == 0)
            ]
        elif filter_name in df_claim.columns:
            df_claim = df_claim.loc[
                df_claim[filter_name] == dct_filter[filter_name]
            ]
        else:
            filtered = 0
            logger.info(
                "Filter %s is currently not supported for %s %sfiles",
                filter_name,
                claim.ftype,
                "(" + subtype + ") " if (subtype is not None) else "",
            )
        if filtered:
            if claim.tmp_folder is None:
                claim.tmp_folder = tmp_folder

            if isinstance(claim, max_file.MAXFile):
                claim.df = df_claim.copy()
                claim.cache_results()
                df_claim = claim.df.copy()
            else:
                claim.dct_files[subtype] = df_claim.copy()
                claim.cache_results(subtype=subtype)
                df_claim = claim.dct_files[subtype].copy()

            df_filter_counts[filter_name] = df_claim.shape[0].compute()
            filter_status = (
                f"Applying {filter_name} = {dct_filter[filter_name]} filter"
                f" reduces {claim.ftype} {'(' + subtype + ') ' if (subtype is not None) else ''}claim count to"
                f" {df_filter_counts[filter_name].values[0]}"
            )
            logger.info(filter_status)
    return claim, df_filter_counts


def extract_cohort(  # pylint: disable=too-many-locals, missing-param-doc
    state: str,
    lst_year: List[int],
    dct_diag_proc_codes: dict,
    dct_filters: dict,
    lst_types_to_export: List[str],
    dct_data_paths: dict,
    cms_format: str = "MAX",
    clean_exports: bool = True,
    preprocess_exports: bool = True,
    logger_name: str = __file__,
):
    """
    Extracts and exports claim files corresponded cohort defined by the input filters

    Parameters
    ----------
    state : str
        State
    lst_year : list of int
        List of years from which cohort should be created
    dct_diag_proc_codes : dict
        Dictionary of diagnosis and procedure codes. Should be in the format
            {'diag_codes': {condition_name: {['incl' / 'excl']: {[9/ 10]: list of codes} },
             'proc_codes': {procedure_name: {procedure_system_code: list of codes} }}
            Eg: {'diag_codes': {'oud_nqf': {'incl': {9: ['3040','3055']}}},
                 'proc_codes': {'methadone_7': {7: 'HZ81ZZZ,HZ84ZZZ,HZ85ZZZ,HZ86ZZZ,HZ91ZZZ,HZ94ZZZ,HZ95ZZZ,'
                                   'HZ96ZZZ'.split(",")}}}
    dct_filters: dict
        Filters to apply to the cohort, and the exported claim files. Filter dictionary should be of the format:
        {'cohort': {claim_type_1: {range_[datatype]_[col_name]: (start, end),
                        excl_[col_name]: [0/1],
                        [col_name]: value,
                        ..},
        'export': {claim_type_1: {range_[datatype]_[col_name]: (start, end),
                        excl_[col_name]: [0/1],
                        [col_name]: value,
                        ..}}
        date and numeric range type filters are currently supported. Filter names beginning with
        `excl_` with values set to 1 will exclude benes that have a positive value for that exclusion flag. Filter
        names that are just column names will restrict the result to benes with the filter value for the
        corresponding column.
        Eg: {'ip': {'range_numeric_age_prncpl_proc': (0, 18),
                                      'missing_dob': 0,
                                      'excl_female': 1}}
                              'ot': {'range_numeric_age_srvc_bgn': (0, 18),
                                      'missing_dob': 0,
                                      'excl_female': 1}}
                              }
        The example filter will exclude the cohort to all IP claims of female benes and also claims with missing
        DOB. The resulting set will also be restricted to those of benes whose age is between 0-18 (inclusive of
        both 0 and 18) as of principal procedure date/ service begin date.
    lst_types_to_export : List[str]
        List of types to export. Currently supported types are ip, ot, rx, ps.
    dct_data_paths : dict
        Dictionary with information on raw claim files root folder and export folder. Should be of the format,
        {'source_root': /path/to/medicaid/folder,
         'export_folder': /path/to/export/data}
    cms_format : {'MAX', 'TAF'}
        CMS file format.
    clean_exports : bool, default=False
        Should the exported datasets be cleaned?
    preprocess_exports : bool, default=False
        Should the exported datasets be preprocessed?
    logger_name : str, default=__file__
        Logger name

    Raises
    ------
    FileNotFoundError
        Raised when any of file types requested to be imported does not exist for the state and year

    """
    logger = logging.getLogger(logger_name)
    dct_data_paths["tmp_folder"] = os.path.join(
        dct_data_paths["export_folder"], "tmp_files"
    )
    lst_year = sorted(lst_year)
    pdf_patients_all_years = pd.DataFrame()
    for year in lst_year:
        dct_claims = {}

        try:
            for claim_type in ["ip", "ot", "ps", "rx"]:
                if claim_type == "rx":
                    # Not yet implemented for TAF
                    if cms_format == "MAX":
                        dct_claims[claim_type] = max_file.MAXFile(
                            "rx",
                            year,
                            state,
                            dct_data_paths["source_root"],
                            clean=False,
                            preprocess=False,
                        )
                else:
                    dct_claims[claim_type] = (
                        max_file.MAXFile.get_claim_instance(
                            claim_type,
                            year,
                            state,
                            dct_data_paths["source_root"],
                            clean=True,
                            preprocess=True,
                            **(
                                {}
                                if claim_type != "ip"
                                else {
                                    "tmp_folder": os.path.join(
                                        dct_data_paths["tmp_folder"], claim_type
                                    )
                                }
                            ),
                        )
                        if cms_format == "MAX"
                        else taf_file.TAFFile.get_claim_instance(
                            claim_type,
                            year,
                            state,
                            dct_data_paths["source_root"],
                            clean=True,
                            preprocess=True,
                            **(
                                {}
                                if claim_type != "ip"
                                else {
                                    "tmp_folder": os.path.join(
                                        dct_data_paths["tmp_folder"], claim_type
                                    )
                                }
                            ),
                        )
                    )
        except FileNotFoundError as ex:
            logger.warning("%d data is missing for %s", year, state)
            logger.exception(ex)
            continue

        os.makedirs(dct_data_paths["export_folder"], exist_ok=True)
        os.makedirs(dct_data_paths["tmp_folder"], exist_ok=True)

        dct_cohort_filter_stats = {}
        if "cohort" in dct_filters:
            for f_type in dct_filters["cohort"]:
                dct_cohort_filter_stats[f_type] = {}
                lst_subtypes = (
                    [None]
                    if (cms_format == "MAX")
                    else ["base"]  # list(dct_claims[f_type].dct_files.keys())
                )
                for subtype in lst_subtypes:
                    (dct_claims[f_type], filter_stats_df) = filter_claim_files(
                        dct_claims[f_type],
                        dct_filters["cohort"],
                        os.path.join(dct_data_paths["tmp_folder"], f_type),
                        subtype=subtype,
                        logger_name=logger_name,
                    )
                    dct_cohort_filter_stats[f_type] = (
                        filter_stats_df.copy()
                        # if (not bool(subtype))
                        # else {
                        #     **dct_cohort_filter_stats[f_type],
                        #     **{subtype: filter_stats_df.copy()},
                        # }
                    )

        pdf_patients = None
        if bool(dct_diag_proc_codes) and (
            ("diag_codes" in dct_diag_proc_codes)
            or ("proc_codes" in dct_diag_proc_codes)
        ):
            pdf_patients, _ = dx_and_proc.get_patient_ids_with_conditions(
                dct_diag_proc_codes["diag_codes"],
                dct_diag_proc_codes["proc_codes"],
                logger_name=logger_name,
                cms_format=cms_format,
                ip=dct_claims["ip"].df.rename(
                    columns={"prncpl_proc_date": "service_date"}
                )[
                    [
                        col
                        for col in dct_claims["ip"].df.columns
                        if col.startswith(
                            (
                                "PRCDR",
                                "DIAG",
                            )
                        )
                    ]
                    + ["service_date"]
                ]
                if (cms_format == "MAX")
                else dct_claims["ip"]
                .dct_files["base"]
                .rename(columns={"prncpl_proc_date": "service_date"})[
                    [
                        col
                        for col in dct_claims["ip"].dct_files["base"].columns
                        if col.startswith(
                            ("DGNS", "ADMTG_DGNS", "PRCDR_CD", "LINE_PRCDR_CD")
                        )
                    ]
                    + ["service_date"]
                ],
                ot=dct_claims["ot"].df.rename(
                    columns={"srvc_bgn_date": "service_date"}
                )[
                    [
                        col
                        for col in dct_claims["ot"].df.columns
                        if col.startswith(
                            (
                                "PRCDR",
                                "DIAG",
                            )
                        )
                    ]
                    + ["service_date"]
                ]
                if (cms_format == "MAX")
                else dct_claims["ot"]
                .dct_files["base"]
                .rename(columns={"srvc_bgn_date": "service_date"})[
                    [
                        col
                        for col in dct_claims["ot"].dct_files["base"].columns
                        if col.startswith(
                            ("DGNS", "ADMTG_DGNS", "PRCDR_CD", "LINE_PRCDR_CD")
                        )
                    ]
                    + ["service_date"]
                ],
                **(
                    {
                        "ot_line": dct_claims["ot"]
                        .dct_files["line"]
                        .rename(columns={"srvc_bgn_date": "service_date"})[
                            [
                                col
                                for col in dct_claims["ot"]
                                .dct_files["line"]
                                .columns
                                if col.startswith(
                                    (
                                        "DGNS",
                                        "ADMTG_DGNS",
                                        "PRCDR_CD",
                                        "LINE_PRCDR_CD",
                                    )
                                )
                            ]
                            + ["service_date"]
                        ]
                    }
                    if cms_format == "TAF"
                    else {}
                ),
            )
            pdf_patients = pdf_patients.assign(include=1)
        else:
            pdf_patients = (
                (
                    pd.concat(
                        [
                            claim.df.assign(
                                **{claim.index_col: claim.df.index, "include": 1}
                            )[[claim.index_col, "include"]].compute()
                            for f_type, claim in dct_claims.items()
                            if (
                                not (
                                    bool(dct_filters) and ("cohort" in dct_filters)
                                )
                            )
                            or (f_type in dct_filters["cohort"])
                        ]
                    )
                    .drop_duplicates()
                    .set_index(dct_claims["ps"].index_col)
                )
                if cms_format == "MAX"
                else (
                    pd.concat(
                        [
                            claim.dct_files["base"]
                            .assign(
                                **{
                                    claim.index_col: claim.dct_files["base"].index,
                                    "include": 1,
                                }
                            )[[claim.index_col, "include"]]
                            .compute()
                            for f_type, claim in dct_claims.items()
                            if (
                                not (
                                    bool(dct_filters) and ("cohort" in dct_filters)
                                )
                            )
                            or (f_type in dct_filters["cohort"])
                        ]
                    )
                    .drop_duplicates()
                    .set_index(dct_claims["ps"].index_col)
                )
            )
        pdf_patients = pdf_patients.assign(YEAR=year, STATE_CD=state)
        logger.info(
            "%s (%d) has %d benes with specified  conditions/ procedures",
            state,
            year,
            pdf_patients.shape[0],
        )

        if ("cohort" not in dct_filters) or ("ps" not in dct_filters["cohort"]):
            dct_cohort_filter_stats["ps"] = pd.DataFrame(
                {
                    "N": dct_claims["ps"].df.shape[0].compute()
                    if (cms_format == "MAX")
                    else dct_claims["ps"].dct_files["base"].shape[0].compute()
                },
                index=[0],
            )
        df_ps = (
            dct_claims["ps"].df.copy()
            if (cms_format == "MAX")
            else dct_claims["ps"].dct_files["base"].copy()
        )
        df_ps = df_ps.loc[
            df_ps.index.isin(
                pdf_patients.loc[pdf_patients["include"] == 1].index.tolist()
            )
        ]
        if cms_format == "MAX":
            dct_claims["ps"].df = df_ps.copy()
            dct_claims["ps"].cache_results(repartition=True)
        else:
            dct_claims["ps"].dct_files["base"] = df_ps.copy()
            dct_claims["ps"].cache_results("base", repartition=True)

        dct_cohort_filter_stats["ps"]["with specified conditions"] = (
            (dct_claims["ps"].df.shape[0].compute())
            if (cms_format == "MAX")
            else dct_claims["ps"].dct_files["base"].shape[0].compute()
        )
        del df_ps

        if ("cohort" not in dct_filters) or ("ps" not in dct_filters["cohort"]):
            (dct_claims["ps"], df_filter_stats) = filter_claim_files(
                dct_claims["ps"],
                {},
                os.path.join(dct_data_paths["tmp_folder"], "ps"),
                subtype="base" if (cms_format == "TAF") else None,
                logger_name=logger_name,
            )
            dct_cohort_filter_stats["ps"] = pd.concat(
                [
                    dct_cohort_filter_stats["ps"],
                    df_filter_stats[
                        [col for col in df_filter_stats.columns if col != "N"]
                    ],
                ],
                axis=1,
            )
        logger.info(
            "%s (%d) has  %d benes  with specified conditions who also meet the cohort inclusion  criteria",
            state,
            year,
            dct_cohort_filter_stats["ps"].iloc[0, -1],
        )
        pdf_patients["include"] = pdf_patients["include"].where(
            pdf_patients.index.isin(
                dct_claims["ps"].df.index.compute().tolist()
                if (cms_format == "MAX")
                else dct_claims["ps"].dct_files["base"].index.compute().tolist()
            ),
            0,
        )
        logger.info(
            "For %s (%d), %d benes remain after cleaning PS",
            state,
            year,
            pdf_patients.loc[pdf_patients["include"] == 1].shape[0],
        )

        pdf_dob = (
            dct_claims["ps"].df[["birth_date"]].compute()
            if (cms_format == "MAX")
            else dct_claims["ps"].dct_files["base"][["birth_date"]].compute()
        )
        for f_type, cohort_filter_stats in dct_cohort_filter_stats.items():
            if isinstance(cohort_filter_stats, dict):
                for (
                    sub_type,
                    df_cohort_filter_stats,
                ) in cohort_filter_stats.items():
                    df_cohort_filter_stats.to_parquet(
                        os.path.join(
                            dct_data_paths["export_folder"],
                            f"cohort_exclusions_{f_type}_{dct_claims[f_type].state}_{dct_claims[f_type].year}_{sub_type}.parquet",
                        ),
                        engine=dct_claims[f_type].pq_engine,
                        index=False,
                    )
            else:
                cohort_filter_stats.to_parquet(
                    os.path.join(
                        dct_data_paths["export_folder"],
                        f"cohort_exclusions_{f_type}_{dct_claims[f_type].state}_{dct_claims[f_type].year}.parquet",
                    ),
                    engine=dct_claims[f_type].pq_engine,
                    index=False,
                )
        del dct_claims
        del dct_cohort_filter_stats
        gc.collect()

        pdf_patients = pdf_patients.merge(
            pdf_dob, left_index=True, right_index=True, how="inner"
        )
        del pdf_dob
        pdf_patients.to_csv(
            os.path.join(
                dct_data_paths["export_folder"], f"cohort_{state}_{year}.csv"
            ),
            index=True,
        )
        pdf_patients_all_years = pd.concat([pdf_patients_all_years,
                                            pdf_patients.reset_index(drop=False)],
                                           ignore_index=False)

        export_cohort_datasets(
            pdf_patients_all_years,
            year,
            state,
            lst_types_to_export,
            dct_filters["export"] if "export" in dct_filters else {},
            dct_data_paths,
            cms_format,
            clean_exports,
            preprocess_exports,
            logger_name,
        )

    shutil.rmtree(dct_data_paths["tmp_folder"], ignore_errors=True)


def export_cohort_datasets(  # pylint: disable=missing-param-doc
    pdf_cohort: pd.DataFrame,
    year: int,
    state: str,
    lst_types_to_export: List[str],
    dct_export_filters: dict,
    dct_data_paths: dict,
    cms_format: str = "MAX",
    clean_exports: bool = False,
    preprocess_exports: bool = False,
    logger_name: str = __file__,
) -> None:
    """
    Exports MAX files corresponding to the cohort as defined by the filters input to this function

    Parameters
    ----------
    pdf_cohort : pd.DataFrame
        Pandas dataframe with patient IDs (BENE_MSIS) and indicator flag denoting inclusion into the cohort (include=1)
    year : int
        Year of the claim files
    state : str
        State
    lst_types_to_export : list of str
        List of file types to export. Supported types are [ip, ot, ps, rx]
    dct_export_filters : dict
        Additional filters that should be applied to the raw claims of the selected cohort while exporting. Filter
        dictionary should be of the format:
        {claim_type_1: {range_[datatype]_[col_name]: (start, end),
                        excl_[col_name]: [0/1],
                        [col_name]: value,
                        ..}
        claim_type_2: ...}} date and numeric range type filters are currently supported. Filter names beginning with
        `excl_` with values set to 1 will exclude benes that have a positive value for that exclusion flag. Filter
        names that are just column names will restrict the result to benes with the filter value for the corresponding
        column.
        Eg: {'ip': {'range_numeric_age_prncpl_proc': (0, 18),
                                      'missing_dob': 0,
                                      'excl_female': 1}}
                              'ot': {'range_numeric_age_srvc_bgn': (0, 18),
                                      'missing_dob': 0,
                                      'excl_female': 1}}
                              }
        The example filter will exclude all IP claims of female benes and also claims with missing DOB. The resulting
        set will also be restricted to those of benes whose age is between 0-18 (inclusive of both 0 and 18) as of
        principal procedure date/ service begin date.
    dct_data_paths : dict
        Dictionary with information on raw claim files root folder and export folder. Should be of the format,
        {'source_root': /path/to/medicaid/folder,
         'export_folder': /path/to/export/data}
    cms_format : {'MAX', TAF'}
        CMS file format.
    clean_exports : bool, default=False
        Should the exported datasets be cleaned?
    preprocess_exports : bool, default=False
        Should the exported datasets be preprocessed?
    logger_name : str, default=__file__
        Logger name

    Raises
    ------
    FileNotFoundError
        Raised when any of file types requested to be imported does not exist for the state and year

    """
    logger = logging.getLogger(logger_name)
    dct_data_paths["tmp_folder"] = os.path.join(
        dct_data_paths["export_folder"], "tmp_files"
    )
    os.makedirs(dct_data_paths["export_folder"], exist_ok=True)
    os.makedirs(dct_data_paths["tmp_folder"], exist_ok=True)
    dct_claims = {}
    try:
        for claim_type in sorted(lst_types_to_export):
            if claim_type == "rx":
                # Not yet implemented for TAF
                if cms_format == "MAX":
                    dct_claims[claim_type] = max_file.MAXFile(
                        "rx",
                        year,
                        state,
                        dct_data_paths["source_root"],
                        clean=False,
                        preprocess=False,
                    )
            else:
                dct_claims[claim_type] = (
                    max_file.MAXFile.get_claim_instance(
                        claim_type,
                        year,
                        state,
                        dct_data_paths["source_root"],
                        clean=False,
                        preprocess=False,
                        **(
                            {}
                            if claim_type != "ip"
                            else {
                                "tmp_folder": os.path.join(
                                    dct_data_paths["tmp_folder"], claim_type
                                )
                            }
                        ),
                    )
                    if cms_format == "MAX"
                    else taf_file.TAFFile.get_claim_instance(
                        claim_type,
                        year,
                        state,
                        dct_data_paths["source_root"],
                        clean=False,
                        preprocess=False,
                        **(
                            {}
                            if claim_type != "ip"
                            else {
                                "tmp_folder": os.path.join(
                                    dct_data_paths["tmp_folder"], claim_type
                                )
                            }
                        ),
                    )
                )
    except FileNotFoundError as ex:
        logger.warning("%d data is missing for %s", year, state)
        logger.exception(ex)
        raise FileNotFoundError from ex

    for f_type in sorted(lst_types_to_export):
        logger.info("Exporting %s for %s (%d)", f_type, state, year)
        if cms_format == "MAX":
            dct_claims[f_type].df = dct_claims[f_type].df.loc[
                dct_claims[f_type].df.index.isin(
                    pdf_cohort.loc[pdf_cohort["include"] == 1].index.tolist()
                )
            ]
        else:
            for subtype in dct_claims[f_type].dct_files:
                dct_claims[f_type].dct_files[subtype] = (
                    dct_claims[f_type]
                    .dct_files[subtype]
                    .loc[
                        dct_claims[f_type]
                        .dct_files[subtype]
                        .index.isin(
                            pdf_cohort.loc[
                                pdf_cohort["include"] == 1
                            ].index.tolist()
                        )
                    ]
                )

        dct_claims[f_type].cache_results()
        if clean_exports or preprocess_exports:
            if clean_exports:
                dct_claims[f_type].clean()
            if preprocess_exports:
                dct_claims[f_type].preprocess()
            dct_claims[f_type].cache_results()
        if bool(dct_export_filters):
            lst_subtypes = (
                [None]
                if (cms_format == "MAX")
                else list(dct_claims[f_type].dct_files.keys())
            )
            for subtype in lst_subtypes:
                (dct_claims[f_type], filter_stats) = filter_claim_files(
                    dct_claims[f_type],
                    dct_export_filters,
                    os.path.join(dct_data_paths["tmp_folder"], f_type),
                    subtype=subtype,
                    logger_name=logger_name,
                )
                filter_stats.to_parquet(
                    os.path.join(
                        dct_data_paths["export_folder"],
                        f"export_exclusions_{f_type}_{dct_claims[f_type].state}_{dct_claims[f_type].year}"
                        + f"{'_' + subtype if bool(subtype) else ''}.parquet",
                    ),
                    engine=dct_claims[f_type].pq_engine,
                    index=False,
                )
        dct_claims[f_type].export(dct_data_paths["export_folder"])

    shutil.rmtree(dct_data_paths["tmp_folder"])

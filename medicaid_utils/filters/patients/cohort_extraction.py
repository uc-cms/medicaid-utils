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
    max_ot,
    max_ps,
    taf_file,
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
        Name of filter
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
        Filters to apply
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
                df_claim[f"excl_{filter_name}"] == int(dct_filter[filter_name])
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
                f" reduces {claim.ftype} {subtype} claim count to"
                f" {df_filter_counts[filter_name].values[0]}"
            )
            logger.info(filter_status)
    return claim, df_filter_counts


def extract_cohort(  # pylint: disable=missing-param-doc, too-many-arguments
    state: str,
    year: int,
    dct_diag_codes: dict,
    dct_proc_codes: dict,
    dct_cohort_filters: dict,
    dct_export_filters: dict,
    lst_types_to_export: List[str],
    data_root: str,
    dest_folder: str,
    clean_exports: bool = True,
    preprocess_exports: bool = True,
    logger_name: str = __file__,
) -> None:
    """
    Extracts and exports claim files corresponded cohort defined by the input filters

    Parameters
    ----------
    state : str
        State
    year : int
        Year
    dct_diag_codes : dict
        Dictionary of diagnosis codes. Should be in the format
            {condition_name: {['incl' / 'excl']: {[9/ 10]: list of codes} }
            Eg: {'oud_nqf': {'incl': {9: ['3040','3055']}}}
    dct_proc_codes : dict
        Dictionary of procedure codes. Should be in the format
            {procedure_name: {procedure_system_code: list of codes} }
            Eg: {'methadone_7': {7: 'HZ81ZZZ,HZ84ZZZ,HZ85ZZZ,HZ86ZZZ,HZ91ZZZ,HZ94ZZZ,HZ95ZZZ,'
                                   'HZ96ZZZ'.split(",")}}
    dct_cohort_filters : dict
        Cohort filters
    dct_export_filters : dict
        Export filters
    lst_types_to_export : List[str]
        List of types to export. Currently supported types are ip, ot, rx, ps.
    data_root : str
        Root folder of raw claim files
    dest_folder : str
        Folder to export the datasets to
    clean_exports : bool, default=False
        Should the exported datasets be cleaned?
    preprocess_exports : bool, default=False
        Should the exported datasets be preprocessed?
    logger_name : bool, default=__file__
        Logger name

    Raises
    ------
    FileNotFoundError
        Raised when any of file types requested to be imported does not exist for the state and year

    """
    logger = logging.getLogger(logger_name)
    tmp_folder = os.path.join(dest_folder, "tmp_files")
    dct_claims = {}
    try:
        dct_claims["ip"] = max_ip.MAXIP(
            year, state, data_root, clean=True, preprocess=True
        )
        dct_claims["ot"] = max_ot.MAXOT(
            year,
            state,
            data_root,
            clean=True,
            preprocess=True,
            tmp_folder=os.path.join(tmp_folder, "ot"),
        )
        dct_claims["rx"] = max_file.MAXFile(
            "rx", year, state, data_root, clean=False, preprocess=False
        )
        dct_claims["ps"] = max_ps.MAXPS(
            year,
            state,
            data_root,
            clean=True,
            preprocess="ps" in dct_cohort_filters,
            tmp_folder=os.path.join(tmp_folder, "ps"),
        )
        logger.info(
            "%s (%d) has %d benes",
            state,
            year,
            dct_claims["ps"].df.shape[0].compute(),
        )
    except FileNotFoundError as ex:
        logger.warning("%d data is missing for %s", year, state)
        logger.exception(ex)
        raise FileNotFoundError from ex
    os.makedirs(dest_folder, exist_ok=True)
    os.makedirs(tmp_folder, exist_ok=True)

    for f_type in dct_cohort_filters:
        dct_claims[f_type] = filter_claim_files(
            dct_claims[f_type],
            dct_cohort_filters,
            os.path.join(tmp_folder, f_type),
            logger_name,
        )
    pdf_patients = None
    if bool(dct_diag_codes) | bool(dct_proc_codes):
        pdf_patients, _ = dx_and_proc.get_patient_ids_with_conditions(
            dct_diag_codes,
            dct_proc_codes,
            logger_name=logger_name,
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
            ],
        )
        pdf_patients = pdf_patients.assign(include=1)
    else:
        pdf_patients = (
            pd.concat(
                [
                    claim.df.assign(
                        **{claim.index_col: claim.df.index, "include": 1}
                    )[[claim.index_col, "include"]].compute()
                    for f_type, claim in dct_claims.items()
                ]
            )
            .drop_duplicates()
            .set_index(dct_claims["ps"].index_col)
        )
    pdf_patients = pdf_patients.assign(YEAR=year, STATE_CD=state)

    logger.info(
        "%s (%d) has %d benes with specified  conditions/ procedures",
        state,
        year,
        pdf_patients.shape[0],
    )

    if "ps" not in dct_cohort_filters:
        dct_claims["ps"].df = (
            dct_claims["ps"]
            .df.loc[
                dct_claims["ps"].df.index.isin(
                    pdf_patients.loc[
                        pdf_patients["include"] == 1
                    ].index.tolist()
                )
            ]
            .persist()
        )
        dct_claims["ps"].df = dct_claims["ps"].cache_results(repartition=True)
        dct_claims["ps"] = filter_claim_files(
            dct_claims["ps"], {}, os.path.join(tmp_folder, "ps"), logger_name
        )
        logger.info(
            "%s (%d) has  %d benes  with specified conditions who also meet the cohort inclusion  criteria",
            state,
            year,
            pdf_patients.loc[pdf_patients["include"] == 1].shape[0],
        )
        pdf_patients["include"] = pdf_patients["include"].where(
            pdf_patients.index.isin(
                dct_claims["ps"].df.index.compute().tolist()
            ),
            0,
        )
        logger.info(
            "For %s (%d), %d benes remain after cleaning PS",
            state,
            year,
            pdf_patients.loc[pdf_patients["include"] == 1].shape[0],
        )
    pdf_dob = dct_claims["ps"].df[["birth_date"]].compute()
    del dct_claims
    gc.collect()

    pdf_patients = pdf_patients.merge(
        pdf_dob, left_index=True, right_index=True, how="inner"
    )
    pdf_patients.to_csv(
        os.path.join(dest_folder, f"cohort_{state}_{year}.csv"), index=True
    )

    shutil.rmtree(tmp_folder)
    export_cohort_max_datasets(
        pdf_patients,
        year,
        state,
        data_root,
        lst_types_to_export,
        dest_folder,
        dct_export_filters,
        clean_exports,
        preprocess_exports,
        logger_name,
    )


def export_cohort_max_datasets(  # pylint: disable=missing-param-doc
    pdf_cohort: pd.DataFrame,
    year: int,
    state: str,
    data_root: str,
    lst_types_to_export: List[str],
    dest_folder: str,
    dct_export_filters: dict,
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
    data_root : str
        Root folder of the raw claim files
    lst_types_to_export : list of str
        List of file types to export. Supported types are [ip, ot, ps, rx]
    dest_folder : str
        Folder to export the datasets to
    dct_export_filters : dict
        Dictionary of filters to apply to the export dataset
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
    tmp_folder = os.path.join(dest_folder, "tmp_files")
    os.makedirs(dest_folder, exist_ok=True)
    os.makedirs(tmp_folder, exist_ok=True)
    dct_claims = {}
    for f_type in sorted(lst_types_to_export):
        try:
            if f_type == "ip":
                dct_claims[f_type] = max_ip.MAXIP(
                    year, state, data_root, clean=False, preprocess=False
                )
            elif f_type == "ot":
                dct_claims[f_type] = max_ot.MAXOT(
                    year,
                    state,
                    data_root,
                    clean=False,
                    preprocess=False,
                    tmp_folder=os.path.join(tmp_folder, f_type),
                )
            elif f_type == "ps":
                dct_claims[f_type] = max_ps.MAXPS(
                    year,
                    state,
                    data_root,
                    clean=False,
                    preprocess=False,
                    tmp_folder=os.path.join(tmp_folder, f_type),
                )
            else:
                dct_claims[f_type] = max_file.MAXFile(
                    f_type,
                    year,
                    state,
                    data_root,
                    clean=False,
                    preprocess=False,
                )
        except FileNotFoundError as ex:
            logger.warning("%d %s data is missing for %s", year, f_type, state)
            logger.exception(ex)
            raise FileNotFoundError from ex
    for f_type in sorted(lst_types_to_export):
        logger.info("Exporting %s for %s (%d)", f_type, state, year)
        dct_claims[f_type].df = dct_claims[f_type].df.loc[
            dct_claims[f_type].df.index.isin(
                pdf_cohort.loc[pdf_cohort["include"] == 1].index.tolist()
            )
        ]
        dct_claims[f_type].df = dct_claims[f_type].cache_results()

        if clean_exports or preprocess_exports:
            if clean_exports:
                dct_claims[f_type].clean()
            if preprocess_exports:
                dct_claims[f_type].preprocess()
            dct_claims[f_type].df = dct_claims[f_type].cache_results()
        if bool(dct_export_filters):
            dct_claims[f_type] = filter_claim_files(
                dct_claims[f_type],
                dct_export_filters,
                os.path.join(tmp_folder, f"{f_type}"),
                logger_name,
            )
        dct_claims[f_type].export(dest_folder)
    shutil.rmtree(tmp_folder)

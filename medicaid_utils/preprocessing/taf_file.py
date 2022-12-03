"""This module has TAFFile class from which is the base class for all TAF file type classes"""
import os
import errno
import shutil
import logging

import numpy as np
import dask.dataframe as dd
import pandas as pd

from medicaid_utils.common_utils import dataframe_utils, links


class TAFFile:
    """Parent class for all TAF file classes, each of which will have clean and preprocess functions"""

    def __init__(
        self,
        ftype: str,
        year: int,
        state: str,
        data_root: str,
        index_col: str = "BENE_MSIS",
        clean: bool = True,
        preprocess: bool = True,
        tmp_folder: str = None,
        pq_engine: str = "pyarrow",
    ):
        """
        Initializes TAF file object by preloading and preprocessing(if opted in) the associated files

        Parameters
        ----------
        ftype : {'ip', 'ot', 'rx', 'ps'}
            TAF Claim type.
        year : int
            Year of claim file
        state : str
            State of claim file
        data_root : str
            Root folder of raw claim files
        index_col : str, default='BENE_MSIS'
            Index column name. Eg. BENE_MSIS or MSIS_ID. The raw file is expected to be already
        sorted with index column
        clean : bool, default=True
            Should the associated files be cleaned?
        preprocess : bool, default=True
            Should the associated files be preprocessed?
        tmp_folder : str, default=None
            Folder location to use for caching intermediate results. Can be turned off by not passing this argument.
        pq_engine : str, default='pyarrow'
            Parquet engine to use

        Raises
        ------
        FileNotFoundError
            Raised when any of the subtype files are missing

        """
        self.data_root = data_root
        self.dct_fileloc = links.get_taf_parquet_loc(
            data_root, ftype, state, year
        )
        self.ftype = ftype
        self.index_col = index_col
        self.year = year
        self.state = state
        self.tmp_folder = tmp_folder
        self.pq_engine = pq_engine
        self.allowed_missing_ftypes = [
            "occurrence_code",
            "disability",
            "mfp",
            "waiver",
            "home_health",
            "managed_care",
        ]
        for subtype in list(self.dct_fileloc.keys()):
            if not os.path.exists(self.dct_fileloc[subtype]):
                print(f"{subtype} does not exist for {state}")
                if subtype not in self.allowed_missing_ftypes:
                    raise FileNotFoundError(
                        errno.ENOENT,
                        os.strerror(errno.ENOENT),
                        self.dct_fileloc[subtype],
                    )
                self.dct_fileloc.pop(subtype)
        self.dct_files = {
            ftype: dd.read_parquet(
                file_loc, index=False, engine=self.pq_engine
            ).set_index(index_col, sorted=True)
            for ftype, file_loc in self.dct_fileloc.items()
        }

        self.dct_collist = {
            ftype: list(claim_df.columns)
            for ftype, claim_df in self.dct_files.items()
        }

        # This dictionary variable can be used to filter out data that will not met minimum quality expections
        self.dct_default_filters = {}
        if clean:
            self.clean()
        if preprocess:
            self.preprocess()

    @classmethod
    def get_claim_instance(
        cls, claim_type, *args, **kwargs
    ):  # pylint: disable=missing-param-doc
        """
        Returns an instance of the requested claim type

        Parameters
        ----------
        claim_type : {'ip', 'ot', 'cc', 'rx'}
            Claim type
        *args : list
            List of position arguments
        **kwargs : dict
            Dictionary of keyword arguments

        """
        return next(
            claim
            for claim in cls.__subclasses__()
            if claim.__name__ == f"TAF{claim_type.upper()}"
        )(*args, **kwargs)

    def cache_results(
        self, subtype=None, repartition=False
    ):  # pylint: disable=missing-param-doc
        """
        Save results in intermediate steps of some lengthy processing. Saving intermediate results speeds up
        processing, and avoid dask cluster crashes for large datasets

        Parameters
        ----------
        subtype : str, default=None
            File type. Eg. 'header'. If empty, all subtypes will be cached
        repartition : bool, default=False
            Repartition the dask dataframe

        """
        for f_subtype in list(self.dct_files.keys()):
            if (subtype is not None) and (f_subtype != subtype):
                continue
            if self.tmp_folder is not None:
                if repartition:
                    self.dct_files[f_subtype] = (
                        self.dct_files[f_subtype]
                        .repartition(partition_size="20MB")
                        .persist()
                    )  # Patch, currently to_parquet results
                    # in error when any of the partitions is empty
                self.pq_export(
                    f_subtype, os.path.join(self.tmp_folder, f_subtype)
                )

    def pq_export(self, f_subtype, dest_path_and_fname):
        """
        Export parquet files (overwrite safe)

        Parameters
        ----------
        f_subtype : str
            File type. Eg. 'header'
        dest_path_and_fname : str
            Destination path

        """
        shutil.rmtree(dest_path_and_fname + "_tmp", ignore_errors=True)
        try:
            self.dct_files[f_subtype].to_parquet(
                dest_path_and_fname + "_tmp",
                engine=self.pq_engine,
                write_index=True,
            )
        except Exception:  # pylint: disable=broad-except
            self.dct_files[f_subtype].to_parquet(
                dest_path_and_fname + "_tmp",
                engine={"fastparquet", "pyarrow"}
                .difference([self.pq_engine])
                .pop(),
                write_index=True,
                # **(
                #     {"schema": "infer"}
                #     if (self.pq_engine == "pyarrow")
                #     else {}
                # ),
            )
        del self.dct_files[f_subtype]
        shutil.rmtree(dest_path_and_fname, ignore_errors=True)
        os.rename(dest_path_and_fname + "_tmp", dest_path_and_fname)
        self.dct_files[f_subtype] = dd.read_parquet(
            dest_path_and_fname, index=False, engine=self.pq_engine
        )
        if (self.dct_files[f_subtype].npartitions > 1) or len(
            self.dct_files[f_subtype]
        ) > 0:
            self.dct_files[f_subtype] = self.dct_files[f_subtype].set_index(
                self.index_col, sorted=True
            )
        else:
            self.dct_files[f_subtype] = self.dct_files[f_subtype].set_index(
                self.index_col
            )

    def clean(self):
        """Cleaning routines to processes date and gender columns, and add duplicate check flags."""
        self.process_date_cols()
        self.flag_duplicates()

    def preprocess(self):
        """Add basic constructed variables"""

    def export(
        self, dest_folder, output_format="csv"
    ):  # pylint: disable=missing-param-doc
        """
        Exports the files.

        Parameters
        ----------
        dest_folder : str
            Destination folder
        output_format : str, default='csv'
            Export format. Csv is the currently supported format

        """
        lst_subtype = list(self.dct_files.keys())
        for subtype in lst_subtype:
            if output_format == "csv":
                self.dct_files[subtype].to_csv(
                    os.path.join(
                        dest_folder,
                        f"{self.ftype}_{subtype}_{self.year}_{self.state}.csv",
                    ),
                    index=True,
                    single_file=True,
                )
            else:
                self.pq_export(
                    subtype,
                    self.dct_fileloc[subtype].split(
                        self.data_root + os.path.sep
                    )[1],
                )

    def clean_diag_codes(self) -> None:
        """Clean diagnostic code columns by removing non-alphanumeric characters and converting them to upper case"""
        for ftype in self.dct_files:
            df = self.dct_files[ftype]
            lst_diag_cd_col = [
                col
                for col in df.columns
                if col.startswith("DGNS_CD_") or (col == ["ADMTG_DGNS_CD"])
            ]
            if len(lst_diag_cd_col) > 0:
                df = df.map_partitions(
                    lambda pdf: pdf.assign(
                        **{
                            col: pdf[col]
                            .str.replace("[^a-zA-Z0-9]+", "", regex=True)
                            .str.upper()
                            for col in lst_diag_cd_col  # pylint: disable=cell-var-from-loop
                        }
                    )
                )
                self.dct_files[ftype] = df

    def clean_proc_codes(self):
        """Clean diagnostic code columns by removing non-alphanumeric characters and converting them to upper case"""
        for ftype in self.dct_files:
            df = self.dct_files[ftype]
            lst_prcdr_cd_col = [
                col
                for col in df.columns
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
            if len(lst_prcdr_cd_col) > 0:
                df = df.map_partitions(
                    lambda pdf: pdf.assign(
                        **{
                            col: pdf[col]
                            .str.replace("[^a-zA-Z0-9]+", "", regex=True)
                            .str.upper()
                            for col in lst_prcdr_cd_col  # pylint: disable=cell-var-from-loop
                        }
                    )
                )
                self.dct_files[ftype] = df

    def flag_duplicates(self):
        """
        Removes duplicated rows.

        Things to note:
            - All TAF claims are monthly files. This function keeps the most recent file version date for each month
            using the variables IP_VRSN, LT_VRSN, OT_VRSN, and RX_VRSN.
            - Retains only the claims with maximum value of production data run ID (DA_RUN_ID) for each claim ID
            (CLM_ID).

        (Identifying beneficiaries with a substance use disorder
        (https://www.medicaid.gov/medicaid/data-and-systems/downloads/macbis/sud_techspecs.docx)

        """
        for ftype in self.dct_files:
            logging.info("Flagging duplicates for %s", ftype)
            df = self.dct_files[ftype]
            df = dataframe_utils.fix_index(df, self.index_col, True)
            df = df.assign(
                **{
                    col: dd.to_numeric(df[col], errors="coerce")
                    .fillna(-1)
                    .astype(int)
                    for col in ["DA_RUN_ID", f"{self.ftype.upper()}_VRSN"]
                    if col in df.columns
                }
            )
            df = dataframe_utils.fix_index(df, self.index_col, True)
            df = df.map_partitions(
                lambda pdf: pdf.assign(
                    excl_duplicated=pdf.assign(_index_col=pdf.index)[
                        [
                            col
                            for col in pdf.columns
                            if col != "excl_duplicated"
                        ]
                    ]
                    .duplicated(keep="first")
                    .astype(int)
                )
            )
            df = df.loc[df["excl_duplicated"] == 0]
            if ("DA_RUN_ID" in df.columns) and ("CLM_ID" in df.columns):
                df = df.map_partitions(
                    lambda pdf: pdf.assign(
                        max_run_id=pdf.groupby("CLM_ID")[
                            "DA_RUN_ID"
                        ].transform("max")
                    )
                )
                df = df.loc[df["DA_RUN_ID"] == df["max_run_id"]].drop(
                    "max_run_id", axis=1
                )
            if "filing_period" in df.columns:
                df = df.map_partitions(
                    lambda pdf: pdf.assign(
                        max_version=pdf.groupby("filing_period")[
                            f"{self.ftype.lower()}_version"
                        ].transform("max")
                    )
                )
                df = df.loc[
                    df[f"{self.ftype.lower()}_version"] == df["max_version"]
                ].drop("max_version", axis=1)
            self.dct_files[ftype] = df

    def flag_ffs_and_encounter_claims(self):
        """
        Flags claims where CLM_TYPE_CD is equal to one of the following values:
            1: A FFS Medicaid or Medicaid-expansion claim
            3: Medicaid or Medicaid-expanding managed care encounter record
            A: Separate CHIP (Title XXI) FFS claim
            C: Separate CHIP (Title XXI) encounter record

        (Identifying beneficiaries with a substance use disorder
        (https://www.medicaid.gov/medicaid/data-and-systems/downloads/macbis/sud_techspecs.docx)

        """
        df = self.dct_files["base"]
        df = df.assign(
            ffs_or_encounter_claim=df["CLM_TYPE_CD"]
            .str.strip()
            .isin([1, 3, "A", "C"])
            .astype(int)
        )
        self.dct_files["base"] = df

    def process_date_cols(self):
        """
        Convert datetime columns to datetime type and add basic date based constructed variables

        New columns:

            - birth_year, birth_month, birth_day - date components of EL_DOB (date of birth)
            - birth_date - birth date (EL_DOB)
            - death - 0 or 1, if DEATH_DT is not empty and falls in the claim year or before
            - age - age in years, integer format
            - age_decimal - age in years, with decimals
            - age_day - age in days
            - adult - 0 or 1, 1 when patient's age is in [18,115]
            - child - 0 or 1, 1 when patient's age is in [0,17]

        If ftype == 'ip':
            Clean/ impute admsn_date and add ip duration related columns
            New column(s):

                - admsn_date - Admission date (ADMSN_DT)
                - srvc_bgn_date - Service begin date (SRVC_BGN_DT)
                - srvc_end_date - Service end date (SRVC_END_DT)
                - prncpl_proc_date - Principal procedure date (PRCDR_CD_DT_1)
                - missing_admsn_date - 0 or 1, 1 when admission date is missing
                - missing_prncpl_proc_date - 0 or 1, 1 when principal procedure date is missing
                - flag_admsn_miss - 0 or 1, 1 when admsn_date was imputed
                - los - ip service duration in days
                - ageday_admsn - age in days as on admsn_date
                - age_admsn - age in years, with decimals, as on admsn_date
                - age_prncpl_proc - age in years as on principal procedure date
                - age_day_prncpl_proc - age in days as on principal procedure date

        if ftype == 'ot':
            Adds duration column, provided service end and begin dates are clean
            New Column(s):

                - srvc_bgn_date - Service begin date (SRVC_BGN_DT)
                - srvc_end_date - Service end date (SRVC_END_DT)
                - diff & duration - duration of service in days
                - age_day_srvc_bgn - age in days as on service begin date
                - age_srvc_bgn - age in years, with decimals, as on service begin date

        if ftype == 'ps:
            New Column(s):

                - date_of_death - Date of death (DEATH_DT)

        """
        for ftype in self.dct_files:
            logging.info("Processing date columns for %s", ftype)
            df = self.dct_files[ftype]
            if self.ftype in ["ip", "lt", "ot", "ps", "rx"]:
                dct_date_col = {
                    "BIRTH_DT": "birth_date",
                    "ADMSM_DT": "admsn_date",
                    "ADMSN_DT": "admsn_date",
                    "PRCDR_CD_DT_1": "prncpl_proc_date",
                    "SRVC_BGN_DT": "srvc_bgn_date",
                    "SRVC_END_DT": "srvc_end_date",
                    "LINE_SRVC_BGN_DT": "srvc_bgn_date",
                    "LINE_SRVC_END_DT": "srvc_end_date",
                    "DEATH_DT": "death_date",
                    "IMGRTN_STUS_5YR_BAR_END_DT": "immigration_status_5yr_bar_end_date",
                    "ENRLMT_START_DT": "enrollment_start_date",
                    "ENRLMT_END_DT": "enrollment_end_date",
                }
                dct_date_col = {
                    col: new_col_name
                    for col, new_col_name in dct_date_col.items()
                    if col in df.columns
                }
                df = df.assign(
                    **{
                        new_name: df[col]
                        for col, new_name in dct_date_col.items()
                    }
                )

                # converting lst_col columns to datetime type
                lst_col_to_convert = [
                    dct_date_col[col]
                    for col in dct_date_col.keys()
                    if (
                        df[[dct_date_col[col]]]
                        .select_dtypes(include=[np.datetime64])
                        .shape[1]
                        == 0
                    )
                ]
                if not bool(lst_col_to_convert):
                    continue
                df = dataframe_utils.convert_ddcols_to_datetime(
                    df, lst_col_to_convert
                )

                if self.ftype in ["ip", "ot"]:
                    if f"{self.ftype.upper()}_FIL_DT" in df.columns:
                        if self.ftype == "ip":
                            df = df.assign(
                                filing_period=df[
                                    f"{self.ftype.upper()}_FIL_DT"
                                ].str[1:7],
                            )
                        else:
                            df = df.assign(
                                filing_period=df[
                                    f"{self.ftype.upper()}_FIL_DT"
                                ].str[0:6],
                            )
                        df = df.assign(
                            year=df.filing_period.str[:4].astype(int)
                        )
                        df = df.map_partitions(
                            lambda pdf: pdf.assign(
                                **{
                                    f"{self.ftype.lower()}_version": pd.to_numeric(
                                        pdf[f"{self.ftype.upper()}_VRSN"],
                                        errors="coerce",
                                    )
                                }
                            )
                        )

                else:
                    df = df.assign(
                        year=dd.to_numeric(
                            df.RFRNC_YR, errors="coerce"
                        ).astype(int)
                    )

                if "birth_date" in df.columns:
                    df = df.assign(
                        birth_year=df.birth_date.dt.year,
                        birth_month=df.birth_date.dt.month,
                        birth_day=df.birth_date.dt.day,
                    )
                    df = df.assign(
                        age_day=(
                            dd.to_datetime(
                                df.year.astype(str) + "1231", format="%Y%m%d"
                            )
                            - df.birth_date
                        ).dt.days,
                        age_decimal=(
                            dd.to_datetime(
                                df.year.astype(str) + "1231", format="%Y%m%d"
                            )
                            - df.birth_date
                        )
                        / np.timedelta64(1, "Y"),
                    )
                    if "AGE" not in df.columns:
                        df = df.assign(age=df.year - df.birth_year)
                if "AGE" in df.columns:
                    df = df.assign(
                        age=dd.to_numeric(df["AGE"], errors="coerce")
                    )
                if "AGE_GRP_CD" in df.columns:
                    df = df.assign(
                        age_group=dd.to_numeric(
                            df["AGE_GRP_CD"], errors="coerce"
                        )
                        .fillna(-1)
                        .astype(int),
                    )
                if "age" in df.columns:
                    df = df.assign(
                        adult=df["age"]
                        .between(18, 64, inclusive="both")
                        .astype(pd.Int64Dtype())
                        .fillna(-1)
                        .astype(int),
                        elderly=(df["age"] >= 65)
                        .astype(pd.Int64Dtype())
                        .fillna(-1)
                        .astype(int),
                        child=(df["age"] <= 17)
                        .astype(pd.Int64Dtype())
                        .fillna(-1)
                        .astype(int),
                    )

                if (self.ftype != "ps") and ("age" in df.columns):
                    df = df.map_partitions(
                        lambda pdf: pdf.assign(
                            adult=pdf.groupby(pdf.index)["adult"].transform(
                                max
                            ),
                            child=pdf.groupby(pdf.index)["child"].transform(
                                max
                            ),
                            age=pdf.groupby(pdf.index)["age"].transform(max),
                            age_day=pdf.groupby(pdf.index)[
                                "age_day"
                            ].transform(max),
                            age_decimal=pdf.groupby(pdf.index)[
                                "age_decimal"
                            ].transform(max),
                        )
                    )
                if "death_date" in df.columns:
                    df = df.assign(
                        death=(
                            df.death_date.dt.year.fillna(df.year + 10).astype(
                                int
                            )
                            <= df.year
                        )
                        .fillna(False)
                        .astype(int)
                    )
                if "DEATH_IND" in df.columns:
                    df = df.assign(
                        death=(
                            dd.to_numeric(df["DEATH_IND"], errors="coerce")
                            == 1
                        )
                        .fillna(False)
                        .astype(int)
                    )
                if (self.ftype == "ip") and ("admsn_date" in df.columns):
                    df = df.assign(
                        missing_admsn_date=df["admsn_date"]
                        .isnull()
                        .astype(int)
                    )

                    df = df.assign(
                        admsn_date=df["admsn_date"].where(
                            ~df["admsn_date"].isnull(), df["srvc_bgn_date"]
                        ),
                        los=(df["srvc_end_date"] - df["admsn_date"]).dt.days
                        + 1,
                    )
                    df = df.assign(
                        los=df["los"].where(
                            (
                                (df["year"] >= df["admsn_date"].dt.year)
                                & (df["admsn_date"] <= df["srvc_end_date"])
                            ),
                            np.nan,
                        )
                    )

                    df = df.assign(
                        age_day_admsn=(
                            df["admsn_date"] - df["birth_date"]
                        ).dt.days,
                    )
                    df = df.assign(
                        age_admsn=(
                            df["age_day_admsn"].fillna(0) / 365.25
                        ).astype(int),
                    )

                if (self.ftype == "ot") and ("srvc_bgn_date" in df.columns):
                    df = df.assign(
                        duration=(
                            df["srvc_end_date"] - df["srvc_bgn_date"]
                        ).dt.days
                    )
                    df = df.assign(
                        duration=df["duration"].where(
                            (df["srvc_bgn_date"] <= df["srvc_end_date"]),
                            np.nan,
                        )
                    )
                    if "birth_date" in df.columns:
                        df = df.assign(
                            age_day_srvc_bgn=(
                                df["srvc_bgn_date"] - df["birth_date"]
                            ).dt.days,
                        )
                        df = df.assign(
                            age_srvc_bgn=(
                                df["age_day_srvc_bgn"].fillna(0) / 365.25
                            ).astype(int),
                        )

                self.dct_files[ftype] = df

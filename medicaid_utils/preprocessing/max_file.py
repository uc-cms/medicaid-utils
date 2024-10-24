"""This module has MAXFile class from which is the base class for all MAX file type classes"""
import os
import errno
import shutil

import dask.dataframe as dd
import numpy as np
import pandas as pd

from medicaid_utils.common_utils import dataframe_utils, links


class MAXFile:
    """Parent class for all MAX file classes, each of which will have clean and preprocess functions"""

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
        Initializes MAX file object by preloading and preprocessing(if opted in) the file

        Parameters
        ----------
        ftype : {'ip', 'ot', 'rx', 'ps', 'cc'}
            MAX Claim type.
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
            Raised when raw claim files are missing

        """
        self.data_root = data_root
        self.fileloc = links.get_max_parquet_loc(data_root, ftype, state, year)
        self.ftype = ftype
        self.index_col = index_col
        self.year = year
        self.state = state
        self.tmp_folder = tmp_folder
        if not os.path.exists(self.fileloc):
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), self.fileloc
            )
        self.pq_engine = pq_engine
        self.df = dd.read_parquet(
            self.fileloc, index=False, engine=self.pq_engine
        )
        if 'BENE_MSIS' not in self.df.columns:
            self.index_col = 'MSIS_ID'
            self.df = self.df.set_index(self.index_col, sorted=False)
            self.cache_results()
        self.df = self.df.assign(
            HAS_BENE=(self.df["BENE_ID"].fillna("").str.len() > 0).astype(int)
        )
        if 'BENE_MSIS' not in self.df.columns:
            self.df = self.df.map_partitions(
                lambda pdf: pdf.assign(
                    BENE_MSIS=pdf['STATE_CD'] + "-" +
                              pdf['HAS_BENE'].astype(str) +
                              "-" + pdf['BENE_ID'].combine_first(
                        pdf.index.astype(str)
                    )
                )
            )
            self.cache_results()
            self.index_col = index_col
            self.df = self.df.set_index(index_col, sorted=False)
            self.cache_results()

        self.df = self.df.set_index(index_col, sorted=True)

        self.lst_raw_col = list(self.df.columns)

        # This dictionary variable can be used to filter out data that will
        # not met minimum quality expections
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
            if claim.__name__ == f"MAX{claim_type.upper()}"
        )(*args, **kwargs)

    def cache_results(
        self, repartition=False
    ):  # pylint: disable=missing-param-doc
        """
        Save results in intermediate steps of some lengthy processing. Saving intermediate results speeds up
        processing

        Parameters
        ----------
        repartition : bool, default=False
            Repartition the dask dataframe
        """
        if self.tmp_folder is not None:
            if repartition:
                self.df = self.df.repartition(
                    partition_size="20MB"
                ).persist()  # Patch, currently to_parquet results
                # in error when any of the partitions is empty
            self.pq_export(self.tmp_folder)

    def pq_export(self, dest_path_and_fname, repartition=False):
        """
        Export parquet files (overwrite safe)

        Parameters
        ----------
        dest_path_and_fname : str
            Destination path
        repartition : bool, default=False
            Repartition the dask dataframe

        """
        shutil.rmtree(dest_path_and_fname + "_tmp", ignore_errors=True)
        os.makedirs(os.path.dirname(dest_path_and_fname), exist_ok=True)
        if repartition:
            self.df = self.df.repartition(partition_size="20MB")
        try:
            self.df.to_parquet(
                dest_path_and_fname + "_tmp",
                engine=self.pq_engine,
                write_index=True,
            )
        except Exception:  # pylint: disable=broad-except
            self.df.to_parquet(
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
        del self.df
        shutil.rmtree(dest_path_and_fname, ignore_errors=True)
        os.rename(dest_path_and_fname + "_tmp", dest_path_and_fname)
        self.df = dd.read_parquet(
            dest_path_and_fname, index=False, engine=self.pq_engine
        ).set_index(self.index_col, sorted=True)
        return self.df

    def clean(self):
        """Cleaning routines to processes date and gender columns"""
        # Date columns will be cleaned and all commonly used date based
        # variables are constructed
        # in this step
        self.process_date_cols()
        self.add_gender()

    def preprocess(self):
        """Add basic constructed variables"""

    def export(
        self, dest_folder, output_format="csv", repartition=False
    ):  # pylint: disable=missing-param-doc
        """
        Exports the files.

        Parameters
        ----------
        dest_folder : str
            Destination folder
        output_format : str, default='csv'
            Export format. Csv is the currently supported format
        repartition : bool, default=False
            Repartition the dask dataframe

        """
        if output_format == "csv":
            self.df.to_csv(
                os.path.join(
                    dest_folder, f"{self.ftype}_{self.year}_{self.state}.csv"
                ),
                index=True,
                single_file=True,
            )
        else:
            self.pq_export(
                self.fileloc.split(self.data_root + os.path.sep)[1],
                repartition=repartition,
            )

    def add_gender(self) -> None:
        """Adds integer 'female' column based on 'EL_SEX_CD' column. Undefined values ('U') in EL_SEX_CD column will
        result in female column taking the value -1"""
        if "EL_SEX_CD" in self.df.columns:
            self.df = self.df.map_partitions(
                lambda pdf: pdf.assign(
                    female=np.select(
                        [
                            pdf["EL_SEX_CD"].str.strip() == "F",
                            pdf["EL_SEX_CD"].str.strip() == "M",
                        ],
                        [1, 0],
                        default=-1,
                    ).astype(int)
                )
            )

    def clean_diag_codes(self):
        """Clean diagnostic code columns by removing non-alphanumeric characters and converting them to upper case"""
        if (
            len([col for col in self.df.columns if col.startswith("DIAG_CD_")])
            > 0
        ):
            self.df = self.df.map_partitions(
                lambda pdf: pdf.assign(
                    **{
                        col: pdf[col]
                        .str.replace("[^a-zA-Z0-9]+", "", regex=True)
                        .str.upper()
                        for col in pdf.columns
                        if col.startswith("DIAG_CD_")
                    }
                )
            )

    def clean_proc_codes(self):
        """Clean diagnostic code columns by removing non-alphanumeric characters and converting them to upper case"""
        if (
            len(
                [
                    col
                    for col in self.df.columns
                    if col.startswith("PRCDR_CD")
                    and (not col.startswith("PRCDR_CD_SYS"))
                ]
            )
            > 0
        ):
            self.df = self.df.map_partitions(
                lambda pdf: pdf.assign(
                    **{
                        col: pdf[col]
                        .str.replace("[^a-zA-Z0-9]+", "", regex=True)
                        .str.upper()
                        for col in pdf.columns
                        if col.startswith("PRCDR_CD")
                        and (not col.startswith("PRCDR_CD_SYS"))
                    }
                )
            )

    def process_date_cols(self):
        """
        Convert datetime columns to datetime type and add basic date based constructed variables

        New columns:

            - birth_year, birth_month, birth_day - date compoments of EL_DOB (date of birth)
            - birth_date - birth date (EL_DOB)
            - death - 0 or 1, if EL_DOD or MDCR_DOD is not empty and falls in the claim year or before
            - age - age in years, integer format
            - age_day - age in days
            - adult - 0 or 1, 1 when patient's age is in [18,115]
            - child - 0 or 1, 1 when patient's age is in [0,17]

        if ftype == 'ip':
            Clean/ impute admsn_date and add ip duration related columns
            New column(s):

                - admsn_date - Admission date (ADMSN_DT)
                - srvc_bgn_date - Service begin date (SRVC_BGN_DT)
                - srvc_end_date - Service end date (SRVC_END_DT)
                - prncpl_proc_date - Principal procedure date (PRNCPL_PRCDR_DT)
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

                - date_of_death - Date of death (EL_DOD)
                - medicare_date_of_death - Medicare date of death (MDCR_DOD)

        """
        if self.ftype in ["ip", "lt", "ot", "ps", "rx"]:
            df = self.df.assign(
                **{
                    "year": self.df[col].astype(int)
                    for col in ["MAX_YR_DT", "YR_NUM"]
                    if col in self.df.columns
                }
            )
            dct_date_col = {
                "EL_DOB": "birth_date",
                "ADMSN_DT": "admsn_date",
                "SRVC_BGN_DT": "srvc_bgn_date",
                "SRVC_END_DT": "srvc_end_date",
                "EL_DOD": "date_of_death",
                "MDCR_DOD": "medicare_date_of_death",
                "PRNCPL_PRCDR_DT": "prncpl_proc_date",
            }

            dct_date_col = {
                col: new_col_name
                for col, new_col_name in dct_date_col.items()
                if col in df.columns
            }
            df = df.assign(
                **{new_name: df[col] for col, new_name in dct_date_col.items()}
            )
            # converting lst_col columns to datetime type
            lst_col_to_convert = [
                new_name
                for col, new_name in dct_date_col.items()
                if (
                    df[[dct_date_col[col]]]
                    .select_dtypes(include=[np.datetime64])
                    .shape[1]
                    == 0
                )
            ]
            df = dataframe_utils.convert_ddcols_to_datetime(
                df, lst_col_to_convert
            )
            df = df.assign(
                birth_year=df.birth_date.dt.year,
                birth_month=df.birth_date.dt.month,
                birth_day=df.birth_date.dt.day,
            )
            df = df.assign(age=df.year - df.birth_year)
            df = df.assign(
                age=df["age"].where(
                    df["age"].between(0, 115, inclusive="both"), np.nan
                )
            )
            df = df.assign(
                age_day=(
                    dd.to_datetime(
                        df.year.astype(str) + "1231", format="%Y%m%d"
                    )
                    - df.birth_date
                ).dt.days
            )
            df = df.assign(
                adult=df["age"]
                .between(18, 115, inclusive="both")
                .astype(pd.Int64Dtype()),
                child=df["age"]
                .between(0, 17, inclusive="both")
                .astype(pd.Int64Dtype()),
            )
            df = df.assign(
                adult=df["adult"].where(~(df["age"].isna()), -1).astype(int),
                child=df["child"].where(~(df["age"].isna()), -1).astype(int),
            )
            if self.ftype != "ps":
                df = df.map_partitions(
                    lambda pdf: pdf.assign(
                        adult=pdf.groupby(pdf.index)["adult"].transform(max),
                        age=pdf.groupby(pdf.index)["age"].transform(max),
                        age_day=pdf.groupby(pdf.index)["age_day"].transform(
                            max
                        )
                    )
                )
            if "date_of_death" in df.columns:
                df = df.assign(
                    death=(
                        (
                            df.date_of_death.dt.year.fillna(
                                df.year + 10
                            ).astype(int)
                            <= df.year
                        )
                        | (
                            df.medicare_date_of_death.dt.year.fillna(
                                df.year + 10
                            ).astype(int)
                            <= df.year
                        )
                    ).astype(int)
                )
            if self.ftype == "ip":
                df = df.assign(
                    missing_admsn_date=df["admsn_date"].isnull().astype(int),
                    missing_prncpl_proc_date=df["prncpl_proc_date"]
                    .isnull()
                    .astype(int),
                )
                df = df.assign(
                    admsn_date=df["admsn_date"].where(
                        ~df["admsn_date"].isnull(), df["srvc_bgn_date"]
                    )
                )
                df = df.assign(
                    los=(df["srvc_end_date"] - df["admsn_date"]).dt.days + 1,
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
                    prncpl_proc_date=df["prncpl_proc_date"].where(
                        ~df["prncpl_proc_date"].isnull(), df["admsn_date"]
                    )
                )

                df = df.assign(
                    age_day_admsn=(
                        df["admsn_date"] - df["birth_date"]
                    ).dt.days,
                    age_day_prncpl_proc=(
                        df["prncpl_proc_date"] - df["birth_date"]
                    ).dt.days,
                )
                df = df.assign(
                    age_admsn=(df["age_day_admsn"].fillna(0) / 365.25).astype(
                        int
                    ),
                    age_prncpl_proc=(
                        df["age_day_prncpl_proc"].fillna(0) / 365.25
                    ).astype(int),
                )

            if self.ftype == "ot":
                df = df.assign(
                    duration=(
                        df["srvc_end_date"] - df["srvc_bgn_date"]
                    ).dt.days,
                    age_day_srvc_bgn=(
                        df["srvc_bgn_date"] - df["birth_date"]
                    ).dt.days,
                )
                df = df.assign(
                    duration=df["duration"].where(
                        (df["srvc_bgn_date"] <= df["srvc_end_date"]), np.nan
                    ),
                    age_srvc_bgn=(
                        df["age_day_srvc_bgn"].fillna(0) / 365.25
                    ).astype(int),
                )
            self.df = df

    def calculate_payment(self):
        """
        Calculates payment amount
        New Column(s):
                pymt_amt - "MDCD_PYMT_AMT" + "TP_PYMT_AMT"

        """
        # cost
        # MDCD_PYMT_AMT=TOTAL AMOUNT OF MONEY PAID BY MEDICAID FOR THIS SERVICE
        # TP_PYMT_AMT=TOTAL AMOUNT OF MONEY PAID BY A THIRD PARTY
        # CHRG_AMT: we never use charge amount for cost analysis
        if self.ftype in ["ot", "rx", "ip"]:
            self.df = self.df.map_partitions(
                lambda pdf: pdf.assign(
                    pymt_amt=pdf[["MDCD_PYMT_AMT", "TP_PYMT_AMT"]]
                    .apply(pd.to_numeric, errors="coerce")
                    .sum(axis=1)
                )
            )

    def flag_ed_use(self) -> None:
        """
        Detects ed use in claims
        New Column(s):

            - ed_cpt - 0 or 1, Claim has a procedure billed in ED code range (99281–99285)
                                (PRCDR_CD_SYS_{1-6} == 01 & PRCDR_CD_{1-6} in (99281–99285))
            - ed_ub92 - 0 or 1, Claim has a revenue center codes (0450 - 0459, 0981) - UB_92_REV_CD_GP_{1-23}
            - ed_tos - 0 or 1, Claim has an outpatient type of service (MAX_TOS = 11) (if ftype == 'ip')
            - ed_pos - 0 or 1, Claim has PLC_OF_SRVC_CD set to 23 (if ftype == 'ot')
            - ed_use - any of ed_cpt, ed_ub92, ed_tos or ed_pos is 1
            - any_ed - 0 or 1, 1 when any other claim from the same visit has ed_use set to 1 (if ftype == 'ot')

        Uses the below as reference:
            - If the patient is a Medicare beneficiary, the general surgeon should bill the level of
            ED code (99281–99285) (http://bulletin.facs.org/2013/02/coding-for-hospital-admission)
            - Inpatient files:  Revenue Center Codes 0450-0459, 0981 (https://www.resdac.org/resconnect/articles/144)

        Returns
        -------

        """
        # reference: If the patient is a Medicare beneficiary, the general surgeon should bill the level of
        # ED code (99281–99285). http://bulletin.facs.org/2013/02/coding-for-hospital-admission
        if self.ftype in ["ot", "ip"]:
            self.df = self.df.map_partitions(
                lambda pdf: pdf.assign(
                    **{
                        "ed_cpt": np.column_stack(
                            [
                                pdf[col].str.startswith(
                                    (
                                        "99281",
                                        "99282",
                                        "99283",
                                        "99284",
                                        "99285",
                                    ),
                                    na=False,
                                )
                                for col in pdf.columns
                                if col.startswith(("PRCDR_CD",))
                                and (not col.startswith(("PRCDR_CD_SYS",)))
                            ]
                        )
                        .any(axis=1)
                        .astype(int)
                    }
                )
            )
            if self.ftype == "ip":
                # Inpatient files:  Revenue Center Codes 0450-0459, 0981,
                # https://www.resdac.org/resconnect/articles/144
                # TOS - Type of Service
                # 11=outpatient hospital ???? not every IP which called outpatient hospital is called ED,
                # this may end up with too many ED
                self.df = self.df.map_partitions(
                    lambda pdf: pdf.assign(
                        **{
                            "ed_ub92": np.column_stack(
                                [
                                    pd.to_numeric(
                                        pdf[col], errors="coerce"
                                    ).isin(
                                        [
                                            450,
                                            451,
                                            452,
                                            453,
                                            454,
                                            455,
                                            456,
                                            457,
                                            458,
                                            459,
                                            981,
                                        ]
                                    )
                                    for col in pdf.columns
                                    if col.startswith("UB_92_REV_CD_GP_")
                                ]
                            )
                            .any(axis=1)
                            .astype(int),
                            "ed_tos": (
                                pd.to_numeric(pdf["MAX_TOS"], errors="coerce")
                                == 11
                            ).astype(int),
                        }
                    )
                )
                self.df = self.df.assign(
                    ed_use=self.df[["ed_ub92", "ed_cpt", "ed_tos"]]
                    .any(axis="columns")
                    .astype(int)
                )
            else:
                # UB92: # ??????? 450,451,452,453,454,455,456,457,458,459,981 ????????
                self.df = self.df.map_partitions(
                    lambda pdf: pdf.assign(
                        **{
                            "ed_ub92": pd.to_numeric(
                                pdf["UB_92_REV_CD"], errors="coerce"
                            )
                            .isin([450, 451, 452, 456, 459, 981])
                            .astype(int),
                            "ed_pos": (
                                pd.to_numeric(
                                    pdf["PLC_OF_SRVC_CD"],
                                    errors="coerce",
                                )
                                == 23
                            ).astype(int),
                        }
                    )
                )
                self.df = self.df.assign(
                    ed_use=self.df[["ed_pos", "ed_cpt", "ed_ub92"]]
                    .any(axis="columns")
                    .astype(int)
                )
                # check ED use in other claims from the same visit
                self.df = self.df.map_partitions(
                    lambda pdf: pdf.assign(
                        any_ed=pdf.groupby([pdf.index, "srvc_bgn_date"])[
                            "ed_use"
                        ].transform("max")
                    )
                )

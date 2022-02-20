import dask.dataframe as dd
import os
import errno
import sys
import numpy as np
import pandas as pd
import shutil

sys.path.append("../../../")
from medicaid_utils.common_utils import dataframe_utils, links


class MAXFile:
    """Parent class for all cms file classes, each of which will have clean and preprocess functions"""

    def __init__(
        self,
        ftype: str,
        year: int,
        st: str,
        data_root: str,
        index_col: str = "BENE_MSIS",
        clean: bool = True,
        preprocess: bool = True,
        tmp_folder: str = None,
    ):
        """
        Initializes cms file object by preloading and preprocessing(if opted in) the file
        :param ftype: Type of CMS file. Options: ip, ot, rx, ps, cc
        :param year: Year
        :param st: State
        :param data_root: Root folder with cms data
        :param index_col: Column to use as index. Eg. BENE_MSIS or MSIS_ID. The raw file is expected to be already
        sorted with index column
        :param clean: Run cleaning routines if True
        :param preprocess: Add commonly used constructed variable columns, if True
        :param tmp_folder: Folder to use to store temporary files
        """
        self.fileloc = links.get_max_parquet_loc(data_root, ftype, st, year)
        self.ftype = ftype
        self.index_col = index_col
        self.year = year
        self.st = st
        self.tmp_folder = tmp_folder
        if not os.path.exists(self.fileloc):
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), self.fileloc
            )
        self.df = dd.read_parquet(
            self.fileloc, index=False, engine="fastparquet"
        ).set_index(index_col, sorted=True)
        self.df = self.df.assign(
            HAS_BENE=(self.df["BENE_ID"].fillna("").str.len() > 0).astype(int)
        )
        self.lst_raw_col = list(self.df.columns)

        # This dictionary variable can be used to filter out data that will not met minimum quality expections
        self.dct_default_filters = {}
        if clean:
            self.clean()
        if preprocess:
            self.preprocess()

    def cache_results(self, repartition=False):
        """Save results in intermediate steps of some lengthy processing. Saving intermediate results speeds up
        processing"""
        if self.tmp_folder is not None:
            if repartition:
                self.df = self.df.repartition(
                    partition_size="20MB"
                ).persist()  # Patch, currently to_parquet results
                # in error when any of the partitions is empty
            return self.pq_export(self.tmp_folder)
        return self.df

    def pq_export(self, dest_name):
        """Export parquet files (overwrite safe)"""
        shutil.rmtree(dest_name + "_tmp", ignore_errors=True)
        try:
            self.df.to_parquet(
                dest_name + "_tmp", engine="fastparquet", write_index=True
            )
        except Exception as ex:
            self.df.to_parquet(
                dest_name + "_tmp", engine="pyarrow", write_index=True
            )
        del self.df
        shutil.rmtree(dest_name, ignore_errors=True)
        os.rename(dest_name + "_tmp", dest_name)
        self.df = dd.read_parquet(
            dest_name, index=False, engine="fastparquet"
        ).set_index(self.index_col, sorted=True)
        return self.df

    def clean(self):
        """Cleaning routines for date and gender columns in all CMS files"""
        # Date columns will be cleaned and all commonly used date based variables are constructed
        # in this step
        self.process_date_cols()
        self.add_gender()

    def preprocess(self):
        """Add basic constructed variables"""
        pass

    def export(self, dest_folder, format="csv"):
        """Exports the file"""
        self.df.to_csv(
            os.path.join(
                dest_folder, f"{self.ftype}_{self.year}_{self.st}.csv"
            ),
            index=True,
            single_file=True,
        )

    def add_gender(self) -> None:
        """
        Adds integer 'female' column based on 'EL_SEX_CD' column
        :param DataFrame df:
        :rtype: None
        """
        if "EL_SEX_CD" in self.df.columns:
            self.df["female"] = -1
            self.df["female"] = self.df["female"].where(
                ~self.df.EL_SEX_CD.isin(["F", "M"]),
                (self.df.EL_SEX_CD == "F").astype(int),
            )
        return None

    def clean_diag_codes(self) -> None:
        """Clean diagnostic code columns by removing non-alphanumeric characters and converting them to upper case"""
        if (
            len([col for col in self.df.columns if col.startswith("DIAG_CD_")])
            > 0
        ):
            self.df = self.df.map_partitions(
                lambda pdf: pdf.assign(
                    **dict(
                        [
                            (
                                col,
                                pdf[col]
                                .str.replace("[^a-zA-Z0-9]+", "", regex=True)
                                .str.upper(),
                            )
                            for col in pdf.columns
                            if col.startswith("DIAG_CD_")
                        ]
                    )
                )
            )
        return None

    def clean_proc_codes(self) -> None:
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
                    **dict(
                        [
                            (
                                col,
                                pdf[col]
                                .str.replace("[^a-zA-Z0-9]+", "", regex=True)
                                .str.upper(),
                            )
                            for col in pdf.columns
                            if col.startswith("PRCDR_CD")
                            and (not col.startswith("PRCDR_CD_SYS"))
                        ]
                    )
                )
            )
        return None

    def process_date_cols(self) -> None:
        """Convert datetime columns to datetime type and add basic date based constructed variables
                New columns:
            birth_year, birth_month, birth_day - date compoments of EL_DOB (date of birth)
            birth_date - birth date (EL_DOB)
            death - 0 or 1, if EL_DOD or MDCR_DOD is not empty and falls in the claim year or before
            age - age in years, integer format
            age_decimal - age in years, with decimals
            age_day - age in days
            adult - 0 or 1, 1 when patient's age is in [18,115]
            child - 0 or 1, 1 when patient's age is in [0,17]
        if ftype == 'ip':
            Clean/ impute admsn_date and add ip duration related columns
                        New column(s):
                                admsn_date - Admission date (ADMSN_DT)
                                srvc_bgn_date - Service begin date (SRVC_BGN_DT)
                                srvc_end_date - Service end date (SRVC_END_DT)
                                prncpl_proc_date - Principal procedure date (PRNCPL_PRCDR_DT)
                                missing_admsn_date - 0 or 1, 1 when admission date is missing
                                missing_prncpl_proc_date - 0 or 1, 1 when principal procedure date is missing
                            flag_admsn_miss - 0 or 1, 1 when admsn_date was imputed
                            los - ip service duration in days
                            ageday_admsn - age in days as on admsn_date
                            age_admsn - age in years, with decimals, as on admsn_date
                            age_prncpl_proc - age in years as on principal procedure date
                            age_day_prncpl_proc - age in days as on principal procedure date
                if ftype == 'ot':
                        Adds duration column, provided service end and begin dates are clean
                        New Column(s):
                                srvc_bgn_date - Service begin date (SRVC_BGN_DT)
                                srvc_end_date - Service end date (SRVC_END_DT)
                    diff & duration - duration of service in days
                    age_day_srvc_bgn - age in days as on service begin date
                    age_srvc_bgn - age in years, with decimals, as on service begin date
            if ftype == 'ps:
                New Column(s):
                    date_of_death - Date of death (EL_DOD)
                    medicare_date_of_death - Medicare date of death (MDCR_DOD)
        :rtype:None
        """
        if self.ftype in ["ip", "lt", "ot", "ps", "rx"]:
            df = self.df.assign(
                **dict(
                    [
                        ("year", self.df[col].astype(int))
                        for col in ["MAX_YR_DT", "YR_NUM"]
                        if col in self.df.columns
                    ]
                )
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
            for date_col in [
                col for col in dct_date_col if col not in df.columns
            ]:
                dct_date_col.pop(date_col, None)

            df = df.assign(
                **dict([(dct_date_col[col], df[col]) for col in dct_date_col])
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
                ).dt.days,
                age_decimal=(
                    dd.to_datetime(
                        df.year.astype(str) + "1231", format="%Y%m%d"
                    )
                    - df.birth_date
                )
                / np.timedelta64(1, "Y"),
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
                adult=df["adult"].where(~(df["age"].isna()), np.nan),
                child=df["child"].where(~(df["age"].isna()), np.nan),
            )
            if self.ftype != "ps":
                df = df.map_partitions(
                    lambda pdf: pdf.assign(
                        adult=pdf.groupby(pdf.index)["adult"].transform(max),
                        age=pdf.groupby(pdf.index)["age"].transform(max),
                        age_day=pdf.groupby(pdf.index)["age_day"].transform(
                            max
                        ),
                        age_decimal=pdf.groupby(pdf.index)[
                            "age_decimal"
                        ].transform(max),
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
                    ),
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
        return None

    def calculate_payment(self) -> None:
        """
        Calculates payment amount
        New Column(s):
                pymt_amt - name of payment amount column
        :param DataFrame df: Claims data
        :rtype: None
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
        return None

    def flag_ed_use(self) -> None:
        """
        Detects ed use in claims
        New Column(s):
        ed_cpt - 0 or 1, Claim has a procedure billed in ED code range (99281–99285)
                                (PRCDR_CD_SYS_{1-6} == 01 & PRCDR_CD_{1-6} in (99281–99285))
        ed_ub92 - 0 or 1, Claim has a revenue center codes (0450 - 0459, 0981) - UB_92_REV_CD_GP_{1-23}
        ed_tos - 0 or 1, Claim has an outpatient type of service (MAX_TOS = 11) (if ftype == 'ip')
        ed_pos - 0 or 1, Claim has PLC_OF_SRVC_CD set to 23 (if ftype == 'ot')
        ed_use - any of ed_cpt, ed_ub92, ed_tos or ed_pos is 1
        any_ed - 0 or 1, 1 when any other claim from the same visit has ed_use set to 1 (if ftype == 'ot')
        :param df_ip:
        :rtype: None
        """
        # reference: If the patient is a Medicare beneficiary, the general surgeon should bill the level of
        # ED code (99281–99285). http://bulletin.facs.org/2013/02/coding-for-hospital-admission
        if self.ftype in ["ot", "ip"]:
            self.df = self.df.map_partitions(
                lambda pdf: pdf.assign(
                    **dict(
                        [
                            (
                                "ed_cpt",
                                np.column_stack(
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
                                        if col.startswith((f"PRCDR_CD",))
                                        and (
                                            not col.startswith(
                                                (f"PRCDR_CD_SYS",)
                                            )
                                        )
                                    ]
                                )
                                .any(axis=1)
                                .astype(int),
                            )
                        ]
                    )
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
                        **dict(
                            [
                                (
                                    "ed_ub92",
                                    np.column_stack(
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
                                            if col.startswith(
                                                "UB_92_REV_CD_GP_"
                                            )
                                        ]
                                    )
                                    .any(axis=1)
                                    .astype(int),
                                ),
                                (
                                    "ed_tos",
                                    (
                                        pd.to_numeric(
                                            pdf["MAX_TOS"], errors="coerce"
                                        )
                                        == 11
                                    ).astype(int),
                                ),
                            ]
                        )
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
                        **dict(
                            [
                                (
                                    "ed_ub92",
                                    pd.to_numeric(
                                        pdf["UB_92_REV_CD"], errors="coerce"
                                    )
                                    .isin([450, 451, 452, 456, 459, 981])
                                    .astype(int),
                                ),
                                (
                                    "ed_pos",
                                    (
                                        pd.to_numeric(
                                            pdf["PLC_OF_SRVC_CD"],
                                            errors="coerce",
                                        )
                                        == 23
                                    ).astype(int),
                                ),
                            ]
                        )
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
        return None

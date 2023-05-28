"""This module has TAFPS class which wraps together cleaning/ preprocessing
routines specific for TAF PS files"""
import os

from itertools import product
import numpy as np
import pandas as pd
import dask.dataframe as dd

from medicaid_utils.preprocessing import taf_file, taf_ip, taf_ot, taf_rx
from medicaid_utils.common_utils import dataframe_utils
from medicaid_utils.adapted_algorithms.py_elixhauser import (
    elixhauser_comorbidity,
)

data_folder = os.path.join(os.path.dirname(__file__), "data")
other_data_folder = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "other_datasets", "data"
)


class TAFPS(taf_file.TAFFile):
    """Scripts to preprocess PS file"""

    def __init__(
        self,
        year: int,
        state: str,
        data_root: str,
        index_col: str = "BENE_MSIS",
        clean: bool = True,
        preprocess: bool = True,
        rural_method: str = "ruca",
        tmp_folder: str = None,
        pq_engine: str = "pyarrow",
    ):
        """

        Parameters
        ----------
        year : int
            Claim year
        state : str
            Claim state
        data_root : str
            Root folder with cms data
        index_col : str, default='BENE_MSIS'
            Column to use as index. Eg. BENE_MSIS or MSIS_ID. The raw file
            is expected to be already sorted with index column
        clean : bool, default=True
            Run cleaning routines if True
        preprocess : bool, default=True
            Add commonly used constructed variable columns if True
        rural_method : {'ruca', 'rucc'}
            Method to use for rural variable construction. Available
            options: 'ruca', 'rucc'
        tmp_folder : str, default=None
            Folder to use to store temporary files
        pq_engine: str, default='pyarrow'
            Parquet Engine
        """
        super().__init__(
            "ps",
            year=year,
            state=state,
            data_root=data_root,
            index_col=index_col,
            clean=False,
            preprocess=False,
            tmp_folder=tmp_folder,
            pq_engine=pq_engine,
        )

        # Default filters to filter out benes that do not meet minimum
        # standard of cleanliness criteria duplicated_bene_id exclusion will
        # remove benes with duplicated BENE_MSIS ids
        self.dct_default_filters = {"duplicated_bene_id": 0}
        if clean:
            self.clean()
        if preprocess:
            self.preprocess(rural_method)

    def clean(self):
        """Runs cleaning routines and created common exclusion flags based
        on default filters"""
        super().clean()
        self.add_gender()
        self.flag_common_exclusions()
        self.cache_results()

    def preprocess(self, rural_method="ruca"):
        """Adds rural and eligibility criteria indicator variables"""
        self.flag_rural(rural_method)
        self.flag_dual()
        self.flag_restricted_benefits()
        self.compute_enrollment_gaps()
        self.add_mas_boe()
        self.flag_tanf()
        self.flag_medicaid_enrolled_months()
        self.flag_managed_care_months()
        self.flag_ffs_months()
        self.cache_results()
        self.add_risk_adjustment_scores()
        self.cache_results("base")

    def flag_common_exclusions(self):
        """
        Adds commonly used exclusion flags
        New Column(s):
            - excl_duplicated_bene_id - 0 or 1, 1 when bene's index column
            is repeated
        """
        df_base = self.dct_files["base"]
        df_base = df_base.assign(**{f"_{self.index_col}": df_base.index})
        # Some BENE_MSIS's are repeated in PS files. Some patients share the
        # same BENE_ID and yet have different MSIS_IDs. Some of them even
        # have different 'dates of birth'. Since we cannot find any
        # explanation for such patterns, we decided on removing these
        # BENE_MSIS's as per issue #29 in FARA project
        # (https://rcg.bsd.uchicago.edu/gitlab/mmurugesan/hrsa_max_feature_extraction/issues/29)
        df_base = df_base.map_partitions(
            lambda pdf: pdf.assign(
                excl_duplicated_bene_id=pdf.duplicated(
                    [f"_{self.index_col}"], keep=False
                ).astype(int)
            )
        )
        df_base = df_base.drop([f"_{self.index_col}"], axis=1)
        self.dct_files["base"] = df_base

    def add_mas_boe(self):
        """
        Adds columns denoting number of months in each Maintenance
        Assistance Status (MAS) and Basis of Eligibility
        (BOE) category. Columns added are,
            - boe_chip_months : Number of months in Separate-CHIP BOE category
            - boe_aged_months : Number of months in Aged BOE category
            - boe_blind_disabled_months : Number of months in Blind/
            Disabled BOE category
            - boe_child_months : Number of months in Children BOE category
            - boe_adults_months : Number of months in Adult BOE category
            - boe_breast_and_cervical_cancer_months : Number of months in
            Breast and Cervical Cancer Prevention and Treatment Act of 2000
            BOE category
            - boe_child_of_unemployed_months : Number of months in Child of
            Unemployed Adult BOE category
            - boe_unemployed_months : Number of months in Unemployed Adult
            BOE category
            - boe_foster_care_children_months : Number of months in Foster
            Care Children BOE category
            - boe_unknown_months : Number of months in Uknown BOE category
            - mas_chip_months : Number of months in Separate-CHIP MAS category
            - mas_cash_sec_1931_months : Number of months in Individuals
            receiving cash assistance or eligible under section 1931 of the
            Act MAS category
            - mas_medically_needy_months : Number of months in Medically
            Needy MAS category
            - mas_poverty_months : Number of months in Poverty Related
            Eligibles MAS category
            - mas_other_months : Number of months in Other Eligibles MAS
            category
            - mas_demonstration_months : Number of months in Section 1115
            Demonstration expansion eligible MAS category
            - mas_unknown_months : Number of months in Unknown MAS category
            - max_mas_type : Top MAS category for the bene
            - max_boe_type : Top BOE category for the bene
        """
        df = self.dct_files["base"]
        dct_boe_codes = {
            "chip": 0,
            "aged": 1,
            "blind_disabled": 2,
            "child": 4,
            "adults": 5,
            "breast_and_cervical_cancer": 11,
            "child_of_unemployed": 6,
            "unemployed": 7,
            "foster_care_children": 8,
            "unknown": 99,
        }
        dct_mas_codes = {
            "chip": 0,
            "cash_sec_1931": 1,
            "medically_needy": 2,
            "poverty": 3,
            "other": 4,
            "demonstration": 5,
            "unknown": 9,
        }
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                **{
                    **{
                        f"boe_{boe_type}_months": np.column_stack(
                            [
                                (
                                    pd.to_numeric(pdf[col], errors="coerce")
                                    .fillna(999)
                                    .astype(int)
                                    .astype(str)
                                    .str.zfill(3)
                                    .str[1:3]
                                    .astype(int)
                                    == dct_boe_codes[boe_type]
                                ).astype(int)
                                for col in [
                                    f"MASBOE_CD_{str(mon).zfill(2)}"
                                    for mon in range(1, 13)
                                ]
                            ]
                        )
                        .sum(axis=1)
                        .astype(int)
                        for boe_type in dct_boe_codes
                    },
                    **{
                        f"mas_{mas_type}_months": np.column_stack(
                            [
                                (
                                    pd.to_numeric(pdf[col], errors="coerce")
                                    .fillna(999)
                                    .astype(int)
                                    .astype(str)
                                    .str.zfill(3)
                                    .str[0]
                                    .astype(int)
                                    == dct_mas_codes[mas_type]
                                ).astype(int)
                                for col in [
                                    f"MASBOE_CD_{str(mon).zfill(2)}"
                                    for mon in range(1, 13)
                                ]
                            ]
                        )
                        .sum(axis=1)
                        .astype(int)
                        for mas_type in dct_mas_codes
                    },
                }
            )
        )
        df = df.assign(
            **{
                f"max_mas_type": df[
                    [f"mas_{mas_type}_months" for mas_type in dct_mas_codes]
                ]
                .idxmax(axis=1)
                .str[4:]
                .str[:-7]
            }
        )
        df = df.assign(
            **{
                f"max_boe_type": df[
                    [f"boe_{boe_type}_months" for boe_type in dct_boe_codes]
                ]
                .idxmax(axis=1)
                .str[4:]
                .str[:-7],
                f"boe_gt_6_mon": (
                    df[
                        [
                            f"boe_{boe_type}_months"
                            for boe_type in dct_boe_codes
                        ]
                    ]
                    > 6
                )
                .astype(int)
                .idxmax(axis=1)
                .str[4:]
                .str[:-7]
                .where(
                    (
                        df[
                            [
                                f"boe_{boe_type}_months"
                                for boe_type in dct_boe_codes
                            ]
                        ]
                        > 6
                    )
                    .astype(int)
                    .any(axis=1),
                    np.nan,
                ),
                f"mas_gt_6_mon": (
                    df[
                        [
                            f"mas_{mas_type}_months"
                            for mas_type in dct_mas_codes
                        ]
                    ]
                    > 6
                )
                .astype(int)
                .idxmax(axis=1)
                .str[4:]
                .str[:-7]
                .where(
                    (
                        df[
                            [
                                f"mas_{mas_type}_months"
                                for mas_type in dct_mas_codes
                            ]
                        ]
                        > 6
                    )
                    .astype(int)
                    .any(axis=1),
                    np.nan,
                ),
            }
        )
        self.dct_files["base"] = df

    def add_gender(self):
        """Adds integer 'female' column based on 'SEX_CD' column. Undefined
        values ('U') in SEX_CD column will result in female column taking
        the value -1"""
        df = self.dct_files["base"]
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                female=np.select(
                    [
                        pdf["SEX_CD"].str.strip().str.upper() == "F",
                        pdf["SEX_CD"].str.strip().str.upper() == "M",
                    ],
                    [1, 0],
                    default=-1,
                ).astype(int)
            )
        )
        self.dct_files["base"] = df

    def flag_rural(
        self, method: str = "ruca"
    ):  # pylint: disable=missing-param-doc
        """
        Classifies benes into rural/ non-rural on the basis of RUCA/ RUCC of
        their resident ZIP/ FIPS codes

        New Columns:
            - resident_state_cd
            - rural - 0/ 1/ np.nan, 1 when bene's residence is in a rural
            location, 0 when not, -1 when zip code is missing
            - pcsa - resident PCSA code
            - {ruca_code/ rucc_code} - resident ruca_code

        This function uses
            - `RUCA 3.1 dataset
            <https://www.ers.usda.gov/webdocs/DataFiles/53241/RUCA2010zipcode.xlsx?v=8673>`_. RUCA
            codes >= 4 denote rural and the rest denote urban as per
            `Cole, Megan B et al <https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6286055/#SD1>`_
            - `RUCC codes <https://www.ers.usda.gov/webdocs/DataFiles/53251/ruralurbancodes2013.xls?v=2372>`_.
            RUCC codes >= 8 denote rural and the rest denote urban.
            - ZCTAs x zipcode crosswalk from `UDSMapper <https://udsmapper.org/zip-code-to-zcta-crosswalk/>`_.
            - zipcodes from multiple sources
            - Distance between centroids of zipcodes using `NBER data <https://nber.org/distance/2016/gaz/zcta5/gaz2016zcta5centroid.csv>`_

        Parameters
        ----------
        method : {'ruca', 'rucc'}
            Method to use for rural variable construction

        """
        df = self.dct_files["base"]
        index_col = df.index.name
        zip_folder = os.path.join(other_data_folder, "zip")
        df = df.assign(**{index_col: df.index})

        # Pad zeroes to the left to make zip codes 9 characters long.
        # RI Zip codes have problems. They are all invalid unless the last
        # character is dropped and a zero is added to the left
        df = df.assign(
            BENE_ZIP_CD=df["BENE_ZIP_CD"]
            .where(
                ~((df["STATE_CD"] == "RI")),
                "0" + df["BENE_ZIP_CD"].str.ljust(9, "0").str[:-1],
            )
            .str.ljust(9, "0")
        )

        # zip_state_pcsa_ruca_zcta.csv was constructed with RUCA 3.1
        # (from https://www.ers.usda.gov/webdocs/DataFiles/53241/RUCA2010zipcode.xlsx?v=8673),
        # ZCTAs x zipcode mappings from UDSMapper (https://udsmapper.org/zip-code-to-zcta-crosswalk/),
        # zipcodes from multiple sources, and distance between centroids of
        # zipcodes using NBER data
        # (https://nber.org/distance/2016/gaz/zcta5/gaz2016zcta5centroid.csv)
        df_zip_state_pcsa = pd.read_csv(
            os.path.join(zip_folder, "zip_state_pcsa_ruca_zcta.csv"),
            dtype=object,
        )
        df_zip_state_pcsa = df_zip_state_pcsa.assign(
            zip=df_zip_state_pcsa["zip"].str.replace(" ", "").str.ljust(9, "0")
        )
        df_zip_state_pcsa = df_zip_state_pcsa.rename(
            columns={
                "zip": "BENE_ZIP_CD",
                "state_cd": "resident_state_cd",
            }
        )
        df = df[
            df.columns.difference(
                ["resident_state_cd", "pcsa", "ruca_code"]
            ).tolist()
        ]

        df = df.assign(
            BENE_ZIP_CD=df["BENE_ZIP_CD"]
            .str.replace("[^a-zA-Z0-9]+", "", regex=True)
            .str.ljust(9, "0")
        )
        df = df.merge(
            df_zip_state_pcsa[
                [
                    "BENE_ZIP_CD",
                    "resident_state_cd",
                    "pcsa",
                    "ruca_code",
                ]
            ],
            how="left",
            on="BENE_ZIP_CD",
        )

        # RUCC codes were downloaded from
        # https://www.ers.usda.gov/webdocs/DataFiles/53251/ruralurbancodes2013.xls?v=2372
        df_rucc = pd.read_excel(
            os.path.join(zip_folder, "ruralurbancodes2013.xls"),
            sheet_name="Rural-urban Continuum Code 2013",
            dtype="object",
        )
        df_rucc = df_rucc.rename(
            columns={
                "State": "resident_state_cd",
                "RUCC_2013": "rucc_code",
                "FIPS": "BENE_CNTY_CD",
            }
        )
        df_rucc = df_rucc.assign(
            BENE_CNTY_CD=df_rucc["BENE_CNTY_CD"].str.strip().str[2:],
            resident_state_cd=df_rucc["resident_state_cd"]
            .str.strip()
            .str.upper(),
        )
        df = df.assign(
            BENE_CNTY_CD=df["BENE_CNTY_CD"].str.strip(),
            resident_state_cd=df["resident_state_cd"].where(
                ~df["resident_state_cd"].isna(), df["STATE_CD"]
            ),
        )

        df = df[[col for col in df.columns if col not in ["rucc_code"]]]
        df = df.merge(
            df_rucc[["BENE_CNTY_CD", "resident_state_cd", "rucc_code"]],
            how="left",
            on=["BENE_CNTY_CD", "resident_state_cd"],
        )
        df = df.assign(
            **{
                col: dd.to_numeric(df[col], errors="coerce")
                for col in ["rucc_code", "ruca_code"]
            }
        )

        # RUCA codes >= 4 denote rural and the rest denote urban
        # as per https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6286055/#SD1
        # and as in FARA year 1 papers
        if method == "ruca":
            df = df.map_partitions(
                lambda pdf: pdf.assign(
                    rural=np.select(
                        [
                            pdf["ruca_code"].between(
                                0, 4, inclusive="neither"
                            ),
                            (pdf["ruca_code"] >= 4),
                        ],
                        [0, 1],
                        default=-1,
                    ).astype(int)
                )
            )
        else:
            # RUCC codes >= 8 denote rural and the rest denote urban
            df = df.map_partitions(
                lambda pdf: pdf.assign(
                    rural=np.select(
                        [
                            pdf["rucc_code"].between(1, 7, inclusive="both"),
                            (pdf["rucc_code"] >= 8),
                        ],
                        [0, 1],
                        default=-1,
                    ).astype(int)
                )
            )
        if df.index.name != index_col:
            df = df.set_index(index_col, sorted=True)
        self.dct_files["base"] = df

    def flag_dual(self):
        """
        Flags benes with  DUAL_ELGBL_CD equal to 1 (full dual), 2 (partial
        dual), or 3 (other dual) in any month are flagged as duals.
        Reference: `Identifying beneficiaries with a substance use
        disorder <https://www.medicaid.gov/medicaid/data-and-systems/downloads/macbis/sud_techspecs.docx>`_
        """
        df = self.dct_files["base"]
        df = df.assign(
            **{
                f"dual_mon_{mon}": dd.to_numeric(
                    df[f"DUAL_ELGBL_CD_{str(mon).zfill(2)}"], errors="coerce"
                )
                .isin([1, 2, 3])
                .astype(int)
                for mon in range(1, 13)
            }
        )
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                any_dual_month=pdf[[f"dual_mon_{mon}" for mon in range(1, 13)]]
                .any(axis=1)
                .astype(int),
                dual_months=pdf[f"dual_mon_1"]
                .astype(str)
                .str.cat(
                    pdf[[f"dual_mon_{mon}" for mon in range(2, 13)]].astype(
                        str
                    ),
                    sep="",
                ),
                total_dual_months=pdf[
                    [f"dual_mon_{mon}" for mon in range(1, 13)]
                ]
                .sum(axis=1)
                .astype(int),
            )
        )
        df = df.drop(columns=[f"dual_mon_{mon}" for mon in range(1, 13)])
        self.dct_files["base"] = df

    def flag_restricted_benefits(self):
        """
        Flags beneficiaries whose benefits are restricted. Benes with the
        below values in their RSTRCTD_BNFTS_CD_XX columns are NOT assumed to
        have restricted benefits:

            1. Individual is eligible for Medicaid or CHIP and entitled to
            the full scope of Medicaid or CHIP benefits.
            4. Individual is eligible for Medicaid or CHIP but only entitled to
            restricted benefits for pregnancy-related services.
            5. Individual is eligible for Medicaid or Medicaid-Expansion
            CHIP but, for reasons other than alien, dual-eligibility or
            pregnancy-related status, is only entitled to restricted
            benefits (e.g., restricted benefits based upon substance abuse,
            medically needy or other criteria).
            7. Individual is eligible for Medicaid and entitled to Medicaid
            benefits under an alternative package of
            benchmark-equivalent coverage, as enacted by the Deficit
            Reduction Act of 2005.

        Reference: `Identifying beneficiaries with a substance use disorder <https://www.medicaid.gov/medicaid/data-and-systems/downloads/macbis/sud_techspecs.docx>`_
        """
        df = self.dct_files["base"]
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                **{
                    f"restricted_benefit_mon_{mon}": (
                        ~pd.to_numeric(
                            pdf[f"RSTRCTD_BNFTS_CD_{str(mon).zfill(2)}"],
                            errors="coerce",
                        ).isin([1, 4, 5, 7])
                    ).astype(int)
                    for mon in range(1, 13)
                }
            )
        )
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                any_restricted_benefit_month=pdf[
                    [f"restricted_benefit_mon_{mon}" for mon in range(1, 13)]
                ]
                .any(axis=1)
                .astype(int),
                restricted_benefit_months=pdf[f"restricted_benefit_mon_1"]
                .astype(str)
                .str.cat(
                    pdf[
                        [
                            f"restricted_benefit_mon_{mon}"
                            for mon in range(2, 13)
                        ]
                    ].astype(str),
                    sep="",
                ),
                total_restricted_benefit_months=pdf[
                    [f"restricted_benefit_mon_{mon}" for mon in range(1, 13)]
                ]
                .sum(axis=1)
                .astype(int),
            )
        )
        df = df.drop(
            columns=[f"restricted_benefit_mon_{mon}" for mon in range(1, 13)]
        )
        self.dct_files["base"] = df

    def compute_enrollment_gaps(self):
        """Computes enrollment gaps using dates file. Adds number of
        enrollment gaps and length of maximum enrollment gap in days columns"""
        df = self.dct_files["dates"]
        df = dataframe_utils.fix_index(
            df, index_name=self.index_col, drop_column=False
        )

        def fill_enrollment_gaps(pdf_dates: pd.DataFrame) -> pd.DataFrame:
            """
            Adds enrollment gap column

            Parameters
            ----------
            pdf_dates : pd.DataFrame
                Dates dataframe

            Returns
            -------
            pd.DataFrame

            """
            pdf_dates = pdf_dates.reset_index(drop=True)
            pdf_dates = pdf_dates.sort_values(
                [self.index_col, "enrollment_start_date"], ascending=True
            )
            pdf_dates = pdf_dates.assign(
                enrollment_end_date=pdf_dates.groupby(
                    [self.index_col, "enrollment_start_date"]
                )["enrollment_end_date"].transform("max")
            )
            pdf_dates = pdf_dates.drop_duplicates(
                [
                    self.index_col,
                    "enrollment_start_date",
                    "enrollment_end_date",
                ]
            )
            pdf_dates = pdf_dates.assign(
                enrollment_end_date=pdf_dates["enrollment_end_date"].fillna(
                    pd.to_datetime(
                        pdf_dates["enrollment_start_date"]
                        .dt.year.fillna(self.year)
                        .astype(str)
                        + "-12-31"
                    )
                )
            )
            pdf_dates = pdf_dates.assign(
                next_enrollment_start_date=pdf_dates.groupby(self.index_col)[
                    "enrollment_start_date"
                ]
                .shift(-1)
                .fillna(
                    pd.to_datetime(
                        pdf_dates["enrollment_end_date"]
                        .dt.year.fillna(self.year)
                        .astype(str)
                        + "-12-31"
                    )
                )
            )
            pdf_dates = pdf_dates.assign(
                enrollment_gap=(
                    pdf_dates["next_enrollment_start_date"]
                    - pdf_dates["enrollment_end_date"]
                ).dt.days
            )
            pdf_enrollment_beginnings = pdf_dates.groupby(
                self.index_col
            ).first()
            pdf_enrollment_beginnings = pdf_enrollment_beginnings.loc[
                pdf_enrollment_beginnings["enrollment_start_date"]
                > pd.to_datetime(f"{self.year}-01-01")
            ]
            pdf_enrollment_beginnings = pdf_enrollment_beginnings.assign(
                enrollment_gap=(
                    pdf_enrollment_beginnings["enrollment_start_date"]
                    - pd.to_datetime(f"{self.year}-01-01")
                ).dt.days
            )
            pdf_enrollment_beginnings = pdf_enrollment_beginnings.assign(
                enrollment_end_date=pdf_enrollment_beginnings[
                    "enrollment_start_date"
                ]
            )
            pdf_dates = pd.concat(
                [pdf_enrollment_beginnings.reset_index(drop=False), pdf_dates],
                ignore_index=True,
            )
            pdf_dates = pdf_dates.set_index(self.index_col)
            return pdf_dates

        df = df.map_partitions(fill_enrollment_gaps)
        self.dct_files["dates"] = df
        df_gaps = df.loc[df["enrollment_gap"] != 0]
        df_gaps = df_gaps.map_partitions(
            lambda pdf: pdf.assign(enrollment_gap=pdf["enrollment_gap"].abs())
            .groupby([self.index_col])
            .agg(
                **{
                    "n_enrollment_gaps": ("enrollment_gap", "size"),
                    "max_enrollment_gap": ("enrollment_gap", "max"),
                    "total_gap_in_enrollment": ("enrollment_gap", "sum"),
                }
            )
        ).compute()
        self.dct_files["base"] = self.dct_files["base"].merge(
            df_gaps, left_index=True, right_index=True, how="left"
        )
        self.dct_files["base"] = self.dct_files["base"].assign(
            **{
                col: self.dct_files["base"][col].fillna(0).astype(int)
                for col in [
                    "n_enrollment_gaps",
                    "max_enrollment_gap",
                    "total_gap_in_enrollment",
                ]
            }
        )

    def flag_medicaid_enrolled_months(self):
        """
        Creates flags for medicaid enrollment for each and computes the
        total number of months enroled in medicaid. Bene has to be enrolled
        for all days of the month without missing eligibility information
        for the month to be considered a medicaid enrolled month.
        """
        df_base = self.dct_files["base"]
        df_base = df_base.assign(
            **{
                f"enrollment_mon_{mon}": (
                    (
                        (
                            dd.to_numeric(
                                df_base[
                                    f"MDCD_ENRLMT_DAYS_"
                                    f""
                                    f"{str(mon).zfill(2)}"
                                ],
                                errors="coerce",
                            )
                            >= (
                                28
                                if (mon == 2)
                                else (
                                    31
                                    if (mon in [1, 3, 5, 7, 8, 10, 12])
                                    else 30
                                )
                            )
                        )
                        | (
                            dd.to_numeric(
                                df_base[
                                    f"CHIP_ENRLMT_DAYS_" f"{str(mon).zfill(2)}"
                                ],
                                errors="coerce",
                            )
                            >= (
                                28
                                if (mon == 2)
                                else (
                                    31
                                    if (mon in [1, 3, 5, 7, 8, 10, 12])
                                    else 30
                                )
                            )
                        )
                    )
                    & (
                        dd.to_numeric(
                            df_base[
                                f"MISG_ENRLMT_TYPE_IND_{str(mon).zfill(2)}"
                            ],
                            errors="coerce",
                        )
                        == 0
                    )
                ).astype(int)
                for mon in range(1, 13)
            }
        )
        df_base = df_base.map_partitions(
            lambda pdf: pdf.assign(
                total_enrolled_months=pdf[
                    [f"enrollment_mon_{mon}" for mon in range(1, 13)]
                ]
                .sum(axis=1)
                .astype(int),
                enrolled_months=pdf[f"enrollment_mon_1"]
                .astype(str)
                .str.cat(
                    pdf[
                        [f"enrollment_mon_{mon}" for mon in range(2, 13)]
                    ].astype(str),
                    sep="",
                ),
            )
        )
        df_base = df_base.drop(
            columns=[f"enrollment_mon_{mon}" for mon in range(1, 13)]
        )
        self.dct_files["base"] = df_base

    def flag_managed_care_months(self):
        """
        Creates flags for 3 categories of managed care plans for each month,
        and adds columns denoting total number of months enrolled in these
        plans and the enrollment sequence pattern.
        """
        df_mc = self.dct_files["managed_care"]
        df_mc = df_mc.assign(
            **{
                f"MC_PLAN_TYPE_CD_"
                f"{str(seq).zfill(2)}_": dd.to_numeric(
                    df_mc[
                        f"MC_PLAN_TYPE_CD_{str(seq).zfill(2)}_{str(mon).zfill(2)}"
                    ],
                    errors="coerce",
                )
                for seq, mon in product(range(1, 17), range(1, 13))
            }
        )
        df_mc = df_mc.assign(
            **{
                **{
                    f"mc_comp_mon_{mon}": df_mc[
                        [
                            f"MC_PLAN_TYPE_CD_"
                            f"{str(seq).zfill(2)}_"
                            f"{str(mon).zfill(2)}"
                            for seq in range(1, 17)
                        ]
                    ]
                    .isin([1, 4, 80])
                    .any(axis=1)
                    .astype(int)
                    for mon in range(1, 13)
                },
                **{
                    f"mc_behav_health_mon_{mon}": df_mc[
                        [
                            f"MC_PLAN_TYPE_CD_"
                            f"{str(seq).zfill(2)}_"
                            f"{str(mon).zfill(2)}"
                            for seq in range(1, 17)
                        ]
                    ]
                    .isin([8, 9, 10, 11, 12, 13])
                    .any(axis=1)
                    .astype(int)
                    for mon in range(1, 13)
                },
                **{
                    f"mc_pccm_mon_{mon}": df_mc[
                        [
                            f"MC_PLAN_TYPE_CD_"
                            f"{str(seq).zfill(2)}_"
                            f"{str(mon).zfill(2)}"
                            for seq in range(1, 17)
                        ]
                    ]
                    .isin([2, 3, 70])
                    .any(axis=1)
                    .astype(int)
                    for mon in range(1, 13)
                },
            }
        )
        df_mc = df_mc.map_partitions(
            lambda pdf: pdf.assign(
                **{
                    **{
                        f"mc_{mc_type}_months": pdf[f"mc_{mc_type}_mon_1"]
                        .astype(str)
                        .str.cat(
                            pdf[
                                [
                                    f"mc_{mc_type}_mon_{mon}"
                                    for mon in range(2, 13)
                                ]
                            ].astype(str),
                            sep="",
                        )
                        for mc_type in ["comp", "behav_health", "pccm"]
                    },
                    **{
                        f"tot_mc_{mc_type}_months": pdf[
                            [
                                f"mc_{mc_type}_mon_" f"{mon}"
                                for mon in range(1, 13)
                            ]
                        ].sum(axis=1)
                        for mc_type in ["comp", "behav_health", "pccm"]
                    },
                }
            )
        )
        df_mc = df_mc.drop(
            columns=[
                f"mc_{mc_type}_mon_{mon}"
                for mc_type, mon in product(
                    ["comp", "behav_health", "pccm"], range(1, 13)
                )
            ]
        )

        self.dct_files["mc"] = df_mc
        self.dct_files["base"] = self.dct_files["base"].merge(
            df_mc[
                [
                    f"mc_{mc_type}_months"
                    for mc_type in ["comp", "behav_health", "pccm"]
                ]
                + [
                    f"total_mc_{mc_type}_months"
                    for mc_type in ["comp", "behav_health", "pccm"]
                ]
            ].compute(),
            left_index=True,
            right_index=True,
            how="left",
        )

    def flag_tanf(self):
        """
        The Temporary Assistance for Needy Families (TANF) program provides
        temporary financial assistance for pregnant women and families with
        one or more dependent children. This provides financial assistance to
        help pay for food, shelter, utilities, and expenses other than
        medical. In TAF files this is identified via

        TANF_CASH_CD:
            - 1: INDIVIDUAL DID NOT RECEIVE TANF BENEFITS DURING THE YEAR;
            - 2: INDIVIDUAL DID RECEIVE TANF BENEFITS DURING THE YEAR
        """
        df_base = self.dct_files["base"]
        df_base = df_base.assign(
            tanf=(
                dd.to_numeric(df_base["TANF_CASH_CD"], errors="coerce") == 2
            ).astype(int)
        )
        self.dct_files["base"] = df_base

    def gather_bene_level_diag_ndc_codes(self):
        """Constructs patient level NDC and diagnosis code list columns and
        saves them to individual file
        """
        lst_util_claim_types = ["ip", "ot", "rx"]
        dct_utilization_claims = {
            claim_type: taf_file.TAFFile.get_claim_instance(
                claim_type,
                self.year,
                self.state,
                self.data_root,
                clean=False,
                preprocess=False,
                pq_engine=self.pq_engine,
                tmp_folder=self.tmp_folder,
            )
            for claim_type in lst_util_claim_types
        }
        for claim_type in lst_util_claim_types:
            claim_file = dct_utilization_claims[claim_type]
            claim_file.clean_codes()
            claim_file.gather_bene_level_diag_ndc_codes()

        df_diag = dd.concat(
            [
                dct_utilization_claims[claim_type]["base_diag_codes"]
                for claim_type in ["ip", "ot"]
            ],
            axis=0,
            interleave_partitions=True,
        )
        df_ndc = dd.concat(
            [
                dct_utilization_claims[claim_type]["line_ndc_codes"]
                for claim_type in ["ip", "ot", "ndc"]
            ],
            axis=0,
            interleave_partitions=True,
        )

        dataframe_utils.fix_index(df_diag, index_name=self.index_col)
        dataframe_utils.fix_index(df_ndc, index_name=self.index_col)
        df_diag = df_diag.assign(
            **{col: df_ndc[col] for col in ["LST_NDC", "LST_NDC_RAW"]}
        )
        self.dct_files["diag_and_ndc_codes"] = df_diag
        self.cache_results("diag_and_ndc_codes")

    def add_risk_adjustment_scores(self):
        """Adds bene level risk adjustment scores. Currently supports
        Elixhauser scores."""
        if "diag_and_ndc_codes" not in self.dct_files:
            self.gather_bene_level_diag_ndc_codes()
        df_diag_ndc = self.dct_files["diag_and_ndc_codes"]
        df_diag_ndc = elixhauser_comorbidity.score(
            df_diag_ndc, "LST_DIAG_CD", cms_format="TAF"
        )
        dataframe_utils.fix_index(df_diag_ndc, "BENE_MSIS")
        self.dct_files["diag_ang_ndc"] = df_diag_ndc
        self.cache_results("diag_and_ndc")
        df_base = self.dct_files["base"]
        df_base = df_base.assign(
            **{
                col: df_diag_ndc[col]
                for col in ["elixhauser_score"]
                + ["ELX_GRP_" + str(i) for i in range(1, 32)]
            }
        )
        self.dct_files["base"] = df_base
        self.cache_results("base")

    def flag_ffs_months(self):
        """
        Creates flags for months enrolled in medicaid without
        enrollment in managed care plans of 3 categories, and adds columns
        denoting total number of months enrolled in these
        plans and the enrollment sequence pattern.
        """
        df_base = self.dct_files["base"]
        df_base = df_base.assign(
            **{
                "ffs_months": df_base.apply(
                    lambda x: "".join(
                        [
                            str(int((enrl == 1) and (mc == 0)))
                            for mc, enrl in zip(
                                x["mc_comp_months"], x["enrolled_months"]
                            )
                        ]
                    ),
                    axis=1,
                ),
                "ffs_no_mc_behav_health_months": df_base.apply(
                    lambda x: "".join(
                        [
                            str(int((enrl == 1) and (mc == 0)))
                            for mc, enrl in zip(
                                x["mc_behav_health_months"],
                                x["enrolled_months"],
                            )
                        ]
                    ),
                    axis=1,
                ),
                "ffs_no_mc_pccm_months": df_base.apply(
                    lambda x: "".join(
                        [
                            str(int((enrl == 1) and (mc == 0)))
                            for mc, enrl in zip(
                                x["mc_pccm_months"], x["enrolled_months"]
                            )
                        ]
                    ),
                    axis=1,
                ),
            }
        )
        df_base = df_base.assign(
            **{
                f"total_ffs_{ffs_type}months": df_base[f"ffs_{ffs_type}months"]
                .str.replace("0", "")
                .str.len()
                for ffs_type in ["", "no_mc_behav_health", "no_mc_pccm"]
            }
        )
        self.dct_files["base"] = df_base

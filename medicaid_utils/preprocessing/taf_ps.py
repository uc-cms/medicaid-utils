"""This module has TAFPS class which wraps together cleaning/ preprocessing routines specific for TAF PS files"""
import os

import numpy as np
import pandas as pd
import dask.dataframe as dd

from medicaid_utils.preprocessing import taf_file
from medicaid_utils.common_utils import dataframe_utils

data_folder = os.path.join(os.path.dirname(__file__), "data")
other_data_folder = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "other_datasets", "data"
)

# MC - 1, 5, 6, 7, 8, 9, 10, 11, 12, 13, 17
# FFS - 2, 14, 15
# unknown - 3, 4, 16, 18, 60, 70, 80

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
        Initializes PS file object by preloading and preprocessing(if opted in) the file
        :param year: Year
        :param state: State
        :param data_root: Root folder with cms data
        :param index_col: Column to use as index. Eg. BENE_MSIS or MSIS_ID. The raw file is expected to be already
        sorted with index column
        :param clean: Run cleaning routines if True
        :param preprocess: Add commonly used constructed variable columns, if True
        :param rural_method: Method to use for rural variable construction. Available options: 'ruca', 'rucc'
        :param tmp_folder: Folder to use to store temporary files
        :param pq_engine: Parquet Engine
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

        # Default filters to filter out benes that do not meet minimum standard of cleanliness criteria
        # duplicated_bene_id exclusion will remove benes with duplicated BENE_MSIS ids
        self.dct_default_filters = {"duplicated_bene_id": 0}
        if clean:
            self.clean()
        if preprocess:
            self.preprocess(rural_method)

    def clean(self):
        """Runs cleaning routines and created common exclusion flags based on default filters"""
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
        # self.add_eligibility_status_columns()
        self.cache_results()

    def flag_common_exclusions(self):
        """
        Adds commonly used exclusion flags
        New Column(s):
            - excl_duplicated_bene_id - 0 or 1, 1 when bene's index column is repeated
        """
        df_base = self.dct_files["base"]
        df_base = df_base.assign(**{f"_{self.index_col}": df_base.index})
        # Some BENE_MSIS's are repeated in PS files. Some patients share the same BENE_ID and yet have different
        # MSIS_IDs. Some of them even have different 'dates of birth'. Since we cannot find any explanation for
        # such patterns, we decided on removing these BENE_MSIS's as per issue #29 in FARA project
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

    def add_gender(self):
        """Adds integer 'female' column based on 'SEX_CD' column. Undefined values ('U') in SEX_CD column will
        result in female column taking the value -1"""
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
        Classifies benes into rural/ non-rural on the basis of RUCA/ RUCC of their resident ZIP/ FIPS codes
        New Columns:

            - resident_state_cd
            - rural - 0/ 1/ np.nan, 1 when bene's residence is in a rural location, 0 when not, -1 when zip code is missing
            - pcsa - resident PCSA code
            - {ruca_code/ rucc_code} - resident ruca_code

        This function uses
            - RUCA 3.1 dataset (from https://www.ers.usda.gov/webdocs/DataFiles/53241/RUCA2010zipcode.xlsx?v=8673). RUCA
            codes >= 4 denote rural and the rest denote urban as per https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6286055/#SD1
            - RUCC codes were downloaded from https://www.ers.usda.gov/webdocs/DataFiles/53251/ruralurbancodes2013.xls?v=2372.
            RUCC codes >= 8 denote rural and the rest denote urban.
            - ZCTAs x zipcode crosswalk from UDSMapper (https://udsmapper.org/zip-code-to-zcta-crosswalk/),
            - zipcodes from multiple sources
            - istance between centroids of zipcodes using NBER data (https://nber.org/distance/2016/gaz/zcta5/gaz2016zcta5centroid.csv)

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
        # RI Zip codes have problems. They are all invalid unless the last character is dropped and
        # a zero is added to the left
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
        # zipcodes from multiple sources, and distance between centroids of zipcodes using NBER data
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
        Flags benes with  DUAL_ELGBL_CD equal to 1 (full dual), 2 (partial dual), or 3 (other dual) in any month are
        flagged as duals. Reference: [Identifying beneficiaries with a substance use disorder]
        (https://www.medicaid.gov/medicaid/data-and-systems/downloads/macbis/sud_techspecs.docx)

        """
        df = self.dct_files["base"]
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                dual=np.column_stack(
                    [
                        pd.to_numeric(pdf[col], errors="coerce").isin(
                            [1, 2, 3]
                        )
                        for col in [
                            f"DUAL_ELGBL_CD_{str(mon).zfill(2)}"
                            for mon in range(1, 13)
                        ]
                    ]
                )
                .any(axis=1)
                .astype(int),
                dual_months=np.column_stack(
                    [
                        pd.to_numeric(pdf[col], errors="coerce").isin(
                            [1, 2, 3]
                        )
                        for col in [
                            f"DUAL_ELGBL_CD_{str(mon).zfill(2)}"
                            for mon in range(1, 13)
                        ]
                    ]
                )
                .sum(axis=1)
                .astype(int),
            )
        )
        self.dct_files["base"] = df

    def flag_restricted_benefits(self):
        """
        Flags beneficiaries whose benefits are restricted. Benes with the below values in their RSTRCTD_BNFTS_CD_XX
        columns are NOT assumed to have restricted benefits:

            1. Individual is eligible for Medicaid or CHIP and entitled to the full scope of Medicaid or CHIP benefits.
            4. Individual is eligible for Medicaid or CHIP but only entitled to restricted benefits for
            pregnancy-related services.
            5. Individual is eligible for Medicaid or Medicaid-Expansion CHIP but, for reasons other than alien,
            dual-eligibility or pregnancy-related status, is only entitled to restricted benefits (e.g., restricted
            benefits based upon substance abuse, medically needy or other criteria).
            7. Individual is eligible for Medicaid and entitled to Medicaid benefits under an alternative package of
            benchmark-equivalent coverage, as enacted by the Deficit Reduction Act of 2005.

        Reference: [Identifying beneficiaries with a substance use disorder](https://www.medicaid.gov/medicaid/data-and-systems/downloads/macbis/sud_techspecs.docx))

        Returns
        -------

        """
        df = self.dct_files["base"]
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                any_restricted_benefit_month=np.column_stack(
                    [
                        ~pd.to_numeric(pdf[col], errors="coerce").isin(
                            [1, 4, 5, 7]
                        )
                        for col in [
                            f"RSTRCTD_BNFTS_CD_{str(mon).zfill(2)}"
                            for mon in range(1, 13)
                        ]
                    ]
                )
                .any(axis=1)
                .astype(int),
                restricted_benefit_months=np.column_stack(
                    [
                        (
                            ~pd.to_numeric(pdf[col], errors="coerce").isin(
                                [1, 4, 5, 7]
                            )
                        ).astype(int)
                        for col in [
                            f"RSTRCTD_BNFTS_CD_{str(mon).zfill(2)}"
                            for mon in range(1, 13)
                        ]
                    ]
                )
                .sum(axis=1)
                .astype(int),
            )
        )
        self.dct_files["base"] = df

    def compute_enrollment_gaps(self):
        """Computes enrollment gaps using dates file. Adds number of enrollment gaps and length of maximum enrollment
        gap in days columns"""
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

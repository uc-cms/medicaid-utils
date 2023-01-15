"""This module has MAXPS class which wraps together cleaning/ preprocessing
routines specific for MAX PS files"""
import os

import numpy as np
import pandas as pd
import dask.dataframe as dd

from medicaid_utils.preprocessing import max_file

data_folder = os.path.join(os.path.dirname(__file__), "data")
other_data_folder = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "other_datasets", "data"
)


class MAXPS(max_file.MAXFile):
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
        Initializes PS file object by preloading and preprocessing(if opted
        in) the file

        Parameters
        ----------
        year : int
            Year of claim file
        state : str
            State of claim file
        data_root : str
            Root folder of raw claim files
        index_col : str, default='BENE_MSIS'
            Index column name. Eg. BENE_MSIS or MSIS_ID. The raw file is
            expected to be already
        sorted with index column
        clean : bool, default=True
            Should the associated files be cleaned?
        preprocess : bool, default=True
            Should the associated files be preprocessed?
        rural_method : {'ruca', 'rucc'}
            Method to use for rural variable construction
        tmp_folder : str, default=None
            Folder location to use for caching intermediate results. Can be
            turned off by not passing this argument.
        pq_engine : str, default='pyarrow'
            Parquet engine to use

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
        """Runs cleaning routines and adds common exclusion flags based on
        default filters"""
        super().clean()
        self.flag_common_exclusions()
        self.cache_results()

    def preprocess(
        self, rural_method="ruca"
    ):  # pylint: disable=missing-param-doc
        """
        Adds rural, eligibility criteria, dual, and restricted benefits
        indicator variables

        Parameters
        ----------
        rural_method : {'ruca', 'rucc'}
            Method to use for rural variable construction

        """
        self.flag_rural(rural_method)
        self.add_eligibility_status_columns()
        self.flag_duals()
        self.flag_restricted_benefits()
        self.flag_tanf()
        self.cache_results()

    def flag_common_exclusions(self):
        """
        Adds exclusion flags
        New Column(s):
            - excl_duplicated_bene_id - 0 or 1, 1 when bene's index column
            is repeated

        """
        self.df = self.df.assign(
            **dict([(f"_{self.index_col}", self.df.index)])
        )
        # Some BENE_MSIS's are repeated in PS files. Some patients share the
        # same BENE_ID and yet have different MSIS_IDs. Some of them even
        # have different 'dates of birth'. Since we cannot find any
        # explanation for such patterns, we decided on removing these
        # BENE_MSIS's as per issue #29 in FARA project
        # (https://rcg.bsd.uchicago.edu/gitlab/mmurugesan/hrsa_max_feature_extraction/issues/29)
        self.df = self.df.map_partitions(
            lambda pdf: pdf.assign(
                excl_duplicated_bene_id=pdf.duplicated(
                    [f"_{self.index_col}"], keep=False
                ).astype(int)
            )
        )
        self.df = self.df.drop([f"_{self.index_col}"], axis=1)

    def flag_duals(self):
        """
        Flags dual patients
        New column(s):
            dual - 0 or 1 column, 0 if 0 <= EL_MDCR_DUAL_ANN <= 9 for years
            2007, 2009, 2011 0 <= EL_MDCR_DUAL_ANN <= 9 for other years
        """
        self.df = self.df.assign(
            dual=(
                ~dd.to_numeric(
                    self.df[
                        "EL_MDCR_DUAL_ANN"
                        if (self.year in [2007, 2009, 2011])
                        else "EL_MDCR_ANN_XOVR_99"
                    ],
                    errors="coerce",
                ).between(0, 9, inclusive="both")
            ).astype(int)
        )

    def flag_rural(
        self, method: str = "ruca"
    ):  # pylint: disable=missing-param-doc
        """
        Classifies benes into rural/ non-rural on the basis of RUCA/ RUCC of
        their resident ZIP/ FIPS codes

        New Columns:

            - resident_state_cd
            - rural - 0/ 1/ -1, 1 when bene's residence is in a rural
            location, 0 when not. -1 when zip code is missing
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
        index_col = self.df.index.name
        zip_folder = os.path.join(other_data_folder, "zip")
        self.df = self.df.assign(**dict([(index_col, self.df.index)]))

        # 2012 RI claims report zip codes have problems. They are all
        # invalid unless the last character is dropped. So
        # dropping it as per email exchange with Alex Knitter & Dr. 
        # Laiteerapong (May 2020)
        self.df = self.df.assign(
            EL_RSDNC_ZIP_CD_LTST=self.df["EL_RSDNC_ZIP_CD_LTST"].where(
                ~((self.df["STATE_CD"] == "RI") & (self.df["year"] == 2012)),
                self.df["EL_RSDNC_ZIP_CD_LTST"].str[:-1],
            )
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
            zip=df_zip_state_pcsa["zip"].str.replace(" ", "").str.zfill(9)
        )
        df_zip_state_pcsa = df_zip_state_pcsa.rename(
            columns={
                "zip": "EL_RSDNC_ZIP_CD_LTST",
                "state_cd": "resident_state_cd",
            }
        )
        self.df = self.df[
            self.df.columns.difference(
                ["resident_state_cd", "pcsa", "ruca_code"]
            ).tolist()
        ]

        self.df = self.df.assign(
            EL_RSDNC_ZIP_CD_LTST=self.df["EL_RSDNC_ZIP_CD_LTST"]
            .str.replace(" ", "")
            .str.zfill(9)
        )
        self.df = self.df.merge(
            df_zip_state_pcsa[
                [
                    "EL_RSDNC_ZIP_CD_LTST",
                    "resident_state_cd",
                    "pcsa",
                    "ruca_code",
                ]
            ],
            how="left",
            on="EL_RSDNC_ZIP_CD_LTST",
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
                "FIPS": "EL_RSDNC_CNTY_CD_LTST",
            }
        )
        df_rucc = df_rucc.assign(
            EL_RSDNC_CNTY_CD_LTST=df_rucc["EL_RSDNC_CNTY_CD_LTST"]
            .str.strip()
            .str[2:],
            resident_state_cd=df_rucc["resident_state_cd"]
            .str.strip()
            .str.upper(),
        )
        self.df = self.df.assign(
            EL_RSDNC_CNTY_CD_LTST=self.df["EL_RSDNC_CNTY_CD_LTST"].str.strip(),
            resident_state_cd=self.df["resident_state_cd"].where(
                ~self.df["resident_state_cd"].isna(), self.df["STATE_CD"]
            ),
        )

        self.df = self.df[
            [col for col in self.df.columns if col not in ["rucc_code"]]
        ]
        self.df = self.df.merge(
            df_rucc[
                ["EL_RSDNC_CNTY_CD_LTST", "resident_state_cd", "rucc_code"]
            ],
            how="left",
            on=["EL_RSDNC_CNTY_CD_LTST", "resident_state_cd"],
        )
        self.df = self.df.assign(
            **{
                col: dd.to_numeric(self.df[col], errors="coerce")
                for col in ["rucc_code", "ruca_code"]
            }
        )

        # RUCA codes >= 4 denote rural and the rest denote urban
        # as per https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6286055/#SD1
        # and as in FARA year 1 papers
        if method == "ruca":
            self.df = self.df.map_partitions(
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
            self.df = self.df.map_partitions(
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
        if self.df.index.name != index_col:
            self.df = self.df.set_index(index_col, sorted=True)

    def add_eligibility_status_columns(self) -> None:
        """
        Add eligibility columns based on MAX_ELG_CD_MO_{month} values for
        each month.
        MAX_ELG_CD_MO:00 = NOT ELIGIBLE, 99 = UNKNOWN ELIGIBILITY  => codes
        to denote ineligibility

        New Column(s):
            - elg_mon_{month} - 0 or 1 value column, denoting eligibility
            for each month
            - total_elg_mon - No. of eligible months
            - elg_full_year - 0 or 1 value column, 1 if total_elg_mon = 12
            - elg_over_9mon - 0 or 1 value column, 1 if total_elg_mon >= 9
            - elg_over_6mon - 0 or 1 value column, 1 if total_elg_mon >= 6
            - elg_cont_6mon - 0 or 1 value column, 1 if patient has 6
            continuous eligible months
            - mas_elg_change - 0 or 1 value column, 1 if patient had
            multiple mas group memberships during claim year
            - mas_assignments - comma separated list of MAS assignments
            - boe_assignments - comma separated list of BOE assignments
            - dominant_boe_group - BOE status held for the most number of
            months
            - boe_elg_change - 0 or 1 value column, 1 if patient had
            multiple boe group memberships during claim year
            - child_boe_elg_change - 0 or 1 value column, 1 if patient had
            multiple boe group memberships during claim year
            - elg_change - 0 or 1 value column, 1 if patient had multiple
            eligibility group memberships during claim year
            - eligibility_aged - Eligibility as aged anytime during the
            claim year
            - eligibility_child - Eligibility as child anytime during the
            claim year
            - max_gap - Maximum gap in enrollment in months
            - max_cont_enrollment - Maximum duration of continuous enrollment
        """
        # MAS & BOE groups are arrived at from MAX_ELG_CD variable
        # (https://resdac.org/sites/datadocumentation.resdac.org/files/MAX%20UNIFORM%20ELIGIBILITY%20CODE%20TABLE.txt)
        # FARA year 2 peds paper decided to collapse BOE assignments,
        # combining disabled and child groups
        self.df = self.df.map_partitions(
            lambda pdf: pdf.assign(
                **{
                    f"MAS_ELG_MON_{mon}": pdf[f"MAX_ELG_CD_MO_{mon}"]
                    .where(
                        ~(
                            pdf[f"MAX_ELG_CD_MO_{mon}"]
                            .str.strip()
                            .isin(["00", "99", "", "."])
                            | pdf[f"MAX_ELG_CD_MO_{mon}"].isna()
                        ),
                        "99",
                    )
                    .astype(str)
                    .str.strip()
                    .str[:1]
                    for mon in range(1, 13)
                }
            )
        )
        self.df = self.df.map_partitions(
            lambda pdf: pdf.assign(
                mas_elg_change=(
                    pdf[[f"MAS_ELG_MON_{mon}" for mon in range(1, 13)]]
                    .replace("9", np.nan)
                    .nunique(axis=1, dropna=True)
                    > 1
                ).astype(int),
                mas_assignments=pdf[
                    [f"MAS_ELG_MON_{mon}" for mon in range(1, 13)]
                ]
                .replace("9", "")
                .apply(
                    lambda x: ",".join(set(",".join(x).split(","))), axis=1
                ),
            )
        )
        self.df = self.df.map_partitions(
            lambda pdf: pdf.assign(
                **{
                    **{
                        f"BOE_ELG_MON_{mon}": np.select(
                            [
                                pdf[f"MAX_ELG_CD_MO_{mon}"]
                                .astype(str)
                                .isin(["11", "21", "31", "41"]),  # aged
                                pdf[f"MAX_ELG_CD_MO_{mon}"]
                                .astype(str)
                                .isin(["12", "22", "32", "42"]),
                                # blind / disabled
                                pdf[f"MAX_ELG_CD_MO_{mon}"]
                                .astype(str)
                                .isin(
                                    ["14", "16", "24", "34", "44", "48"]
                                ),  # child
                                pdf[f"MAX_ELG_CD_MO_{mon}"]
                                .astype(str)
                                .isin(
                                    ["15", "17", "25", "35", "3A", "45"]
                                ),  # adult
                                pdf[f"MAX_ELG_CD_MO_{mon}"]
                                .astype(str)
                                .isin(["51", "52", "54", "55"]),
                            ],
                            # state demonstration EXPANSION
                            [1, 2, 3, 4, 5],
                            default=6,
                        )
                        for mon in range(1, 13)
                    },
                    **{
                        f"CHILD_BOE_ELG_MON_{mon}": np.select(
                            [
                                pdf[f"MAX_ELG_CD_MO_{mon}"]
                                .astype(str)
                                .isin(["11", "21", "31", "41"]),  # aged
                                pdf[f"MAX_ELG_CD_MO_{mon}"]
                                .astype(str)
                                .isin(
                                    [
                                        "12",
                                        "22",
                                        "32",
                                        "42",
                                        "14",
                                        "16",
                                        "24",
                                        "34",
                                        "44",
                                        "48",
                                    ]
                                ),
                                # blind / disabled OR CHILD
                                pdf[f"MAX_ELG_CD_MO_{mon}"]
                                .astype(str)
                                .isin(
                                    ["15", "17", "25", "35", "3A", "45"]
                                ),  # adult
                                pdf[f"MAX_ELG_CD_MO_{mon}"]
                                .astype(str)
                                .isin(["51", "52", "54", "55"]),
                            ],
                            # state demonstration EXPANSION
                            [1, 2, 3, 4],
                            default=5,
                        )
                        for mon in range(1, 13)
                    },
                }
            )
        )
        self.df = self.df.map_partitions(
            lambda pdf: pdf.assign(
                boe_elg_change=(
                    pdf[[f"BOE_ELG_MON_{mon}" for mon in range(1, 13)]]
                    .replace(6, np.nan)
                    .nunique(axis=1, dropna=True)
                    > 1
                ).astype(int),
                child_boe_elg_change=(
                    pdf[[f"CHILD_BOE_ELG_MON_{mon}" for mon in range(1, 13)]]
                    .replace(5, np.nan)
                    .nunique(axis=1, dropna=True)
                    > 1
                ).astype(int),
                boe_assignments=pdf[
                    [f"BOE_ELG_MON_{mon}" for mon in range(1, 13)]
                ]
                .astype(str)
                .apply(
                    lambda x: ",".join(set(",".join(x).split(","))), axis=1
                ),
                dominant_boe_grp=pdf[
                    [f"BOE_ELG_MON_{mon}" for mon in range(1, 13)]
                ]
                .replace(6, np.nan)
                .mode(axis=1, dropna=True)[0]
                .fillna(6)
                .astype(int),
            )
        )

        self.df = self.df.assign(
            elg_change=(
                self.df[["boe_elg_change", "mas_elg_change"]].sum(axis=1) > 0
            ).astype(int),
            eligibility_aged=(
                self.df[[f"BOE_ELG_MON_{mon}" for mon in range(1, 13)]] == 1
            ).any(axis="columns"),
            eligibility_child=(
                self.df[[f"BOE_ELG_MON_{mon}" for mon in range(1, 13)]] == 3
            ).any(axis="columns"),
        )

        self.df = self.df.map_partitions(
            lambda pdf: pdf.assign(
                **{
                    f"elg_mon_{mon}": ~(
                        pdf[f"MAX_ELG_CD_MO_{mon}"]
                        .str.strip()
                        .isin(["00", "99", "", "."])
                        | pdf[f"MAX_ELG_CD_MO_{mon}"].isna()
                    ).astype(int)
                    for mon in range(1, 13)
                }
            )
        )
        self.df = self.df.assign(
            total_elg_mon=self.df[[f"elg_mon_{i}" for i in range(1, 13)]].sum(
                axis=1
            )
        )
        self.df = self.df.assign(
            elg_full_year=(self.df["total_elg_mon"] == 12).astype(int),
            elg_over_9mon=(self.df["total_elg_mon"] >= 9).astype(int),
            elg_over_6mon=(self.df["total_elg_mon"] >= 6).astype(int),
        )

        self.df = self.df.map_partitions(
            lambda pdf: pdf.assign(
                max_gap=pdf[[f"elg_mon_{mon}" for mon in range(1, 13)]]
                .astype(int)
                .astype(str)
                .apply(
                    lambda x: max([len(gap) for gap in "".join(x).split("1")]),
                    axis=1,
                ),
                max_cont_enrollment=pdf[
                    ["elg_mon_" + str(mon) for mon in range(1, 13)]
                ]
                .astype(int)
                .astype(str)
                .apply(
                    lambda x: max(
                        [
                            len(enrolled_months)
                            for enrolled_months in "".join(x).split("0")
                        ]
                    ),
                    axis=1,
                ),
            )
        )
        self.df = self.df.assign(
            elg_cont_6mon=(self.df["max_cont_enrollment"] >= 6).astype(int)
        )

        lst_cols_to_delete = [f"MAS_ELG_MON_{mon}" for mon in range(1, 13)] + [
            f"BOE_ELG_MON_{mon}" for mon in range(1, 13)
        ]
        self.df = self.df.drop(lst_cols_to_delete, axis=1)

    def flag_restricted_benefits(self):
        """
        Checks individual's eligibility for various medicaid services,
        based on EL_RSTRCT_BNFT_FLG_{month} values,
            - 1 = full scope; INDIVIDUAL IS ELIGIBLE FOR MEDICAID DURING THE
            MONTH AND IS ENTITLED TO THE FULL SCOPE OF MEDICAID BENEFITS.
            - 2 = alien; INDIVIDUAL IS ELIGIBLE FOR MEDICAID DURING THE
            MONTH BUT ONLY ENTITLED TO RESTRICTED BENEFITS
                BASED ON ALIEN STATUS
            - 3 = dual
            - 4 = pregnancy
            - 5 = other, eg. substance abuse, medically needy
            - 6 = family planning
            - 7 = alternative package of benchmark equivalent coverage,
            2011 data had no values of 7 and 8
            - 8 = "money follows the person" rebalancing demonstration,
            2011 data had no values of 7 and 8
            - 9 = unknown
            - A = Psychiatric residential treatments demonstration
            - B = Health Opportunity Account
            - C = CHIP dental coverage, supplemental to employer sponsored
            insurance
            - W = Medicaid health insurance premium payment assistance (MA,
            NJ, VT, OK)
            - X = rx drug
            - Y = drug and dual
            - Z = drug and dual, but Medicaid was not paying for the benefits.

        Benefits are non-comprehensive (restricted) when
        EL_RSTRCT_BNFT_FLG_{month} has any of the below values:
            - "2", "3", "6":  for states other than "AR", "ID", "SD"
            - "2", "4", "3", "6":  for states "AR", "ID", "SD"

        New column(s):
            - any_restricted_benefit_month: 0 or 1, 1 when bene's benefits
            are restricted for atleast 1 month
            - restricted_benefit_months: Number of restricted benefit months
            - restricted_benefits: 0 or 1, 1 when number of restricted
            benefit months are more than the number of number of months the
            bene was enrolled in medicaid

        """
        lst_excluded_restricted_benefit_code = (
            ["2", "3", "6"]
            if self.state not in ["AR", "ID", "SD"]
            else ["2", "4", "3", "6"]
        )

        self.df = self.df.map_partitions(
            lambda pdf: pdf.assign(
                any_restricted_benefit_month=np.column_stack(
                    [
                        pdf[col]
                        .fillna("0")
                        .str.strip()
                        .isin([lst_excluded_restricted_benefit_code])
                        for col in [
                            f"EL_RSTRCT_BNFT_FLG_{str(mon)}"
                            for mon in range(1, 13)
                        ]
                    ]
                )
                .any(axis=1)
                .astype(int),
                restricted_benefit_months=np.column_stack(
                    [
                        pdf[col]
                        .fillna("0")
                        .str.strip()
                        .isin([lst_excluded_restricted_benefit_code])
                        for col in [
                            f"EL_RSTRCT_BNFT_FLG_{str(mon)}"
                            for mon in range(1, 13)
                        ]
                    ]
                )
                .sum(axis=1)
                .astype(int),
            )
        )
        self.df = self.df.assign(
            restricted_benefits=(
                self.df["restricted_benefit_months"]
                > (12 - self.df["total_elg_mon"])
            ).astype(int)
        )

    def flag_tanf(self):
        """
        The Temporary Assistance for Needy Families (TANF) program provides
        temporary financial assistance for pregnant women and families with
        one or more dependent children. This provides financial assistance to
        help pay for food, shelter, utilities, and expenses other than
        medical. In MAX files this is identified via

        EL_TANF_CASH_FLG:
            - 1 = INDIVIDUAL DID NOT RECEIVE TANF BENEFITS DURING THE MONTH;
            - 2 = INDIVIDUAL DID RECEIVE TANF BENEFITS DURING THE MONTH. CO
            and ID either 0 or 9

        New Column(s):
            - tanf : 0 or 1, denoting usage of TANF benefits in any of the
            months
        """

        self.df = self.df.map_partitions(
            lambda pdf: pdf.assign(
                tanf=np.column_stack(
                    [
                        pd.to_numeric(pdf[col], errors="coerce").isin(
                            [2] if self.state not in ["CO", "ID"] else [0, 9]
                        )
                        for col in [
                            f"EL_TANF_CASH_FLG_{str(mon)}"
                            for mon in range(1, 13)
                        ]
                    ]
                )
                .any(axis=1)
                .astype(int)
            )
        )

import dask.dataframe as dd
import pandas as pd
import sys

from medicaid_utils.preprocessing import max_file
from medicaid_utils.common_utils import dataframe_utils


class MAXOT(max_file.MAXFile):
    def __init__(
        self,
        year,
        state,
        data_root,
        index_col="BENE_MSIS",
        clean=True,
        preprocess=True,
        df_ip=None,
        tmp_folder=None,
    ):
        super(MAXOT, self).__init__(
            "ot", year, state, data_root, index_col, False, False, tmp_folder
        )
        self.dct_default_filters = {
            "missing_dob": 0,
            "duplicated": 0,
            "missing_srvc_bgn_date": 0,
        }
        if clean:
            self.clean()
        if preprocess:
            self.preprocess()

    def clean(self):
        super(MAXOT, self).clean()
        self.df = self.cache_results()
        self.clean_diag_codes()
        self.clean_proc_codes()
        self.flag_common_exclusions()
        self.flag_duplicates()
        self.df = self.cache_results()

    def preprocess(self):
        super(MAXOT, self).preprocess()
        self.calculate_payment()
        self.flag_ed_use()
        self.flag_transport()
        self.flag_dental()
        self.flag_em()
        self.df = self.cache_results()

    def flag_ip_overlaps_and_ed(self, df_ip):
        self.find_ot_ip_overlaps(df_ip)
        self.add_ot_flags()
        self.df = self.cache_results()

    def flag_common_exclusions(self) -> None:
        self.df = dataframe_utils.fix_index(
            self.df, self.index_col, drop_column=True
        )
        self.df = self.df.map_partitions(
            lambda pdf: pdf.assign(
                excl_missing_dob=pdf["birth_date"].isnull().astype(int),
                excl_missing_srvc_bgn_date=pdf["srvc_bgn_date"]
                .isnull()
                .astype(int),
                excl_encounter_claim=(
                    (pd.to_numeric(pdf["PHP_TYPE"], errors="coerce") == 77)
                    | (
                        (pd.to_numeric(pdf["PHP_TYPE"], errors="coerce") == 88)
                        & (
                            pd.to_numeric(pdf["TYPE_CLM_CD"], errors="coerce")
                            == 3
                        )
                    )
                ).astype(int),
                excl_capitation_claim=(
                    (pd.to_numeric(pdf["PHP_TYPE"], errors="coerce") == 88)
                    & (pd.to_numeric(pdf["TYPE_CLM_CD"], errors="coerce") == 2)
                ).astype(int),
                excl_ffs_claim=(
                    ~(
                        (pd.to_numeric(pdf["PHP_TYPE"], errors="coerce") == 77)
                        | (
                            (
                                pd.to_numeric(pdf["PHP_TYPE"], errors="coerce")
                                == 88
                            )
                            & pd.to_numeric(
                                pdf["TYPE_CLM_CD"], errors="coerce"
                            ).isin([2, 3])
                        )
                    )
                ).astype(int),
                excl_female=(pdf["female"] == 1).astype(int),
            )
        )

    def flag_duplicates(self):
        self.df = dataframe_utils.fix_index(self.df, self.index_col, True)
        self.df = self.df.map_partitions(
            lambda pdf: pdf.assign(
                excl_duplicated=pdf.assign(_index_col=pdf.index)[
                    [col for col in pdf.columns if col != "excl_duplicated"]
                ]
                .duplicated(keep="first")
                .astype(int)
            )
        )

    def flag_em(self) -> None:
        """
        Flag claim if procedure code belongs to E/M category
        New Column(s):
            EM - 0 or 1, 1 when PRCDR_CD in [99201, 99215] or [99301, 99350]
        :param dask.DataFrame df: OT dataframe
        :rtype: None
        """
        self.df = self.df.map_partitions(
            lambda pdf: pdf.assign(
                EM=(
                    (pd.to_numeric(pdf["PRCDR_CD_SYS"], errors="coerce") == 1)
                    & (
                        pd.to_numeric(
                            pdf["PRCDR_CD"], errors="coerce"
                        ).between(99201, 99205, inclusive="both")
                        | pd.to_numeric(
                            pdf["PRCDR_CD"], errors="coerce"
                        ).between(99211, 99215, inclusive="both")
                        | pd.to_numeric(
                            pdf["PRCDR_CD"], errors="coerce"
                        ).between(99381, 99387, inclusive="both")
                        | pd.to_numeric(
                            pdf["PRCDR_CD"], errors="coerce"
                        ).between(99391, 99397, inclusive="both")
                    )
                ).astype(int)
            )
        )
        return None

    def flag_dental(self) -> None:
        """
        Flag dental claims
        New Column(s):
            dental_TOS - 0 or 1, 1 when MAX_TOS = 9
            dental_PRCDR - 0 or 1, 1 when PRCDR_CD starts with 'D'
            dental - 0 or 1, 1 when any of dental_TOS or dental_PRCDR
        :param dask.DataFrame df: OT Dataframe
        :rtype: None
        """
        self.df = self.df.assign(
            dental_TOS=(
                dd.to_numeric(self.df["MAX_TOS"], errors="coerce") == 9
            ).astype(int)
        )
        self.df = self.df.assign(
            dental_PROC=(
                self.df.PRCDR_CD.astype(str).str.slice(stop=1) == "D"
            ).astype(int)
        )
        self.df["dental"] = (
            self.df[["dental_TOS", "dental_PROC"]]
            .any(axis="columns")
            .astype(int)
        )
        return None

    def flag_transport(self) -> None:
        """
        Flag transport claims
        New Column(s):
            transport_TOS - 0 or 1, 1 when MAX_TOS = 26
            transport_POS - 0 or 1, 1 when PLC_OF_SRVC_CD is 41 or 42
            transport - 0 or 1, 1 when any of transport_TOS or transport_POS
        :param dask.DataFrame df: OT Dataframe
        :rtype: dask.DataFrame
        """
        self.df = self.df.map_partitions(
            lambda pdf: pdf.assign(
                transport_TOS=(
                    pd.to_numeric(
                        pdf["MAX_TOS"], errors="coerce"  # TOS: TYPE OF SERVICE
                    )
                    == 26
                ).astype(int),
                transport_POS=pd.to_numeric(
                    pdf["PLC_OF_SRVC_CD"],
                    # # 41 = AMBULANCE - LAND 42 = AMBULANCE - AIR OR WATER
                    errors="coerce",
                )
                .isin([41, 42])
                .astype(int),
            )
        )

        self.df["transport"] = (
            self.df[["transport_TOS", "transport_POS"]]
            .any(axis="columns")
            .astype(int)
        )
        return None

    def find_ot_ip_overlaps(self, df_ip: dd.DataFrame) -> None:
        """
        Checks for OT claims that have an overlapping IP claim
        New Column(s):
            overlap - 0 or 1, 1 when OT claim has an overlapping IP claim
        :param DataFrame df_ot: OT DataFrame
        :param DataFrame df_ip: IP DataFrame
        :return: None
        """
        df_ot_ip = (
            self.df[["srvc_bgn_date", "srvc_end_date"]]
            .repartition(partition_size="10MB")
            .merge(
                df_ip[["admsn_date", "srvc_end_date"]].rename(
                    columns={"srvc_end_date": "ip_srvc_end_date"}
                ),
                left_index=True,
                right_index=True,
                how="left",
            )
        )
        df_ot_ip = dataframe_utils.fix_index(df_ot_ip, self.index_col)

        df_ot_ip = df_ot_ip.map_partitions(
            lambda pdf: pdf.assign(
                overlap=(
                    pdf["srvc_bgn_date"].between(
                        pdf["admsn_date"],
                        pdf["ip_srvc_end_date"],
                        inclusive=True,
                    )
                    | pdf["srvc_end_date"].between(
                        pdf["admsn_date"],
                        pdf["ip_srvc_end_date"],
                        inclusive=True,
                    )
                ).astype(int)
            )
        )

        df_ot_ip = df_ot_ip.map_partitions(
            lambda pdf: pdf.groupby(
                [self.index_col, "srvc_bgn_date", "srvc_end_date"]
            )["overlap"]
            .max()
            .reset_index(drop=False)
            .set_index(self.index_col)
        )
        df_ot_ip = dataframe_utils.fix_index(df_ot_ip, self.index_col)

        df_ot_ip = df_ot_ip.compute()

        self.df = self.df[self.df.columns.difference(["overlap"])].merge(
            df_ot_ip,
            on=[self.index_col, "srvc_bgn_date", "srvc_end_date"],
            how="inner",
        )
        self.df = dataframe_utils.fix_index(self.df, self.index_col)
        return None

    def add_ot_flags(self) -> None:
        """
        Assign flags for IP, OT and ED calculation
        Based on hierarchical principal: IP first,then ED, and then OT
        Marks claims that have overlapping IP claims, has ED services or have only OT services
        New Column(s):
            ip_incl - 0 or 1, 1 when has no dental and transport claims, and has an overlapping IP claim
            ed_incl - 0 or 1, 1 when has no dental and transport claims, has no overlapping IP claim, and has an ED
                      service in any visits corresponding this claim
            ot_incl - 0 or 1, 1 when has no dental and transport claims, has no overlapping IP claim, and has no ED
                      service in any visits corresponding this claim
            flag_drop - 0 or 1, 1 when ip_incl, ed_incl and ot_incl are all null
        :param df:
        :rtype: None
        """
        self.df["ip_incl"] = 0
        self.df["ed_incl"] = 0
        self.df["ed_incl_dental"] = 0
        self.df["ot_incl"] = 0

        transport_mask = self.df["transport"] != 1
        dental_mask = self.df["dental"] != 1
        overlap_mask = self.df["overlap"] == 1
        ed_claim_mask = self.df["any_ed"] == 1
        duration_mask = (self.df["duration"] >= 0) | (
            self.df["srvc_end_date"].isna()
            & (self.df.srvc_bgn_date.dt.year <= self.df.year)
        )

        ip_mask = transport_mask & dental_mask & overlap_mask
        ed_mask = (
            transport_mask
            & dental_mask
            & (~overlap_mask)
            & duration_mask
            & ed_claim_mask
        )
        ed_mask_dental_allowed = (
            transport_mask & (~overlap_mask) & duration_mask & ed_claim_mask
        )
        ot_mask = (
            transport_mask
            & dental_mask
            & (~overlap_mask)
            & duration_mask
            & (~ed_claim_mask)
        )

        self.df["ip_incl"] = self.df["ip_incl"].where(~ip_mask, 1)
        self.df["ed_incl"] = self.df["ed_incl"].where(~ed_mask, 1)
        self.df["ed_incl_dental"] = self.df["ed_incl_dental"].where(
            ~ed_mask_dental_allowed, 1
        )
        self.df["ot_incl"] = self.df["ot_incl"].where(~ot_mask, 1)
        self.df["flag_drop"] = (
            self.df[["ip_incl", "ed_incl", "ot_incl"]].sum(axis=1) < 1
        ).astype(int)
        return None

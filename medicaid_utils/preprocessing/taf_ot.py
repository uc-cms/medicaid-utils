import numpy as np
import pandas as pd
import sys

from medicaid_utils.preprocessing import taf_file
from medicaid_utils.common_utils import dataframe_utils


class TAFOT(taf_file.TAFFile):
    def __init__(
        self,
        year,
        state,
        data_root,
        index_col="BENE_MSIS",
        clean=True,
        preprocess=True,
        tmp_folder=None,
    ):
        super(TAFOT, self).__init__(
            "ot", year, state, data_root, index_col, False, False, tmp_folder
        )
        self.dct_default_filters = {"missing_dob": 0, "duplicated": 0}
        if clean:
            self.clean()
        if preprocess:
            self.preprocess()

    def clean(self):
        super(TAFOT, self).clean()
        self.clean_diag_codes()
        self.clean_proc_codes()
        self.flag_common_exclusions()
        self.flag_duplicates()

    def flag_common_exclusions(self) -> None:
        self.flag_ffs_and_encounter_claims()
        self.df = self.df.map_partitions(
            lambda pdf: pdf.assign(
                excl_missing_dob=pdf["birth_date"].isnull().astype(int),
                excl_missing_srvc_bgn_date=pdf["srvc_bgn_date"]
                .isnull()
                .astype(int),
            )
        )

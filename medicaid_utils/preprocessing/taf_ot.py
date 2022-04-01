import numpy as np
import pandas as pd
import sys

from medicaid_utils.preprocessing import taf_file
from medicaid_utils.common_utils import dataframe_utils


class TAFOT(taf_file.TAFFile):
    def __init__(
        self,
        year,
        st,
        data_root,
        index_col="BENE_MSIS",
        clean=True,
        preprocess=True,
        tmp_folder=None,
    ):
        super(TAFOT, self).__init__(
            "ot", year, st, data_root, index_col, False, False, tmp_folder
        )
        self.dct_default_filters = {"missing_dob": 0, "duplicated": 0}
        if clean:
            self.clean()
        if preprocess:
            self.preprocess()

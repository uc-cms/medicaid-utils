"""This module has MAXCC class which wraps together cleaning/ preprocessing routines specific for MAX CC files"""
from typing import List

import pandas as pd
import numpy as np

from medicaid_utils.preprocessing import max_file


class MAXCC(max_file.MAXFile):
    """Scripts to preprocess CC file"""

    def __init__(
        self,
        year: int,
        state: str,
        data_root: str,
        index_col: str = "BENE_MSIS",
        clean: bool = True,
        preprocess: bool = True,
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
        pq_engine : str, default='pyarrow'
            Parquet engine to use

        """

        # CC files are not separated by state. So load the single CC file for the year and filter to the specific state
        super().__init__(
            "cc",
            year,
            "",
            data_root,
            index_col,
            clean,
            preprocess,
            pq_engine=pq_engine,
        )
        self.df = self.df.loc[self.df["STATE_CD"] == state]

        if clean:
            pass
        if preprocess:
            lst_conditions = [
                col.removesuffix("_COMBINED").lower()
                for col in self.df.columns
                if col.endswith("_COMBINED")
            ]
            self.agg_conditions(lst_conditions)

    def agg_conditions(self, lst_conditions: List[str]):
        """
        Aggregate condition indicators across payer levels

        Parameters
        ----------
        lst_conditions : list of str
            List of condition columns to aggregate

        """
        self.df = self.df.map_partitions(
            lambda pdf: pdf.assign(
                **{
                    condn
                    + "_combined": np.column_stack(
                        [
                            pd.to_numeric(
                                pdf[f"{condn.upper()}_{payer_type}"],
                                errors="coerce",
                            ).isin([1, 3])
                            for payer_type in [
                                "MEDICAID",
                                "MEDICARE",
                                "COMBINED",
                            ]
                        ]
                    )
                    .any(axis=1)
                    .astype(int)
                    for condn in lst_conditions
                }
            )
        )

    def get_chronic_conditions(
        self, lst_conditions: List[str] = None
    ):  # pylint: disable=missing-param-doc
        """
        Get chronic condition indidcators

        Parameters
        ----------
        lst_conditions : list of str, default=None
            List of condition columns to check

        """
        if lst_conditions is None:
            lst_conditions = [
                col.removesuffix("_COMBINED").lower()
                for col in self.df.columns
                if col.endswith("_COMBINED")
            ]
        if not {condn + "_combined" for condn in lst_conditions}.issubset(
            set(list(self.df.columns))
        ):
            self.agg_conditions(lst_conditions)
        return self.df[[condn + "_combined" for condn in lst_conditions]]

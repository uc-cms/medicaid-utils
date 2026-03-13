"""This module has MAXCC class which wraps together cleaning/ preprocessing routines specific for MAX CC files"""
from typing import List, Optional

import dask.dataframe as dd
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
        tmp_folder: Optional[str] = None,
        pq_engine: str = "pyarrow",
    ) -> None:
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

        """

        super().__init__(
            "cc",
            year=year,
            state=state,
            data_root=data_root,
            index_col=index_col,
            clean=False,
            preprocess=False,
            tmp_folder=tmp_folder,
            pq_engine=pq_engine,
        )

        if preprocess:
            lst_conditions = [
                col.removesuffix("_COMBINED").lower()
                for col in self.df.columns
                if col.endswith("_COMBINED")
            ]
            self.agg_conditions(lst_conditions)

    def agg_conditions(self, lst_conditions: List[str]) -> None:
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
        self, lst_conditions: Optional[List[str]] = None
    ) -> dd.DataFrame:
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

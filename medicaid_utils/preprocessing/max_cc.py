import pandas as pd
import numpy as np
import sys

sys.path.append("../../")
from medicaid_utils.preprocessing import max_file


class MAXCC(max_file.MAXFile):
    """Scripts to preprocess CC file"""

    def __init__(
        self,
        year,
        st,
        data_root,
        index_col="BENE_MSIS",
        clean=True,
        preprocess=True,
    ):

        # CC files are not separated by state. So load the single CC file for the year and filter to the specific state
        super(MAXCC, self).__init__(
            "cc", year, "", data_root, index_col, clean, preprocess
        )
        self.df = self.df.loc[self.df["STATE_CD"] == st]

        if clean:
            pass
        if preprocess:
            lst_conditions = [
                col.removesuffix("_COMBINED").lower()
                for col in self.df.columns
                if col.endswith("_COMBINED")
            ]
            self.agg_conditions(lst_conditions)

    def agg_conditions(self, lst_conditions):
        """Aggregate condition indicators across payer levels"""
        self.df = self.df.map_partitions(
            lambda pdf: pdf.assign(
                **dict(
                    [
                        (
                            condn + "_combined",
                            np.column_stack(
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
                            .astype(int),
                        )
                        for condn in lst_conditions
                    ]
                )
            )
        )

    def get_chronic_conditions(self, lst_conditions=None):
        """Get chronic condition indidcators"""
        if lst_conditions is None:
            lst_conditions = [
                col.removesuffix("_COMBINED").lower()
                for col in self.df.columns
                if col.endswith("_COMBINED")
            ]
        if not set([condn + "_combined" for condn in lst_conditions]).issubset(
            set(list(self.df.columns))
        ):
            self.agg_conditions(lst_conditions)
        return self.df[[condn + "_combined" for condn in lst_conditions]]

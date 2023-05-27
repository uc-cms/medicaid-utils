"""This module has TAFOT class which wraps together cleaning/ preprocessing routines specific for TAF OT files"""

from medicaid_utils.preprocessing import taf_file


class TAFOT(taf_file.TAFFile):
    def __init__(
        self,
        year: int,
        state: str,
        data_root: str,
        index_col: str = "BENE_MSIS",
        clean: bool = True,
        preprocess: bool = True,
        tmp_folder: str = None,
        pq_engine: str = "pyarrow",
    ):
        """
        Initializes TAF OT file object by preloading and preprocessing(if opted in) the associated files

        Parameters
        ----------
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
            "ot",
            year=year,
            state=state,
            data_root=data_root,
            index_col=index_col,
            clean=False,
            preprocess=False,
            tmp_folder=tmp_folder,
            pq_engine=pq_engine,
        )
        self.dct_default_filters = {"missing_dob": 0, "duplicated": 0}
        if clean:
            self.clean()
        if preprocess:
            self.preprocess()

    def clean(self):
        """Cleaning routines to clean diagnosis & procedure code columns, processes date and gender columns,
        and add duplicate check flags."""
        super().clean()
        self.clean_codes()
        self.flag_common_exclusions()
        self.cache_results()

    def preprocess(self):
        """Add basic constructed variables"""

    def flag_common_exclusions(self):
        """
        Adds commonly used IP claim exclusion flag columns.
        New Columns:

            - ffs_or_encounter_claim, 0 or 1, 1 when base claim is an FFS or Encounter claim
            - excl_missing_dob, 0 or 1, 1 when base claim does not have birth date
            - excl_missing_srvc_bgn_date, 0 or 1, 1 when base claim does not have service begin date

        """
        self.flag_ffs_and_encounter_claims()
        df = self.dct_files["base"]
        df = df.map_partitions(
            lambda pdf: pdf.assign(
                excl_missing_dob=pdf["birth_date"].isnull().astype(int),
                excl_missing_srvc_bgn_date=pdf["srvc_bgn_date"]
                .isnull()
                .astype(int),
            )
        )
        self.dct_files["base"] = df

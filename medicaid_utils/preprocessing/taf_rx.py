"""This module has TAFRX class which wraps together cleaning/ preprocessing
routines specific for TAF Pharmacy files"""

from medicaid_utils.preprocessing import taf_file


class TAFRX(taf_file.TAFFile):
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
        Initializes TAF RX file object by preloading and preprocessing(if
        opted in) the associated files

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
        tmp_folder : str, default=None
            Folder location to use for caching intermediate results. Can be
            turned off by not passing this argument.
        pq_engine : str, default='pyarrow'
            Parquet engine to use

        """
        super().__init__(
            "rx",
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
        """Cleaning routines to clean diagnosis & procedure code columns,
        processes date and gender columns, and add duplicate check flags."""
        super().clean()
        self.clean_ndc_codes()
        self.flag_duplicates()

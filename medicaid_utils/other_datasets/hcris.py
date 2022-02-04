import sys
import os
import requests
import copy
import re
from fuzzywuzzy import process, fuzz
import ast
from collections import Counter
import dask.dataframe as dd
import pandas as pd
import glob
import numpy as np
import usaddress

hcris_lookup_folder = os.path.join(os.path.dirname(__file__), "data", "hcris")


def combine_and_clean_hcris_files():
    """Combines HCRIS provider info files and standardizes column names
    Provider id is cleaned by removing non-alphanumeric columns.
    Additional version of provider id without leading zeroes is created.
    Files are downloaded from multiple years, starting from 2014 when archives are available.
    2014: https://web.archive.org/web/20141120072219/http://downloads.cms.gov/Files/hcris/HCLINIC-REPORTS.zip
    2015: https://web.archive.org/web/20151230005431/http://downloads.cms.gov/Files/hcris/HCLINIC-REPORTS.zip
    2016: https://web.archive.org/web/20161228145429/http://downloads.cms.gov/Files/hcris/HCLINIC-REPORTS.zip
    2017: https://web.archive.org/web/20171220152135/http://downloads.cms.gov/Files/hcris/HCLINIC-REPORTS.zip
    2018: https://web.archive.org/web/20181226170727/http://downloads.cms.gov/Files/hcris/HCLINIC-REPORTS.zip
    2019: https://web.archive.org/web/20191226132614/http://downloads.cms.gov/Files/hcris/FQHC14-REPORTS.zip
    2020: https://web.archive.org/web/20201225141117/https://downloads.cms.gov/Files/hcris/FQHC14-REPORTS.zip
    2021: https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Cost-Reports/FQHC-224-2014-form
    """

    dct_col_names = {
        "prvdr_num": "hclinic_provider_id",
        "fqhc_name": "hclinic_name",
        "street_addr": "hclinic_street_address",
        "po_box": "hclinic_po_box",
        "city": "hclinic_city",
        "state": "hclinic_state",
        "zip_code": "hclinic_zipcode",
        "county": "hclinic_county",
        "PROVIDER_NUMBER": "hclinic_provider_id",
        "FQHC14_Name": "hclinic_name",
        "Street_Addr": "hclinic_street_address",
        "PO_Box": "hclinic_po_box",
        "City": "hclinic_city",
        "State": "hclinic_state",
        "Zip_Code": "hclinic_zipcode",
        "County": "hclinic_county",
        "hclinic_Name": "hclinic_name",
        "Po_Box": "hclinic_po_box",
    }
    lst_col = list(set(list(dct_col_names.values())))
    lstdf_hcris = []
    for fname in glob.glob(
        os.path.join(hcris_lookup_folder, "hclinic_provider_info_*.csv")
    ):
        df_hcris = pd.read_csv(fname, dtype=object)
        df_hcris = df_hcris.rename(columns=dct_col_names)
        df_hcris = df_hcris[lst_col]
        df_hcris = df_hcris.assign(
            ccn=pd.to_numeric(
                df_hcris["hclinic_provider_id"].str.strip().str[-4:],
                errors="coerce",
            )
        )
        # Removing Rural Health Centers
        df_hcris = df_hcris.loc[
            df_hcris["ccn"].between(1800, 1989, inclusive="both")
            | df_hcris["ccn"].between(1000, 1199, inclusive="both")
        ]
        df_hcris = df_hcris.drop(["ccn"], axis=1)
        lstdf_hcris.append(df_hcris)

    df_hcris = (
        pd.concat(lstdf_hcris, ignore_index=True)
        .reset_index(drop=True)
        .drop_duplicates()
    )
    df_hcris = df_hcris.assign(
        hclinic_provider_id_cleaned=df_hcris["hclinic_provider_id"]
        .str.replace("[^a-zA-Z0-9]+", "", regex=True)
        .str.upper()
    )
    df_hcris = df_hcris.assign(
        hclinic_provider_id_no_leading_zero=df_hcris[
            "hclinic_provider_id_cleaned"
        ].str.lstrip("0")
    )
    df_hcris.to_pickle(
        os.path.join(hcris_lookup_folder, "hcris_all_years.pickle")
    )

import os
import re
import logging
import requests
import usaddress
from math import ceil
from datetime import datetime
from fuzzywuzzy import process, fuzz
import numpy as np
import pandas as pd
import dask.dataframe as dd

from medicaid_utils.common_utils.usps_address import USPSAddress


constructed_folder = os.path.join(
    os.path.dirname(__file__), "data", "constructed"
)
nppes_lookup_folder = os.path.join(
    os.path.dirname(__file__), "data", "lookups", "nppes_taxonomy"
)
fara_folder = os.path.join(
    os.path.dirname(__file__), "data", "lookups", "nppes_taxonomy"
)
fqhc_lookup_folder = os.path.join(
    os.path.dirname(__file__), "data", "lookups", "fqhc"
)
delivery_folder = os.path.join(
    os.path.dirname(__file__), "data", "deliveries", "adults"
)
uds_delivery_folder = os.path.join(
    os.path.dirname(__file__), "data", "deliveries", "uds_factors"
)


def generate_oscar_fqhc_npis(lst_year=None, pq_engine="fastparquet"):
    """Saves list of NPIs with FQHC range oscar provider ids into a pickle file"""
    if lst_year is None:
        lst_year = list(range(2009, datetime.now().year + 1))
    df_npi_provider = dd.concat(
        [
            dd.read_parquet(
                os.path.join(
                    nppes_lookup_folder,
                    str(yr),
                    "npi_provider_parquet_cleaned",
                ),
                engine=pq_engine,
                index=False,
            )
            for yr in lst_year
        ]
    )
    df_npi_provider["ccn"] = (
        df_npi_provider["provider_id"].str.strip().str[-4:]
    )
    df_npi_provider = df_npi_provider.map_partitions(
        lambda pdf: pdf.assign(ccn=pd.to_numeric(pdf["ccn"], errors="coerce"))
    )

    # Oscar provider numbers for FQHCs end in the range 1800 - 1989 or 1000 - 1199
    pdf_fqhc_npi = df_npi_provider.loc[
        df_npi_provider["ccn"].between(1800, 1989, inclusive="both")
        | df_npi_provider["ccn"].between(1000, 1199, inclusive="both")
    ].compute()
    pdf_fqhc_npi.loc[(pdf_fqhc_npi["provider_id_type"] == 6)].to_pickle(
        os.path.join(nppes_lookup_folder, "nppes_fqhc_range_npis.pickle")
    )


def get_file_name_dict(source):
    dct_files = {
        "uds": "uds_all_years.pickle",
        "hcris": "hcris_all_years.pickle",
        "nppes_matches": f"{source}_nppes_based_matches.pickle",
        "api_matches": f"{source}_api_perfect_matches.pickle",
        "api_and_nppes_matches": f"{source}_api_and_nppes_perfect_matches.pickle",
        "api_nppes_state_relaxed_matches": f"{source}_api_and_nppes_perfect_matches_with_state_relaxed.pickle",
        "text_merged": f"nppes_{source}_text_merged.pickle",
        "text_merged_with_match_purity": f"nppes_{source}_text_merged_with_match_purity.pickle",
        "text_matches": f"{source}_text_based_perfect_matches.pickle",
        "perfect_matches": f"{source}_perfect_matches.pickle",
        "fuzzy_matches": f"{source}_fuzzy_matches.pickle",
        "no_leading_zeros_matches": f"{source}_nppes_based_matches_no_leading_zeros.pickle",
        "bhcmisid_perfect_matches": "bhcmisid_npi_perfect_matches.pickle",
        "bhcmisid_fuzzy_matches": "bhcmisid_npi_fuzzy_matches.pickle",
        "bhcmisid_x_npi": "bhcmisid_x_npi.pickle",
        "fqhc_x_npi": "fqhc_x_npi.ftr",
    }
    return dct_files


def get_fqhc_crosswalk(start_year, data_folder=fqhc_lookup_folder):
    """Returns FQHC cross walk with FQHC NPI's seen in UDS datasets till the start_year"""
    pdf_fqhc_crosswalk = pd.read_feather(
        os.path.join(data_folder, get_file_name_dict("uds")["fqhc_x_npi"])
    )
    pdf_fqhc_crosswalk = pdf_fqhc_crosswalk.sort_values(["start_year"])
    pdf_fqhc_crosswalk = pdf_fqhc_crosswalk.assign(
        bhcmisid=pdf_fqhc_crosswalk["bhcmisid"].fillna("")
    )
    pdf_fqhc_crosswalk = pdf_fqhc_crosswalk.assign(
        bhcmisid=pdf_fqhc_crosswalk.groupby(["npi"])["bhcmisid"].transform(
            lambda x: ",".join(x)
        )
    )
    pdf_fqhc_crosswalk = pdf_fqhc_crosswalk.assign(
        bhcmisid=pdf_fqhc_crosswalk["bhcmisid"].replace("", np.nan)
    )
    pdf_fqhc_crosswalk = pdf_fqhc_crosswalk.drop_duplicates(["npi"])
    pdf_fqhc_crosswalk = pdf_fqhc_crosswalk.rename(
        columns={
            "perfect_fqhc": "taxonomy_perfect_fqhc",
            "fuzzy_fqhc": "taxonomy_fuzzy_fqhc",
        }
    )
    return pdf_fqhc_crosswalk.loc[
        pdf_fqhc_crosswalk["start_year"] <= start_year
    ]

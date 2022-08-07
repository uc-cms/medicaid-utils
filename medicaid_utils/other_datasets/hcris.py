import sys
import os
import requests
import re
import glob
import logging
from math import ceil
from time import sleep
from datetime import datetime
from fuzzywuzzy import process, fuzz
import dask.dataframe as dd
import pandas as pd
import numpy as np
import usaddress

from medicaid_utils.common_utils.usps_address import USPSAddress

hcris_lookup_folder = os.path.join(os.path.dirname(__file__), "data", "hcris")
nppes_lookup_folder = os.path.join(os.path.dirname(__file__), "data", "nppes")
fqhc_lookup_folder = os.path.join(os.path.dirname(__file__), "data", "fqhc")
pq_engine = "pyarrow"

# NPI Lookup params
npi_api = "https://npiregistry.cms.hhs.gov/api/?"
dct_params = {"enumeration_type": "NPI-2", "version": 2.1, "limit": 100}


def combine_and_clean_hcris_files(logger_name):
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
    logger = logging.getLogger(logger_name)
    lst_years = []
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
        year = os.path.splitext(fname)[0][-4:]
        lst_years.append(int(year))
        logger.info(f"Processing {year} HCRIS dataset")
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
    # Cleaning provider_id column by removing non-alphanumeric characters
    df_hcris = df_hcris.assign(
        hclinic_provider_id_cleaned=df_hcris["hclinic_provider_id"]
        .str.replace("[^a-zA-Z0-9]+", "", regex=True)
        .str.upper()
    )
    # Cleaning provider_id column by removing non-alphanumeric characters & leading zeros
    df_hcris = df_hcris.assign(
        hclinic_provider_id_no_leading_zero=df_hcris[
            "hclinic_provider_id_cleaned"
        ].str.lstrip("0")
    )
    fname_pickle = os.path.join(hcris_lookup_folder, "hcris_all_years.pickle")
    fname_csv = os.path.join(
        hcris_lookup_folder, f"hcris_{min(lst_years)}_{max(lst_years)}.csv"
    )
    logger.info(
        f"{','.join([str(yr) for yr in lst_years])} HCRIS datasets were merged and cleaned, and is saved at "
        f"{fname_pickle} & {fname_csv}"
    )
    df_hcris.to_csv(fname_csv, index=False)
    df_hcris.to_pickle(fname_pickle)


def clean_hclinic_name(hclinic_name):
    return re.sub(" +", " ", re.sub("[^a-zA-Z0-9 ]+", "", hclinic_name))


def clean_zip(zipcode):
    return str(zipcode).replace("-", "")


def get_taxonomies(lstdct_tax):
    return ",".join([dct_tax["code"] for dct_tax in lstdct_tax])


def filter_partial_matches(df):
    """Filter source x nppes matches that have some similarity between their names and address, with a distance score
    of at least 60"""
    df = df.replace(to_replace=[None, "None"], value=np.nan).fillna("")
    df = df.assign(
        **dict(
            [
                (col, df[col].astype(str))
                for col in [
                    "provider_id_state",
                    "p_loc_line_1",
                    "p_loc_line_2",
                    "p_loc_city",
                    "p_loc_state",
                    "p_mail_line_1",
                    "p_mail_line_2",
                    "p_mail_city",
                    "p_mail_state",
                    "hclinic_name",
                    "hclinic_street_address",
                    "hclinic_city",
                    "hclinic_state",
                    "hclinic_business_address",
                    "hclinic_business_city",
                    "hclinic_business_state",
                    "hclinic_mail_address",
                    "hclinic_mail_city",
                    "hclinic_mail_state",
                ]
            ]
        )
    )
    df = df.map_partitions(lambda pdf: compute_basic_match_purity(pdf))
    df = df.loc[
        (df[["name_score", "address_score"]] > 60).any(axis=1)
        & (df["city_score"] >= 70)
    ].compute()
    return df


def nppes_api_requests(pdf, logger_name, **dct_request_params):
    """Make NPPES API calls using hclinic name, city, state & zipcode in the request"""

    logger = logging.getLogger(logger_name)
    # NPI Lookup params
    npi_api = "https://npiregistry.cms.hhs.gov/api/?"
    dct_params = {"enumeration_type": "NPI-2", "version": 2.1, "limit": 100}

    # Using hclinic name, city, state and zipcode in the API call
    df_api_based = dd.from_pandas(pdf, npartitions=30)
    while True:
        try:
            df_api_based = df_api_based.map_partitions(
                lambda pdf: pdf.assign(
                    npi_json=pdf.apply(
                        lambda row: requests.get(
                            npi_api,
                            params={
                                **dct_params,
                                **{
                                    "organization_name": clean_hclinic_name(
                                        row[
                                            dct_request_params[
                                                "organization_name"
                                            ]
                                        ]
                                    ),
                                    "city": row[dct_request_params["city"]],
                                    "state": row[dct_request_params["state"]],
                                    "postal_code": clean_zip(
                                        row[dct_request_params["postal_code"]]
                                    ),
                                },
                            },
                        ).json(),
                        axis=1,
                    )
                )
            ).compute()
            break
        except Exception as ex:
            logger.info("Retrying API calls")
            continue
    df_api_based = df_api_based.reset_index(drop=True)
    df_api_based = df_api_based.merge(
        df_api_based["npi_json"].apply(pd.Series)[["result_count", "results"]],
        left_index=True,
        right_index=True,
    )

    df_api_based_relaxed_city = df_api_based.loc[
        df_api_based["result_count"] < 1
    ]
    df_api_based_matched = df_api_based.loc[df_api_based["result_count"] >= 1]
    logger.info(
        f"{df_api_based_matched.hclinic_provider_id_cleaned.nunique()} HCLINIC providers were matched using "
        f"NPPES API calls with {dct_request_params['organization_name']}, {dct_request_params['city']},"
        f" {dct_request_params['state']} and {dct_request_params['postal_code']} "
    )

    # Using hclinic name, state and zipcode in the API call
    df_api_based_relaxed_city = dd.from_pandas(
        df_api_based_relaxed_city, npartitions=30
    )
    while True:
        try:
            df_api_based_relaxed_city = df_api_based_relaxed_city.map_partitions(
                lambda pdf: pdf.assign(
                    npi_json=pdf.apply(
                        lambda row: requests.get(
                            npi_api,
                            params={
                                **dct_params,
                                **{
                                    "organization_name": clean_hclinic_name(
                                        row[
                                            dct_request_params[
                                                "organization_name"
                                            ]
                                        ]
                                    ),
                                    "state": row[dct_request_params["state"]],
                                    "postal_code": clean_zip(
                                        row[dct_request_params["postal_code"]]
                                    ),
                                },
                            },
                        ).json(),
                        axis=1,
                    )
                )
            ).compute()
            break
        except Exception as ex:
            logger.info("Retrying API calls")
            continue
    df_api_based_relaxed_city = df_api_based_relaxed_city.reset_index(
        drop=True
    ).drop(["result_count", "results"], axis=1)
    df_api_based_relaxed_city = df_api_based_relaxed_city.merge(
        df_api_based_relaxed_city["npi_json"].apply(pd.Series)[
            ["result_count", "results"]
        ],
        left_index=True,
        right_index=True,
    )

    df_api_based_relaxed_name = df_api_based_relaxed_city.loc[
        df_api_based_relaxed_city["result_count"] < 1
    ]
    df_api_based_relaxed_city = df_api_based_relaxed_city.loc[
        df_api_based_relaxed_city["result_count"] >= 1
    ]
    logger.info(
        f"{df_api_based_relaxed_city.hclinic_provider_id_cleaned.nunique()} HCLINIC providers were matched using "
        f"NPPES API calls with {dct_request_params['organization_name']}, {dct_request_params['state']} and "
        f"{dct_request_params['postal_code']} "
    )
    df_api_based_matched = pd.concat(
        [df_api_based_matched, df_api_based_relaxed_city], ignore_index=True
    ).reset_index(drop=True)

    # Using hclinic name (starting with), state and zipcode in the API call
    df_api_based_relaxed_name = dd.from_pandas(
        df_api_based_relaxed_name, npartitions=30
    )
    while True:
        try:
            df_api_based_relaxed_name = df_api_based_relaxed_name.map_partitions(
                lambda pdf: pdf.assign(
                    npi_json=pdf.apply(
                        lambda row: requests.get(
                            npi_api,
                            params={
                                **dct_params,
                                **{
                                    "organization_name": clean_hclinic_name(
                                        row[
                                            dct_request_params[
                                                "organization_name"
                                            ]
                                        ]
                                    )
                                    + "*",
                                    "state": row[dct_request_params["state"]],
                                    "postal_code": clean_zip(
                                        row[dct_request_params["postal_code"]]
                                    ),
                                },
                            },
                        ).json(),
                        axis=1,
                    )
                )
            ).compute()
            break
        except Exception as ex:
            logger.info("Retrying API calls")
            continue
    df_api_based_relaxed_name = df_api_based_relaxed_name.reset_index(
        drop=True
    ).drop(["result_count", "results"], axis=1)
    df_api_based_relaxed_name = df_api_based_relaxed_name.merge(
        df_api_based_relaxed_name["npi_json"].apply(pd.Series)[
            ["result_count", "results"]
        ],
        left_index=True,
        right_index=True,
    )

    df_api_based_relaxed_more_relaxed_name = df_api_based_relaxed_name.loc[
        df_api_based_relaxed_name["result_count"] < 1
    ]
    df_api_based_relaxed_name = df_api_based_relaxed_name.loc[
        df_api_based_relaxed_name["result_count"] >= 1
    ]
    logger.info(
        f"{df_api_based_relaxed_name.hclinic_provider_id_cleaned.nunique()} HCLINIC providers were matched using "
        f"NPPES API calls with "
        f"{dct_request_params['organization_name']} (starting with), {dct_request_params['state']} "
        f"and {dct_request_params['postal_code']}"
    )
    df_api_based_matched = pd.concat(
        [df_api_based_matched, df_api_based_relaxed_name], ignore_index=True
    ).reset_index(drop=True)
    return df_api_based_matched


def standardize_addresses(pdf):
    """Standardize address using USPS API"""

    # sleep_time = 0 if (datetime.today().weekday() in [5, 6]) else 0.12

    def parse_address(address, city, state, zipcode):
        """Make calls to USPS API
        New columns:
        standardized_address - Address returned by USPS API
        standardized - 0 or 1, indicates successful standardization
        address_error - Error text returned by USPS API, if any
        """
        zipcode = (
            zipcode if len(zipcode) <= 5 else zipcode[:4] + "-" + zipcode[5:]
        )
        usps_address = USPSAddress(
            name="", street=address, city=city, state=state, zip4=zipcode
        )
        standardized_address = ""
        standardized_street_address = ""
        address_error = ""
        try:
            standardized_address = usps_address.standardized()
            standardized_street_address = street_address(standardized_address)
            address_error = usps_address._error
        except Exception as ex:
            address_error = "API call error"
        standardized = int(bool(standardized_address))
        # sleep(sleep_time)

        return [
            standardized_address,
            standardized_street_address,
            standardized,
            address_error,
        ]

    lst_standardized_address_col = [
        "standardized_address",
        "standardized_street_address",
        "standardized",
        "address_error",
    ]

    pdf = pdf.merge(
        pdf.apply(
            lambda row: pd.Series(
                parse_address(
                    row["address_cleaned"],
                    row["city"],
                    row["state"],
                    row["zip"],
                ),
                index=lst_standardized_address_col,
            ),
            axis=1,
            result_type="expand",
        ),
        left_index=True,
        right_index=True,
    )
    pdf = pdf.replace(to_replace=[None, "None"], value=np.nan).fillna("")
    return pdf


def flatten_nppes_query_result(pdf):
    def json_to_list(
        lstdct_match,
        target_name,
        target_name2,
        target_address,
        target_city,
        target_state,
        lst_priority_taxonomies=["261QF0400X", "261QR1300X"],
    ):
        lst_res = []
        lst_res = [
            expand_query_result(
                dct_res,
                target_name,
                target_name2,
                target_address,
                target_city,
                target_state,
            )
            for dct_res in lstdct_match
        ]
        return lst_res

    def expand_query_result(
        dct_res,
        target_name,
        target_name2,
        target_address,
        target_city,
        target_state,
    ):
        fqhc = 0
        rural = 0
        npi = dct_res["number"]
        lst_name = (
            [dct_res["basic"]["name"]] if "name" in dct_res["basic"] else []
        )
        enumeration_date = (
            dct_res["basic"]["enumeration_date"]
            if "enumeration_date" in dct_res["basic"]
            else ""
        )
        lst_name = (
            lst_name + [dct_res["basic"]["organization_name"]]
            if ("organization_name" in dct_res["basic"])
            else []
        )
        if "other_names" in dct_res:
            for dct_oth in dct_res["other_names"]:
                lst_name = (
                    lst_name + [dct_oth["organization_name"]]
                    if ("organization_name" in dct_oth)
                    else []
                )
        orgname = (
            process.extractOne(target_name, lst_name, scorer=fuzz.ratio)
            if (len(lst_name) > 0)
            else None
        )
        orgnameoth = ""
        if orgname is not None:
            orgname = orgname[0]
            lst_othname = [n for n in lst_name if n != orgname]
            orgnameoth = (
                process.extractOne(
                    target_name2 if pd.notnull(target_name2) else target_name,
                    lst_othname,
                    scorer=fuzz.ratio,
                )
                if (len(lst_othname) > 0)
                else None
            )
            if orgnameoth is not None:
                orgnameoth = orgnameoth[0]

        lstdct_address = (
            dct_res["addresses"] if ("addresses" in dct_res) else []
        )
        lstdct_address.extend(
            dct_res["practiceLocations"]
            if ("practiceLocations" in dct_res)
            else []
        )
        lst_addresses = [
            dct_adrs["address_1"]
            + " "
            + dct_adrs["address_2"]
            + " "
            + dct_adrs["city"]
            + " "
            + dct_adrs["state"]
            for dct_adrs in lstdct_address
        ]
        target_address_full = (
            (target_address if pd.notnull(target_address) else "")
            + " "
            + (target_city if pd.notnull(target_city) else "")
            + " "
            + (target_state if pd.notnull(target_state) else "")
        )
        address = (
            process.extractOne(
                target_address_full, lst_addresses, scorer=fuzz.ratio
            )
            if (len(lst_addresses) > 0)
            else None
        )
        plocline1 = (
            plocline2
        ) = (
            pmailline1
        ) = (
            pmailline2
        ) = (
            ploccityname
        ) = (
            pmailcityname
        ) = plocstatename = pmailstatename = ploczip = pmailzip = ""
        if address is not None:
            address = address[0]
            adr_id = lst_addresses.index(address)
            plocline1 = lstdct_address[adr_id]["address_1"]
            plocline2 = lstdct_address[adr_id]["address_2"]
            ploccityname = lstdct_address[adr_id]["city"]
            plocstatename = lstdct_address[adr_id]["state"]
            ploczip = lstdct_address[adr_id]["postal_code"]
            lst_other_address = [
                adr for adr in lst_addresses if adr != address
            ]
            othaddress = (
                process.extractOne(
                    target_address_full, lst_other_address, scorer=fuzz.ratio
                )
                if (len(lst_other_address) > 0)
                else None
            )
            if othaddress is not None:
                othaddress = othaddress[0]
                othadr_id = lst_addresses.index(othaddress)
                pmailline1 = lstdct_address[othadr_id]["address_1"]
                pmailline2 = lstdct_address[othadr_id]["address_2"]
                pmailcityname = lstdct_address[othadr_id]["city"]
                pmailstatename = lstdct_address[othadr_id]["state"]
                pmailzip = lstdct_address[othadr_id]["postal_code"]

        lst_tax = [dcttax["code"] for dcttax in dct_res["taxonomies"]]
        if "261QF0400X" in lst_tax:
            fqhc = 1
        if "261QR1300X" in lst_tax:
            rural = 1
        return (
            npi,
            enumeration_date,
            orgname,
            orgnameoth,
            plocline1,
            plocline2,
            ploccityname,
            plocstatename,
            ploczip,
            pmailline1,
            pmailline2,
            pmailcityname,
            pmailstatename,
            pmailzip,
            fqhc,
            rural,
        )

    pdf = pdf.assign(
        lst_matches=pdf.apply(
            lambda row: json_to_list(
                row["results"],
                row["hclinic_name"],
                np.nan,
                row["hclinic_street_address"],
                row["hclinic_city"],
                row["hclinic_state"],
            ),
            axis=1,
        )
    )
    pdf = pdf.explode("lst_matches").reset_index(drop=True)
    pdf = pdf.merge(
        pdf["lst_matches"].apply(
            pd.Series,
            index=[
                "npi",
                "enumeration_date",
                "p_org_name",
                "p_org_name_oth",
                "p_loc_line_1",
                "p_loc_line_2",
                "p_loc_city",
                "p_loc_state",
                "p_loc_zip",
                "p_mail_line_1",
                "p_mail_line_2",
                "p_mail_city",
                "p_mail_state",
                "p_mail_zip",
                "fqhc",
                "rural",
            ],
        ),
        left_index=True,
        right_index=True,
    )
    pdf = pdf.drop(["lst_matches"], axis=1)
    return pdf


def clean_address(address):
    """Cleaning address columns by removing placenames in street address lines and "
    "concatenating multiple occupancy identifier/ street numbers"""
    dct_address = dict(usaddress.parse(address))
    dct_address_cleaned = {}
    for k, v in dct_address.items():
        if (v == "OccupancyIdentifier" and k.lower().strip() == "and") or (
            v in ["NotAddress", "PlaceName", "Recipient", "StateName"]
        ):
            continue
        dct_address_cleaned[v] = (
            k
            if v not in dct_address_cleaned
            else (
                "-"
                if v.endswith(
                    (
                        "Identifier",
                        "Number",
                    )
                )
                else " "
            ).join([dct_address_cleaned[v], k])
        )
    return " ".join(
        [dct_address_cleaned[k].strip() for k in dct_address_cleaned]
    )


def street_address(address):
    """Strips placenames, state and city names and returns street address component"""
    dct_address = dict(usaddress.parse(address))
    dct_address_cleaned = {}
    for k, v in dct_address.items():
        if (v == "OccupancyIdentifier" and k.lower().strip() == "and") or (
            v
            in ["NotAddress", "PlaceName", "Recipient", "StateName", "ZipCode"]
        ):
            continue
        dct_address_cleaned[v] = (
            k
            if v not in dct_address_cleaned
            else (
                "-"
                if v.endswith(
                    (
                        "Identifier",
                        "Number",
                    )
                )
                else " "
            ).join([dct_address_cleaned[v], k])
        )
    return " ".join(
        [dct_address_cleaned[k].strip() for k in dct_address_cleaned]
    )


def compute_basic_match_purity(pdf):
    """Computes levenshtein distance based similarity scores for address, name & city, and equality match for state"""

    def match_purity(
        p_org_name,
        p_org_name_oth,
        provider_id_state,
        hclinic_name,
        hclinic_site_name,
        hclinic_street_address,
        hclinic_street_address_line_2,
        hclinic_po_box,
        hclinic_city,
        hclinic_state,
        hclinic_business_address,
        hclinic_business_city,
        hclinic_business_state,
        hclinic_mail_address,
        hclinic_mail_city,
        hclinic_mail_state,
        p_loc_line_1,
        p_loc_line_2,
        p_loc_city,
        p_loc_state,
        p_mail_line_1,
        p_mail_line_2,
        p_mail_city,
        p_mail_state,
    ):
        hclinic_address = (
            (hclinic_street_address + " ").lstrip()
            + (hclinic_po_box + " ").lstrip()
            + (hclinic_street_address_line_2 + " ").lstrip()
        )
        p_loc_address = (p_loc_line_1 + " ").lstrip() + (
            p_loc_line_2 + " "
        ).lstrip()
        p_mail_address = (p_mail_line_1 + " ").lstrip() + (
            p_mail_line_2 + " "
        ).lstrip()
        hclinic_name = clean_hclinic_name(hclinic_name)
        hclinic_site_name = clean_hclinic_name(hclinic_site_name)
        p_org_name = clean_hclinic_name(p_org_name)
        p_org_name_oth = clean_hclinic_name(p_org_name_oth)
        p_loc_line_2 = clean_hclinic_name(p_loc_line_2)
        p_mail_line_2 = clean_hclinic_name(p_mail_line_2)

        name_score = (
            0
            if (not bool(hclinic_name or hclinic_site_name))
            else max(
                0
                if (not bool(p_org_name))
                else max(
                    fuzz.ratio(p_org_name, hclinic_name),
                    fuzz.ratio(p_org_name, hclinic_site_name),
                ),
                0
                if (not bool(p_org_name_oth))
                else max(
                    fuzz.ratio(p_org_name_oth, hclinic_name),
                    fuzz.ratio(p_org_name_oth, hclinic_site_name),
                ),
                0
                if (not bool(p_loc_line_2))
                else max(
                    fuzz.ratio(p_loc_line_2, hclinic_name),
                    fuzz.ratio(p_loc_line_2, hclinic_site_name),
                ),
                0
                if (not bool(p_mail_line_2))
                else max(
                    fuzz.ratio(p_mail_line_2, hclinic_name),
                    fuzz.ratio(p_mail_line_2, hclinic_site_name),
                ),
            )
        )

        address_score = (
            0
            if (
                not bool(
                    hclinic_address
                    or hclinic_business_address
                    or hclinic_mail_address
                )
            )
            else max(
                0
                if (not bool(p_loc_address))
                else max(
                    fuzz.ratio(p_loc_address, hclinic_address),
                    fuzz.ratio(p_loc_address, hclinic_business_address),
                    fuzz.ratio(p_loc_address, hclinic_mail_address),
                ),
                0
                if (not bool(p_mail_address))
                else max(
                    fuzz.ratio(p_mail_address, hclinic_address),
                    fuzz.ratio(p_mail_address, hclinic_business_address),
                    fuzz.ratio(p_mail_address, hclinic_mail_address),
                ),
                0
                if (not bool(p_loc_line_1))
                else max(
                    fuzz.ratio(p_loc_line_1, hclinic_address),
                    fuzz.ratio(p_loc_line_1, hclinic_business_address),
                    fuzz.ratio(p_loc_line_1, hclinic_mail_address),
                ),
                0
                if (not bool(p_mail_line_1))
                else max(
                    fuzz.ratio(p_mail_line_1, hclinic_address),
                    fuzz.ratio(p_mail_line_1, hclinic_business_address),
                    fuzz.ratio(p_mail_line_1, hclinic_mail_address),
                ),
            )
        )

        city_score = (
            0
            if (
                not bool(
                    hclinic_city or hclinic_business_city or hclinic_mail_city
                )
            )
            else max(
                0
                if not bool(p_loc_city)
                else max(
                    fuzz.ratio(p_loc_city, hclinic_city),
                    fuzz.ratio(p_loc_city, hclinic_business_city),
                    fuzz.ratio(p_loc_city, hclinic_mail_city),
                ),
                0
                if not bool(p_mail_city)
                else max(
                    fuzz.ratio(p_mail_city, hclinic_city),
                    fuzz.ratio(p_mail_city, hclinic_business_city),
                    fuzz.ratio(p_mail_city, hclinic_mail_city),
                ),
            )
        )
        state_match = int(
            (
                bool(hclinic_state)
                and (
                    hclinic_state
                    in [provider_id_state, p_loc_state, p_mail_state]
                )
            )
            or (
                bool(hclinic_business_state)
                and (
                    hclinic_business_state
                    in [provider_id_state, p_loc_state, p_mail_state]
                )
            )
            or (
                bool(hclinic_mail_state)
                and (
                    hclinic_mail_state
                    in [provider_id_state, p_loc_state, p_mail_state]
                )
            )
        )

        return [name_score, address_score, city_score, state_match]

    if "provider_id_state" not in pdf.columns:
        pdf["provider_id_state"] = ""
        pdf = pdf.assign()
    pdf = pdf.replace(to_replace=[None, "None"], value=np.nan).fillna("")
    pdf = pdf.assign(
        **dict(
            [
                (col, pdf[col].str.strip().astype(str))
                for col in [
                    "provider_id_state",
                    "p_loc_line_1",
                    "p_loc_line_2",
                    "p_loc_city",
                    "p_loc_state",
                    "p_mail_line_1",
                    "p_mail_line_2",
                    "p_mail_city",
                    "p_mail_state",
                    "hclinic_name",
                    "hclinic_street_address",
                    "hclinic_po_box",
                    "hclinic_city",
                    "hclinic_state",
                    "hclinic_site_name",
                    "hclinic_street_address_line_2",
                    "hclinic_business_address",
                    "hclinic_business_state",
                    "hclinic_business_city",
                    "hclinic_business_zipcode",
                    "hclinic_mail_address",
                    "hclinic_mail_state",
                    "hclinic_mail_city",
                    "hclinic_mail_zipcode",
                ]
            ]
        )
    )

    pdf = pdf.merge(
        pdf.apply(
            lambda row: pd.Series(
                match_purity(
                    row["p_org_name"],
                    row["p_org_name_oth"],
                    row["provider_id_state"],
                    row["hclinic_name"],
                    row["hclinic_site_name"],
                    row["hclinic_street_address"],
                    row["hclinic_street_address_line_2"],
                    row["hclinic_po_box"],
                    row["hclinic_city"],
                    row["hclinic_state"],
                    row["hclinic_business_address"],
                    row["hclinic_business_city"],
                    row["hclinic_business_state"],
                    row["hclinic_mail_address"],
                    row["hclinic_mail_city"],
                    row["hclinic_mail_state"],
                    row["p_loc_line_1"],
                    row["p_loc_line_2"],
                    row["p_loc_city"],
                    row["p_loc_state"],
                    row["p_mail_line_1"],
                    row["p_mail_line_2"],
                    row["p_mail_city"],
                    row["p_mail_state"],
                ),
                index=[
                    "name_score",
                    "address_score",
                    "city_score",
                    "state_match",
                ],
            ),
            axis=1,
            result_type="expand",
        ),
        left_index=True,
        right_index=True,
    )
    return pdf


def process_address_columns(pdf, logger_name, source="hcris"):
    """Combines street level components; concatenates multiple values belonging to the same address component type,
    such as multiple street or occupancy identifier numbers"""
    logger = logging.getLogger(logger_name)
    pdf = pdf.replace(to_replace=[None, "None"], value=np.nan).fillna("")
    pdf = pdf.assign(
        p_loc_address=(pdf["p_loc_line_1"] + " ").str.lstrip()
        + (pdf["p_loc_line_2"] + " ").str.lstrip(),
        p_mail_address=(pdf["p_mail_line_1"] + " ").str.lstrip()
        + (pdf["p_mail_line_2"] + " ").str.lstrip(),
    )
    if source == "hcris":
        pdf = pdf.assign(
            hclinic_address=(pdf["hclinic_street_address"] + " ").str.lstrip()
            + (pdf["hclinic_po_box"] + " ").str.lstrip(),
            hclinic_business_address="",
            hclinic_mail_address="",
        )

    if source == "uds":
        pdf = pdf.assign(
            hclinic_address=(pdf["hclinic_street_address"] + " ").str.lstrip()
            + (pdf["hclinic_street_address_line_2"] + " ").str.lstrip(),
            hclinic_business_address=pdf[
                "hclinic_business_address"
            ].str.strip(),
            hclinic_mail_address=pdf["hclinic_mail_address"].str.strip(),
        )

    logger.info(
        "Cleaning address columns by removing placenames in street address lines and "
        "concatenating multiple occupancy identifier/ street numbers"
    )
    pdf = (
        dd.from_pandas(pdf, npartitions=ceil(pdf.shape[0] / 10000))
        .map_partitions(
            lambda pdf_partition: pdf_partition.assign(
                **dict(
                    [
                        (
                            f"{col}_cleaned",
                            pdf_partition[col].apply(clean_address),
                        )
                        for col in [
                            "hclinic_address",
                            "hclinic_business_address",
                            "hclinic_mail_address",
                            "p_loc_address",
                            "p_mail_address",
                        ]
                    ]
                )
            )
        )
        .compute()
    )
    logger.info("Get numeric components from address fields")
    pdf = (
        dd.from_pandas(pdf, npartitions=ceil(pdf.shape[0] / 10000))
        .map_partitions(
            lambda pdf_partition: pdf_partition.assign(
                **dict(
                    [
                        (
                            f"{col}_digits",
                            pdf_partition[f"{col}_cleaned"]
                            .str.findall(r"(\d+)")
                            .str.join(""),
                        )
                        for col in [
                            "hclinic_address",
                            "hclinic_business_address",
                            "hclinic_mail_address",
                            "p_loc_address",
                            "p_mail_address",
                        ]
                    ]
                )
            )
        )
        .compute()
    )
    pdf = pdf.replace(to_replace=[None, "None"], value=np.nan).fillna("")

    # Combining addresses to prepare for standardization

    pdf_hclinic_addresses = pdf[
        [
            "hclinic_address_cleaned",
            "hclinic_city",
            "hclinic_state",
            "hclinic_zipcode",
        ]
    ]
    pdf_hclinic_addresses = pdf_hclinic_addresses.rename(
        columns=dict(
            [
                (col, col.replace("hclinic_", "").replace("code", ""))
                for col in pdf_hclinic_addresses
            ]
        )
    )
    if source == "uds":
        pdf_hclinic_business_addresses = pdf[
            [
                "hclinic_business_address_cleaned",
                "hclinic_business_city",
                "hclinic_business_state",
                "hclinic_business_zipcode",
            ]
        ].drop_duplicates()
        pdf_hclinic_business_addresses = pdf_hclinic_business_addresses.rename(
            columns=dict(
                [
                    (col, "_".join(col.split("_")[2:]).replace("code", ""))
                    for col in pdf_hclinic_business_addresses
                ]
            )
        )
        pdf_hclinic_mail_addresses = pdf[
            [
                "hclinic_mail_address_cleaned",
                "hclinic_mail_city",
                "hclinic_mail_state",
                "hclinic_mail_zipcode",
            ]
        ].drop_duplicates()
        pdf_hclinic_mail_addresses = pdf_hclinic_mail_addresses.rename(
            columns=dict(
                [
                    (col, "_".join(col.split("_")[2:]).replace("code", ""))
                    for col in pdf_hclinic_mail_addresses
                ]
            )
        )
        pdf_hclinic_addresses = (
            pd.concat(
                [
                    pdf_hclinic_addresses,
                    pdf_hclinic_business_addresses,
                    pdf_hclinic_mail_addresses,
                ],
                ignore_index=True,
            )
            .drop_duplicates()
            .reset_index(drop=True)
        )

    pdf_loc_addresses = pdf[
        ["p_loc_address_cleaned", "p_loc_city", "p_loc_state", "p_loc_zip"]
    ]
    pdf_loc_addresses = pdf_loc_addresses.rename(
        columns=dict(
            [(col, col.replace("p_loc_", "")) for col in pdf_loc_addresses]
        )
    )

    pdf_mail_addresses = pdf[
        ["p_mail_address_cleaned", "p_mail_city", "p_mail_state", "p_mail_zip"]
    ]
    pdf_mail_addresses = pdf_mail_addresses.rename(
        columns=dict(
            [(col, col.replace("p_mail_", "")) for col in pdf_mail_addresses]
        )
    )

    pdf_addresses = (
        pd.concat(
            [pdf_hclinic_addresses, pdf_loc_addresses, pdf_mail_addresses],
            ignore_index=True,
        )
        .drop_duplicates()
        .reset_index(drop=True)
    )
    lst_address_col = list(pdf_addresses.columns)
    fname_standardized_address = os.path.join(
        fqhc_lookup_folder, "standardized_addresses.pickle"
    )
    pdf_matched = pd.DataFrame(columns=lst_address_col)
    if os.path.exists(fname_standardized_address):
        pdf_addresses = pdf_addresses.merge(
            pd.read_pickle(fname_standardized_address),
            on=lst_address_col,
            how="left",
        )
        pdf_matched = pdf_addresses.loc[
            pdf_addresses["standardized"].isin([0, 1])
        ]
        pdf_addresses = pdf_addresses.loc[
            ~pdf_addresses["standardized"].isin([0, 1])
        ][lst_address_col]
        logger.info(
            f"{pdf_matched.shape[0]} addresses standardized using existing standardized addresses list"
        )

    logger.info("Standardize address using USPS API")
    if pdf_addresses.shape[0] > 0:
        pdf_addresses = (
            dd.from_pandas(
                pdf_addresses, npartitions=ceil(pdf_addresses.shape[0] / 10000)
            )
            .map_partitions(lambda pdf: standardize_addresses(pdf))
            .compute()
        )

        logger.info(
            f"{pdf_addresses.shape[0]} addresses standardized using USPS API"
        )
        if pdf_matched.shape[0] > 0:
            pdf_addresses = pd.concat(
                [pdf_matched, pdf_addresses], ignore_index=True
            ).reset_index(drop=True)

        # Saving standardized addresses
        pd.concat(
            [pd.read_pickle(fname_standardized_address), pdf_addresses],
            ignore_index=True,
        ).sort_values(["standardized"], ascending=False).drop_duplicates(
            lst_address_col
        ).reset_index(
            drop=True
        ).to_pickle(
            fname_standardized_address
        )
        logger.info(
            f"Standardized address list is saved at {fname_standardized_address}"
        )
    else:
        pdf_addresses = pdf_matched
    pdf = (
        pdf.merge(
            pdf_addresses.rename(
                columns=dict(
                    [
                        (col, f'hclinic_{col.replace("zip", "zipcode")}')
                        for col in pdf_addresses.columns
                    ]
                )
            ),
            on=[
                f'hclinic_{col.replace("zip", "zipcode")}'
                for col in ["address_cleaned", "city", "state", "zip"]
            ],
            how="left",
        )
        .merge(
            pdf_addresses.rename(
                columns=dict(
                    [(col, f"p_loc_{col}") for col in pdf_addresses.columns]
                )
            ),
            on=[
                f"p_loc_{col}"
                for col in ["address_cleaned", "city", "state", "zip"]
            ],
            how="left",
        )
        .merge(
            pdf_addresses.rename(
                columns=dict(
                    [(col, f"p_mail_{col}") for col in pdf_addresses.columns]
                )
            ),
            on=[
                f"p_mail_{col}"
                for col in ["address_cleaned", "city", "state", "zip"]
            ],
            how="left",
        )
        .merge(
            pdf_addresses.rename(
                columns=dict(
                    [
                        (
                            col,
                            f'hclinic_business_{col.replace("zip", "zipcode")}',
                        )
                        for col in pdf_addresses.columns
                    ]
                )
            ),
            on=[
                f'hclinic_business_{col.replace("zip", "zipcode")}'
                for col in ["address_cleaned", "city", "state", "zip"]
            ],
            how="left",
        )
        .merge(
            pdf_addresses.rename(
                columns=dict(
                    [
                        (col, f'hclinic_mail_{col.replace("zip", "zipcode")}')
                        for col in pdf_addresses.columns
                    ]
                )
            ),
            on=[
                f'hclinic_mail_{col.replace("zip", "zipcode")}'
                for col in ["address_cleaned", "city", "state", "zip"]
            ],
            how="left",
        )
    )
    # Create door no columns
    logger.info("Computing door number info")
    pdf = compute_door_no_info(pdf)
    return pdf


def compute_match_purity(pdf):
    """Computes levenshtein distance based similarity scores for address & name columns"""

    def match_purity(
        p_org_name,
        p_org_name_oth,
        provider_id_state,
        hclinic_name,
        hclinic_site_name,
        hclinic_address_cleaned,
        hclinic_standardized_address,
        hclinic_standardized_street_address,
        hclinic_standardized,
        hclinic_address_digits,
        hclinic_post_box_id,
        hclinic_post_box_group_id,
        hclinic_address_no,
        hclinic_occupancy_no,
        hclinic_city,
        hclinic_state,
        hclinic_business_address_cleaned,
        hclinic_business_standardized_address,
        hclinic_business_standardized_street_address,
        hclinic_business_standardized,
        hclinic_business_address_digits,
        hclinic_business_post_box_id,
        hclinic_business_post_box_group_id,
        hclinic_business_address_no,
        hclinic_business_occupancy_no,
        hclinic_business_city,
        hclinic_business_state,
        hclinic_mail_address_cleaned,
        hclinic_mail_standardized_address,
        hclinic_mail_standardized_street_address,
        hclinic_mail_standardized,
        hclinic_mail_address_digits,
        hclinic_mail_post_box_id,
        hclinic_mail_post_box_group_id,
        hclinic_mail_address_no,
        hclinic_mail_occupancy_no,
        hclinic_mail_city,
        hclinic_mail_state,
        p_loc_address_cleaned,
        p_loc_standardized_address,
        p_loc_standardized_street_address,
        p_loc_address_digits,
        p_loc_post_box_id,
        p_loc_post_box_group_id,
        p_loc_address_no,
        p_loc_occupancy_no,
        p_loc_line_1,
        p_loc_line_2,
        p_loc_city,
        p_loc_state,
        p_mail_address_cleaned,
        p_mail_standardized_address,
        p_mail_standardized_street_address,
        p_mail_address_digits,
        p_mail_post_box_id,
        p_mail_post_box_group_id,
        p_mail_address_no,
        p_mail_occupancy_no,
        p_mail_line_1,
        p_mail_line_2,
        p_mail_city,
        p_mail_state,
    ):
        hclinic_name = clean_hclinic_name(hclinic_name)
        hclinic_site_name = clean_hclinic_name(hclinic_site_name)
        p_org_name = clean_hclinic_name(p_org_name)
        p_org_name_oth = clean_hclinic_name(p_org_name_oth)
        p_loc_line_2 = clean_hclinic_name(p_loc_line_2)
        p_mail_line_2 = clean_hclinic_name(p_mail_line_2)

        name_score = (
            0
            if (not bool(hclinic_name or hclinic_site_name))
            else max(
                0
                if (not bool(p_org_name))
                else max(
                    fuzz.ratio(p_org_name, hclinic_name),
                    fuzz.ratio(p_org_name, hclinic_site_name),
                ),
                0
                if (not bool(p_org_name_oth))
                else max(
                    fuzz.ratio(p_org_name_oth, hclinic_name),
                    fuzz.ratio(p_org_name_oth, hclinic_site_name),
                ),
                0
                if (not bool(p_loc_line_2))
                else max(
                    fuzz.ratio(p_loc_line_2, hclinic_name),
                    fuzz.ratio(p_loc_line_2, hclinic_site_name),
                ),
                0
                if (not bool(p_mail_line_2))
                else max(
                    fuzz.ratio(p_mail_line_2, hclinic_name),
                    fuzz.ratio(p_mail_line_2, hclinic_site_name),
                ),
            )
        )

        address_score = (
            0
            if (
                not bool(
                    hclinic_address_cleaned
                    or hclinic_business_address_cleaned
                    or hclinic_mail_address_cleaned
                )
            )
            else max(
                0
                if (not bool(p_loc_address_cleaned))
                else max(
                    fuzz.ratio(p_loc_address_cleaned, hclinic_address_cleaned),
                    fuzz.ratio(
                        p_loc_address_cleaned, hclinic_business_address_cleaned
                    ),
                    fuzz.ratio(
                        p_loc_address_cleaned, hclinic_mail_address_cleaned
                    ),
                ),
                0
                if (not bool(p_mail_address_cleaned))
                else max(
                    fuzz.ratio(
                        p_mail_address_cleaned, hclinic_address_cleaned
                    ),
                    fuzz.ratio(
                        p_mail_address_cleaned,
                        hclinic_business_address_cleaned,
                    ),
                    fuzz.ratio(
                        p_mail_address_cleaned, hclinic_mail_address_cleaned
                    ),
                ),
                0
                if (not bool(p_loc_line_1))
                else max(
                    fuzz.ratio(p_loc_line_1, hclinic_address_cleaned),
                    fuzz.ratio(p_loc_line_1, hclinic_business_address_cleaned),
                    fuzz.ratio(p_loc_line_1, hclinic_mail_address_cleaned),
                ),
                0
                if (not bool(p_mail_line_1))
                else max(
                    fuzz.ratio(p_mail_line_1, hclinic_address_cleaned),
                    fuzz.ratio(
                        p_mail_line_1, hclinic_business_address_cleaned
                    ),
                    fuzz.ratio(p_mail_line_1, hclinic_mail_address_cleaned),
                ),
            )
        )

        digits_match = (
            0
            if (
                not bool(
                    hclinic_address_digits
                    or hclinic_business_address_digits
                    or hclinic_mail_address_digits
                )
            )
            else int(
                (
                    bool(hclinic_address_digits)
                    and (
                        hclinic_address_digits
                        in [p_loc_address_digits, p_mail_address_digits]
                    )
                )
                or (
                    bool(hclinic_business_address_digits)
                    and (
                        hclinic_business_address_digits
                        in [p_loc_address_digits, p_mail_address_digits]
                    )
                )
                or (
                    bool(hclinic_mail_address_digits)
                    and (
                        hclinic_mail_address_digits
                        in [p_loc_address_digits, p_mail_address_digits]
                    )
                )
            )
        )

        def match_door_no(
            source_post_box_id,
            source_post_box_group_id,
            source_address_no,
            source_occupancy_no,
        ):
            return (
                0
                if not bool(
                    source_post_box_id
                    or source_post_box_group_id
                    or source_address_no
                    or source_occupancy_no
                )
                else int(
                    (
                        (bool(source_address_no))
                        & (
                            (
                                (source_address_no == p_loc_address_no)
                                & (
                                    (source_occupancy_no == p_loc_occupancy_no)
                                    | (
                                        not bool(
                                            source_occupancy_no
                                            or p_loc_occupancy_no
                                        )
                                    )
                                )
                            )
                            | (
                                (source_address_no == p_mail_address_no)
                                & (
                                    (
                                        source_occupancy_no
                                        == p_mail_occupancy_no
                                    )
                                    | (
                                        not bool(
                                            source_occupancy_no
                                            or p_mail_occupancy_no
                                        )
                                    )
                                )
                            )
                        )
                    )
                    | (
                        (bool(source_post_box_id))
                        & (
                            (
                                (source_post_box_id == p_loc_post_box_id)
                                & (
                                    (
                                        source_post_box_group_id
                                        == p_loc_post_box_group_id
                                    )
                                    | (
                                        not bool(
                                            source_post_box_group_id
                                            or p_loc_post_box_group_id
                                        )
                                    )
                                )
                            )
                            | (
                                (source_post_box_id == p_mail_post_box_id)
                                & (
                                    (
                                        source_post_box_group_id
                                        == p_mail_post_box_group_id
                                    )
                                    | (
                                        not bool(
                                            source_post_box_group_id
                                            or p_mail_post_box_group_id
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )

        door_no_match = max(
            match_door_no(
                hclinic_post_box_id,
                hclinic_post_box_group_id,
                hclinic_address_no,
                hclinic_occupancy_no,
            ),
            match_door_no(
                hclinic_business_post_box_id,
                hclinic_business_post_box_group_id,
                hclinic_business_address_no,
                hclinic_business_occupancy_no,
            ),
            match_door_no(
                hclinic_mail_post_box_id,
                hclinic_mail_post_box_group_id,
                hclinic_mail_address_no,
                hclinic_mail_occupancy_no,
            ),
        )

        standardized_street_address_score = (
            0
            if (
                not bool(
                    hclinic_standardized_street_address
                    or hclinic_business_standardized_street_address
                    or hclinic_mail_standardized_street_address
                )
            )
            else max(
                0
                if (not bool(p_loc_standardized_street_address))
                else max(
                    0
                    if (not bool(hclinic_standardized))
                    else fuzz.ratio(
                        p_loc_standardized_street_address,
                        hclinic_standardized_street_address,
                    ),
                    0
                    if (not bool(hclinic_business_standardized))
                    else fuzz.ratio(
                        p_loc_standardized_street_address,
                        hclinic_business_standardized_street_address,
                    ),
                    0
                    if (not bool(hclinic_mail_standardized))
                    else fuzz.ratio(
                        p_loc_standardized_street_address,
                        hclinic_mail_standardized_street_address,
                    ),
                ),
                0
                if (not bool(p_mail_standardized_street_address))
                else max(
                    0
                    if (not bool(hclinic_standardized))
                    else fuzz.ratio(
                        p_mail_standardized_street_address,
                        hclinic_standardized_street_address,
                    ),
                    0
                    if (not bool(hclinic_business_standardized))
                    else fuzz.ratio(
                        p_mail_standardized_street_address,
                        hclinic_business_standardized_street_address,
                    ),
                    0
                    if (not bool(hclinic_mail_standardized))
                    else fuzz.ratio(
                        p_mail_standardized_street_address,
                        hclinic_mail_standardized_street_address,
                    ),
                ),
            )
        )

        standardized_address_score = (
            0
            if (
                not bool(
                    hclinic_standardized_address
                    or hclinic_business_standardized_address
                    or hclinic_mail_standardized_address
                )
            )
            else max(
                0
                if (not bool(p_loc_standardized_address))
                else max(
                    0
                    if (not bool(hclinic_standardized))
                    else fuzz.ratio(
                        p_loc_standardized_address,
                        hclinic_standardized_address,
                    ),
                    0
                    if (not bool(hclinic_business_standardized))
                    else fuzz.ratio(
                        p_loc_standardized_address,
                        hclinic_business_standardized_address,
                    ),
                    0
                    if (not bool(hclinic_mail_standardized))
                    else fuzz.ratio(
                        p_loc_standardized_address,
                        hclinic_mail_standardized_address,
                    ),
                ),
                0
                if (not bool(p_mail_standardized_address))
                else max(
                    0
                    if (not bool(hclinic_standardized))
                    else fuzz.ratio(
                        p_mail_standardized_address,
                        hclinic_standardized_address,
                    ),
                    0
                    if (not bool(hclinic_business_standardized))
                    else fuzz.ratio(
                        p_mail_standardized_address,
                        hclinic_business_standardized_address,
                    ),
                    0
                    if (not bool(hclinic_mail_standardized))
                    else fuzz.ratio(
                        p_mail_standardized_address,
                        hclinic_mail_standardized_address,
                    ),
                ),
            )
        )

        city_score = (
            0
            if (
                not bool(
                    hclinic_city or hclinic_business_city or hclinic_mail_city
                )
            )
            else max(
                0
                if not bool(p_loc_city)
                else max(
                    fuzz.ratio(p_loc_city, hclinic_city),
                    fuzz.ratio(p_loc_city, hclinic_business_city),
                    fuzz.ratio(p_loc_city, hclinic_mail_city),
                ),
                0
                if not bool(p_mail_city)
                else max(
                    fuzz.ratio(p_mail_city, hclinic_city),
                    fuzz.ratio(p_mail_city, hclinic_business_city),
                    fuzz.ratio(p_mail_city, hclinic_mail_city),
                ),
            )
        )
        state_match = int(
            (
                bool(hclinic_state)
                and (
                    hclinic_state
                    in [provider_id_state, p_loc_state, p_mail_state]
                )
            )
            or (
                bool(hclinic_business_state)
                and (
                    hclinic_business_state
                    in [provider_id_state, p_loc_state, p_mail_state]
                )
            )
            or (
                bool(hclinic_mail_state)
                and (
                    hclinic_mail_state
                    in [provider_id_state, p_loc_state, p_mail_state]
                )
            )
        )

        return [
            name_score,
            address_score,
            digits_match,
            door_no_match,
            standardized_street_address_score,
            standardized_address_score,
            city_score,
            state_match,
        ]

    if "provider_id_state" not in pdf.columns:
        pdf["provider_id_state"] = ""
    pdf = pdf.replace(to_replace=[None, "None"], value=np.nan).fillna("")
    pdf = pdf.assign(
        **dict(
            [
                (col, pdf[col].str.strip())
                for col in [
                    "hclinic_post_box_id",
                    "hclinic_post_box_group_id",
                    "hclinic_address_no",
                    "hclinic_occupancy_no",
                    "hclinic_business_post_box_id",
                    "hclinic_business_post_box_group_id",
                    "hclinic_business_address_no",
                    "hclinic_business_occupancy_no",
                    "hclinic_mail_post_box_id",
                    "hclinic_mail_post_box_group_id",
                    "hclinic_mail_address_no",
                    "hclinic_mail_occupancy_no",
                    "p_loc_post_box_id",
                    "p_loc_post_box_group_id",
                    "p_loc_address_no",
                    "p_loc_occupancy_no",
                    "p_mail_post_box_id",
                    "p_mail_post_box_group_id",
                    "p_mail_address_no",
                    "p_mail_occupancy_no",
                ]
            ]
        )
    )

    pdf = (
        dd.from_pandas(pdf, npartitions=ceil(pdf.shape[0] / 10000))
        .map_partitions(
            lambda pdf_partition: pdf_partition.merge(
                pdf_partition.apply(
                    lambda row: pd.Series(
                        match_purity(
                            row["p_org_name"],
                            row["p_org_name_oth"],
                            row["provider_id_state"],
                            row["hclinic_name"],
                            row["hclinic_site_name"],
                            row["hclinic_address_cleaned"],
                            row["hclinic_standardized_address"],
                            row["hclinic_standardized_street_address"],
                            row["hclinic_standardized"],
                            row["hclinic_address_digits"],
                            row["hclinic_post_box_id"],
                            row["hclinic_post_box_group_id"],
                            row["hclinic_address_no"],
                            row["hclinic_occupancy_no"],
                            row["hclinic_city"],
                            row["hclinic_state"],
                            row["hclinic_business_address_cleaned"],
                            row["hclinic_business_standardized_address"],
                            row[
                                "hclinic_business_standardized_street_address"
                            ],
                            row["hclinic_business_standardized"],
                            row["hclinic_business_address_digits"],
                            row["hclinic_business_post_box_id"],
                            row["hclinic_business_post_box_group_id"],
                            row["hclinic_business_address_no"],
                            row["hclinic_business_occupancy_no"],
                            row["hclinic_business_city"],
                            row["hclinic_business_state"],
                            row["hclinic_mail_address_cleaned"],
                            row["hclinic_mail_standardized_address"],
                            row["hclinic_mail_standardized_street_address"],
                            row["hclinic_mail_standardized"],
                            row["hclinic_mail_address_digits"],
                            row["hclinic_mail_post_box_id"],
                            row["hclinic_mail_post_box_group_id"],
                            row["hclinic_mail_address_no"],
                            row["hclinic_mail_occupancy_no"],
                            row["hclinic_mail_city"],
                            row["hclinic_mail_state"],
                            row["p_loc_address_cleaned"],
                            row["p_loc_standardized_address"],
                            row["p_loc_standardized_street_address"],
                            row["p_loc_address_digits"],
                            row["p_loc_post_box_id"],
                            row["p_loc_post_box_group_id"],
                            row["p_loc_address_no"],
                            row["p_loc_occupancy_no"],
                            row["p_loc_line_1"],
                            row["p_loc_line_2"],
                            row["p_loc_city"],
                            row["p_loc_state"],
                            row["p_mail_address_cleaned"],
                            row["p_mail_standardized_address"],
                            row["p_mail_standardized_street_address"],
                            row["p_mail_address_digits"],
                            row["p_mail_post_box_id"],
                            row["p_mail_post_box_group_id"],
                            row["p_mail_address_no"],
                            row["p_mail_occupancy_no"],
                            row["p_mail_line_1"],
                            row["p_mail_line_2"],
                            row["p_mail_city"],
                            row["p_mail_state"],
                        ),
                        index=[
                            "name_score",
                            "address_score",
                            "digits_match",
                            "door_no_match",
                            "standardized_street_address_score",
                            "standardized_address_score",
                            "city_score",
                            "state_match",
                        ],
                    ),
                    axis=1,
                    result_type="expand",
                ),
                left_index=True,
                right_index=True,
            )
        )
        .compute()
    )

    return pdf


def compute_door_no_info(pdf):
    def parse_address(
        hclinic_address_cleaned,
        hclinic_standardized_address,
        hclinic_standardized,
        hclinic_business_address_cleaned,
        hclinic_business_standardized_address,
        hclinic_business_standardized,
        hclinic_mail_address_cleaned,
        hclinic_mail_standardized_address,
        hclinic_mail_standardized,
        p_loc_address_cleaned,
        p_loc_standardized_address,
        p_loc_standardized,
        p_mail_address_cleaned,
        p_mail_standardized_address,
        p_mail_standardized,
    ):
        dct_hclinic_address = dict(
            usaddress.parse(
                hclinic_standardized_address
                if hclinic_standardized
                else hclinic_address_cleaned
            )
        )
        dct_hclinic_business_address = dict(
            usaddress.parse(
                hclinic_business_standardized_address
                if hclinic_business_standardized
                else hclinic_business_address_cleaned
            )
        )
        dct_hclinic_mail_address = dict(
            usaddress.parse(
                hclinic_mail_standardized_address
                if hclinic_mail_standardized
                else hclinic_mail_address_cleaned
            )
        )

        dct_p_loc_address = dict(
            usaddress.parse(
                p_loc_standardized_address
                if p_loc_standardized
                else p_loc_address_cleaned
            )
        )

        dct_p_mail_address = dict(
            usaddress.parse(
                p_mail_standardized_address
                if p_mail_standardized
                else p_mail_address_cleaned
            )
        )

        def cleanup_address_components(dct_address):
            dct_address_cleaned = {}
            for k, v in dct_address.items():
                if (
                    v == "OccupancyIdentifier" and k.lower().strip() == "and"
                ) or (
                    v in ["NotAddress", "PlaceName", "Recipient", "StateName"]
                ):
                    pass
                dct_address_cleaned[v] = (
                    k
                    if v not in dct_address_cleaned
                    else (
                        "-"
                        if v.endswith(
                            (
                                "Identifier",
                                "Number",
                            )
                        )
                        else " "
                    ).join([dct_address_cleaned[v], k])
                )
            return dct_address_cleaned

        dct_hclinic_address = cleanup_address_components(dct_hclinic_address)
        dct_p_loc_address = cleanup_address_components(dct_p_loc_address)
        dct_p_mail_address = cleanup_address_components(dct_p_mail_address)

        hclinic_post_box_id = dct_hclinic_address.get("USPSBoxID", np.nan)
        hclinic_post_box_group_id = dct_hclinic_address.get(
            "USPSBoxGroupID", np.nan
        )
        hclinic_address_no = dct_hclinic_address.get("AddressNumber", np.nan)
        hclinic_occupancy_no = dct_hclinic_address.get(
            "OccupancyIdentifier", np.nan
        )

        hclinic_business_post_box_id = dct_hclinic_business_address.get(
            "USPSBoxID", np.nan
        )
        hclinic_business_post_box_group_id = dct_hclinic_business_address.get(
            "USPSBoxGroupID", np.nan
        )
        hclinic_business_address_no = dct_hclinic_business_address.get(
            "AddressNumber", np.nan
        )
        hclinic_business_occupancy_no = dct_hclinic_business_address.get(
            "OccupancyIdentifier", np.nan
        )

        hclinic_mail_post_box_id = dct_hclinic_mail_address.get(
            "USPSBoxID", np.nan
        )
        hclinic_mail_post_box_group_id = dct_hclinic_mail_address.get(
            "USPSBoxGroupID", np.nan
        )
        hclinic_mail_address_no = dct_hclinic_mail_address.get(
            "AddressNumber", np.nan
        )
        hclinic_mail_occupancy_no = dct_hclinic_mail_address.get(
            "OccupancyIdentifier", np.nan
        )

        p_loc_post_box_id = dct_p_loc_address.get("USPSBoxID", np.nan)
        p_loc_post_box_group_id = dct_p_loc_address.get(
            "USPSBoxGroupID", np.nan
        )
        p_loc_address_no = dct_p_loc_address.get("AddressNumber", np.nan)
        p_loc_occupancy_no = dct_p_loc_address.get(
            "OccupancyIdentifier", np.nan
        )

        p_mail_post_box_id = dct_p_mail_address.get("USPSBoxID", np.nan)
        p_mail_post_box_group_id = dct_p_mail_address.get(
            "USPSBoxGroupID", np.nan
        )
        p_mail_address_no = dct_p_mail_address.get("AddressNumber", np.nan)
        p_mail_occupancy_no = dct_p_mail_address.get(
            "OccupancyIdentifier", np.nan
        )

        return [
            hclinic_post_box_id,
            hclinic_post_box_group_id,
            hclinic_address_no,
            hclinic_occupancy_no,
            hclinic_business_post_box_id,
            hclinic_business_post_box_group_id,
            hclinic_business_address_no,
            hclinic_business_occupancy_no,
            hclinic_mail_post_box_id,
            hclinic_mail_post_box_group_id,
            hclinic_mail_address_no,
            hclinic_mail_occupancy_no,
            p_loc_post_box_id,
            p_loc_post_box_group_id,
            p_loc_address_no,
            p_loc_occupancy_no,
            p_mail_post_box_id,
            p_mail_post_box_group_id,
            p_mail_address_no,
            p_mail_occupancy_no,
        ]

    pdf = pdf.assign(
        **dict(
            [
                (col, pdf[col].fillna("").str.strip())
                for col in "hclinic_address_cleaned,hclinic_standardized_address,"
                "hclinic_business_address_cleaned,hclinic_business_standardized_address,"
                "hclinic_mail_address_cleaned,hclinic_mail_standardized_address,"
                "p_loc_address_cleaned,p_loc_standardized_address,"
                "p_mail_address_cleaned,p_mail_standardized_address".split(",")
            ]
            + [
                (col, pdf[col].fillna(0).astype(int))
                for col in "hclinic_standardized,hclinic_business_standardized,hclinic_mail_standardized,"
                "p_loc_standardized,p_mail_standardized".split(",")
            ]
        )
    )
    pdf = pdf.replace(to_replace=[None, "None"], value=np.nan).fillna("")

    pdf = (
        dd.from_pandas(pdf, npartitions=ceil(pdf.shape[0] / 10000))
        .map_partitions(
            lambda pdf_partition: pdf_partition.merge(
                pdf_partition.apply(
                    lambda row: pd.Series(
                        parse_address(
                            row["hclinic_address_cleaned"],
                            row["hclinic_standardized_address"],
                            row["hclinic_standardized"],
                            row["hclinic_business_address_cleaned"],
                            row["hclinic_business_standardized_address"],
                            row["hclinic_business_standardized"],
                            row["hclinic_mail_address_cleaned"],
                            row["hclinic_mail_standardized_address"],
                            row["hclinic_mail_standardized"],
                            row["p_loc_address_cleaned"],
                            row["p_loc_standardized_address"],
                            row["p_loc_standardized"],
                            row["p_mail_address_cleaned"],
                            row["p_mail_standardized_address"],
                            row["p_mail_standardized"],
                        ),
                        index=[
                            "hclinic_post_box_id",
                            "hclinic_post_box_group_id",
                            "hclinic_address_no",
                            "hclinic_occupancy_no",
                            "hclinic_business_post_box_id",
                            "hclinic_business_post_box_group_id",
                            "hclinic_business_address_no",
                            "hclinic_business_occupancy_no",
                            "hclinic_mail_post_box_id",
                            "hclinic_mail_post_box_group_id",
                            "hclinic_mail_address_no",
                            "hclinic_mail_occupancy_no",
                            "p_loc_post_box_id",
                            "p_loc_post_box_group_id",
                            "p_loc_address_no",
                            "p_loc_occupancy_no",
                            "p_mail_post_box_id",
                            "p_mail_post_box_group_id",
                            "p_mail_address_no",
                            "p_mail_occupancy_no",
                        ],
                    ),
                    axis=1,
                    result_type="expand",
                ),
                left_index=True,
                right_index=True,
            )
        )
        .compute()
    )
    pdf = pdf.replace(to_replace=[None, "None"], value=np.nan).fillna("")
    return pdf


def fuzzy_match(
    df, df_npi_provider, source="hcris", logger_name="fuzzy_match"
):
    logger = logging.getLogger(logger_name)
    logger.info(
        f"Attempting fuzzy text matching. Merging {source} dataset with NPPES using state columns"
    )
    df = df.reset_index(drop=True)
    chunk_size = 100
    list_df = [
        df[i: i + chunk_size] for i in range(0, df.shape[0], chunk_size)
    ]

    fname_fuzzy_source_nppes_merge = os.path.join(
        os.path.join(fqhc_lookup_folder, f"nppes_{source}_text_merged.pickle")
    )
    if os.path.isfile(fname_fuzzy_source_nppes_merge):
        os.remove(fname_fuzzy_source_nppes_merge)
    for chunk_id, df_source in enumerate(list_df):
        logger.info(f"Processing chunk {chunk_id + 1} of {len(list_df)}")

        df_nppes_source_p_id_state = df_npi_provider.loc[
            df_npi_provider["entity"].str.strip() == "2"
        ].merge(
            df_source.loc[
                df_source["hclinic_state"].str.strip().str.len() > 0
            ],
            left_on=["provider_id_state"],
            right_on=["hclinic_state"],
            how="inner",
        )

        df_nppes_source_p_loc_state = df_npi_provider.loc[
            (df_npi_provider["entity"].str.strip() == "2")
            & (
                df_npi_provider["provider_id_state"]
                != df_npi_provider["p_loc_state"]
            )
        ].merge(
            df_source.loc[
                df_source["hclinic_state"].str.strip().str.len() > 0
            ],
            left_on=["p_loc_state"],
            right_on=["hclinic_state"],
            how="inner",
        )

        df_nppes_source_p_mail_state = df_npi_provider.loc[
            (df_npi_provider["entity"].str.strip() == "2")
            & (
                (
                    df_npi_provider["provider_id_state"]
                    != df_npi_provider["p_mail_state"]
                )
                & (
                    (
                        df_npi_provider["p_loc_state"]
                        != df_npi_provider["p_mail_state"]
                    )
                )
            )
        ].merge(
            df_source.loc[
                df_source["hclinic_state"].str.strip().str.len() > 0
            ],
            left_on=["p_mail_state"],
            right_on=["hclinic_state"],
            how="inner",
        )
        df_nppes_source = dd.concat(
            [
                df_nppes_source_p_id_state,
                df_nppes_source_p_loc_state,
                df_nppes_source_p_mail_state,
            ],
            ignore_index=True,
        )
        if source == "uds":
            df_nppes_source_business_p_id_state = df_npi_provider.loc[
                df_npi_provider["entity"].str.strip() == "2"
            ].merge(
                df_source.loc[
                    df_source["hclinic_business_state"].str.strip().str.len()
                    > 0
                ],
                left_on=["provider_id_state"],
                right_on=["hclinic_business_state"],
                how="inner",
            )

            df_nppes_source_business_p_loc_state = df_npi_provider.loc[
                (df_npi_provider["entity"].str.strip() == "2")
                & (
                    df_npi_provider["provider_id_state"]
                    != df_npi_provider["p_loc_state"]
                )
            ].merge(
                df_source.loc[
                    df_source["hclinic_business_state"].str.strip().str.len()
                    > 0
                ],
                left_on=["p_loc_state"],
                right_on=["hclinic_business_state"],
                how="inner",
            )

            df_nppes_source_business_p_mail_state = df_npi_provider.loc[
                (df_npi_provider["entity"].str.strip() == "2")
                & (
                    (
                        df_npi_provider["provider_id_state"]
                        != df_npi_provider["p_mail_state"]
                    )
                    & (
                        (
                            df_npi_provider["p_loc_state"]
                            != df_npi_provider["p_mail_state"]
                        )
                    )
                )
            ].merge(
                df_source.loc[
                    df_source["hclinic_business_state"].str.strip().str.len()
                    > 0
                ],
                left_on=["p_mail_state"],
                right_on=["hclinic_business_state"],
                how="inner",
            )

            df_nppes_source_mail_p_id_state = df_npi_provider.loc[
                df_npi_provider["entity"].str.strip() == "2"
            ].merge(
                df_source.loc[
                    df_source["hclinic_mail_state"].str.strip().str.len() > 0
                ],
                left_on=["provider_id_state"],
                right_on=["hclinic_mail_state"],
                how="inner",
            )

            df_nppes_source_mail_p_loc_state = df_npi_provider.loc[
                (df_npi_provider["entity"].str.strip() == "2")
                & (
                    df_npi_provider["provider_id_state"]
                    != df_npi_provider["p_loc_state"]
                )
            ].merge(
                df_source.loc[
                    df_source["hclinic_mail_state"].str.strip().str.len() > 0
                ],
                left_on=["p_loc_state"],
                right_on=["hclinic_mail_state"],
                how="inner",
            )

            df_nppes_source_mail_p_mail_state = df_npi_provider.loc[
                (df_npi_provider["entity"].str.strip() == "2")
                & (
                    (
                        df_npi_provider["provider_id_state"]
                        != df_npi_provider["p_mail_state"]
                    )
                    & (
                        (
                            df_npi_provider["p_loc_state"]
                            != df_npi_provider["p_mail_state"]
                        )
                    )
                )
            ].merge(
                df_source.loc[
                    df_source["hclinic_mail_state"].str.strip().str.len() > 0
                ],
                left_on=["p_mail_state"],
                right_on=["hclinic_mail_state"],
                how="inner",
            )
            df_nppes_source = dd.concat(
                [
                    df_nppes_source,
                    df_nppes_source_business_p_id_state,
                    df_nppes_source_business_p_loc_state,
                    df_nppes_source_business_p_mail_state,
                    df_nppes_source_mail_p_id_state,
                    df_nppes_source_mail_p_loc_state,
                    df_nppes_source_mail_p_mail_state,
                ],
                ignore_index=True,
            )

        df_nppes_source = filter_partial_matches(df_nppes_source)
        if os.path.isfile(fname_fuzzy_source_nppes_merge):
            df_nppes_source = (
                pd.concat(
                    [
                        pd.read_pickle(fname_fuzzy_source_nppes_merge),
                        df_nppes_source,
                    ],
                    ignore_index=True,
                )
                .drop_duplicates()
                .reset_index(drop=True)
            )
        df_nppes_source.to_pickle(fname_fuzzy_source_nppes_merge)
        del df_nppes_source
        if (chunk_id + 1) == len(list_df):
            logger.info(
                f"HCLINIC providers were matched on the basis of text (name & address) columns with organization "
                f"NPIs from NPPES dataset, resulting in {pd.read_pickle(fname_fuzzy_source_nppes_merge).shape[0]} "
                f"potential matches. This dataset is saved at {fname_fuzzy_source_nppes_merge}"
            )

            df_nppes_source_fuzzy_matched = pd.read_pickle(
                fname_fuzzy_source_nppes_merge
            )
            df_nppes_source_fuzzy_matched = process_address_columns(
                df_nppes_source_fuzzy_matched, logger_name, source
            )
            logger.info(
                f"Processing address columns for the {df_nppes_source_fuzzy_matched.shape[0]} text based matches"
            )

            df_nppes_source_fuzzy_matched = compute_match_purity(
                df_nppes_source_fuzzy_matched[
                    [
                        col
                        for col in df_nppes_source_fuzzy_matched.columns
                        if col
                        not in [
                            "name_score_x",
                            "address_score_x",
                            "city_score_x",
                            "name_score_y",
                            "address_score_y",
                            "name_score",
                            "address_score",
                            "city_score",
                            "standardized_address_score",
                            "city_score_y",
                        ]
                    ]
                ]
            )
            fname_fuzzy_source_nppes_merge_with_match_purity = os.path.join(
                fqhc_lookup_folder,
                f"nppes_{source}_text_merged_with_match_purity.pickle",
            )
            logger.info(
                f"Computing match purity for the {df_nppes_source_fuzzy_matched.shape[0]} text-based matches. This is "
                f"saved at  {fname_fuzzy_source_nppes_merge_with_match_purity}"
            )
            df_nppes_source_fuzzy_matched.to_pickle(
                fname_fuzzy_source_nppes_merge_with_match_purity
            )

            df_nppes_source_address_matched = (
                df_nppes_source_fuzzy_matched.loc[
                    (
                        df_nppes_source_fuzzy_matched[
                            [
                                "standardized_street_address_score",
                                "address_score",
                            ]
                        ]
                        >= 70
                    ).any(axis=1)
                    & (df_nppes_source_fuzzy_matched["door_no_match"] == 1)
                    & (df_nppes_source_fuzzy_matched["name_score"] >= 50)
                    & (df_nppes_source_fuzzy_matched["city_score"] >= 80)
                ]
            )
            df_nppes_source_name_matched = df_nppes_source_fuzzy_matched.loc[
                (df_nppes_source_fuzzy_matched["city_score"] >= 80)
                & (df_nppes_source_fuzzy_matched["name_score"] >= 80)
            ]

            logger.info(
                f"Perfect address matches were arrived at by filtering matches with >= 70 as the string distance score "
                f"for street addresses, 100% matching door & street numbers, >=50 as the string distance score for "
                f"names  and >= 80 as the string distance score for cities. "
                f"{df_nppes_source_address_matched.hclinic_provider_id.nunique()} were matched this way."
            )

            logger.info(
                f"Perfect name matches were arrived at by filtering matches with >= 80 as the string distance score "
                f"for names and >= 80 as the string distance score for cities."
                f"{df_nppes_source_name_matched.hclinic_provider_id.nunique()} were matched this way."
            )

            df_text_based_perfect_matches = (
                pd.concat(
                    [
                        df_nppes_source_address_matched,
                        df_nppes_source_name_matched,
                    ],
                    ignore_index=True,
                )
                .drop_duplicates()
                .reset_index(drop=True)
            )

            df_nppes_source_fuzzy_matched = df_nppes_source_fuzzy_matched.loc[
                (
                    df_nppes_source_fuzzy_matched[
                        ["standardized_street_address_score", "address_score"]
                    ]
                    >= 70
                ).any(axis=1)
                & (
                    (df_nppes_source_fuzzy_matched["door_no_match"] == 1)
                    | (df_nppes_source_fuzzy_matched["name_score"] >= 50)
                )
                & (df_nppes_source_fuzzy_matched["city_score"] >= 80)
            ]

            return df_text_based_perfect_matches, df_nppes_source_fuzzy_matched


def create_npi_fqhc_crosswalk(
    source: str = "hcris",
    logger_name: str = "fqhc_crosswalk",
):
    """Merge HCRIS, UDS and NPPES and datasets to create FQHC NPI list"""

    logger = logging.getLogger(logger_name)
    # Load all available years of hcris datasets
    if not os.path.exists(
        os.path.join(hcris_lookup_folder, "hcris_all_years.pickle")
    ):
        logger.info(
            "Cleaning and combining all available years of HCRIS datasets"
        )
        combine_and_clean_hcris_files(logger_name)
    df_source = pd.read_pickle(
        os.path.join(hcris_lookup_folder, "hcris_all_years.pickle")
    )

    lst_source_col = df_source.columns
    lst_disparate_col = [
        "hclinic_po_box",
        "hclinic_site_name",
        "hclinic_street_address_line_2",
        "hclinic_business_address",
        "hclinic_business_state",
        "hclinic_business_city",
        "hclinic_business_zipcode",
        "hclinic_mail_address",
        "hclinic_mail_state",
        "hclinic_mail_city",
        "hclinic_mail_zipcode",
    ]
    df_source = df_source.assign(
        **dict(
            [
                (col, "")
                for col in lst_disparate_col
                if col not in lst_source_col
            ]
        )
    )

    # Additional filters for UDS data
    df_source_with_site = pd.DataFrame()
    df_source_with_address = pd.DataFrame()
    df_source_with_mail_address = pd.DataFrame()
    df_source_with_business_address = pd.DataFrame()
    df_source_with_site_and_address = pd.DataFrame()
    df_source_with_site_and_mail_address = pd.DataFrame()
    df_source_with_site_and_business_address = pd.DataFrame()

    lst_year = list(range(2009, datetime.now().year + 1))

    # Load NPPES provider data for all available years
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
    # Create a version of NPPES datasets with cleaned providers id columns without leading zeros
    df_npi_provider_cleaned = df_npi_provider.assign(
        provider_id=df_npi_provider["provider_id"].str.rstrip("0")
    )

    # Load NPI data for all available years
    df_npi = dd.concat(
        [
            dd.read_parquet(
                os.path.join(
                    nppes_lookup_folder, str(yr), "npi_cleaned_parquet"
                ),
                engine=pq_engine,
                index=False,
            )
            for yr in range(2009, 2022)
        ]
    )

    # Merge source & NPPES data on provider id and state columns
    df_source_matched = df_npi_provider.merge(
        df_source,
        left_on=["provider_id", "provider_id_state"],
        right_on=["hclinic_provider_id_cleaned", "hclinic_state"],
        how="inner",
    ).compute()

    logger.info(
        f"{df_source_matched.hclinic_provider_id_cleaned.nunique()} HCLINIC providers were matched using "
        f"provider_id and provider_id_state columns"
    )
    df_source_matched_ploc = df_npi_provider.merge(
        df_source,
        left_on=["provider_id", "p_loc_state"],
        right_on=["hclinic_provider_id_cleaned", "hclinic_state"],
        how="inner",
    ).compute()
    logger.info(
        f"{df_source_matched_ploc.hclinic_provider_id_cleaned.nunique()} HCLINIC providers were matched using "
        f"provider_id and p_loc_state columns"
    )

    df_source_matched_mloc = df_npi_provider.merge(
        df_source,
        left_on=["provider_id", "p_mail_state"],
        right_on=["hclinic_provider_id_cleaned", "hclinic_state"],
        how="inner",
    ).compute()

    logger.info(
        f"{df_source_matched_mloc.hclinic_provider_id_cleaned.nunique()} HCLINIC providers were matched using "
        f"provider_id and m_loc_state columns"
    )

    df_source_matched = (
        pd.concat(
            [
                df_source_matched,
                df_source_matched_ploc,
                df_source_matched_mloc,
            ],
            ignore_index=True,
        )
        .reset_index()
        .sort_values(["hclinic_provider_id_cleaned", "npi"])
        .drop_duplicates(keep="first")
        .reset_index(drop=True)
    )
    fname_nppes_state_matched = os.path.join(
        fqhc_lookup_folder, f"{source}_nppes_based_matches.pickle"
    )
    logger.info(
        f"In total, {df_source_matched.hclinic_provider_id_cleaned.nunique()} HCLINIC providers were matched using "
        f"provider_id and state columns from NPPES. These matched are saved at {fname_nppes_state_matched}"
    )

    df_source_matched.to_pickle(fname_nppes_state_matched)

    # Using NPPES API
    # Using hclinic name, city, state and zipcode in the API call
    df_api_based_matched = nppes_api_requests(
        df_source,
        logger_name,
        **{
            "organization_name": "hclinic_name",
            "city": "hclinic_city",
            "state": "hclinic_state",
            "postal_code": "hclinic_zipcode",
        },
    )

    logger.info(
        f"In total, {df_api_based_matched.hclinic_provider_id_cleaned.nunique()} HCLINIC providers were matched using "
        f"NPPES API calls"
    )

    # Expanding multiple NPPES matches in json data returned by NPPES API
    logger.info(
        "Expanding multiple NPPES matches in json data returned by NPPES API"
    )
    df_api_based_matched_flattened = flatten_nppes_query_result(
        df_api_based_matched
    )
    df_api_based_matched_flattened = (
        df_api_based_matched_flattened.reset_index(drop=True)
    )
    # Cleaning address columns
    logger.info("Cleaning & standarding address columns")
    df_api_based_matched_flattened = process_address_columns(
        df_api_based_matched_flattened, logger_name, source
    )

    # Compute match purity
    df_api_based_matched_flattened = compute_match_purity(
        df_api_based_matched_flattened
    )
    df_api_based_matched_flattened = df_api_based_matched_flattened.loc[
        (df_api_based_matched_flattened["name_score"] >= 70)
        | (
            (
                df_api_based_matched_flattened[
                    ["address_score", "standardized_street_address_score"]
                ]
                >= 70
            ).any(axis=1)
            & (df_api_based_matched_flattened["door_no_match"] == 1)
        )
    ]
    logger.info(
        f"{df_api_based_matched.hclinic_provider_id_cleaned.nunique()} HCLINIC providers were matched using API calls"
        f"after removing less clean matches (name score >70 or (address score >= 70 and same door number))"
    )
    fname_api_based_matches = os.path.join(
        fqhc_lookup_folder, f"{source}_api_perfect_matches.pickle"
    )
    logger.info(f"API based matches are saved at {fname_api_based_matches}")

    df_api_based_matched_flattened.to_pickle(fname_api_based_matches)
    df_api_based_matched_flattened = df_api_based_matched_flattened.drop(
        ["npi_json", "result_count", "results"], axis=1
    )
    df_api_based_matched_flattened["entity"] = "2"

    df_perfect_matches = (
        pd.concat(
            [df_api_based_matched_flattened, df_source_matched],
            ignore_index=True,
        )
        .sort_values(by=["hclinic_provider_id", "npi"])
        .reset_index(drop=True)
    )

    fname_api_and_nppes_perfect_matches = os.path.join(
        fqhc_lookup_folder, f"{source}_api_and_nppes_perfect_matches.pickle"
    )
    logger.info(
        f"In total, {df_perfect_matches.hclinic_provider_id_cleaned.nunique()} HCLINIC providers were matched "
        f"using exports and NPPES API calls. These matches are saved at {fname_api_and_nppes_perfect_matches}"
    )

    df_perfect_matches.to_pickle(fname_api_and_nppes_perfect_matches)

    df_unmatched = df_source.loc[
        ~df_source["hclinic_provider_id"].isin(
            df_perfect_matches["hclinic_provider_id"]
        )
    ]

    # Attempting match with NPPES export with only provider_id
    logger.info("Attempting match with NPPES export with only provider_id")
    df_state_relaxed = df_npi_provider.merge(
        df_unmatched,
        left_on=["provider_id"],
        right_on=["hclinic_provider_id"],
        how="inner",
    ).compute()

    logger.info("Cleaning & standarding address in state relaxed matches")
    df_state_relaxed = process_address_columns(
        df_state_relaxed, logger_name, source
    )

    # Computing match purity
    df_state_relaxed = compute_match_purity(df_state_relaxed)

    logger.info("Keeping only the matches with name score > 80")
    df_state_relaxed = df_state_relaxed.loc[
        (df_state_relaxed["name_score"] >= 80)
    ]
    df_state_relaxed = (
        df_state_relaxed.drop_duplicates(
            ["hclinic_provider_id", "npi"], keep="first"
        )
        .sort_values(by=["hclinic_provider_id", "npi"])
        .reset_index(drop=True)
    )

    logger.info(
        f"{df_state_relaxed.hclinic_provider_id.nunique()} "
        f"hclinic provider ids were matched in this step"
    )

    df_source_matched = (
        pd.concat([df_perfect_matches, df_state_relaxed], ignore_index=True)
        .sort_values(by=["hclinic_provider_id", "npi"])
        .reset_index(drop=True)
    )
    fname_with_state_relaxed = os.path.join(
        fqhc_lookup_folder,
        f"{source}_api_and_nppes_perfect_matches_with_state_relaxed.pickle",
    )
    logger.info(
        f"In total {df_source_matched.hclinic_provider_id.nunique()} "
        f"hclinic provider ids were matched as of this step. All matches till this step are saved "
        f"at {fname_with_state_relaxed}"
    )
    df_source_matched.to_pickle(fname_with_state_relaxed)

    df_unmatched = df_source.loc[
        ~df_source["hclinic_provider_id"].isin(
            df_source_matched["hclinic_provider_id"]
        )
    ]
    logger.info(
        f"{df_unmatched.hclinic_provider_id.nunique()} are yet to be matched"
    )

    df_text_based_perfect_matches, df_nppes_source_fuzzy_matched = fuzzy_match(
        df_source, df_npi_provider, source, logger_name
    )
    fname_source_text_based_perfect_matches = os.path.join(
        fqhc_lookup_folder, f"{source}_text_based_perfect_matches.pickle"
    )
    logger.info(
        f"In total, perfect matches for {df_text_based_perfect_matches.hclinic_provider_id.nunique()} provider "
        f"ids were arrived at using text distance metrics. These matches are saved at "
        f"{fname_source_text_based_perfect_matches}"
    )
    df_text_based_perfect_matches.to_pickle(
        fname_source_text_based_perfect_matches
    )

    df_source_matched = (
        pd.concat(
            [df_source_matched, df_text_based_perfect_matches],
            ignore_index=True,
        )
        .sort_values(by=["hclinic_provider_id", "npi"])
        .drop_duplicates()
        .reset_index(drop=True)
    )
    fname_source_perfect_matches = os.path.join(
        fqhc_lookup_folder, f"h{source}_perfect_matches.pickle"
    )
    df_source_matched.to_pickle(fname_source_perfect_matches)
    logger.info(
        f"In total, {df_source_matched.hclinic_provider_id.nunique()} of the "
        f"{df_source.hclinic_provider_id.nunique()} provider ids have perfect matches. All the perfect matches "
        f"are saved at {fname_source_perfect_matches}"
    )

    df_nppes_source_fuzzy_matched = df_nppes_source_fuzzy_matched[
        ~df_nppes_source_fuzzy_matched["npi"].isin(
            df_source_matched["npi"].tolist()
        )
    ]
    fname_source_fuzzy_matches = os.path.join(
        fqhc_lookup_folder, f"{source}_fuzzy_matches.pickle"
    )
    df_nppes_source_fuzzy_matched.to_pickle(fname_source_fuzzy_matches)
    logger.info(
        f"Less perfect matches were arrived at by filtering matches with >= 70 as the string distance score "
        f"for address and >= 80 as the string distance score for cities. Door number match requirement was "
        f"relaxed and matches that either meet this criteria or have >= 50 as the string distance score for "
        f"names were allowed in this set. {df_nppes_source_fuzzy_matched.hclinic_provider_id.nunique()} were "
        f"matched this way. "
        f"{df_nppes_source_fuzzy_matched[~df_nppes_source_fuzzy_matched['hclinic_provider_id'].isin(df_source_matched.hclinic_provider_id)].hclinic_provider_id.nunique()}"
        f" of these providers do not have perfect matches. Less perfect matches are saved at "
        f"{fname_source_fuzzy_matches}"
    )

    df_source_unmatched = df_source.loc[
        ~df_source["hclinic_provider_id"].isin(
            df_source_matched["hclinic_provider_id"]
        )
    ]

    # Merge source & NPPES data on provider id (with no leading zeros) and state columns
    logger.info(
        "Attempting provider id based matches after removing leading zeros, as some sites such as NBER clean "
        "them by removing leading zeros"
    )
    df_source_matched_no_zeros = df_npi_provider_cleaned.merge(
        df_source,
        left_on=["provider_id", "provider_id_state"],
        right_on=["hclinic_provider_id_no_leading_zero", "hclinic_state"],
        how="inner",
    ).compute()

    logger.info(
        f"{df_source_matched_no_zeros.hclinic_provider_id_cleaned.nunique()} HCLINIC providers were matched using "
        f"provider_id and provider_id_state columns (no leading zeros)"
    )
    df_source_matched_ploc_no_zeros = df_npi_provider_cleaned.merge(
        df_source,
        left_on=["provider_id", "p_loc_state"],
        right_on=["hclinic_provider_id_no_leading_zero", "hclinic_state"],
        how="inner",
    ).compute()
    logger.info(
        f"{df_source_matched_ploc_no_zeros.hclinic_provider_id_cleaned.nunique()} HCLINIC providers were matched using "
        f"provider_id and p_loc_state columns (no leading zeros)"
    )

    df_source_matched_mloc_no_zeros = df_npi_provider_cleaned.merge(
        df_source,
        left_on=["provider_id", "p_mail_state"],
        right_on=["hclinic_provider_id_no_leading_zero", "hclinic_state"],
        how="inner",
    ).compute()
    logger.info(
        f"{df_source_matched_mloc_no_zeros.hclinic_provider_id_cleaned.nunique()} HCLINIC providers were matched using "
        f"provider_id and m_loc_state columns (no leading zeros)"
    )

    df_source_matched_no_zeros = (
        pd.concat(
            [
                df_source_matched_no_zeros,
                df_source_matched_ploc_no_zeros,
                df_source_matched_mloc_no_zeros,
            ],
            ignore_index=True,
        )
        .reset_index()
        .drop_duplicates(keep="first")
        .reset_index(drop=True)
    )

    fname_nppes_matched_no_zeros = os.path.join(
        fqhc_lookup_folder,
        f"{source}_nppes_based_matches_no_leading_zeros.pickle",
    )
    df_source_matched_no_zeros.to_pickle(fname_nppes_matched_no_zeros)

    df_source_matched_no_zeros = process_address_columns(
        df_source_matched_no_zeros, logger_name
    )
    df_source_matched_no_zeros = compute_match_purity(
        df_source_matched_no_zeros
    )
    df_source_matched_no_zeros.to_pickle(fname_nppes_matched_no_zeros)
    logger.info(
        f"{df_source_matched_no_zeros.hclinic_provider_id_cleaned.nunique()} HCLINIC providers were matched using "
        f"NPPES (no leading zeros). These matches are saved at {fname_nppes_matched_no_zeros}"
    )
    df_source_matched = (
        pd.concat(
            [df_source_matched, df_source_matched_no_zeros], ignore_index=True
        )
        .sort_values(by=["hclinic_provider_id", "npi"])
        .drop_duplicates()
        .reset_index(drop=True)
    )
    fname_source_perfect_matches = os.path.join(
        fqhc_lookup_folder, f"{source}_perfect_matches.pickle"
    )
    df_source_matched.to_pickle(fname_source_perfect_matches)
    logger.info(
        f"In total, {df_source_matched.hclinic_provider_id.nunique()} of the "
        f"{df_source.hclinic_provider_id.nunique()} provider ids have perfect matches. All the perfect matches "
        f"are saved at {fname_source_perfect_matches}"
    )

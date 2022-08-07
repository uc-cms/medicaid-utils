import os
import logging
import pandas as pd
import dask.dataframe as dd
import requests
import re
from dask import delayed
import numpy as np
from datetime import datetime
from math import ceil
from fuzzywuzzy import process, fuzz
import usaddress
import shutil
from typing import List

from medicaid_utils.common_utils.usps_address import USPSAddress

nppes_lookup_folder = os.path.join(os.path.dirname(__file__), "data", "nppes")
uds_lookup_folder = os.path.join(os.path.dirname(__file__), "data", "uds")
fqhc_lookup_folder = os.path.join(os.path.dirname(__file__), "data", "fqhc")
zip_folder = os.path.join(os.path.dirname(__file__), "data", "zip")
uds_data_folder = "/gpfs/data/chin-lab/HRSA-FARA/Data/UDS"
pq_engine = 'pyarrow'


def combine_and_clean_uds_files(lst_year=None, logger_name="uds"):
    """Standardize UDS column names and flatten files"""
    logger = logging.getLogger(logger_name)
    dct_col_names = {
        "UDS Reporting Year": "reporting_year",
        "ReportingYear": "reporting_year",
        "UDS Number": "bhcmisid",
        "Grant Number": "grant_number",
        "Grantee Name": "hclinic_name",
        "GrantNumber": "grant_number",
        "GranteeName": "hclinic_name",
        "HealthCenterName": "hclinic_name",
        "Street_Address": "hclinic_streetaddress",
        "GranteeAddress": "hclinic_streetaddress",
        "GranteeOtherAddress": "hclinic_street_address_line_2",
        "HealthCenterStreetAddress": "hclinic_streetaddress",
        "HealthCenterOtherAddress": "hclinic_street_address_line_2",
        "City": "hclinic_city",
        "GranteeCity": "hclinic_city",
        "HealthCenterCity": "hclinic_city",
        "State": "hclinic_state",
        "GranteeState": "hclinic_state",
        "HealthCenterState": "hclinic_state",
        "Zip_Code": "hclinic_zipcode",
        "GranteeZipCode": "hclinic_zipcode",
        "HealthCenterZIPCode": "hclinic_zipcode",
        "Urban Flag": "hclinic_urban",
        "UrbanRuralFlag": "hclinic_urban",
        "Editor_Name": "editor_name",
        "ReviewerName": "editor_name",
        "UDS_Contact_Name": "uds_contact_name",
        "UDS_Contact_Phone": "uds_contact_phone",
        "Phone_Ext": "uds_phone_ext",
        "UDS_Contact_Email": "uds_contact_email",
        "UDSContactName": "uds_contact_name",
        "UDSContactPhone": "uds_contact_phone",
        "UDSContactPhoneExt": "uds_phone_ext",
        "UDSContactEmail": "uds_contact_email",
        "Funding_CH": "funding_CH",
        "Funding_MH": "funding_MH",
        "FundingCHC": "funding_CHC",
        "FundingMHC": "funding_MHC",
        "Funding_HCH": "funding_HCH",
        "Funding_PHPC": "funding_PHPC",
        "Funding_HO": "funding_HO",
        "Funding_PH": "funding_PH",
        "BHCMISID": "bhcmisid",
        "FundingCH": "funding_CH",
        "FundingMH": "funding_MH",
        "FundingHCH": "funding_HCH",
        "FundingPHPC": "funding_PHPC",
        "SiteName": "hclinic_site_name",
        "SiteStreetAddress": "hclinic_businessaddress",
        "SiteCity": "hclinic_businesscity",
        "SiteState": "hclinic_businessstate",
        "SiteZIPCode": "hclinic_businesszipcode",
        "MailingStreetAddress": "hclinic_mailstreetaddress",
        "MailingCity": "hclinic_mailcity",
        "MailingState": "hclinic_mailstate",
        "MailingZIPCode": "hclinic_mailzipcode",
        "SiteType": "hclinic_site_type",
        "SiteStatus": "hclinic_site_status",
        "LocationType": "hclinic_location_type",
        "LocationSetting": "hclinic_location_setting",
        "MedicaidNumber": "hclinic_provider_id",
        "MedicaidPharmNumber": "hclinic_rx_id",
    }
    dct_site_info = {
        2011: "GranteeSiteInfo",
        2012: "HealthCenterSiteInfo",
        2013: "HealthCenterSiteInfo",
        2014: "HealthCenterSiteInfo",
        2015: "HealthCenterSiteInfo",
        2016: "HealthCenterSiteInfo",
        2017: "HealthCenterSiteInfo",
        2018: "HealthCenterSiteInfo",
        2019: "HealthCenterSiteInfo",
    }
    dct_sheet_name = {
        2008: "Grantee Level Data",
        2009: "Grantee_Level_Data",
        2010: "Grantee_Level_Data",
        2011: "GranteeInfo",
        2012: "HealthCenterInfo",
        2013: "HealthCenterInfo",
        2014: "HealthCenterInfo",
        2015: "HealthCenterInfo",
        2016: "HealthCenterInfo",
        2017: "HealthCenterInfo",
        2018: "HealthCenterInfo",
        2019: "HealthCenterInfo",
    }
    dct_filename = {
        2008: "UDS_2008_DataDump_31Jul2009.xlsx",
        2009: "UDS_2009_DataDump_12May2010.xlsx",
        2010: "UDS_2010_DataDump_16Apr2014.xlsx",
        2011: "UDS_2011_DataDump_NCA_14May2012.xlsx",
        2012: "UDS_2012_DataDump_NCA_19Jul2013.xlsx",
        2013: "UDS_2013_DataDump_NCA_3Jul2014.xlsx",
        2014: "UDS2014DataDumpNCA29May2015.xlsx",
        2015: "UDS2015DataDumpNCA16Jun2016.xlsx",
        2016: "UDS2016DataDumpNCA15Jun2017.xlsx",
        2017: "UDS2017DataDumpNCA21Jun2018.xlsx",
        2018: "UDS 2018 Data Dump.xlsx",
    }
    pdf_uds = None
    if lst_year is None:
        lst_year = list(range(2008, 2019))
    for year in lst_year:
        logger.info(f"Processing {year} UDS dataset")
        df = pd.read_excel(
            os.path.join(uds_data_folder, dct_filename[year]),
            sheet_name=dct_sheet_name[year],
            dtype=object,
        )
        df = df.rename(columns=dct_col_names)
        if year in dct_site_info.keys():
            df_site = pd.read_excel(
                os.path.join(uds_data_folder, dct_filename[year]),
                sheet_name=dct_site_info[year],
                dtype=object,
            )
            df_site = df_site.rename(columns=dct_col_names)
            df = df.merge(df_site, on=["grant_number", "bhcmisid"], how="left")

        lst_columns = list(set(list(dct_col_names.values())))
        df = df.assign(
            **dict([(col, "") for col in lst_columns if col not in df.columns])
        )

        pdf_uds = (
            df[lst_columns]
            if pdf_uds is None
            else pd.concat([pdf_uds, df[lst_columns]], ignore_index=True)
        )

    pdf_uds = pdf_uds.assign(
        **dict(
            [
                (
                    col,
                    (
                        pdf_uds[col].astype(str).str.lower().str.strip()
                        == "true"
                    ).astype(int),
                )
                for col in pdf_uds.columns
                if col.startswith("funding_")
            ]
        )
    )
    pdf_uds = pdf_uds.assign(
        fqhc=pdf_uds[
            [col for col in pdf_uds.columns if col.startswith("funding_")]
        ]
        .any(axis="columns")
        .astype(int)
    )

    # Further standandardize column names, to match with NPPES & HCRIS column name format
    dct_col_names = {
        "hclinic_streetaddress": "hclinic_street_address",
        "funding_CH": "funding_ch",
        "funding_MH": "funding_mh",
        "funding_CHC": "funding_chc",
        "funding_MHC": "funding_mhc",
        "funding_HCH": "funding_hch",
        "funding_PHPC": "funding_phpc",
        "funding_HO": "funding_ho",
        "funding_PH": "funding_ph",
        "hclinic_businessaddress": "hclinic_business_address",
        "hclinic_businesscity": "hclinic_business_city",
        "hclinic_businessstate": "hclinic_business_state",
        "hclinic_businesszipcode": "hclinic_business_zipcode",
        "hclinic_mailstreetaddress": "hclinic_mail_address",
        "hclinic_mailcity": "hclinic_mail_city",
        "hclinic_mailstate": "hclinic_mail_state",
        "hclinic_mailzipcode": "hclinic_mail_zipcode",
    }
    pdf_uds = pdf_uds.rename(columns=dct_col_names)
    pdf_uds = pdf_uds.replace(to_replace=[None, "None"], value=np.nan).fillna(
        ""
    )
    pdf_uds = pdf_uds.assign(
        hclinic_provider_id_cleaned=pdf_uds["hclinic_provider_id"]
        .str.replace("[^a-zA-Z0-9]+", "", regex=True)
        .str.upper(),
        hclinic_provider_id_no_leading_zero=pdf_uds["hclinic_provider_id"]
        .str.replace("[^a-zA-Z0-9]+", "", regex=True)
        .str.upper()
        .str.lstrip("0"),
    )
    pdf_uds = pdf_uds.assign(
        **dict(
            [
                (col, pdf_uds[col].astype(str).str.strip())
                for col in [
                    "hclinic_name",
                    "hclinic_site_name",
                    "hclinic_provider_id",
                    "hclinic_provider_id_cleaned",
                    "hclinic_provider_id_no_leading_zero",
                    "hclinic_street_address",
                    "hclinic_street_address_line_2",
                    "hclinic_state",
                    "hclinic_city",
                    "hclinic_zipcode",
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
    fname_pickle = os.path.join(uds_lookup_folder, "uds_all_years.pickle")
    fname_csv = os.path.join(
        uds_lookup_folder, f"uds_{lst_year[0]}_{lst_year[-1]}.csv"
    )
    logger.info(
        f"{','.join([str(yr) for yr in lst_year])} UDS datasets were merged and cleaned, and is saved at "
        f"{fname_pickle} & {fname_csv}"
    )
    pdf_uds.to_csv(fname_csv, index=False)
    pdf_uds.to_pickle(fname_pickle)

    pdf_uds = pd.read_pickle(
        os.path.join(uds_lookup_folder, "uds_all_years.pickle")
    )
    pdf_uds = pdf_uds.assign(
        **dict(
            [
                (col, pdf_uds[col].astype(int))
                for col in ["reporting_year", "fqhc"]
            ]
        )
    )

    dct_fqhc_zips = {}
    for year in range(2008, 2019):
        dct_fqhc_zips[year] = (
            pdf_uds.loc[
                (pdf_uds.reporting_year <= year)
                & (pdf_uds.fqhc == 1)
                & (~pdf_uds.hclinic_zipcode.isna())
                & (pdf_uds.hclinic_zipcode.str.len() > 0)
            ]["hclinic_zipcode"]
            .astype(int)
            .astype(str)
            .str.zfill(5)
            .unique()
            .tolist()
        )

    # for year in dct_fqhc_zips:
    #     logger.info(
    #         f"Computing distance to nearest FQHC from all zipcodes for the year {year}"
    #     )
    #     pdf_zips_distances_filtered = get_closest_distance_to_target_locations(
    #         dct_fqhc_zips[year], logger_name
    #     )
    #     fname_filtered = os.path.join(
    #         zip_folder,
    #         f"zip_state_pcsa_ruca_zcta_census_with_fqhc_{year}_filtered.csv",
    #     )
    #
    #     pdf_zips_distances_filtered = pdf_zips_distances_filtered.rename(
    #         columns={"distance_to_target": "distance_to_fqhc"}
    #     )
    #     pdf_zips_distances_filtered.to_csv(fname_filtered, index=False)
    #     logger.info(
    #         f"Zipcodes with distances to nearest FQHCs are saved at {fname_filtered}"
    #     )


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
        "bhcmisid_x_npi": "bhcmisid_x_npi.pickle",
        "fqhc_x_npi": "fqhc_x_npi.pickle",
    }
    return dct_files


def clean_hclinic_name(hclinic_name):
    """Clean HCLINIC names by removing special characters and extra spaces"""
    return re.sub(" +", " ", re.sub("[^a-zA-Z0-9 ]+", "", hclinic_name))


def clean_zip(zipcode):
    """Remove hyphens in zipcodes"""
    return str(zipcode).replace("-", "")


def get_taxonomies(lstdct_tax):
    """Concat taxonomies"""
    return ",".join([dct_tax["code"] for dct_tax in lstdct_tax])


def filter_partial_matches(df, less_constrained=False):
    """Filter source x nppes matches that have some similarity between their names and address, with a distance score
    of at least 60"""
    df = df.replace(to_replace=[None, "None"], value=np.nan).fillna("")
    df = df.assign(
        **dict(
            [
                (col, df[col].astype(str).str.strip())
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
    if less_constrained:
        df = df.loc[
            (df[["name_score", "address_score"]] > 50).any(axis=1)
        ].compute()
    else:
        df = df.loc[
            (df[["name_score", "address_score"]] > 60).any(axis=1)
            & (df["city_score"] >= 70)
        ].compute()
    return df


def nppes_api_requests(pdf, source, logger_name, **dct_request_params):
    """Make NPPES API calls using hclinic name, city, state & zipcode in the request"""

    logger = logging.getLogger(logger_name)
    # NPI Lookup params
    npi_api = "https://npiregistry.cms.hhs.gov/api/?"
    dct_params = {"enumeration_type": "NPI-2", "version": 2.1, "limit": 100}

    # Using hclinic name, city, state and zipcode in the API call
    pdf = pdf.assign(
        cleaned_param_org_name=pdf[dct_request_params["organization_name"]]
        .apply(clean_hclinic_name)
        .str.strip(),
        cleaned_param_city=pdf[dct_request_params["city"]].str.strip(),
        cleaned_param_state=pdf[dct_request_params["state"]].str.strip(),
        cleaned_param_postal_code=pdf[dct_request_params["postal_code"]]
        .apply(clean_zip)
        .str.strip(),
    )

    df_api_based = dd.from_pandas(pdf, npartitions=30)
    while True:
        try:
            df_api_based = df_api_based.map_partitions(
                lambda pdf_partition: pdf_partition.assign(
                    npi_json=pdf_partition.apply(
                        lambda row: requests.get(
                            npi_api,
                            params={
                                **dct_params,
                                **{
                                    "organization_name": row[
                                        "cleaned_param_org_name"
                                    ],
                                    "city": row["cleaned_param_city"],
                                    "state": row["cleaned_param_state"],
                                    "postal_code": row[
                                        "cleaned_param_postal_code"
                                    ],
                                },
                            },
                        ).json()
                        if (
                            bool(row["cleaned_param_org_name"])
                            and bool(row["cleaned_param_city"])
                            and bool(row["cleaned_param_state"])
                            and bool(row["cleaned_param_postal_code"])
                        )
                        else '{"result_count":0, "results":[]}',
                        axis=1,
                    )
                )
            ).compute()
            break
        except Exception as ex:
            logger.info(ex)
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
        f"{df_api_based_matched['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
        f"HCLINIC providers were matched using NPPES API calls with {dct_request_params['organization_name']}, "
        f"{dct_request_params['city']},  {dct_request_params['state']} and {dct_request_params['postal_code']} "
    )

    # Using hclinic name, state and zipcode in the API call
    df_api_based_relaxed_city = dd.from_pandas(
        df_api_based_relaxed_city, npartitions=30
    )
    while True:
        try:
            df_api_based_relaxed_city = (
                df_api_based_relaxed_city.map_partitions(
                    lambda pdf_partition: pdf_partition.assign(
                        npi_json=pdf_partition.apply(
                            lambda row: requests.get(
                                npi_api,
                                params={
                                    **dct_params,
                                    **{
                                        "organization_name": row[
                                            "cleaned_param_org_name"
                                        ],
                                        "state": row["cleaned_param_state"],
                                        "postal_code": row[
                                            "cleaned_param_postal_code"
                                        ],
                                    },
                                },
                            ).json()
                            if (
                                bool(row["cleaned_param_org_name"])
                                and bool(row["cleaned_param_state"])
                                and bool(row["cleaned_param_postal_code"])
                            )
                            else '{"result_count":0, "results":[]}',
                            axis=1,
                        )
                    )
                ).compute()
            )
            break
        except Exception as ex:
            logger.info(ex)
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
        f"{df_api_based_relaxed_city['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
        f"HCLINIC providers were matched using NPPES API calls with {dct_request_params['organization_name']}, "
        f"{dct_request_params['state']} and {dct_request_params['postal_code']} "
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
            df_api_based_relaxed_name = (
                df_api_based_relaxed_name.map_partitions(
                    lambda pdf_partition: pdf_partition.assign(
                        npi_json=pdf_partition.apply(
                            lambda row: requests.get(
                                npi_api,
                                params={
                                    **dct_params,
                                    **{
                                        "organization_name": row[
                                            "cleaned_param_org_name"
                                        ]
                                        + "*",
                                        "state": row["cleaned_param_state"],
                                        "postal_code": row[
                                            "cleaned_param_postal_code"
                                        ],
                                    },
                                },
                            ).json()
                            if (
                                bool(row["cleaned_param_org_name"])
                                and bool(row["cleaned_param_state"])
                                and bool(row["cleaned_param_postal_code"])
                            )
                            else '{"result_count":0, "results":[]}',
                            axis=1,
                        )
                    )
                ).compute()
            )
            break
        except Exception as ex:
            logger.info(ex)
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
        f"{df_api_based_relaxed_name['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
        f"HCLINIC providers were matched using NPPES API calls with {dct_request_params['organization_name']} "
        f"(starting with), {dct_request_params['state']} and {dct_request_params['postal_code']}"
    )
    df_api_based_matched = pd.concat(
        [df_api_based_matched, df_api_based_relaxed_name], ignore_index=True
    ).reset_index(drop=True)
    df_api_based_matched = df_api_based_matched[
        df_api_based_matched.columns.difference(
            [
                "cleaned_param_org_name",
                "cleaned_param_city",
                "cleaned_param_state",
                "cleaned_param_postal_code",
            ]
        )
    ]
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
            process.extractOne(
                target_name,
                lst_name,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=30,
            )
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
                    scorer=fuzz.token_sort_ratio,
                    score_cutoff=30,
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
                target_address_full,
                lst_addresses,
                scorer=fuzz.ratio,
                score_cutoff=30,
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
                    target_address_full,
                    lst_other_address,
                    scorer=fuzz.ratio,
                    score_cutoff=30,
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
                    fuzz.token_sort_ratio(
                        p_org_name, hclinic_name, score_cutoff=30
                    ),
                    fuzz.token_sort_ratio(
                        p_org_name, hclinic_site_name, score_cutoff=30
                    ),
                ),
                0
                if (not bool(p_org_name_oth))
                else max(
                    fuzz.token_sort_ratio(
                        p_org_name_oth, hclinic_name, score_cutoff=30
                    ),
                    fuzz.token_sort_ratio(
                        p_org_name_oth, hclinic_site_name, score_cutoff=30
                    ),
                ),
                0
                if (not bool(p_loc_line_2))
                else max(
                    fuzz.token_sort_ratio(
                        p_loc_line_2, hclinic_name, score_cutoff=30
                    ),
                    fuzz.token_sort_ratio(
                        p_loc_line_2, hclinic_site_name, score_cutoff=30
                    ),
                ),
                0
                if (not bool(p_mail_line_2))
                else max(
                    fuzz.token_sort_ratio(
                        p_mail_line_2, hclinic_name, score_cutoff=30
                    ),
                    fuzz.token_sort_ratio(
                        p_mail_line_2, hclinic_site_name, score_cutoff=30
                    ),
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
                    fuzz.ratio(
                        p_loc_address, hclinic_address, score_cutoff=30
                    ),
                    fuzz.ratio(
                        p_loc_address,
                        hclinic_business_address,
                        score_cutoff=30,
                    ),
                    fuzz.ratio(
                        p_loc_address, hclinic_mail_address, score_cutoff=30
                    ),
                ),
                0
                if (not bool(p_mail_address))
                else max(
                    fuzz.ratio(
                        p_mail_address, hclinic_address, score_cutoff=30
                    ),
                    fuzz.ratio(
                        p_mail_address,
                        hclinic_business_address,
                        score_cutoff=30,
                    ),
                    fuzz.ratio(
                        p_mail_address, hclinic_mail_address, score_cutoff=30
                    ),
                ),
                0
                if (not bool(p_loc_line_1))
                else max(
                    fuzz.ratio(p_loc_line_1, hclinic_address, score_cutoff=30),
                    fuzz.ratio(
                        p_loc_line_1, hclinic_business_address, score_cutoff=30
                    ),
                    fuzz.ratio(
                        p_loc_line_1, hclinic_mail_address, score_cutoff=30
                    ),
                ),
                0
                if (not bool(p_mail_line_1))
                else max(
                    fuzz.ratio(
                        p_mail_line_1, hclinic_address, score_cutoff=30
                    ),
                    fuzz.ratio(
                        p_mail_line_1,
                        hclinic_business_address,
                        score_cutoff=30,
                    ),
                    fuzz.ratio(
                        p_mail_line_1, hclinic_mail_address, score_cutoff=30
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
                    fuzz.ratio(p_loc_city, hclinic_city, score_cutoff=30),
                    fuzz.ratio(
                        p_loc_city, hclinic_business_city, score_cutoff=30
                    ),
                    fuzz.ratio(p_loc_city, hclinic_mail_city, score_cutoff=30),
                ),
                0
                if not bool(p_mail_city)
                else max(
                    fuzz.ratio(p_mail_city, hclinic_city, score_cutoff=30),
                    fuzz.ratio(
                        p_mail_city, hclinic_business_city, score_cutoff=30
                    ),
                    fuzz.ratio(
                        p_mail_city, hclinic_mail_city, score_cutoff=30
                    ),
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
            .map_partitions(lambda pdf_partition: standardize_addresses(pdf_partition))
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
                    fuzz.token_sort_ratio(p_org_name, hclinic_name),
                    fuzz.token_sort_ratio(p_org_name, hclinic_site_name),
                ),
                0
                if (not bool(p_org_name_oth))
                else max(
                    fuzz.token_sort_ratio(p_org_name_oth, hclinic_name),
                    fuzz.token_sort_ratio(p_org_name_oth, hclinic_site_name),
                ),
                0
                if (not bool(p_loc_line_2))
                else max(
                    fuzz.token_sort_ratio(p_loc_line_2, hclinic_name),
                    fuzz.token_sort_ratio(p_loc_line_2, hclinic_site_name),
                ),
                0
                if (not bool(p_mail_line_2))
                else max(
                    fuzz.token_sort_ratio(p_mail_line_2, hclinic_name),
                    fuzz.token_sort_ratio(p_mail_line_2, hclinic_site_name),
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
    """Parse address components and get door number info"""

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
    df,
    df_npi_provider,
    source="hcris",
    logger_name="fuzzy_match",
    export_folder=fqhc_lookup_folder,
    chunk_size=300,
    less_constrained=False,
):
    """Perform string distance based matches between HCRIS, UDS and NPPES datasets.
    Create two sets of matches: perfect matches that meet strict criteria with respect to name & address similarity,
    and less perfect matches with more relaxed criteria."""
    logger = logging.getLogger(logger_name)
    logger.info(
        f"Attempting fuzzy text matching. Merging {source} dataset with NPPES using state columns"
    )
    dct_filenames = get_file_name_dict(source)
    df = df.sort_values(
        ["hclinic_provider_id_cleaned" if (source == "hcris") else "bhcmisid"]
    )
    df = df.reset_index(drop=True)

    list_df = [
        df[i : i + chunk_size] for i in range(0, df.shape[0], chunk_size)
    ]

    fname_fuzzy_source_nppes_merge = os.path.join(
        os.path.join(export_folder, dct_filenames["text_merged"])
    )
    if os.path.isfile(fname_fuzzy_source_nppes_merge):
        os.remove(fname_fuzzy_source_nppes_merge)
    for chunk_id, df_source in enumerate(list_df):
        logger.info(f"Processing chunk {chunk_id + 1} of {len(list_df)}")

        df_source_with_state = df_source.loc[
            df_source["hclinic_state"].str.strip().str.len() > 0
        ]

        if df_source_with_state.shape[0] > 0:
            df_nppes_source_p_id_state = filter_partial_matches(
                df_npi_provider.loc[
                    df_npi_provider["entity"].str.strip() == "2"
                ].merge(
                    df_source_with_state,
                    left_on=["provider_id_state"],
                    right_on=["hclinic_state"],
                    how="inner",
                )
            )

            if os.path.isfile(fname_fuzzy_source_nppes_merge):
                df_nppes_source_p_id_state = (
                    pd.concat(
                        [
                            pd.read_pickle(fname_fuzzy_source_nppes_merge),
                            df_nppes_source_p_id_state,
                        ],
                        ignore_index=True,
                    )
                    .drop_duplicates()
                    .reset_index(drop=True)
                )
            df_nppes_source_p_id_state.to_pickle(
                fname_fuzzy_source_nppes_merge
            )
            del df_nppes_source_p_id_state

            df_nppes_source_p_loc_state = filter_partial_matches(
                df_npi_provider.loc[
                    (df_npi_provider["entity"].str.strip() == "2")
                    & (
                        df_npi_provider["provider_id_state"]
                        != df_npi_provider["p_loc_state"]
                    )
                ].merge(
                    df_source_with_state,
                    left_on=["p_loc_state"],
                    right_on=["hclinic_state"],
                    how="inner",
                )
            )

            if os.path.isfile(fname_fuzzy_source_nppes_merge):
                df_nppes_source_p_loc_state = (
                    pd.concat(
                        [
                            pd.read_pickle(fname_fuzzy_source_nppes_merge),
                            df_nppes_source_p_loc_state,
                        ],
                        ignore_index=True,
                    )
                    .drop_duplicates()
                    .reset_index(drop=True)
                )
            df_nppes_source_p_loc_state.to_pickle(
                fname_fuzzy_source_nppes_merge
            )
            del df_nppes_source_p_loc_state

            df_nppes_source_p_mail_state = filter_partial_matches(
                df_npi_provider.loc[
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
                    df_source_with_state,
                    left_on=["p_mail_state"],
                    right_on=["hclinic_state"],
                    how="inner",
                )
            )

            if os.path.isfile(fname_fuzzy_source_nppes_merge):
                df_nppes_source_p_mail_state = (
                    pd.concat(
                        [
                            pd.read_pickle(fname_fuzzy_source_nppes_merge),
                            df_nppes_source_p_mail_state,
                        ],
                        ignore_index=True,
                    )
                    .drop_duplicates()
                    .reset_index(drop=True)
                )
            df_nppes_source_p_mail_state.to_pickle(
                fname_fuzzy_source_nppes_merge
            )
            del df_nppes_source_p_mail_state
            del df_source_with_state

        if source == "uds":
            df_source_with_business_state = df_source.loc[
                df_source["hclinic_business_state"].str.strip().str.len() > 0
            ]
            if df_source_with_business_state.shape[0] > 0:
                df_nppes_source_business_p_id_state = filter_partial_matches(
                    df_npi_provider.loc[
                        df_npi_provider["entity"].str.strip() == "2"
                    ].merge(
                        df_source_with_business_state,
                        left_on=["provider_id_state"],
                        right_on=["hclinic_business_state"],
                        how="inner",
                    )
                )

                if os.path.isfile(fname_fuzzy_source_nppes_merge):
                    df_nppes_source_business_p_id_state = (
                        pd.concat(
                            [
                                pd.read_pickle(fname_fuzzy_source_nppes_merge),
                                df_nppes_source_business_p_id_state,
                            ],
                            ignore_index=True,
                        )
                        .drop_duplicates()
                        .reset_index(drop=True)
                    )
                df_nppes_source_business_p_id_state.to_pickle(
                    fname_fuzzy_source_nppes_merge
                )
                del df_nppes_source_business_p_id_state

                df_nppes_source_business_p_loc_state = filter_partial_matches(
                    df_npi_provider.loc[
                        (df_npi_provider["entity"].str.strip() == "2")
                        & (
                            df_npi_provider["provider_id_state"]
                            != df_npi_provider["p_loc_state"]
                        )
                    ].merge(
                        df_source_with_business_state,
                        left_on=["p_loc_state"],
                        right_on=["hclinic_business_state"],
                        how="inner",
                    )
                )

                if os.path.isfile(fname_fuzzy_source_nppes_merge):
                    df_nppes_source_business_p_loc_state = (
                        pd.concat(
                            [
                                pd.read_pickle(fname_fuzzy_source_nppes_merge),
                                df_nppes_source_business_p_loc_state,
                            ],
                            ignore_index=True,
                        )
                        .drop_duplicates()
                        .reset_index(drop=True)
                    )
                df_nppes_source_business_p_loc_state.to_pickle(
                    fname_fuzzy_source_nppes_merge
                )
                del df_nppes_source_business_p_loc_state

                df_nppes_source_business_p_mail_state = filter_partial_matches(
                    df_npi_provider.loc[
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
                        df_source_with_business_state,
                        left_on=["p_mail_state"],
                        right_on=["hclinic_business_state"],
                        how="inner",
                    )
                )

                if os.path.isfile(fname_fuzzy_source_nppes_merge):
                    df_nppes_source_business_p_mail_state = (
                        pd.concat(
                            [
                                pd.read_pickle(fname_fuzzy_source_nppes_merge),
                                df_nppes_source_business_p_mail_state,
                            ],
                            ignore_index=True,
                        )
                        .drop_duplicates()
                        .reset_index(drop=True)
                    )
                df_nppes_source_business_p_mail_state.to_pickle(
                    fname_fuzzy_source_nppes_merge
                )
                del df_nppes_source_business_p_mail_state
                del df_source_with_business_state

            df_source_with_mail_state = df_source.loc[
                df_source["hclinic_mail_state"].str.strip().str.len() > 0
            ]
            if df_source_with_mail_state.shape[0] > 0:
                df_nppes_source_mail_p_id_state = filter_partial_matches(
                    df_npi_provider.loc[
                        df_npi_provider["entity"].str.strip() == "2"
                    ].merge(
                        df_source_with_mail_state,
                        left_on=["provider_id_state"],
                        right_on=["hclinic_mail_state"],
                        how="inner",
                    )
                )
                if os.path.isfile(fname_fuzzy_source_nppes_merge):
                    df_nppes_source_mail_p_id_state = (
                        pd.concat(
                            [
                                pd.read_pickle(fname_fuzzy_source_nppes_merge),
                                df_nppes_source_mail_p_id_state,
                            ],
                            ignore_index=True,
                        )
                        .drop_duplicates()
                        .reset_index(drop=True)
                    )
                df_nppes_source_mail_p_id_state.to_pickle(
                    fname_fuzzy_source_nppes_merge
                )
                del df_nppes_source_mail_p_id_state

                df_nppes_source_mail_p_loc_state = filter_partial_matches(
                    df_npi_provider.loc[
                        (df_npi_provider["entity"].str.strip() == "2")
                        & (
                            df_npi_provider["provider_id_state"]
                            != df_npi_provider["p_loc_state"]
                        )
                    ].merge(
                        df_source_with_mail_state,
                        left_on=["p_loc_state"],
                        right_on=["hclinic_mail_state"],
                        how="inner",
                    )
                )

                if os.path.isfile(fname_fuzzy_source_nppes_merge):
                    df_nppes_source_mail_p_loc_state = (
                        pd.concat(
                            [
                                pd.read_pickle(fname_fuzzy_source_nppes_merge),
                                df_nppes_source_mail_p_loc_state,
                            ],
                            ignore_index=True,
                        )
                        .drop_duplicates()
                        .reset_index(drop=True)
                    )
                df_nppes_source_mail_p_loc_state.to_pickle(
                    fname_fuzzy_source_nppes_merge
                )
                del df_nppes_source_mail_p_loc_state

                df_nppes_source_mail_p_mail_state = filter_partial_matches(
                    df_npi_provider.loc[
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
                        df_source_with_mail_state,
                        left_on=["p_mail_state"],
                        right_on=["hclinic_mail_state"],
                        how="inner",
                    )
                )

                if os.path.isfile(fname_fuzzy_source_nppes_merge):
                    df_nppes_source_mail_p_mail_state = (
                        pd.concat(
                            [
                                pd.read_pickle(fname_fuzzy_source_nppes_merge),
                                df_nppes_source_mail_p_mail_state,
                            ],
                            ignore_index=True,
                        )
                        .drop_duplicates()
                        .reset_index(drop=True)
                    )
                df_nppes_source_mail_p_mail_state.to_pickle(
                    fname_fuzzy_source_nppes_merge
                )
                del df_nppes_source_mail_p_mail_state
                del df_source_with_mail_state

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
                export_folder, dct_filenames["text_merged_with_match_purity"]
            )
            logger.info(
                f"Computing match purity for the {df_nppes_source_fuzzy_matched.shape[0]} text-based matches. This is "
                f"saved at  {fname_fuzzy_source_nppes_merge_with_match_purity}"
            )
            df_nppes_source_fuzzy_matched.to_pickle(
                fname_fuzzy_source_nppes_merge_with_match_purity
            )

            if less_constrained:
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
                        & (df_nppes_source_fuzzy_matched["name_score"] >= 50)
                    ]
                )
                df_nppes_source_name_matched = (
                    df_nppes_source_fuzzy_matched.loc[
                        (df_nppes_source_fuzzy_matched["name_score"] >= 70)
                    ]
                )
                logger.info(
                    f"Perfect address matches were arrived at by filtering matches with >= 70 as the string distance "
                    f"score for street addresses and >=50 as the string distance score for names. "
                    f"{df_nppes_source_address_matched['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
                    f"were matched this way."
                )

                logger.info(
                    f"Perfect name matches were arrived at by filtering matches with >= 70 as the string distance "
                    f"score for names. "
                    f"{df_nppes_source_name_matched['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
                    f"were matched this way."
                )
            else:
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
                df_nppes_source_name_matched = (
                    df_nppes_source_fuzzy_matched.loc[
                        (df_nppes_source_fuzzy_matched["city_score"] >= 80)
                        & (df_nppes_source_fuzzy_matched["name_score"] >= 80)
                    ]
                )

                logger.info(
                    f"Perfect address matches were arrived at by filtering matches with >= 70 as the string distance "
                    f"score for street addresses, 100% matching door & street numbers, >=50 as the string distance "
                    f"score for names and >= 80 as the string distance score for cities. "
                    f"{df_nppes_source_address_matched['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
                    f"were matched this way."
                )

                logger.info(
                    f"Perfect name matches were arrived at by filtering matches with >= 80 as the string distance "
                    f"score for names and >= 80 as the string distance score for cities."
                    f"{df_nppes_source_name_matched['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
                    f"were matched this way."
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

            if less_constrained:
                df_nppes_source_fuzzy_matched = (
                    df_nppes_source_fuzzy_matched.loc[
                        (
                            df_nppes_source_fuzzy_matched[
                                [
                                    "standardized_street_address_score",
                                    "address_score",
                                    "name_score",
                                ]
                            ]
                            >= 60
                        ).any(axis=1)
                    ]
                )
            else:
                df_nppes_source_fuzzy_matched = (
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
                        & (
                            (
                                df_nppes_source_fuzzy_matched["door_no_match"]
                                == 1
                            )
                            | (
                                df_nppes_source_fuzzy_matched["name_score"]
                                >= 50
                            )
                        )
                        & (df_nppes_source_fuzzy_matched["city_score"] >= 80)
                    ]
                )

            return df_text_based_perfect_matches, df_nppes_source_fuzzy_matched


def create_npi_fqhc_crosswalk(
    source: str = "hcris",
    year: int = None,
    logger_name: str = "fqhc_crosswalk",
):
    """Merge HCRIS, UDS and NPPES and datasets to create FQHC NPI list"""

    logger = logging.getLogger(logger_name)
    export_folder = fqhc_lookup_folder
    dct_filenames = get_file_name_dict(source)
    # Load all available years of hcris datasets
    if not os.path.exists(
        os.path.join(uds_lookup_folder, dct_filenames["uds"])
    ):
        logger.info(
            "Cleaning and combining all available years of UDS datasets"
        )
        combine_and_clean_uds_files(logger_name=logger_name)
    df_source = pd.read_pickle(
        os.path.join(uds_lookup_folder, dct_filenames["uds"])
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
    if source == "uds":
        if year is not None:
            logger.info(f"Fetching NPIs for FQHCs in {year} {source} file")
            df_source = df_source.loc[
                df_source["reporting_year"].astype(int) == year
            ]
            export_folder = os.path.join(fqhc_lookup_folder, str(year))
        df_source = df_source.loc[df_source["fqhc"].astype(int) == 1]
        df_source = df_source.assign(hclinic_po_box="")
        df_source_with_site = df_source.loc[
            (df_source["hclinic_site_name"].str.len() > 0)
        ]
        df_source_with_address = df_source.loc[
            (df_source["hclinic_name"].str.len() > 0)
            & (df_source["hclinic_city"].str.len() > 0)
            & (df_source["hclinic_state"].str.len() > 0)
            & (df_source["hclinic_zipcode"].str.len() > 0)
        ]
        df_source_with_mail_address = df_source.loc[
            (df_source["hclinic_name"].str.len() > 0)
            & (df_source["hclinic_mail_city"].str.len() > 0)
            & (df_source["hclinic_mail_state"].str.len() > 0)
            & (df_source["hclinic_mail_zipcode"].str.len() > 0)
        ]
        df_source_with_business_address = df_source.loc[
            (df_source["hclinic_name"].str.len() > 0)
            & (df_source["hclinic_business_city"].str.len() > 0)
            & (df_source["hclinic_business_state"].str.len() > 0)
            & (df_source["hclinic_business_zipcode"].str.len() > 0)
        ]
        df_source_with_site_and_address = df_source.loc[
            (df_source["hclinic_site_name"].str.len() > 0)
            & (df_source["hclinic_city"].str.len() > 0)
            & (df_source["hclinic_state"].str.len() > 0)
            & (df_source["hclinic_zipcode"].str.len() > 0)
        ]
        df_source_with_site_and_mail_address = df_source.loc[
            (df_source["hclinic_site_name"].str.len() > 0)
            & (df_source["hclinic_mail_city"].str.len() > 0)
            & (df_source["hclinic_mail_state"].str.len() > 0)
            & (df_source["hclinic_mail_zipcode"].str.len() > 0)
        ]
        df_source_with_site_and_business_address = df_source.loc[
            (df_source["hclinic_site_name"].str.len() > 0)
            & (df_source["hclinic_business_city"].str.len() > 0)
            & (df_source["hclinic_business_state"].str.len() > 0)
            & (df_source["hclinic_business_zipcode"].str.len() > 0)
        ]

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

    if not ((source == "uds") and (year == 2013)):
        # # Load NPI data for all available years
        # df_npi = dd.concat([dd.read_parquet(os.path.join(nppes_lookup_folder,
        #                                                  str(yr),
        #                                                  'npi_cleaned_parquet'),
        #                                     engine=pq_engine,
        #                                     index=False) for yr in range(2009, 2022)])

        # Merge source & NPPES data on provider id and state columns
        logger.info(
            "Merging source & NPPES data on provider id and state columns"
        )
        df_source_matched = df_npi_provider.merge(
            df_source.loc[
                (df_source["hclinic_provider_id_cleaned"].str.len() > 0)
                & (df_source["hclinic_state"].str.len() > 0)
            ],
            left_on=["provider_id", "provider_id_state"],
            right_on=["hclinic_provider_id_cleaned", "hclinic_state"],
            how="inner",
        ).compute()

        if source == "uds":
            logger.info(
                f"{df_source_matched['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
                f"HCLINIC providers were matched using hclinic_state, and nppes provider_id and provider_id_state "
                f"columns"
            )
            df_source_matched_business_state = df_npi_provider.merge(
                df_source.loc[
                    (
                        df_source["hclinic_state"]
                        != df_source["hclinic_business_state"]
                    )
                    & (df_source["hclinic_provider_id_cleaned"].str.len() > 0)
                    & (df_source["hclinic_business_state"].str.len() > 0)
                ],
                left_on=["provider_id", "provider_id_state"],
                right_on=[
                    "hclinic_provider_id_cleaned",
                    "hclinic_business_state",
                ],
                how="inner",
            ).compute()
            logger.info(
                f"{df_source_matched_business_state['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
                f"HCLINIC providers were matched using hclinic_business_state, and nppes provider_id and "
                f"provider_id_state columns,"
                f"{df_source_matched_business_state.loc[~df_source_matched_business_state['hclinic_provider_id_cleaned' if (source=='hcris') else 'bhcmisid'].isin(df_source_matched['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'])]['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()}"
                f"of which were not previously matched"
            )
            df_source_matched_mail_state = df_npi_provider.merge(
                df_source.loc[
                    (
                        df_source["hclinic_state"]
                        != df_source["hclinic_mail_state"]
                    )
                    & (
                        df_source["hclinic_business_state"]
                        != df_source["hclinic_mail_state"]
                    )
                    & (df_source["hclinic_provider_id_cleaned"].str.len() > 0)
                    & (df_source["hclinic_mail_state"].str.len() > 0)
                ],
                left_on=["provider_id", "provider_id_state"],
                right_on=["hclinic_provider_id_cleaned", "hclinic_mail_state"],
                how="inner",
            ).compute()
            logger.info(
                f"{df_source_matched_mail_state['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
                f"HCLINIC providers were matched using hclinic_mail_state, and nppes provider_id and provider_id_state "
                f"columns, "
                f"{df_source_matched_mail_state.loc[~df_source_matched_mail_state['hclinic_provider_id_cleaned' if (source=='hcris') else 'bhcmisid'].isin(df_source_matched['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'])]['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()}"
                f"of which were not previously matched"
            )
            df_source_matched = (
                pd.concat(
                    [
                        df_source_matched,
                        df_source_matched_business_state,
                        df_source_matched_mail_state,
                    ],
                    ignore_index=True,
                )
                .sort_values(["hclinic_provider_id_cleaned", "npi"])
                .drop_duplicates()
                .reset_index(drop=True)
            )

        logger.info(
            f"{df_source_matched['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
            f"HCLINIC providers were matched using provider_id and provider_id_state columns"
        )
        df_source_matched_ploc = df_npi_provider.merge(
            df_source.loc[
                (df_source["hclinic_provider_id_cleaned"].str.len() > 0)
                & (df_source["hclinic_state"].str.len() > 0)
            ],
            left_on=["provider_id", "p_loc_state"],
            right_on=["hclinic_provider_id_cleaned", "hclinic_state"],
            how="inner",
        ).compute()
        if source == "uds":
            logger.info(
                f"{df_source_matched_ploc['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
                f"HCLINIC providers were matched using hclinic_state, and nppes provider_id and p_loc_state columns"
            )
            df_source_matched_business_state = df_npi_provider.merge(
                df_source.loc[
                    (
                        df_source["hclinic_state"]
                        != df_source["hclinic_business_state"]
                    )
                    & (df_source["hclinic_provider_id_cleaned"].str.len() > 0)
                    & (df_source["hclinic_business_state"].str.len() > 0)
                ],
                left_on=["provider_id", "p_loc_state"],
                right_on=[
                    "hclinic_provider_id_cleaned",
                    "hclinic_business_state",
                ],
                how="inner",
            ).compute()
            logger.info(
                f"{df_source_matched_business_state['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
                f"HCLINIC providers were matched using hclinic_business_state, and nppes provider_id and p_loc_state "
                f"columns, "
                f"{df_source_matched_business_state.loc[~df_source_matched_business_state['hclinic_provider_id_cleaned' if (source=='hcris') else 'bhcmisid'].isin(df_source_matched_ploc['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'])]['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()}"
                f"of which were not previously matched"
            )
            df_source_matched_mail_state = df_npi_provider.merge(
                df_source.loc[
                    (
                        df_source["hclinic_state"]
                        != df_source["hclinic_mail_state"]
                    )
                    & (
                        df_source["hclinic_business_state"]
                        != df_source["hclinic_mail_state"]
                    )
                    & (df_source["hclinic_provider_id_cleaned"].str.len() > 0)
                    & (df_source["hclinic_mail_state"].str.len() > 0)
                ],
                left_on=["provider_id", "p_loc_state"],
                right_on=["hclinic_provider_id_cleaned", "hclinic_mail_state"],
                how="inner",
            ).compute()
            logger.info(
                f"{df_source_matched_mail_state['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
                f"HCLINIC providers were matched using hclinic_mail_state, and nppes provider_id and "
                f"p_loc_state columns, "
                f"{df_source_matched_mail_state.loc[~df_source_matched_mail_state['hclinic_provider_id_cleaned' if (source=='hcris') else 'bhcmisid'].isin(df_source_matched_ploc['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'])]['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()}"
                f"of which were not previously matched"
            )
            df_source_matched_ploc = (
                pd.concat(
                    [
                        df_source_matched_ploc,
                        df_source_matched_business_state,
                        df_source_matched_mail_state,
                    ],
                    ignore_index=True,
                )
                .sort_values(["hclinic_provider_id_cleaned", "npi"])
                .drop_duplicates()
                .reset_index(drop=True)
            )
        logger.info(
            f"{df_source_matched_ploc['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
            f"HCLINIC providers were matched using provider_id and p_loc_state columns"
        )

        df_source_matched_mloc = df_npi_provider.merge(
            df_source.loc[
                (df_source["hclinic_provider_id_cleaned"].str.len() > 0)
                & (df_source["hclinic_state"].str.len() > 0)
            ],
            left_on=["provider_id", "p_mail_state"],
            right_on=["hclinic_provider_id_cleaned", "hclinic_state"],
            how="inner",
        ).compute()

        if source == "uds":
            logger.info(
                f"{df_source_matched_mloc['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
                f"HCLINIC providers were matched using hclinic_state, and nppes provider_id and p_mail_state columns"
            )
            df_source_matched_business_state = df_npi_provider.merge(
                df_source.loc[
                    (
                        df_source["hclinic_state"]
                        != df_source["hclinic_business_state"]
                    )
                    & (df_source["hclinic_provider_id_cleaned"].str.len() > 0)
                    & (df_source["hclinic_business_state"].str.len() > 0)
                ],
                left_on=["provider_id", "p_mail_state"],
                right_on=[
                    "hclinic_provider_id_cleaned",
                    "hclinic_business_state",
                ],
                how="inner",
            ).compute()
            logger.info(
                f"{df_source_matched_business_state['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
                f"HCLINIC providers were matched using hclinic_business_state, and nppes provider_id and p_mail_state "
                f"columns, "
                f"{df_source_matched_business_state.loc[~df_source_matched_business_state['hclinic_provider_id_cleaned' if (source=='hcris') else 'bhcmisid'].isin(df_source_matched_mloc['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'])]['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()}"
                f"of which were not previously matched"
            )
            df_source_matched_mail_state = df_npi_provider.merge(
                df_source.loc[
                    (
                        df_source["hclinic_state"]
                        != df_source["hclinic_mail_state"]
                    )
                    & (
                        df_source["hclinic_business_state"]
                        != df_source["hclinic_mail_state"]
                    )
                    & (df_source["hclinic_provider_id_cleaned"].str.len() > 0)
                    & (df_source["hclinic_mail_state"].str.len() > 0)
                ],
                left_on=["provider_id", "p_mail_state"],
                right_on=["hclinic_provider_id_cleaned", "hclinic_mail_state"],
                how="inner",
            ).compute()
            logger.info(
                f"{df_source_matched_mail_state['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
                f"HCLINIC providers were matched using hclinic_mail_state, and nppes provider_id and p_mail_state "
                f"columns, "
                f"{df_source_matched_mail_state.loc[~df_source_matched_mail_state['hclinic_provider_id_cleaned' if (source=='hcris') else 'bhcmisid'].isin(df_source_matched_mloc['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'])]['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()}"
                f"of which were not previously matched"
            )
            df_source_matched_mloc = (
                pd.concat(
                    [
                        df_source_matched_mloc,
                        df_source_matched_business_state,
                        df_source_matched_mail_state,
                    ],
                    ignore_index=True,
                )
                .sort_values(["hclinic_provider_id_cleaned", "npi"])
                .drop_duplicates()
                .reset_index(drop=True)
            )
        logger.info(
            f"{df_source_matched_mloc['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
            f"HCLINIC providers were matched using provider_id and m_loc_state columns"
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
            export_folder, dct_filenames["nppes_matches"]
        )
        logger.info(
            f"In total, {df_source_matched['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
            f"HCLINIC providers were matched using provider_id and state columns from NPPES. These matched are saved "
            f"at {fname_nppes_state_matched}"
        )

        df_source_matched.to_pickle(fname_nppes_state_matched)

        # Using NPPES API
        # Using hclinic name, city, state and zipcode in the API call
        df_api_based_matched = (
            nppes_api_requests(
                df_source if source == "hcris" else df_source_with_address,
                source,
                logger_name,
                **{
                    "organization_name": "hclinic_name",
                    "city": "hclinic_city",
                    "state": "hclinic_state",
                    "postal_code": "hclinic_zipcode",
                },
            )
            if df_source_with_address.shape[0] > 0
            else pd.DataFrame()
        )

        if source == "uds":
            # Using HCLINIC name, business city, state and zip code
            df_api_based_business_matched = (
                nppes_api_requests(
                    df_source_with_business_address,
                    source,
                    logger_name,
                    **{
                        "organization_name": "hclinic_name",
                        "city": "hclinic_business_city",
                        "state": "hclinic_business_state",
                        "postal_code": "hclinic_business_zipcode",
                    },
                )
                if df_source_with_business_address.shape[0] > 0
                else pd.DataFrame()
            )

            # Using HCLINIC name, mail city, state and zip code
            df_api_based_mail_matched = (
                nppes_api_requests(
                    df_source_with_mail_address,
                    source,
                    logger_name,
                    **{
                        "organization_name": "hclinic_name",
                        "city": "hclinic_mail_city",
                        "state": "hclinic_mail_state",
                        "postal_code": "hclinic_mail_zipcode",
                    },
                )
                if df_source_with_mail_address.shape[0] > 0
                else pd.DataFrame()
            )

            # Using HCLINIC site name, city, state and zip code
            df_api_based_site_matched = (
                nppes_api_requests(
                    df_source_with_site_and_address,
                    source,
                    logger_name,
                    **{
                        "organization_name": "hclinic_site_name",
                        "city": "hclinic_city",
                        "state": "hclinic_state",
                        "postal_code": "hclinic_zipcode",
                    },
                )
                if df_source_with_site_and_address.shape[0] > 0
                else pd.DataFrame()
            )

            # Using HCLINIC site name, business city, state and zip code
            df_api_based_site_business_matched = (
                nppes_api_requests(
                    df_source_with_site_and_business_address,
                    source,
                    logger_name,
                    **{
                        "organization_name": "hclinic_site_name",
                        "city": "hclinic_business_city",
                        "state": "hclinic_business_state",
                        "postal_code": "hclinic_business_zipcode",
                    },
                )
                if df_source_with_site_and_business_address.shape[0] > 0
                else pd.DataFrame()
            )

            # Using HCLINIC site name, mail city, state and zip code
            df_api_based_site_mail_matched = (
                nppes_api_requests(
                    df_source_with_site_and_mail_address,
                    source,
                    logger_name,
                    **{
                        "organization_name": "hclinic_site_name",
                        "city": "hclinic_mail_city",
                        "state": "hclinic_mail_state",
                        "postal_code": "hclinic_mail_zipcode",
                    },
                )
                if df_source_with_site_and_mail_address.shape[0] > 0
                else pd.DataFrame()
            )

            lst_df = [
                df
                for df in [
                    df_api_based_matched,
                    df_api_based_business_matched,
                    df_api_based_mail_matched,
                    df_api_based_site_matched,
                    df_api_based_site_business_matched,
                    df_api_based_site_mail_matched,
                ]
                if df.shape[0] > 0
            ]

            if len(lst_df) > 0:
                df_api_based_matched = (
                    pd.concat(lst_df, ignore_index=True)
                    .sort_values(
                        [
                            "hclinic_provider_id_cleaned"
                            if (source == "hcris")
                            else "bhcmisid"
                        ]
                    )
                    .reset_index(drop=True)
                )
        if df_api_based_matched.shape[0] > 0:
            logger.info(
                f"In total, {df_api_based_matched['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
                f"HCLINIC providers were matched using NPPES API calls"
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
            df_api_based_matched_flattened = (
                df_api_based_matched_flattened.loc[
                    (df_api_based_matched_flattened["name_score"] >= 70)
                    | (
                        (
                            df_api_based_matched_flattened[
                                [
                                    "address_score",
                                    "standardized_street_address_score",
                                ]
                            ]
                            >= 70
                        ).any(axis=1)
                        & (
                            df_api_based_matched_flattened["door_no_match"]
                            == 1
                        )
                    )
                ]
            )
            logger.info(
                f"{df_api_based_matched['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} HCLINIC"
                f" providers were matched using API calls after removing less clean matches (name score >70 or "
                f"(address score >= 70 and same door number))"
            )
            fname_api_based_matches = os.path.join(
                export_folder, dct_filenames["api_matches"]
            )
            logger.info(
                f"API based matches are saved at {fname_api_based_matches}"
            )

            df_api_based_matched_flattened.to_pickle(fname_api_based_matches)
            df_api_based_matched_flattened = (
                df_api_based_matched_flattened.drop(
                    ["npi_json", "result_count", "results"], axis=1
                )
            )
            df_api_based_matched_flattened["entity"] = "2"

            df_source_matched = (
                pd.concat(
                    [df_api_based_matched_flattened, df_source_matched],
                    ignore_index=True,
                )
                .sort_values(by=["hclinic_provider_id", "npi"])
                .reset_index(drop=True)
            )

            fname_api_and_nppes_perfect_matches = os.path.join(
                export_folder, dct_filenames["api_and_nppes_matches"]
            )
            logger.info(
                f"In total, {df_source_matched['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
                f"HCLINIC providers were matched using exports and NPPES API calls. These matches are saved at "
                f"{fname_api_and_nppes_perfect_matches}"
            )

            df_source_matched.to_pickle(fname_api_and_nppes_perfect_matches)

        df_unmatched = df_source.loc[
            ~df_source[
                "hclinic_provider_id_cleaned"
                if (source == "hcris")
                else "bhcmisid"
            ].isin(
                df_source_matched[
                    "hclinic_provider_id_cleaned"
                    if (source == "hcris")
                    else "bhcmisid"
                ]
            )
        ]

        # Attempting match with NPPES export with only provider_id
        logger.info("Attempting match with NPPES export with only provider_id")
        df_state_relaxed = df_npi_provider.merge(
            df_unmatched.loc[
                df_unmatched["hclinic_provider_id"].str.len() > 0
            ],
            left_on=["provider_id"],
            right_on=["hclinic_provider_id"],
            how="inner",
        ).compute()

        if df_state_relaxed.shape[0] > 0:
            logger.info(
                "Cleaning & standarding address in state relaxed matches"
            )
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
                f"{df_state_relaxed['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
                f"hclinic provider ids were matched in this step"
            )

            df_source_matched = (
                pd.concat(
                    [df_source_matched, df_state_relaxed], ignore_index=True
                )
                .sort_values(by=["hclinic_provider_id", "npi"])
                .reset_index(drop=True)
            )
            fname_with_state_relaxed = os.path.join(
                export_folder, dct_filenames["api_nppes_state_relaxed_matches"]
            )
            logger.info(
                f"In total {df_source_matched['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
                f"hclinic provider ids were matched as of this step. All matches till this step are saved "
                f"at {fname_with_state_relaxed}"
            )
            df_source_matched.to_pickle(fname_with_state_relaxed)
    else:
        df_source_matched = pd.read_pickle(
            os.path.join(
                export_folder, dct_filenames["api_nppes_state_relaxed_matches"]
            )
        )
    df_unmatched = df_source.loc[
        ~df_source[
            "hclinic_provider_id_cleaned"
            if (source == "hcris")
            else "bhcmisid"
        ].isin(
            df_source_matched[
                "hclinic_provider_id_cleaned"
                if (source == "hcris")
                else "bhcmisid"
            ]
        )
    ]
    logger.info(
        f"{df_unmatched['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} are yet to be "
        f"matched"
    )

    df_text_based_perfect_matches, df_nppes_source_fuzzy_matched = fuzzy_match(
        df_source,
        df_npi_provider,
        source,
        logger_name,
        export_folder,
        chunk_size=ceil(df_source.shape[0] / 10),
    )
    fname_source_text_based_perfect_matches = os.path.join(
        export_folder, dct_filenames["text_matches"]
    )
    logger.info(
        f"In total, perfect matches for "
        f"{df_text_based_perfect_matches['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
        f"provider ids were arrived at using text distance metrics. These matches are saved at "
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
        export_folder, dct_filenames["perfect_matches"]
    )
    df_source_matched.to_pickle(fname_source_perfect_matches)
    logger.info(
        f"In total, {df_source_matched['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()}"
        f" of the {df_source['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
        f"provider ids have perfect matches. All the perfect matches are saved at {fname_source_perfect_matches}"
    )

    df_nppes_source_fuzzy_matched = df_nppes_source_fuzzy_matched[
        ~df_nppes_source_fuzzy_matched["npi"].isin(
            df_source_matched["npi"].tolist()
        )
    ]
    fname_source_fuzzy_matches = os.path.join(
        export_folder, dct_filenames["fuzzy_matches"]
    )
    df_nppes_source_fuzzy_matched.to_pickle(fname_source_fuzzy_matches)
    logger.info(
        f"Less perfect matches were arrived at by filtering matches with >= 70 as the string distance score "
        f"for address and >= 80 as the string distance score for cities. Door number match requirement was "
        f"relaxed and matches that either meet this criteria or have >= 50 as the string distance score for "
        f"names were allowed in this set. "
        f"{df_nppes_source_fuzzy_matched['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
        f"were matched this way. "
        f"{df_nppes_source_fuzzy_matched[~df_nppes_source_fuzzy_matched['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].isin(df_source_matched['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'])]['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()}"
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
        df_source.loc[
            (df_source["hclinic_provider_id_no_leading_zero"].str.len() > 0)
            & (df_source["hclinic_state"].str.len() > 0)
        ],
        left_on=["provider_id", "provider_id_state"],
        right_on=["hclinic_provider_id_no_leading_zero", "hclinic_state"],
        how="inner",
    ).compute()
    if source == "uds":
        logger.info(
            f"{df_source_matched_no_zeros['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
            f"HCLINIC providers were matched using hclinic_state, and nppes provider_id and provider_id_state "
            f"columns (no leading zeros)"
        )
        df_source_matched_business_state_no_zeros = (
            df_npi_provider_cleaned.merge(
                df_source.loc[
                    (
                        df_source["hclinic_state"]
                        != df_source["hclinic_business_state"]
                    )
                    & (
                        df_source[
                            "hclinic_provider_id_no_leading_zero"
                        ].str.len()
                        > 0
                    )
                    & (df_source["hclinic_business_state"].str.len() > 0)
                ],
                left_on=["provider_id", "provider_id_state"],
                right_on=[
                    "hclinic_provider_id_no_leading_zero",
                    "hclinic_business_state",
                ],
                how="inner",
            ).compute()
        )
        logger.info(
            f"{df_source_matched_business_state_no_zeros['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
            f"HCLINIC providers were matched using hclinic_business_state, and nppes provider_id and provider_id_state "
            f"columns (no leading zeros)"
        )
        df_source_matched_mail_state_no_zeros = df_npi_provider_cleaned.merge(
            df_source.loc[
                (df_source["hclinic_state"] != df_source["hclinic_mail_state"])
                & (
                    df_source["hclinic_business_state"]
                    != df_source["hclinic_mail_state"]
                )
                & (
                    df_source["hclinic_provider_id_no_leading_zero"].str.len()
                    > 0
                )
                & (df_source["hclinic_mail_state"].str.len() > 0)
            ],
            left_on=["provider_id", "provider_id_state"],
            right_on=[
                "hclinic_provider_id_no_leading_zero",
                "hclinic_mail_state",
            ],
            how="inner",
        ).compute()
        logger.info(
            f"{df_source_matched_mail_state_no_zeros['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
            f"HCLINIC providers were matched using hclinic_mail_state, and nppes provider_id and provider_id_state "
            f"columns (no leading zeros)"
        )
        df_source_matched_no_zeros = (
            pd.concat(
                [
                    df_source_matched_no_zeros,
                    df_source_matched_business_state_no_zeros,
                    df_source_matched_mail_state_no_zeros,
                ],
                ignore_index=True,
            )
            .sort_values(["hclinic_provider_id_no_leading_zero", "npi"])
            .drop_duplicates()
            .reset_index(drop=True)
        )

    logger.info(
        f"{df_source_matched_no_zeros['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
        f"HCLINIC providers were matched using provider_id and provider_id_state columns (no leading zeros)"
    )
    df_source_matched_ploc_no_zeros = df_npi_provider_cleaned.merge(
        df_source.loc[
            (df_source["hclinic_provider_id_no_leading_zero"].str.len() > 0)
            & (df_source["hclinic_state"].str.len() > 0)
        ],
        left_on=["provider_id", "p_loc_state"],
        right_on=["hclinic_provider_id_no_leading_zero", "hclinic_state"],
        how="inner",
    ).compute()
    if source == "uds":
        logger.info(
            f"{df_source_matched_ploc_no_zeros['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
            f"HCLINIC providers were matched using hclinic_state, and nppes provider_id and p_loc_state columns (no "
            f"leading zeros)"
        )
        df_source_matched_business_state_no_zeros = (
            df_npi_provider_cleaned.merge(
                df_source.loc[
                    (
                        df_source["hclinic_state"]
                        != df_source["hclinic_business_state"]
                    )
                    & (
                        df_source[
                            "hclinic_provider_id_no_leading_zero"
                        ].str.len()
                        > 0
                    )
                    & (df_source["hclinic_business_state"].str.len() > 0)
                ],
                left_on=["provider_id", "p_loc_state"],
                right_on=[
                    "hclinic_provider_id_no_leading_zero",
                    "hclinic_business_state",
                ],
                how="inner",
            ).compute()
        )
        logger.info(
            f"{df_source_matched_business_state_no_zeros['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
            f"HCLINIC providers were matched using hclinic_business_state, and nppes provider_id and p_loc_state "
            f"columns (no leading zeros)"
        )
        df_source_matched_mail_state_no_zeros = df_npi_provider_cleaned.merge(
            df_source.loc[
                (df_source["hclinic_state"] != df_source["hclinic_mail_state"])
                & (
                    df_source["hclinic_business_state"]
                    != df_source["hclinic_mail_state"]
                )
                & (
                    df_source["hclinic_provider_id_no_leading_zero"].str.len()
                    > 0
                )
                & (df_source["hclinic_mail_state"].str.len() > 0)
            ],
            left_on=["provider_id", "p_loc_state"],
            right_on=[
                "hclinic_provider_id_no_leading_zero",
                "hclinic_mail_state",
            ],
            how="inner",
        ).compute()
        logger.info(
            f"{df_source_matched_mail_state_no_zeros['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
            f"HCLINIC providers were matched using hclinic_mail_state, and nppes provider_id and p_loc_state columns "
            f"(no leading zeros)"
        )
        df_source_matched_ploc_no_zeros = (
            pd.concat(
                [
                    df_source_matched_ploc_no_zeros,
                    df_source_matched_business_state_no_zeros,
                    df_source_matched_mail_state_no_zeros,
                ],
                ignore_index=True,
            )
            .sort_values(["hclinic_provider_id_no_leading_zero", "npi"])
            .drop_duplicates()
            .reset_index(drop=True)
        )
    logger.info(
        f"{df_source_matched_ploc_no_zeros['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()}"
        f" HCLINIC providers were matched using provider_id and p_loc_state columns (no leading zeros)"
    )

    df_source_matched_mloc_no_zeros = df_npi_provider_cleaned.merge(
        df_source.loc[
            (df_source["hclinic_provider_id_no_leading_zero"].str.len() > 0)
            & (df_source["hclinic_state"].str.len() > 0)
        ],
        left_on=["provider_id", "p_mail_state"],
        right_on=["hclinic_provider_id_no_leading_zero", "hclinic_state"],
        how="inner",
    ).compute()
    if source == "uds":
        logger.info(
            f"{df_source_matched_mloc_no_zeros['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
            f"HCLINIC providers were matched using hclinic_state, and nppes provider_id and p_mail_state columns"
            f" (no leading zeros)"
        )
        df_source_matched_business_state_no_zeros = (
            df_npi_provider_cleaned.merge(
                df_source.loc[
                    (
                        df_source["hclinic_state"]
                        != df_source["hclinic_business_state"]
                    )
                    & (
                        df_source[
                            "hclinic_provider_id_no_leading_zero"
                        ].str.len()
                        > 0
                    )
                    & (df_source["hclinic_business_state"].str.len() > 0)
                ],
                left_on=["provider_id", "p_mail_state"],
                right_on=[
                    "hclinic_provider_id_no_leading_zero",
                    "hclinic_business_state",
                ],
                how="inner",
            ).compute()
        )
        logger.info(
            f"{df_source_matched_business_state_no_zeros['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
            f"HCLINIC providers were matched using hclinic_business_state, and nppes provider_id and p_mail_state "
            f"columns (no leading zeros)"
        )
        df_source_matched_mail_state_no_zeros = df_npi_provider_cleaned.merge(
            df_source.loc[
                (df_source["hclinic_state"] != df_source["hclinic_mail_state"])
                & (
                    df_source["hclinic_business_state"]
                    != df_source["hclinic_mail_state"]
                )
                & (
                    df_source["hclinic_provider_id_no_leading_zero"].str.len()
                    > 0
                )
                & (df_source["hclinic_mail_state"].str.len() > 0)
            ],
            left_on=["provider_id", "p_mail_state"],
            right_on=[
                "hclinic_provider_id_no_leading_zero",
                "hclinic_mail_state",
            ],
            how="inner",
        ).compute()
        logger.info(
            f"{df_source_matched_mail_state_no_zeros['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
            f"HCLINIC providers were matched using hclinic_mail_state, and nppes provider_id and p_mail_state columns "
            f"(no leading zeros)"
        )
        df_source_matched_mloc_no_zeros = (
            pd.concat(
                [
                    df_source_matched_mloc_no_zeros,
                    df_source_matched_business_state_no_zeros,
                    df_source_matched_mail_state_no_zeros,
                ],
                ignore_index=True,
            )
            .sort_values(["hclinic_provider_id_no_leading_zero", "npi"])
            .drop_duplicates()
            .reset_index(drop=True)
        )
    logger.info(
        f"{df_source_matched_mloc_no_zeros['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} "
        f"HCLINIC providers were matched using provider_id and m_loc_state columns (no leading zeros)"
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

    if df_source_matched_no_zeros.shape[0] > 0:
        fname_nppes_matched_no_zeros = os.path.join(
            export_folder, dct_filenames["no_leading_zeros_matches"]
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
            f"{df_source_matched_no_zeros['hclinic_provider_id_cleaned' if (source == 'hcris') else 'bhcmisid'].nunique()} HCLINIC "
            f"providers were matched using NPPES (no leading zeros). These matches are saved at "
            f"{fname_nppes_matched_no_zeros}"
        )
        df_source_matched = (
            pd.concat(
                [df_source_matched, df_source_matched_no_zeros],
                ignore_index=True,
            )
            .sort_values(by=["hclinic_provider_id", "npi"])
            .drop_duplicates()
            .reset_index(drop=True)
        )
        fname_source_perfect_matches = os.path.join(
            export_folder, dct_filenames["perfect_matches"]
        )
        df_source_matched.to_pickle(fname_source_perfect_matches)
        logger.info(
            f"In total, {df_source_matched['hclinic_provider_id' if (source == 'hcris') else 'bhcmisid'].nunique()} "
            f"of the {df_source['hclinic_provider_id' if (source == 'hcris') else 'bhcmisid'].nunique()} provider "
            f"ids have perfect matches. All the perfect matches are saved at {fname_source_perfect_matches}"
        )


def combine_uds_fqhc_npi_crosswalks(lst_year, logger_name, source="uds"):
    """Combine UDS x NPPES matches from all years"""
    logger = logging.getLogger(logger_name)
    dct_file_names = get_file_name_dict(source)
    for fname in dct_file_names:
        logger.info(f"Merging {source} {fname}")
        lst_df = [
            pd.read_pickle(
                os.path.join(
                    fqhc_lookup_folder, str(year), dct_file_names[fname]
                )
            )
            for year in lst_year
            if os.path.isfile(
                os.path.join(
                    fqhc_lookup_folder, str(year), dct_file_names[fname]
                )
            )
        ]
        if len(lst_df) > 0:
            df = (
                pd.concat(lst_df, ignore_index=True)
                .sort_values(
                    [
                        "hclinic_provider_id_cleaned"
                        if (source == "hcris")
                        else "bhcmisid",
                        "npi",
                    ]
                )
                .drop_duplicates()
                .reset_index(drop=True)
            )
            df.to_pickle(
                os.path.join(fqhc_lookup_folder, dct_file_names[fname])
            )
        else:
            logger.info(f"No {source} matches of type {fname} were found")
    df_perfect = pd.read_pickle(
        os.path.join(fqhc_lookup_folder, dct_file_names["perfect_matches"])
    )
    df_fuzzy = pd.read_pickle(
        os.path.join(fqhc_lookup_folder, dct_file_names["fuzzy_matches"])
    )

    df_fuzzy = df_fuzzy[~df_fuzzy["npi"].isin(df_perfect["npi"].tolist())]
    df_fuzzy.reset_index(drop=True).to_pickle(
        os.path.join(fqhc_lookup_folder, dct_file_names["fuzzy_matches"])
    )


def generate_bhcmisid_fqhc_crosswalk(
    lst_perfect_npi, lst_fuzzy_npi, logger_name
):
    """Map all identified NPIs to BHCMISID to filter FQHCs by years they were in FQHC status"""
    logger = logging.getLogger(logger_name)
    dct_filenames = get_file_name_dict("uds")
    df_uds = pd.read_pickle(dct_filenames["uds"])
    df_uds = df_uds.assign(
        **dict(
            [(col, df_uds.astype(int)) for col in ["reporting_year", "fqhc"]]
        )
    )
    df_uds = df_uds.loc[df_uds["fqhc"] == 1]

    lst_year = list(range(2009, datetime.now().year + 1))

    df_perfect_matches = pd.read_pickle(
        os.path.join(fqhc_lookup_folder, dct_filenames["perfect_matches"])
    )
    df_fuzzy_matches = pd.read_pickle(
        os.path.join(fqhc_lookup_folder, dct_filenames["fuzzy_matches"])
    )

    df_bhcmisid_x_npi = (
        pd.concat(
            [
                df_perfect_matches[["npi", "bhcmisid"]].drop_duplicates(),
                df_fuzzy_matches[["npi", "bhcmisid"]].drop_duplicates(),
            ],
            ignore_index=True,
        )
        .drop_duplicates()
        .reset_index(drop=True)
    )

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
    df_npi_provider = df_npi_provider.loc[
        (~df_npi_provider["npi"].isin(df_bhcmisid_x_npi["npi"]))
        & df_npi_provider["npi"].isin(lst_perfect_npi + lst_fuzzy_npi)
    ].compute()

    df_npi_provider = dd.from_pandas(
        df_npi_provider, npartitions=ceil(df_npi_provider.shape[0] / 5000)
    )

    export_folder = os.path.join(fqhc_lookup_folder, "bhcmisid_x_npi")
    df_text_based_perfect_matches, df_nppes_source_fuzzy_matched = fuzzy_match(
        df_uds,
        df_npi_provider,
        "uds",
        logger_name,
        export_folder,
        chunk_size=700,
        less_constrained=True,
    )

    df_nppes_source_fuzzy_matched = df_nppes_source_fuzzy_matched.loc[
        ~df_nppes_source_fuzzy_matched.npi.isin(
            df_text_based_perfect_matches.npi
        )
    ]

    df_bhcmisid_x_npi_matched = (
        pd.concat(
            [
                df_text_based_perfect_matches[
                    ["npi", "bhcmisid"]
                ].drop_duplicates(),
                df_nppes_source_fuzzy_matched[
                    ["npi", "bhcmisid"]
                ].drop_duplicates(),
            ],
            ignore_index=True,
        )
        .drop_duplicates()
        .reset_index(drop=True)
    )

    logger.info(
        f"{df_bhcmisid_x_npi_matched.npi.nunique()} NPIs were matched with BHCMISIDs with a fresh text based "
        f"match with relaxed constraints. (>=70 as the distance score for any of name and address columns). "
        f"{df_bhcmisid_x_npi_matched.npi.nunique() + df_bhcmisid_x_npi.nunique()} of the "
        f"{len(lst_perfect_npi) + len(lst_fuzzy_npi)} NPIs were matched to BHCMISIDs."
    )

    pd.concat(
        [df_bhcmisid_x_npi_matched, df_bhcmisid_x_npi], ignore_index=True
    ).sort_values(["bhcmisid", "npi"]).drop_duplicates().reset_index(
        drop=True
    ).to_pickle(
        os.path.join(fqhc_lookup_folder, dct_filenames["bhcmisid_x_npi"])
    )


def generate_and_combine_fqhc_npi_crosswalks(logger_name):
    """Get NPIs for FQHCs in HCRIS, UDS datasets. Combine this with NPIs with FQHC taxonomy/ FQHC range CCN
    in NPPES dataset. Merge UDS with all the identified NPIs to generate BHMISID x FQHC crosswalk"""
    logger = logging.getLogger(logger_name)
    logger.info("Generate list of NPIs with FQHC range CCN in NPPES dataset")
    generate_oscar_fqhc_npis()

    logger.info("Get NPIs for FQHCs in HCRIS")
    create_npi_fqhc_crosswalk(source="hcris", logger_name=logger_name)

    logger.info("Get NPIs for FQHCs in UDS")
    for year in range(2009, 2019):
        os.makedirs(os.path.join(fqhc_lookup_folder, str(year)), exist_ok=True)
        create_npi_fqhc_crosswalk(
            source="uds", logger_name=logger_name, year=year
        )

    combine_uds_fqhc_npi_crosswalks(range(2009, 2019), logger_name)

    df_perfect_matches = (
        pd.concat(
            [
                pd.read_pickle(
                    os.path.join(
                        fqhc_lookup_folder,
                        get_file_name_dict(source)["perfect_matches"],
                    )
                )
                for source in ["uds", "hcris"]
            ],
            ignore_index=True,
        )
        .drop_duplicates()
        .sort_values(["npi", "hclinic_provider_id_cleaned"])
        .reset_index(drop=True)
    )
    df_ccn_fqhcs = pd.read_pickle(
        os.path.join(nppes_lookup_folder, "nppes_fqhc_range_npis.pickle")
    )

    df_fuzzy_matches = (
        pd.concat(
            [
                pd.read_pickle(
                    os.path.join(
                        fqhc_lookup_folder,
                        get_file_name_dict(source)["fuzzy_matches"],
                    )
                )
                for source in ["uds", "hcris"]
            ],
            ignore_index=True,
        )
        .drop_duplicates()
        .sort_values(["npi", "hclinic_provider_id_cleaned"])
        .reset_index(drop=True)
    )

    lst_perfect_npi = list(
        set(
            df_perfect_matches["npi"].astype(str).str.strip().tolist()
            + df_ccn_fqhcs["npi"].astype(str).str.strip().tolist()
        )
    )

    df_fuzzy_matches = df_fuzzy_matches.loc[
        ~df_fuzzy_matches["npi"].astype(str).str.strip().isin(lst_perfect_npi)
    ]

    lst_fuzzy_npi = (
        df_fuzzy_matches.npi.astype(str).str.strip().unique().tolist()
    )

    generate_bhcmisid_fqhc_crosswalk(
        lst_perfect_npi, lst_fuzzy_npi, logger_name
    )

    df_uds = pd.read_pickle(get_file_name_dict("uds")["uds"])
    df_uds = df_uds.assign(
        **dict(
            [(col, df_uds.astype(int)) for col in ["reporting_year", "fqhc"]]
        )
    )
    df_uds = df_uds.loc[df_uds["fqhc"] == 1]

    df_uds = (
        df_uds.groupby("bhcmisid")["reporting_year"]
        .min()
        .reset_index(drop=False)
    )
    df_uds = df_uds.rename(columns={"reporting_year": "start_year"})

    df_fqhc_npi = pd.DataFrame(
        [
            {"npi": npi, "perfect_fqhc": 1, "fuzzy_fqhc": 0}
            for npi in lst_perfect_npi
        ]
        + [
            {"npi": npi, "perfect_fqhc": 0, "fuzzy_fqhc": 1}
            for npi in lst_fuzzy_npi
        ]
    )
    df_bhcmisid_x_npi = pd.read_pickle(
        os.path.join(
            fqhc_lookup_folder, get_file_name_dict("uds")["bhcmisid_x_npi"]
        )
    )

    df_bhcmisid_x_npi = df_bhcmisid_x_npi.merge(
        df_uds, on="bhcmisid", how="left"
    )
    df_fqhc_npi = df_fqhc_npi.merge(df_bhcmisid_x_npi, on="npi", how="left")

    df_fqhc_npi = df_fqhc_npi.assign(
        start_year=df_fqhc_npi["start_year"].astype("Int64")
    )
    df_fqhc_npi = df_fqhc_npi.sort_values(["npi", "bhcmisid"]).reset_index(
        drop=True
    )
    df_fqhc_npi.to_pickle(
        os.path.join(
            fqhc_lookup_folder, get_file_name_dict("uds")["fqhc_x_npi"]
        )
    )

    df_fqhc_npi.to_csv(
        os.path.join(fqhc_lookup_folder, "fqhc_x_npi.csv"), index=False
    )

    logger.info(
        f"NPI X FQHC croswalk is saved at {get_file_name_dict('uds')['fqhc_x_npi']} and "
        "{os.path.join(fqhc_lookup_folder, 'fqhc_x_npi.csv')}"
    )

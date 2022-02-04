import os
import pandas as pd
import dask.dataframe as dd
from dask import delayed
import numpy as np
from math import ceil
import shutil
from typing import List

uds_lookup_folder = os.path.join(os.path.dirname(__file__), "data", "nppes")
zip_folder = os.path.join(os.path.dirname(__file__), "data", "zip")
uds_data_folder = "/gpfs/data/chin-lab/HRSA-FARA/Data/UDS"


def clean_uds_files(lst_year):
    """Standardize UDS column names and flatten files"""
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
        "HealthCenterStreetAddress": "hclinic_streetaddress",
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

    for year in lst_year:
        df = pd.read_excel(
            os.path.join(uds_data_folder, dct_filename[year]),
            sheet_name=dct_sheet_name[year],
            dtype=object,
        )
        df = df.rename(
            columns=dict(
                [
                    (col, dct_col_names[col])
                    for col in dct_col_names.keys()
                    if col in df.columns
                ]
            )
        )
        if year in dct_site_info.keys():
            df_site = pd.read_excel(
                os.path.join(uds_data_folder, dct_filename[year]),
                sheet_name=dct_site_info[year],
                dtype=object,
            )
            df_site = df_site.rename(
                columns=dict(
                    [
                        (col, dct_col_names[col])
                        for col in dct_col_names.keys()
                        if col in df_site.columns
                    ]
                )
            )
            df = df.merge(df_site, on=["grant_number", "bhcmisid"], how="left")
        lst_columns = list(set(list(dct_col_names.values())))
        for col in lst_columns:
            if col not in df.columns:
                df[col] = np.nan
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
                            pdf_uds[col]
                            .fillna("False")
                            .astype(str)
                            .str.lower()
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
                [col for col in pdf_uds if col.startswith("funding_")]
            ]
            .any(axis="columns")
            .astype(int)
        )

        pdf_uds.to_csv(
            os.path.join(
                uds_lookup_folder, f"uds_{lst_year[0]}_{lst_year[-1]}.csv"
            ),
            index=False,
        )
        pdf_uds.to_pickle(os.path.join(uds_lookup_folder, "uds_all.pickle"))

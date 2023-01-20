"""This module has functions that can be used to identify OUD related
medications and behavioral health service claims"""
import os
import pandas as pd
import dask.dataframe as dd
from ...filters.claims import dx_and_proc, rx

data_folder = os.path.join(os.path.dirname(__file__), "data")


def flag_proc_buprenorphine(df_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds indicator column denoting presence of buprenorphine treatment
    procedure codes in claims.

    New Column(s):
    - proc_buprenorphine - integer column, 1 when claim has
    buprenorphine treatment procedure codes and 0 otherwise

    Parameters
    ----------
    df_claims : dd.DataFrame
            IP or OT claim file

    Returns
    -------
    dd.DataFrame

    """
    dct_proc_codes = {"buprenorphine": {7: ["J0571"]}}
    df_claims = dx_and_proc.flag_diagnoses_and_procedures(
        dct_proc_codes, {}, df_claims
    )
    return df_claims


def flag_rx_buprenorphine(df_rx_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds indicator column denoting presence of buprenorphine NDC codes in
    claims.

    New Column(s):
    - rx_buprenorphine - integer column, 1 when claim has buprenorphine
    NDC codes and 0 otherwise

    Parameters
    ----------
    df_claims : dd.DataFrame
            IP or OT claim file

    Returns
    -------
    dd.DataFrame

    """
    pdf_buprenorphine = pd.read_excel(
        os.path.join(data_folder, "rxmix_ndc_codes_cleaned_final.xlsx"),
        sheet_name="Buprenorphine",
        engine="openpyxl",
    )
    dct_ndc_codes = {
        "buprenorphine": pdf_buprenorphine["ndc"]
        .astype(str)
        .str.zfill(12)
        .unique()
        .tolist()
    }
    df_claims = rx.flag_prescriptions(dct_ndc_codes, df_rx_claims, True)
    return df_claims


def flag_proc_buprenorphine_naloxone(df_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds indicator column denoting presence of Buprenorphine/ Naloxone
    treatment
    procedure codes in claims.

    New Column(s):
    - proc_buprenorphine_naloxone - integer column, 1 when claim has
    Buprenorphine/ Naloxone treatment procedure codes and 0 otherwise

    Parameters
    ----------
    df_claims : dd.DataFrame
            IP or OT claim file

    Returns
    -------
    dd.DataFrame

    """
    dct_proc_codes = {
        "buprenorphine_naloxone": {7: ["J0572", "J0573", "J0574", "J0575"]}
    }
    df_claims = dx_and_proc.flag_diagnoses_and_procedures(
        dct_proc_codes, {}, df_claims
    )
    return df_claims


def flag_rx_buprenorphine_naloxone(df_rx_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds indicator column denoting presence of Buprenorphine/ Naloxone NDC
    codes in
    claims.

    New Column(s):
    - rx_buprenorphine_naloxone - integer column, 1 when claim has
    buprenorphine NDC codes and 0 otherwise

    Parameters
    ----------
    df_claims : dd.DataFrame
            IP or OT claim file

    Returns
    -------
    dd.DataFrame

    """
    pdf_buprenorphine = pd.read_excel(
        os.path.join(data_folder, "rxmix_ndc_codes_cleaned_final.xlsx"),
        sheet_name="Buprenorphine",
        engine="openpyxl",
    )
    pdf_naloxone = pd.read_excel(
        os.path.join(data_folder, "rxmix_ndc_codes_cleaned_final.xlsx"),
        sheet_name="Naloxone",
        dtype=object,
        engine="openpyxl",
    )
    dct_ndc_codes = {
        "buprenorphine_naloxone": pdf_buprenorphine["ndc"]
        .astype(str)
        .str.zfill(12)
        .unique()
        .tolist()
        + pdf_naloxone["ndc"].astype(str).str.zfill(12).unique().tolist(),
    }
    df_claims = rx.flag_prescriptions(dct_ndc_codes, df_rx_claims, True)
    return df_claims


def flag_proc_injectable_naltrexone(df_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds indicator column denoting presence of Injectable Naltrexone
    procedure codes in claims.

    New Column(s):
    - proc_injectable_naltrexone - integer column, 1 when claim has
    Injectable Naltrexone treatment procedure codes and 0 otherwise

    Parameters
    ----------
    df_claims : dd.DataFrame
            IP or OT claim file

    Returns
    -------
    dd.DataFrame

    """
    dct_proc_codes = {"injectable_naltrexone": {7: ["J2315"]}}
    df_claims = dx_and_proc.flag_diagnoses_and_procedures(
        dct_proc_codes, {}, df_claims
    )
    return df_claims


def flag_rx_oral_naltrexone(df_rx_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds indicator column denoting presence of Oral Naltrexone NDC
    codes in claims.

    New Column(s):
    - rx_oral_naltrexone - integer column, 1 when claim has
    Oral Naltrexone NDC codes and 0 otherwise

    Parameters
    ----------
    df_claims : dd.DataFrame
            IP or OT claim file

    Returns
    -------
    dd.DataFrame

    """
    pdf_naltrexone = pd.read_excel(
        os.path.join(data_folder, "rxmix_ndc_codes_cleaned_final.xlsx"),
        sheet_name="Naltrexone",
        dtype=object,
        engine="openpyxl",
    )
    dct_ndc_codes = {
        "oral_naltrexone": pdf_naltrexone["ndc"]
        .astype(str)
        .str.zfill(12)
        .unique()
        .tolist(),
    }
    df_claims = rx.flag_prescriptions(dct_ndc_codes, df_rx_claims, True)
    return df_claims


def flag_proc_methadone(df_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds indicator column denoting presence of Methadone
    procedure codes in claims.

    New Column(s):
    - proc_methadone - integer column, 1 when claim has
    Methadone treatment procedure codes and 0 otherwise

    Parameters
    ----------
    df_claims : dd.DataFrame
            IP or OT claim file

    Returns
    -------
    dd.DataFrame

    """
    dct_proc_codes = {"methadone": {7: ["H0020"]}}
    df_claims = dx_and_proc.flag_diagnoses_and_procedures(
        dct_proc_codes, {}, df_claims
    )
    return df_claims


def flag_rx_methadone(df_rx_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds indicator column denoting presence of Methadone NDC codes in claims.

    New Column(s):
    - rx_methadone_le_30mg - integer column, 1 when claim has
    Methadone NDC codes with dosage less than or equal to 30 mg and 0
    otherwise
    - rx_methadone_gt_30mg - integer column, 1 when claim has
    Methadone NDC codes with dosage greater than or equal to 30 mg and 0
    otherwise

    Parameters
    ----------
    df_claims : dd.DataFrame
            IP or OT claim file

    Returns
    -------
    dd.DataFrame

    """
    pdf_methadone = pd.read_excel(
        os.path.join(data_folder, "rxmix_ndc_codes_cleaned_final.xlsx"),
        sheet_name="Methadone",
        dtype=object,
        engine="openpyxl",
    )
    dct_ndc_codes = {
        "methadone_le_30mg": pdf_methadone.loc[
            pdf_methadone["rx_name"]
            .str.split("MG")
            .str[0]
            .str.split()
            .str[-1]
            .astype(int)
            <= 30
        ]["ndc"]
        .astype(str)
        .str.zfill(12)
        .unique()
        .tolist(),
        "methadone_gt_30mg": pdf_methadone.loc[
            pdf_methadone["rx_name"]
            .str.split("MG")
            .str[0]
            .str.split()
            .str[-1]
            .astype(int)
            > 30
        ]["ndc"]
        .astype(str)
        .str.zfill(12)
        .unique()
        .tolist(),
    }

    df_claims = rx.flag_prescriptions(dct_ndc_codes, df_rx_claims, True)
    return df_claims


def flag_proc_behavioral_health_trtmt(df_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds indicator column denoting presence of Behavioral Health treatment
    procedure codes in claims.

    New Column(s):
    - proc_behavioral_health_trtmt - integer column, 1 when claim has
    Behavioral Health treatment procedure codes and 0 otherwise

    Parameters
    ----------
    df_claims : dd.DataFrame
            IP or OT claim file

    Returns
    -------
    dd.DataFrame

    """
    dct_proc_codes = {
        "behavioral_health_trtmt": {
            7: [
                "90785",
                "90832",
                "90834",
                "90837",
                "90846",
                "90847",
                "90849",
                "90853",
                "H0001",
                "H0004",
                "H0005",
                "H0006",
                "H0007",
                "H0008",
                "H0010",
                "H0012",
                "H0013",
                "H0014",
                "H0015",
                "H0022",
                "H0028",
                "H0038",
                "H0047",
                "H0049",
                "H0050",
                "H2011",
                "H2019",
                "H2027",
                "H2034",
                "H2035",
                "H2036",
                "T1006",
                "T1007",
                "T1012",
                "90875",
            ]
        }
    }
    df_claims = dx_and_proc.flag_diagnoses_and_procedures(
        dct_proc_codes, {}, df_claims
    )
    return df_claims


def flag_rx_benzos_opioids(df_rx_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds indicator column denoting presence of Benzodiazepines/ Opioids NDC
    codes in claims.

    New Column(s):
    - rx_benzodiazepines - integer column, 1 when claim has
    Benzodiazepines NDC codes and 0 otherwise
    - rx_opioids - integer column, 1 when claim has Opioids NDC codes and 0
    otherwise

    Parameters
    ----------
    df_claims : dd.DataFrame
            IP or OT claim file

    Returns
    -------
    dd.DataFrame

    """
    pdf_opioids = pd.read_excel(
        os.path.join(data_folder, "benzos_opioids_ndc.xlsx"),
        sheet_name="Opioids",
        dtype=object,
        engine="openpyxl",
    )
    pdf_benzos = pd.read_excel(
        os.path.join(data_folder, "benzos_opioids_ndc.xlsx"),
        sheet_name="Benzos",
        dtype=object,
        engine="openpyxl",
    )
    dct_ndc_codes = {
        "opioids": pdf_opioids.loc[
            ~pdf_opioids.Drug.str.lower().str.strip().isin(["buprenorphine"])
        ]["NDC"]
        .astype(str)
        .str.strip()
        .str.zfill(12)
        .unique()
        .tolist(),
        "benzodiazepines": pdf_benzos["NDC"]
        .astype(str)
        .str.strip()
        .str.zfill(12)
        .unique()
        .tolist(),
    }

    df_claims = rx.flag_prescriptions(dct_ndc_codes, df_rx_claims, True)
    return df_claims

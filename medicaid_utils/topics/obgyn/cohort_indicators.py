"""This module has functions used to flag cohort designations used in OB/GYN
studies"""
import dask.dataframe as dd
import pandas as pd
import numpy as np
import os

data_folder = os.path.join(os.path.dirname(__file__), "data")


def flag_religious_npis(df_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds columns denoting categorization of NPIs in claims.

    New columns:
        - catholic_npi: 0 or 1, 1 when claim contains an NPI that is a catholic
          hospital.
        - religious_npi: 0 or 1, 1 when claim contains an NPI that is a
          hospital with any religious affiliation.
        - secular_npi: : 0 or 1, 1 when claim contains an NPI that is a
          hospital with no reglious affiliation.
        - rural_npi: 0 or 1, when claim contains an NPI that is located in a
          rural location

    Parameters
    ----------
    df_claims: dd.DataFrame
        Claims dataframe

    Returns
    -------
    dd.DataFrame

    """
    pdf_religious_aff = pd.read_excel(
        os.path.join(data_folder, "religious_provider_npis.xlsx"),
        dtype="object",
        engine="openpyxl",
    )
    pdf_religious_aff = pdf_religious_aff.loc[
        pdf_religious_aff["hosp_state"].str.strip() == "IL"
    ]
    pdf_religious_aff = pdf_religious_aff.assign(
        rel_aff=pd.to_numeric(
            pdf_religious_aff["rel_aff"], errors="coerce"
        ).astype(int),
        npi=pdf_religious_aff["npi"].astype(str).str.strip(),
    )
    lst_cath_npi = pdf_religious_aff.loc[
        pdf_religious_aff["rel_aff"] == 1
    ].npi.tolist()
    lst_religious_npi = pdf_religious_aff.loc[
        pdf_religious_aff["rel_aff"] == 2
    ].npi.tolist()
    lst_nonreligious_npi = pdf_religious_aff.loc[
        pdf_religious_aff["rel_aff"] == 3
    ].npi.tolist()

    df_claims = df_claims.map_partitions(
        lambda pdf: pdf.assign(
            catholic_npi=pdf["NPI"].str.strip().isin(lst_cath_npi).astype(int),
            religious_npi=pdf["NPI"]
            .str.strip()
            .isin(lst_religious_npi)
            .astype(int),
            secular_npi=pdf["NPI"]
            .str.strip()
            .isin(lst_nonreligious_npi)
            .astype(int),
            rural_npi=np.select(
                [
                    pdf["NPI"]
                    .str.strip()
                    .isin(
                        pdf_religious_aff.loc[
                            pdf_religious_aff["urbn_rrl"]
                            .str.strip()
                            .str.upper()
                            == "R"
                        ]["npi"].tolist()
                    ),
                    pdf["NPI"]
                    .str.strip()
                    .isin(
                        pdf_religious_aff.loc[
                            pdf_religious_aff["urbn_rrl"]
                            .str.strip()
                            .str.upper()
                            == "U"
                        ]["npi"].tolist()
                    ),
                ],
                [1, 0],
                default=-1,
            ),
        )
    )

    return df_claims


def flag_transfers(df_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds indicator columns denoting whether the claim has a discharge status
    indicating a transfer. Currently only supports MAX files.

    Parameters
    ----------
    df_claims: dd.DataFrame
        IP or LT claim file

    Returns
    -------
    dd.DataFrame

    """
    return df_claims.map_partitions(
        lambda pdf: pdf.assign(
            transfer=pd.to_numeric(pdf["PATIENT_STATUS_CD"], errors="coerce")
            .isin([2, 3, 4, 5, 61, 65, 66, 71])
            .astype(int)
        )
    )

import dask.dataframe as dd
import pandas as pd
import numpy as np
import os

data_folder = os.path.join(os.path.dirname(__file__), "data")


def flag_religious_npis(df_claims: dd.DataFrame) -> dd.DataFrame:
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
            catholic=pdf["NPI"].str.strip().isin(lst_cath_npi).astype(int),
            religious=pdf["NPI"]
            .str.strip()
            .isin(lst_religious_npi)
            .astype(int),
            secular=pdf["NPI"]
            .str.strip()
            .isin(lst_nonreligious_npi)
            .astype(int),
            hosp_rural=np.select(
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
    return df_claims.map_partitions(
        lambda pdf: pdf.assign(
            transfer=pd.to_numeric(pdf["PATIENT_STATUS_CD"], errors="coerce")
            .isin([2, 3, 4, 5, 61, 65, 66, 71])
            .astype(int)
        )
    )

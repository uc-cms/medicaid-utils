import dask.dataframe as dd
import pandas as pd
import os


def flag_religious_npis(df_claims: dd.DataFrame) -> dd.DataFrame:
    lst_cath_ein = pd.read_csv(os.path.join('data', "catholic.csv"),
                               skipinitialspace=True, na_filter=False,
                               engine='c', dtype='object', names=["EIN"]
                               )['EIN'].str.replace("[^a-zA-Z0-9]+", "").tolist()
    pdf_cath_npi = pd.read_csv(os.path.join('data', "catholic-npi.csv"),
                               skipinitialspace=True, na_filter=False, engine='c',
                               dtype='object', names=["NPI", "CODE"])
    pdf_cath_npi = pdf_cath_npi.assign(**dict([(col, pdf_cath_npi[col].str.replace("[^a-zA-Z0-9]+", ""))
                                               for col in ["NPI", "CODE"]]))
    lst_cath_npi = pdf_cath_npi.loc[pdf_cath_npi["CODE"] == "1"].NPI.tolist()
    lst_religious_npi = pdf_cath_npi.loc[pdf_cath_npi["CODE"] == "2"].NPI.tolist()
    lst_nonreligious_npi = pdf_cath_npi.loc[pdf_cath_npi["CODE"] == "3"].NPI.tolist()

    df_claims = df_claims.assign(catholic=(df_claims["NPI"].isin(lst_cath_npi) |
                                           df_claims["EIN"].isin(lst_cath_ein)).astype(int),
                                 religious=df_claims["NPI"].isin(lst_religious_npi).astype(int),
                                 secular=(df_claims["NPI"].isin(lst_nonreligious_npi) &
                                          (~df_claims["EIN"].isin(lst_cath_ein))).astype(int)
                                 )

    return(df_claims)

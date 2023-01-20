"""This module has functions to add NDC code based indicator flags to claims"""
import pandas as pd
import dask.dataframe as dd


def flag_prescriptions(
    dct_ndc_codes: dict,
    df_claims: dd.DataFrame,
    ignore_missing_days_supply: bool = False,
):
    """
    Flags claims based on NDC codes

    Parameters
    ----------
    dct_ndc_codes : dict
        Dictionary of NDC. Should be in the format
            {condition_name: list of codes}
            Eg: {'buprenorphine': ['00378451905', '00378451993',
            '00378617005', '00378617077']}
    df_claims : dd.DataFrame
        Claims dataframe
    ignore_missing_days_supply : bool, default=False
        Always flag claims with missing, negative or 0 days of supply as 0

    Returns
    -------
    dd.DataFrame

    """
    dct_ndc_codes = {
        condn: [
            str(ndc_code).replace(" ", "").zfill(12)
            for ndc_code in dct_ndc_codes[condn]
        ]
        for condn in dct_ndc_codes
    }
    df_claims = df_claims.map_partitions(
        lambda pdf: pdf.assign(
            **{
                f"rx_{condn}": (
                    pdf["NDC"].isin(dct_ndc_codes[condn])
                    & (
                        (ignore_missing_days_supply is False)
                        | pd.to_numeric(pdf["DAYS_SUPPLY"], errors="coerce")
                        > 0
                    )
                ).astype(int)
                for condn in dct_ndc_codes
            }
        )
    )
    return df_claims

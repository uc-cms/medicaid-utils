"""This module has functions that can be used to identify OUD care settings"""
import dask.dataframe as dd


def flag_care_settings(df_ot: dd.DataFrame):
    """Creates claim level flags for FQHC, Outpatient Hospital, Physician
    Office, Behavioral Health Centers, Hospital & Office-based care
    settings.

    Parameters
    ----------
    df_ot : dd.DataFrame
        OT header claim dataframe

    Returns
    -------
    dd.DataFrame

    References
    -----------
    - `Gertner, 2020 <https://www.healthaffairs.org/doi/full/10.1377/hlthaff
      .2019.01559>`_
    - FARA, 2022

    """
    df_ot = df_ot.assign(
        **{
            setting: 0
            for setting in [
                "fqhc",
                "outpatient_hospital",
                "physician_office",
                "behavioral_health_centers",
                "hospital",
                "office_based",
            ]
        }
    )
    if ("POS_CD" in df_ot.columns) and ("BLG_PRVDR_TXNMY_CD" in df_ot.columns):
        df_ot = df_ot.assign(
            **{
                "fqhc": (
                    (dd.to_numeric(df_ot["POS_CD"], errors="coerce") == 50)
                    | (df_ot["BLG_PRVDR_TXNMY_CD"].str.strip() == "261QF0400X")
                    | (df_ot["BILL_TYPE_CD"])
                    .astype(str)
                    .str[1:3]
                    .isin(["73", "77"])
                    | (
                        dd.to_numeric(df_ot["PGM_TYPE_CD"], errors="coerce")
                        == 4
                    )
                ).astype(int),
                "outpatient_hospital": dd.to_numeric(
                    df_ot["POS_CD"], errors="coerce"
                )
                .isin(
                    [
                        12,
                        22,
                    ]
                )
                .astype(int),
                "physician_office": (
                    dd.to_numeric(df_ot["POS_CD"], errors="coerce") == 11
                ).astype(int),
                "behavioral_health_centers": (
                    dd.to_numeric(df_ot["POS_CD"], errors="coerce").isin(
                        [
                            52,
                            53,
                            55,
                            56,
                            57,
                            58,
                        ]
                    )
                    | df_ot["BLG_PRVDR_TXNMY_CD"]
                    .str.strip()
                    .isin(
                        [
                            "261QM0855X",
                            "261QM0801X",
                            "261QM2800X",
                            "261QR0405X",
                        ]
                    )
                ).astype(int),
                "hospital": dd.to_numeric(df_ot["POS_CD"], errors="coerce")
                .isin([21, 22, 23])
                .astype(int),
                "office_based": dd.to_numeric(df_ot["POS_CD"], errors="coerce")
                .isin([11, 17, 19, 20, 49, 71, 72])
                .astype(int),
            }
        )
    if "TOS_CD" in df_ot.columns:
        df_ot = df_ot.assign(
            **{
                "physician_office": dd.to_numeric(
                    df_ot["TOS_CD"], errors="coerce"
                )
                .isin([8, 12, 36, 37])
                .astype(int),
                "fqhc": (
                    (
                        dd.to_numeric(df_ot["BNFT_TYPE_CD"], errors="coerce")
                        == 4
                    )
                    | (
                        dd.to_numeric(
                            df_ot["XIX_SRVC_CTGRY_CD"], errors="coerce"
                        )
                        == 28
                    )
                ).astype(int),
            }
        )

    return df_ot

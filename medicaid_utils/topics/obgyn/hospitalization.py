import dask.dataframe as dd
import pandas as pd
import numpy as np
from ...filters.claims import dx_and_proc


def flag_preterm(df_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Detects preterm birth related hospitalization in claims

    New Column(s):
        - hosp_preterm: integer column, 1 when claim has codes denoting
          preterm birth and 0 otherwise

    Parameters
    ----------
    df_claims: dd.DataFrame
        Claims dataframe

    Returns
    -------
    dd.DataFrame

    """
    dct_diag_codes = {
        "preterm": {
            "incl": {9: ["6440", "6442", "7651", "7650", "7652"]},
            "excl": {9: ["76520", "76529"]},
        }
    }
    df_claims = dx_and_proc.flag_diagnoses_and_procedures(
        dct_diag_codes, {}, df_claims
    )
    df_claims = df_claims.rename(columns={"diag_preterm": "hosp_preterm"})
    return df_claims


def flag_multiple_births(df_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Identifies multiple births

    New Column(s):
        - hosp_multiple_births: 0 or 1, 1 when claim has codes indicating a
          multiple birt hdelivery

    Parameters
    ----------
    df_claims: dd.DataFrame
        Claims dataframe

    Returns
    -------
    dd.DataFrame

    """
    dct_diag_codes = {
        "multiple_births": {
            "incl": {9: ["V272", "V273", "V274", "V275", "V276", "V277"]}
        }
    }
    df_claims = dx_and_proc.flag_diagnoses_and_procedures(
        dct_diag_codes, {}, df_claims
    )
    df_claims = df_claims.rename(
        columns={"diag_multiple_births": "hosp_multiple_births"}
    )
    return df_claims


def flag_delivery_mode(df_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Identifies mode of birth

    New Column(s):
        - hosp_vag_dlvry: 0 or 1, 1 denotes vaginal delivery
        - hosp_csrn_dlvry: 0 or 1, 1 denotes caesarian delivery

    Parameters
    ----------
    df_claims: dd.DataFrame
        Claims dataframe

    Returns
    -------
    dd.DataFrame

    """
    dct_proc_codes = {
        "vag_dlvry": {
            1: [str(cd) for cd in range(59400, 59411)],
            6: [str(cd) for cd in range(59400, 59411)],
        },
        "csrn_dlvry": {
            1: [str(cd) for cd in range(59510, 59516)]
            + [str(cd) for cd in range(59618, 59623)],
            2: ["74"],
            6: [str(cd) for cd in range(59510, 59516)]
            + [str(cd) for cd in range(59618, 59623)],
        },
    }
    df_claims = dx_and_proc.flag_diagnoses_and_procedures(
        {}, dct_proc_codes, df_claims
    )
    df_claims = df_claims.rename(
        columns={
            "proc_vag_dlvry": "hosp_vag_dlvry",
            "proc_csrn_dlvry": "hosp_csrn_dlvry",
        }
    )
    return df_claims


def flag_delivery(df_ip_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Detects normal and stillbirths related hospitalization in claims

    New Column(s):
        - hosp_birth: integer column, 1 when claim denotes live or still
          birth and 0 otherwise

    Parameters
    ----------
    df_ip_claims: dd.DataFrame
        IP claims dataframe

    Returns
    -------
    dd.DataFrame

    """
    df_ip_claims = df_ip_claims.assign(
        hosp_birth=(
            dd.to_numeric(df_ip_claims["RCPNT_DLVRY_CD"], errors="coerce") == 1
        ).astype(int)
    )
    return df_ip_claims


def flag_abnormal_pregnancy(df_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Detects ectopic, molar, or abnormal pregnancy, spontaneous or induced
    abortion related hospitalization

    New Column(s):
        - hosp_abnormal_pregnancy:  integer column, 1 when claim denotes
          ectopic, molar, or abnormal pregnancy, spontaneous or induced
          abortion and 0 otherwise

    Parameters
    ----------
    df_claims: dd.DataFrame
        Claims dataframe

    Returns
    -------
    dd.DataFrame
    """
    dct_diag_codes = {"abnormal_pregnancy": {"incl": {9: ["63"]}}}
    df_claims = dx_and_proc.flag_diagnoses_and_procedures(
        dct_diag_codes, {}, df_claims
    )
    df_claims = df_claims.rename(
        columns={"diag_abnormal_pregnancy": "hosp_abnormal_pregnancy"}
    )
    return df_claims


def flag_prenatal(df_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds flag columns denoting presence of codes indicating pre-natal care

    New Columns:
        - prenatal: 0 or 1, 1 when claim has codes indicating pre-natal care

    Parameters
    ----------
    df_claims: dd.DataFrame
        Claims dataframe

    Returns
    -------
    dd.DataFrame

    """
    dct_diag_codes = {"prenatal": {"incl": {9: ["V22", "V23"]}}}
    df_claims = dx_and_proc.flag_diagnoses_and_procedures(
        dct_diag_codes, {}, df_claims
    )
    df_claims = df_claims.rename(columns={"diag_prenatal": "hosp_prenatal"})
    return df_claims


def flag_smm_events(
    df_ip_claims: dd.DataFrame, index_admsn_date_col="index_admsn_date"
) -> dd.DataFrame:
    """
    Adds flags for SMM related hospitaliztions

    New Columns:
        - hosp_smm_myo
        - hosp_smm_aneurysm
        - hosp_smm_renal
        - hosp_smm_respiratory
        - hosp_smm_embolism
        - hosp_smm_cardiac
        - hosp_smm_coagulation
        - hosp_smm_eclampsia
        - hosp_smm_heart
        - hosp_smm_cerebrovascular
        - hosp_smm_edema
        - hosp_smm_anesthesia
        - hosp_smm_sepsis
        - hosp_smm_shock
        - hosp_smm_sickle
        - hosp_smm_thrombotic
        - hosp_smm_cardiac_rhythm
        - hosp_smm_transfusion
        - hosp_smm_hysterectomy
        - hosp_smm_tracheostomy
        - hosp_smm_ventilation
        - hosp_smm: Any SMM related hospitalization
        - hosp_smm_no_blood: Any SMM related hospitalization,
          with transfusion not as the sole cause

    Parameters
    ----------
    df_ip_claims: dd.DataFrame
        IP claims dataframe
    index_admsn_date_col: str, default='index_admsn_date'
        Name of column containing delivery date

    Returns
    -------
    dd.DataFrame

    """
    dct_diag_codes = {
        "smm_myo": {"incl": {9: ["410"], 10: ["I21", "I22"]}},
        "smm_aneurysm": {"incl": {9: ["441"], 10: ["I71", "I790"]}},
        "smm_renal": {
            "incl": {
                9: ["5845", "5846", "5847", "5848", "5849", "6693"],
                10: ["N17", "O904"],
            }
        },
        "smm_respiratory": {
            "incl": {
                9: ["5185", "51881", "51882", "51884", "7991"],
                10: [
                    "J80",
                    "J951",
                    "J952",
                    "J953",
                    "J9582",
                    "J960",
                    "J962",
                    "J969",
                    "R0603",
                    "R092",
                ],
            }
        },
        "smm_embolism": {
            "incl": {
                9: ["6731"],
                10: ["O88112", "O88113", "O88119", "O8812", "O8813"],
            }
        },
        "smm_cardiac_arrest": {
            "incl": {9: ["42741", "42742", "4275"], 10: ["I46", "I490"]}
        },
        "smm_coagulation": {
            "incl": {
                9: ["2866", "2869", "6413", "6663"],
                10: [
                    "D65",
                    "D688",
                    "D689",
                    "O45002",
                    "O45003",
                    "O45009",
                    "O45012",
                    "O45013",
                    "O45019",
                    "O45022",
                    "O45023",
                    "O45029",
                    "O45092",
                    "O45093",
                    "O45099",
                    "O46002",
                    "O46003",
                    "O46009",
                    "O46012",
                    "O46013",
                    "O46019",
                    "O46022",
                    "O46023",
                    "O46029",
                    "O46092",
                    "O46093",
                    "O46099",
                    "O670",
                    "O723",
                ],
            }
        },
        "smm_eclampsia": {"incl": {9: ["6426"], 10: ["O15"]}},
        "smm_heart_failure": {
            "incl": {
                9: ["9971"],
                10: [
                    "I97120",
                    "I97121",
                    "I97130",
                    "I97131",
                    "I97710",
                    "I97711",
                ],
            }
        },
        "smm_cerebrovascular": {
            "incl": {
                9: [
                    "0463",
                    "34839",
                    "36234",
                    "430",
                    "431",
                    "432",
                    "433",
                    "434",
                    "435",
                    "436",
                    "437",
                    "6715",
                    "6740",
                    "99702",
                ],
                10: [
                    "A812",
                    "G45",
                    "G46",
                    "G9349",
                    "H340",
                    "I60",
                    "I61",
                    "I62",
                    "I6300",
                    "I6301",
                    "I631",
                    "I632",
                    "I633",
                    "I634",
                    "I635",
                    "I636",
                    "I638",
                    "I639",
                    "I65",
                    "I66",
                    "I67",
                    "I68",
                    "O2250",
                    "O2252",
                    "O2253",
                    "I97810",
                    "I97811",
                    "I97820",
                    "I97821",
                    "O873",
                ],
            }
        },
        "smm_edema": {
            "incl": {
                9: [
                    "4280",
                    "4281",
                    "42820",
                    "42821",
                    "42823",
                    "43830",
                    "42831",
                    "42833",
                    "42840",
                    "42841",
                    "42843",
                    "4289",
                    "5184",
                ],
                10: [
                    "I501",
                    "I5020",
                    "I5021",
                    "I5023",
                    "I5030",
                    "I5031",
                    "I5033",
                    "I5040",
                    "I5041",
                    "I5043",
                    "I50810",
                    "I50811",
                    "I50813",
                    "I50814",
                    "I5082",
                    "I5083",
                    "I5084",
                    "I5089",
                    "I509",
                    "J810",
                ],
            }
        },
        "smm_anesthesia": {
            "incl": {
                9: ["6680", "6681", "6682", "9954", "99586"],
                10: [
                    "O29112",
                    "O29113",
                    "O29114",
                    "O29115",
                    "O29116",
                    "O29117",
                    "O29118",
                    "O29119",
                    "O29122",
                    "O29123",
                    "O29124",
                    "O29125",
                    "O29126",
                    "O29127",
                    "O29128",
                    "O29129",
                    "O29192",
                    "O29193",
                    "O29194",
                    "O29195",
                    "O29196",
                    "O29197",
                    "O29198",
                    "O29199",
                    "O29212",
                    "O29213",
                    "O29214",
                    "O29215",
                    "O29216",
                    "O29217",
                    "O29218",
                    "O29219",
                    "O29292",
                    "O29293",
                    "O29294",
                    "O29295",
                    "O29296",
                    "O29297",
                    "O29298",
                    "O29299",
                    "O740",
                    "O741",
                    "O742",
                    "O743",
                    "O890",
                    "O891",
                    "O892",
                    "T882XXA",
                    "T883XXA",
                ],
            }
        },
        "smm_sepsis": {
            "incl": {
                9: ["038", "449", "6702", "78552", "99591", "99592", "99802"],
                10: [
                    "A327",
                    "A40",
                    "A41",
                    "I76",
                    "O85",
                    "O8604",
                    "R6520",
                    "R6521",
                    "T8112XA",
                    "T8144XA",
                ],
            }
        },
        "smm_shock": {
            "incl": {
                9: [
                    "6691",
                    "78550",
                    "78551",
                    "78559",
                    "9950",
                    "9980",
                    "99800",
                    "99801",
                    "99809",
                ],
                10: [
                    "O751",
                    "R57",
                    "T782XXA",
                    "T8110XA",
                    "T8111XA",
                    "T8119XA",
                    "T886XXA",
                ],
            }
        },
        "smm_sickle": {
            "incl": {
                9: ["28242", "28262", "28264", "28269", "28952"],
                10: [
                    "D5700",
                    "D5701",
                    "D5702",
                    "D57211",
                    "D57212",
                    "D57219",
                    "D57411",
                    "D57412",
                    "D57419",
                    "D57811",
                    "D57812",
                    "D57819",
                ],
            }
        },
        "smm_thrombotic": {
            "incl": {
                9: ["4150", "4151", "6730", "6732", "6733", "6738"],
                10: [
                    "I26",
                    "O88012",
                    "O88013",
                    "O88019",
                    "08802",
                    "O8803",
                    "O88212",
                    "O88213",
                    "O88219",
                    "O8822",
                    "O8823",
                    "O88312",
                    "O88313",
                    "O88319",
                    "O8832",
                    "O8833",
                    "O88812",
                    "O88813",
                    "O88819",
                    "O8882",
                    "O8883",
                    "T800XXA",
                ],
            }
        },
    }
    dct_proc_codes = {
        "smm_cardiac_rhythm": {2: ["996"], 7: ["5A12012", "5A2204Z"]},
        "smm_transfusion": {
            2: ["990"],
            7: [
                "30230H0",
                "30230K0",
                "30230L0",
                "30230M0",
                "30230N0",
                "30230P0",
                "30230R0",
                "30230T0",
                "30230H1",
                "30230K1",
                "30230L1",
                "30230M1",
                "30230N1",
                "30230P1",
                "30230R1",
                "30230T1",
                "30233H0",
                "30233K0",
                "30233L0",
                "30233M0",
                "30233N0",
                "30233P0",
                "30233R0",
                "30233T0",
                "30233H1",
                "30233K1",
                "30233L1",
                "30233M1",
                "30233N1",
                "30233P1",
                "30233R1",
                "30233T1",
                "30240H0",
                "30240K0",
                "30240L0",
                "30240M0",
                "30240N0",
                "30240P0",
                "30240R0",
                "30240T0",
                "30240H1",
                "30240K1",
                "30240L1",
                "30240M1",
                "30240N1",
                "30240P1",
                "30240R1",
                "30240T1",
                "30243H0",
                "30243K0",
                "30243L0",
                "30243M0",
                "30243N0",
                "30243P0",
                "30243R0",
                "30243T0",
                "30243H1",
                "30243K1",
                "30243L1",
                "30243M1",
                "30243N1",
                "30243P1",
                "30243R1",
                "30243T1",
            ],
        },
        "smm_hysterectomy": {
            2: [
                "6839",
                "6849",
                "6859",
                "6869",
                "6879",
                "689",
                "683",
                "684",
                "685",
                "686",
                "687",
                "689",
            ],
            7: ["0UT90ZL", "0UT90ZZ", "0UT97ZL", "0UT97ZZ"],
        },
        "smm_tracheostomy": {2: ["311"], 7: ["0B110F4", "0B113F4", "0B114F4"]},
        "smm_ventilation": {2: ["9670", "9671", "9672"]},
    }
    df_ip_claims = dx_and_proc.flag_diagnoses_and_procedures(
        dct_diag_codes, dct_proc_codes, df_ip_claims
    )
    df_ip_claims = df_ip_claims.rename(
        columns=dict(
            [(f"diag_{condn}", f"hosp_{condn}") for condn in dct_diag_codes]
            + [(f"proc_{condn}", f"hosp_{condn}") for condn in dct_proc_codes]
        )
    )
    df_ip_claims = df_ip_claims.assign(
        hosp_smm=df_ip_claims[
            df_ip_claims.columns[
                df_ip_claims.columns.str.startswith("hosp_smm_")
            ]
        ].max(axis=1),
        hosp_smm_no_blood=df_ip_claims[
            [
                col
                for col in df_ip_claims.columns
                if (
                    col.startswith("hosp_smm_")
                    and (col != "hosp_smm_transfusion")
                )
            ]
        ].max(axis=1),
    )

    if index_admsn_date_col in df_ip_claims.columns:
        admsn_col_name = (
            "admsn_date"
            if "admsn_date" in df_ip_claims.columns
            else "srvc_bgn_date"
        )
        lst_smm_col = [
            col for col in df_ip_claims.columns if col.startswith("hosp_smm")
        ]
        df_ip_claims = df_ip_claims.assign(
            **dict(
                [
                    (
                        col + "_on_index_hosp",
                        (
                            (df_ip_claims[col] == 1)
                            & (
                                df_ip_claims[admsn_col_name]
                                == df_ip_claims[index_admsn_date_col]
                            )
                        ).astype(int),
                    )
                    for col in lst_smm_col
                ]
            )
        )
        df_ip_claims = df_ip_claims.map_partitions(
            lambda pdf: pdf.assign(
                **dict(
                    [
                        (
                            col + "_12weeks_after_index_hosp",
                            (
                                (pdf[col] == 1)
                                & pdf[admsn_col_name].between(
                                    pdf[index_admsn_date_col],
                                    pdf[index_admsn_date_col]
                                    + pd.Timedelta(days=83),
                                    inclusive=True,
                                )
                            ).astype(int),
                        )
                        for col in lst_smm_col
                    ]
                )
            )
        )
        df_ip_claims = df_ip_claims.map_partitions(
            lambda pdf: pdf.assign(
                **dict(
                    [
                        (
                            col + "_6weeks_after_index_hosp",
                            (
                                (pdf[col] == 1)
                                & pdf[admsn_col_name].between(
                                    pdf[index_admsn_date_col],
                                    pdf[index_admsn_date_col]
                                    + pd.Timedelta(days=41),
                                    inclusive=True,
                                )
                            ).astype(int),
                        )
                        for col in lst_smm_col
                    ]
                )
            )
        )
        df_ip_claims = df_ip_claims.map_partitions(
            lambda pdf: pdf.assign(
                **dict(
                    [
                        (
                            col + "_90days_after_index_hosp",
                            (
                                (pdf[col] == 1)
                                & pdf[admsn_col_name].between(
                                    pdf[index_admsn_date_col],
                                    pdf[index_admsn_date_col]
                                    + pd.Timedelta(days=90),
                                    inclusive=True,
                                )
                            ).astype(int),
                        )
                        for col in lst_smm_col
                    ]
                )
            )
        )
    return df_ip_claims


def calculate_conception(
    df_claims: dd.DataFrame,
) -> (dd.DataFrame, dd.DataFrame):
    """
    Estimates conception date based on type of delivery and delivery date.

    Conception date is calculated as,
        - 75 days before date of abortive claims
        - 255 days before full term deliveries
        - 230 days before pre-term deliveries
        - 45 days before pre-natal claims

    New columns:
        - conception_date: Date of conception.

    Parameters
    ----------
    df_claims: dd.DataFrame
        Claims dataframe that has indicator columns for type of deliveries,
        viz., hosp_abnormal_pregnancy, hosp_birth, hosp_preterm,
        and hosp_prenatal

    Returns
    -------
    dd.DataFrame

    """
    lst_hosp_pregnancy_col = [
        "hosp_abnormal_pregnancy",
        "hosp_birth",
        "hosp_preterm",
        "hosp_prenatal",
    ]
    lst_missing_col = [
        col for col in lst_hosp_pregnancy_col if col not in df_claims.columns
    ]
    if len(lst_missing_col) > 0:
        df_claims = df_claims.assign(
            **dict([(col, 0) for col in lst_missing_col])
        )

    lst_conception_col = [
        "abortive",
        "full_term_delivery",
        "preterm_delivery",
        "prenatal",
    ]

    df_claims = df_claims.assign(
        abortive=(df_claims["hosp_abnormal_pregnancy"] == 1).astype(int),
        full_term_delivery=(
            (df_claims["hosp_abnormal_pregnancy"] == 0)
            & (df_claims["hosp_birth"] == 1)
            & (df_claims["hosp_preterm"] == 0)
        ).astype(int),
        preterm_delivery=(
            (df_claims["hosp_abnormal_pregnancy"] == 0)
            & (df_claims["hosp_birth"] == 1)
            & (df_claims["hosp_preterm"] == 1)
        ).astype(int),
        prenatal=(
            (df_claims["hosp_abnormal_pregnancy"] == 0)
            & (df_claims["hosp_birth"] == 0)
            & (df_claims["hosp_prenatal"] == 1)
        ).astype(int),
    )
    df_conception = df_claims.loc[
        df_claims[lst_conception_col].sum(axis=1) == 1
    ]

    df_conception = df_conception.map_partitions(
        lambda pdf: pdf.assign(
            conception_date=pdf["service_date"]
            - np.select(
                [
                    (pdf["abortive"] == 1),
                    (pdf["full_term_delivery"] == 1),
                    (pdf["preterm_delivery"] == 1),
                    (pdf["prenatal"] == 1),
                ],
                [
                    pd.Timedelta(days=75),
                    pd.Timedelta(days=270) - pd.Timedelta(days=15),
                    pd.Timedelta(days=245) - pd.Timedelta(days=15),
                    pd.Timedelta(days=45),
                ],
                default=np.nan,
            )
        )
    )
    df_conception = df_conception.drop(lst_conception_col, axis=1)

    return df_conception

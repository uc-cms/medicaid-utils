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

    - proc_buprenorphine: 0 or 1, 1 when claim has Buprenorphine
      treatment procedure codes and 0 otherwise

    Parameters
    ----------
    df_claims : dd.DataFrame
            IP or OT claim file

    Returns
    -------
    dd.DataFrame

    """
    dct_proc_codes = {"buprenorphine": {6: ["J0571"], 1: ["J0571"]}}
    df_claims = dx_and_proc.flag_diagnoses_and_procedures(
        dct_proc_codes, {}, df_claims
    )
    return df_claims


def flag_rx_buprenorphine(df_rx_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds indicator column denoting presence of buprenorphine NDC codes in
    claims.

    New Column(s):

    - rx_buprenorphine: 0 or 1, 1 when claim has buprenorphine NDC codes
      and 0 otherwise

    Parameters
    ----------
    df_claims : dd.DataFrame
            IP or OT claim file

    Returns
    -------
    dd.DataFrame

    """
    dct_ndc_codes = {
        "buprenorphine": [
            "000093360021",
            "000093360040",
            "000093360121",
            "000093360140",
            "000093360221",
            "000093360240",
            "000093360321",
            "000093360340",
            "000093365621",
            "000093365640",
            "000093365721",
            "000093365740",
            "000093365821",
            "000093365840",
            "000093365921",
            "000093365940",
            "042858035340",
            "042858049340",
            "042858058640",
            "042858075040",
            "042858083940",
            "055700056804",
            "055700057904",
            "063275988401",
            "063275988402",
            "063275988404",
            "063275988405",
            "000054017613",
            "000054017713",
            "000074201201",
            "000093537856",
            "000093537956",
            "000228315303",
            "000228315603",
            "000378092393",
            "000378092493",
            "000409201203",
            "000409201232",
            "000517072501",
            "000517072505",
            "021695051510",
            "035356055530",
            "035356055630",
            "038779088800",
            "038779088801",
            "038779088803",
            "038779088805",
            "038779088806",
            "038779088809",
            "040042001001",
            "042023017901",
            "042023017905",
            "042858050103",
            "042858050203",
            "043063066706",
            "043063075306",
            "049452129201",
            "049452129202",
            "049452130403",
            "049452825301",
            "049452825302",
            "049452825303",
            "049452825304",
            "050090157100",
            "050090292400",
            "050383092493",
            "050383093093",
            "051552076501",
            "051552076506",
            "051552076509",
            "051927101200",
            "053217024630",
            "054569657800",
            "055390010010",
            "055700030230",
            "055700030330",
            "062756045964",
            "062756045983",
            "062756046064",
            "062756046083",
            "062991158301",
            "062991158302",
            "062991158303",
            "062991158304",
            "062991158306",
            "062991158307",
            "062991158308",
            "063275992201",
            "063275992202",
            "063275992203",
            "063275992204",
            "063275992205",
            "063275992207",
            "063275992208",
            "063275992209",
            "063370090506",
            "063370090509",
            "063370090510",
            "063370090515",
            "063629712501",
            "063629712502",
            "063629712503",
            "063629712504",
            "063629712505",
            "063629712506",
            "063629712507",
            "063629712601",
            "063629712602",
            "063629712603",
            "063629712604",
            "063629712605",
            "063629712606",
            "063629712607",
            "063629712608",
            "064725093003",
            "064725093004",
            "064725192403",
            "064725192404",
            "068258299103",
            "068308020230",
            "068308020830",
            "071335035301",
            "071335035302",
            "071335035303",
            "071335035304",
            "071335035305",
            "071335035306",
            "071335035307",
            "076519117000",
            "076519117001",
            "076519117002",
            "076519117003",
            "076519117004",
            "076519117005",
            "052440010014",
            "058284010014",
            "012496010001",
            "012496010002",
            "012496010005",
            "012496030001",
            "012496030002",
            "012496030005",
            "012496127802",
            "012496131002",
            "049999063830",
            "049999063930",
            "063629409201",
            "063629409202",
            "063874117303",
            "063874117403",
        ]
    }
    df_claims = rx.flag_prescriptions(dct_ndc_codes, df_rx_claims, True)
    return df_claims


def flag_proc_buprenorphine_naloxone(df_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds indicator column denoting presence of Buprenorphine/ Naloxone
    treatment procedure codes in claims.

    New Column(s):

    - proc_buprenorphine_naloxone: 0 or 1, 1 when claim has
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
        "buprenorphine_naloxone": {
            6: ["J0572", "J0573", "J0574", "J0575"],
            1: ["J0572", "J0573", "J0574", "J0575"],
        }
    }
    df_claims = dx_and_proc.flag_diagnoses_and_procedures(
        dct_proc_codes, {}, df_claims
    )
    return df_claims


def flag_rx_buprenorphine_naloxone(df_rx_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds indicator column denoting presence of Buprenorphine/ Naloxone NDC
    codes in claims.

    New Column(s):

    - rx_buprenorphine_naloxone: 0 or 1, 1 when claim has buprenorphine
      NDC codes and 0 otherwise

    Parameters
    ----------
    df_claims : dd.DataFrame
            IP or OT claim file

    Returns
    -------
    dd.DataFrame

    """
    dct_ndc_codes = {
        "buprenorphine_naloxone": [
            "059385001201",
            "059385001230",
            "059385001401",
            "059385001430",
            "059385001601",
            "059385001630",
            "000054018813",
            "000054018913",
            "000093572056",
            "000093572156",
            "000228315403",
            "000228315473",
            "000228315503",
            "000228315567",
            "000228315573",
            "000378876716",
            "000378876793",
            "000378876816",
            "000378876893",
            "000406192303",
            "000406192403",
            "000406192409",
            "000406800503",
            "000406802003",
            "000781721664",
            "000781722764",
            "000781723806",
            "000781723864",
            "000781724964",
            "042291017430",
            "042291017530",
            "043598057901",
            "043598057930",
            "043598058001",
            "043598058030",
            "043598058101",
            "043598058130",
            "043598058201",
            "043598058230",
            "047781035503",
            "047781035603",
            "047781035703",
            "047781035803",
            "047781071203",
            "047781071211",
            "050268014411",
            "050268014415",
            "050268014511",
            "050268014515",
            "050383028793",
            "050383029493",
            "052427069203",
            "052427069211",
            "052427069403",
            "052427069411",
            "052427069803",
            "052427069811",
            "053217013830",
            "054569640800",
            "055700018430",
            "060429058630",
            "060429058633",
            "060429058730",
            "060429058733",
            "060846097003",
            "062175045232",
            "062175045832",
            "062756096964",
            "062756096983",
            "062756097064",
            "062756097083",
            "063629507401",
            "063629727001",
            "063629727002",
            "065162041503",
            "065162041509",
            "065162041603",
            "065162041609",
            "050268014411",
            "050268014415",
            "050268014515",
            "000490005100",
            "000490005130",
            "000490005160",
            "000490005190",
            "012496120201",
            "012496120203",
            "012496120401",
            "012496120403",
            "012496120801",
            "012496120803",
            "012496121201",
            "012496121203",
            "012496128302",
            "012496130602",
            "016590066605",
            "016590066630",
            "016590066705",
            "016590066730",
            "016590066790",
            "023490927003",
            "023490927006",
            "023490927009",
            "035356000407",
            "035356000430",
            "043063018407",
            "043063018430",
            "049999039507",
            "049999039515",
            "049999039530",
            "052959030430",
            "052959074930",
            "054569549600",
            "054569573900",
            "054569573901",
            "054569573902",
            "054569639900",
            "054868570700",
            "054868570701",
            "054868570702",
            "054868570703",
            "054868570704",
            "054868575000",
            "055045378403",
            "055700014730",
            "055887031204",
            "055887031215",
            "063629403401",
            "063629403402",
            "063629403403",
            "063874108403",
            "063874108503",
            "066336001530",
            "066336001630",
            "068071138003",
            "068071151003",
            "068258299903",
            "054123011430",
            "054123090730",
            "054123091430",
            "054123092930",
            "054123095730",
            "054123098630",
        ],
    }
    df_claims = rx.flag_prescriptions(dct_ndc_codes, df_rx_claims, True)
    return df_claims


def flag_proc_injectable_naltrexone(df_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds indicator column denoting presence of Injectable Naltrexone
    procedure codes in claims.

    New Column(s):

    - proc_injectable_naltrexone: 0 or 1, 1 when claim has Injectable
      Naltrexone treatment procedure codes and 0 otherwise

    Parameters
    ----------
    df_claims : dd.DataFrame
            IP or OT claim file

    Returns
    -------
    dd.DataFrame

    """
    dct_proc_codes = {"injectable_naltrexone": {6: ["J2315"], 1: ["J2315"]}}
    df_claims = dx_and_proc.flag_diagnoses_and_procedures(
        dct_proc_codes, {}, df_claims
    )
    return df_claims


def flag_rx_oral_naltrexone(df_rx_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds indicator column denoting presence of Oral Naltrexone NDC
    codes in claims.

    New Column(s):

    - rx_oral_naltrexone: 0 or 1, 1 when claim has Oral Naltrexone NDC
      codes and 0 otherwise

    Parameters
    ----------
    df_claims : dd.DataFrame
            IP or OT claim file

    Returns
    -------
    dd.DataFrame

    """
    dct_ndc_codes = {
        "oral_naltrexone": [
            "010695004305",
            "010695004310",
            "049452483901",
            "051927354800",
            "052372075101",
            "052372075102",
            "052372075103",
            "054868557400",
            "062991312501",
            "062991312502",
            "062991312503",
            "062991312504",
            "069364314306",
            "070350782001",
            "000185003901",
            "000185003930",
            "000395808319",
            "000395808335",
            "000395808362",
            "000406117001",
            "000406117003",
            "000555090201",
            "000555090202",
            "010695006014",
            "010695006017",
            "016729008101",
            "016729008110",
            "038779088703",
            "038779088704",
            "038779088705",
            "038779088706",
            "038779088708",
            "042291063230",
            "043063059115",
            "047335032683",
            "047335032688",
            "049452483501",
            "049452483502",
            "049452483503",
            "049452483505",
            "050090286600",
            "050090307600",
            "050436010501",
            "051224020630",
            "051224020650",
            "051552073701",
            "051552073702",
            "051552073704",
            "051927275300",
            "051927360200",
            "051927437700",
            "052152010502",
            "052152010504",
            "052152010530",
            "053217026130",
            "054569672000",
            "054569913900",
            "058597840701",
            "058597840702",
            "058597840704",
            "058597840706",
            "062991124301",
            "062991124302",
            "062991124303",
            "062991124304",
            "063275990101",
            "063275990102",
            "063275990103",
            "063275990104",
            "063275990105",
            "063370015810",
            "063370015815",
            "063370015825",
            "063370015835",
            "065694010003",
            "065694010010",
            "068084029111",
            "068084029121",
            "068094085359",
            "068094085362",
            "068115068030",
            "076519116005",
            "063459030042",
            "065757030001",
            "000056001122",
            "000056001130",
            "000056001170",
            "000056007950",
            "051285027501",
            "051285027502",
        ]
    }
    df_claims = rx.flag_prescriptions(dct_ndc_codes, df_rx_claims, True)
    return df_claims


def flag_proc_methadone(df_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds indicator column denoting presence of Methadone procedure codes in
    claims.

    New Column(s):

    - proc_methadone: 0 or 1, 1 when claim has Methadone treatment
      procedure codes and 0 otherwise

    Parameters
    ----------
    df_claims : dd.DataFrame
            IP or OT claim file

    Returns
    -------
    dd.DataFrame

    """
    dct_proc_codes = {"methadone": {6: ["H0020"], 1: ["H0020"]}}
    df_claims = dx_and_proc.flag_diagnoses_and_procedures(
        dct_proc_codes, {}, df_claims
    )
    return df_claims


def flag_rx_methadone(df_rx_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Adds indicator column denoting presence of Methadone NDC codes in claims.

    New Column(s):

    - rx_methadone_le_30mg: 0 or 1, 1 when claim has Methadone NDC codes
      with dosage less than or equal to 30 mg and 0 otherwise
    - rx_methadone_gt_30mg: 0 or 1, 1 when claim has Methadone NDC codes
      with dosage greater than or equal to 30 mg and 0 otherwise

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

    - proc_behavioral_health_trtmt: 0 or 1, 1 when claim has Behavioral
      Health treatment procedure codes and 0 otherwise

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
            1: [
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
            ],
            1: [
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
            ],
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

    - rx_benzodiazepines: 0 or 1, 1 when claim has Benzodiazepines NDC
      codes and 0 otherwise
    - rx_opioids: 0 or 1, 1 when claim has Opioids NDC codes and 0
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

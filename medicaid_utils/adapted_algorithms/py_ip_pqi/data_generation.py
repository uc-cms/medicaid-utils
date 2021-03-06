import pandas as pd
import os

package_folder, filename = os.path.split(__file__)
data_folder = os.path.join(package_folder, "data")


def generate_dxgrp_file():
    df_pqi_dxgroup = pd.DataFrame(
        columns=["primary_outcome", "lst_dx", "var_name", "comments"]
    )
    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Diabetes with Short Term Complications",
                "lst_dx": ",".join(
                    [
                        "25010",
                        "25011",
                        "25012",
                        "25013",
                        "25020",
                        "25021",
                        "25022",
                        "25023",
                        "25030",
                        "25031",
                        "25032",
                        "25033",
                    ]
                ),
                "var_name": "ACDIASD",
                "comments": (
                    "Diabetes short term complication rate (ACDIAS) - ACSC #1"
                ),
            },
            index=[0],
        ),
        ignore_index=True,
    )
    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Gastroenteritis",
                "lst_dx": ",".join(
                    [
                        "00861",
                        "00862",
                        "00863",
                        "00864",
                        "00865",
                        "00866",
                        "00867",
                        "00869",
                        "0088 ",
                        "0090 ",
                        "0091 ",
                        "0092 ",
                        "0093 ",
                        "5589 ",
                    ]
                ),
                "var_name": "ACPGASD",
                "comments": "",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Perforated Appendix",
                "lst_dx": ",".join(["5400 ", "5401 "]),
                "var_name": "ACSAPPD",
                "comments": "Perforated Appendix rate (ACSAPP) - ACSC #2",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Appendicitis (Population at Risk)",
                "lst_dx": ",".join(["5400 ", "5401 ", "5409 ", "541  "]),
                "var_name": "ACSAP2D",
                "comments": "",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Diabetes with Long Term Complication",
                "lst_dx": ",".join(
                    [
                        "25040",
                        "25041",
                        "25042",
                        "25043",
                        "25050",
                        "25051",
                        "25052",
                        "25053",
                        "25060",
                        "25061",
                        "25062",
                        "25063",
                        "25070",
                        "25071",
                        "25072",
                        "25073",
                        "25080",
                        "25081",
                        "25082",
                        "25083",
                        "25090",
                        "25091",
                        "25092",
                        "25093",
                    ]
                ),
                "var_name": "ACDIALD",
                "comments": (
                    "Diabetes long term complication rate (ACDIAL) - ACSC #3"
                ),
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Asthma",
                "lst_dx": ",".join(
                    [
                        "49300",
                        "49301",
                        "49302",
                        "49310",
                        "49311",
                        "49312",
                        "49320",
                        "49321",
                        "49322",
                        "49381",
                        "49382",
                        "49390",
                        "49391",
                        "49392",
                    ]
                ),
                "var_name": "ACSASTD",
                "comments": "Asthma rate (ACSAST) - ACSC #15 - Adult ",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": (
                    "EXCLUDE: CYSTIC FIBROSIS AND ANOMALIES OF RESPIRATORY"
                    " SYSTEM"
                ),
                "lst_dx": ",".join(
                    [
                        "27700",
                        "27701",
                        "27702",
                        "27703",
                        "27709",
                        "51661",
                        "51662",
                        "51663",
                        "51664",
                        "51669",
                        "74721",
                        "7483 ",
                        "7484 ",
                        "7485 ",
                        "74860",
                        "74861",
                        "74869",
                        "7488 ",
                        "7489 ",
                        "7503 ",
                        "7593 ",
                        "7707 ",
                    ]
                ),
                "var_name": "RESPAN",
                "comments": "",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "COPD (#1)",
                "lst_dx": ",".join(
                    [
                        "4910 ",
                        "4911 ",
                        "49120",
                        "49121",
                        "4918 ",
                        "4919 ",
                        "4920 ",
                        "4928 ",
                        "494  ",
                        "4940 ",
                        "4941 ",
                        "496  ",
                    ]
                ),
                "var_name": "ACCOPDD",
                "comments": "COPD rate (ACCOPD) - ACSC #5",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "COPD (#2)",
                "lst_dx": ",".join(["4660 ", "490  "]),
                "var_name": "ACCPD2D",
                "comments": "",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Hypertension",
                "lst_dx": ",".join(
                    [
                        "4010 ",
                        "4019 ",
                        "40200",
                        "40210",
                        "40290",
                        "40300",
                        "40310",
                        "40390",
                        "40400",
                        "40410",
                        "40490",
                    ]
                ),
                "var_name": "ACSHYPD",
                "comments": "Hypertension rate (ACSHYP) - ACSC #7",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": (
                    "Exclude: Stage I-IV Kidney primary_outcome"
                ),
                "lst_dx": ",".join(
                    ["40300", "40310", "40390", "40400", "40410", "40490"]
                ),
                "var_name": "ACSHY2D",
                "comments": "",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "EXCLUDE: CHRONIC RENAL FAILURE",
                "lst_dx": ",".join(
                    [
                        "40300",
                        "40301",
                        "40310",
                        "40311",
                        "40390",
                        "40391",
                        "40400",
                        "40401",
                        "40402",
                        "40403",
                        "40410",
                        "40411",
                        "40412",
                        "40413",
                        "40490",
                        "40491",
                        "40492",
                        "40493",
                        "585  ",
                        "5855 ",
                        "5856 ",
                    ]
                ),
                "var_name": "CRENLFD",
                "comments": "",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Renal Failure",
                "lst_dx": ",".join(
                    ["5845", "5846", "5847", "5848", "5849", "586 ", "9975"]
                ),
                "var_name": "PHYSIDB",
                "comments": "",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Congestive Heart Failure",
                "lst_dx": ",".join(
                    [
                        "39891",
                        "40201",
                        "40211",
                        "40291",
                        "40401",
                        "40403",
                        "40411",
                        "40413",
                        "40491",
                        "40493",
                        "4280 ",
                        "4281 ",
                        "42820",
                        "42821",
                        "42822",
                        "42823",
                        "42830",
                        "42831",
                        "42832",
                        "42833",
                        "42840",
                        "42841",
                        "42842",
                        "42843",
                        "4289 ",
                    ]
                ),
                "var_name": "ACSCHFD",
                "comments": "Congestive Heart Failure rate (ACSCHF) - ACSC #8",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Congestive Heart Failure",
                "lst_dx": ",".join(
                    [
                        "39891",
                        "4280 ",
                        "4281 ",
                        "42820",
                        "42821",
                        "42822",
                        "42823",
                        "42830",
                        "42831",
                        "42832",
                        "42833",
                        "42840",
                        "42841",
                        "42842",
                        "42843",
                        "4289 ",
                    ]
                ),
                "var_name": "ACSCH2D",
                "comments": "",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Liveborn (Populaton at Risk)",
                "lst_dx": ",".join(
                    [
                        "V3000",
                        "V3001",
                        "V3100",
                        "V3101",
                        "V3200",
                        "V3201",
                        "V3300",
                        "V3301",
                        "V3400",
                        "V3401",
                        "V3500",
                        "V3501",
                        "V3600",
                        "V3601",
                        "V3700",
                        "V3701",
                        "V3900",
                        "V3901",
                    ]
                ),
                "var_name": "LIVEBND",
                "comments": "Low Birth Weight rate (ACSLBW) - ACSC #9",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Liveborn (Populaton at Risk)",
                "lst_dx": ",".join(
                    ["V290", "V291", "V292", "V293", "V298", "V299"]
                ),
                "var_name": "V29D",
                "comments": "",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Liveborn (Populaton at Risk)",
                "lst_dx": ",".join(
                    [
                        "V301 ",
                        "V302 ",
                        "V311 ",
                        "V312 ",
                        "V321 ",
                        "V322 ",
                        "V331 ",
                        "V332 ",
                        "V341 ",
                        "V342 ",
                        "V351 ",
                        "V352 ",
                        "V361 ",
                        "V362 ",
                        "V371 ",
                        "V372 ",
                        "V391 ",
                        "V392 ",
                    ]
                ),
                "var_name": "LIVEB2D",
                "comments": "",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Dehydration",
                "lst_dx": ",".join(["2765 ", "27650", "27651", "27652"]),
                "var_name": "ACSDEHD",
                "comments": "Dehydration rate (ACSDEH) - ACSC #10",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Bacterial Pneumonia",
                "lst_dx": ",".join(
                    [
                        "481  ",
                        "4822 ",
                        "48230",
                        "48231",
                        "48232",
                        "48239",
                        "48241",
                        "48242",
                        "4829 ",
                        "4830 ",
                        "4831 ",
                        "4838 ",
                        "485  ",
                        "486  ",
                    ]
                ),
                "var_name": "ACSBACD",
                "comments": "Bacterial Pneumonia rate (ACSBAC) - ACSC #11",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Exclude: Sickle Cell",
                "lst_dx": ",".join(
                    [
                        "28241",
                        "28242",
                        "28260",
                        "28261",
                        "28262",
                        "28263",
                        "28264",
                        "28268",
                        "28269",
                    ]
                ),
                "var_name": "ACSBA2D",
                "comments": "",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "HYPEROSMOLALITY AND /OR HYPERNATREMIA",
                "lst_dx": ",".join(["2760 "]),
                "var_name": "HYPERID",
                "comments": "",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Urinary Infection",
                "lst_dx": ",".join(
                    [
                        "59010",
                        "59011",
                        "5902 ",
                        "5903 ",
                        "59080",
                        "59081",
                        "5909 ",
                        "5950 ",
                        "5959 ",
                        "5990 ",
                    ]
                ),
                "var_name": "ACSUTID",
                "comments": "Urinary Infection rate (ACSUTI) - ACSC #12",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "EXCLUDE: IMMUNOCOMPROMISED",
                "lst_dx": ",".join(
                    [
                        "042  ",
                        "1363 ",
                        "1992 ",
                        "23873",
                        "23876",
                        "23877",
                        "23879",
                        "260  ",
                        "261  ",
                        "262  ",
                        "27900",
                        "27901",
                        "27902",
                        "27903",
                        "27904",
                        "27905",
                        "27906",
                        "27909",
                        "27910",
                        "27911",
                        "27912",
                        "27913",
                        "27919",
                        "2792 ",
                        "2793 ",
                        "2794 ",
                        "27941",
                        "27949",
                        "27950",
                        "27951",
                        "27952",
                        "27953",
                        "2798 ",
                        "2799 ",
                        "28409",
                        "2841 ",
                        "28411",
                        "28412",
                        "28419",
                        "2880 ",
                        "28800",
                        "28801",
                        "28802",
                        "28803",
                        "28809",
                        "2881 ",
                        "2882 ",
                        "2884 ",
                        "28850",
                        "28851",
                        "28859",
                        "28953",
                        "28983",
                        "40301",
                        "40311",
                        "40391",
                        "40402",
                        "40403",
                        "40412",
                        "40413",
                        "40492",
                        "40493",
                        "5793 ",
                        "585  ",
                        "5855 ",
                        "5856 ",
                        "9968 ",
                        "99680",
                        "99681",
                        "99682",
                        "99683",
                        "99684",
                        "99685",
                        "99686",
                        "99687",
                        "99688",
                        "99689",
                        "V420 ",
                        "V421 ",
                        "V426 ",
                        "V427 ",
                        "V428 ",
                        "V4281",
                        "V4282",
                        "V4283",
                        "V4284",
                        "V4289",
                        "V451 ",
                        "V4511",
                        "V560 ",
                        "V561 ",
                        "V562 ",
                    ]
                ),
                "var_name": "IMMUNID",
                "comments": "",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "XCLUDE: KIDNEY OR URINARY TRACT DISORDER",
                "lst_dx": ",".join(
                    [
                        "59000",
                        "59001",
                        "59370",
                        "59371",
                        "59372",
                        "59373",
                        "7530 ",
                        "75310",
                        "75311",
                        "75312",
                        "75313",
                        "75314",
                        "75315",
                        "75316",
                        "75317",
                        "75319",
                        "75320",
                        "75321",
                        "75322",
                        "75323",
                        "75329",
                        "7533 ",
                        "7534 ",
                        "7535 ",
                        "7536 ",
                        "7538 ",
                        "7539 ",
                    ]
                ),
                "var_name": "KIDNEY",
                "comments": "",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Angina",
                "lst_dx": ",".join(
                    ["4111 ", "41181", "41189", "4130 ", "4131 ", "4139 "]
                ),
                "var_name": "ACSANGD",
                "comments": "Angina (w/o procedure) rate (ACSANG) - ACSC #13",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Diabetes uncontrolled",
                "lst_dx": ",".join(["25002", "25003"]),
                "var_name": "ACDIAUD",
                "comments": "Diabetes uncontrolled rate (ACDIAU) - ACSC #14",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Include only: Diabetes",
                "lst_dx": ",".join(
                    [
                        "25000",
                        "25001",
                        "25002",
                        "25003",
                        "25010",
                        "25011",
                        "25012",
                        "25013",
                        "25020",
                        "25021",
                        "25022",
                        "25023",
                        "25030",
                        "25031",
                        "25032",
                        "25033",
                        "25040",
                        "25041",
                        "25042",
                        "25043",
                        "25050",
                        "25051",
                        "25052",
                        "25053",
                        "25060",
                        "25061",
                        "25062",
                        "25063",
                        "25070",
                        "25071",
                        "25072",
                        "25073",
                        "25080",
                        "25081",
                        "25082",
                        "25083",
                        "25090",
                        "25091",
                        "25092",
                        "25093",
                    ]
                ),
                "var_name": "ACSLEAD",
                "comments": "",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "Exclude: Trauma",
                "lst_dx": ",".join(
                    [
                        "8950 ",
                        "8951 ",
                        "8960 ",
                        "8961 ",
                        "8962 ",
                        "8963 ",
                        "8970 ",
                        "8971 ",
                        "8972 ",
                        "8973 ",
                        "8974 ",
                        "8975 ",
                        "8976 ",
                        "8977 ",
                    ]
                ),
                "var_name": "ACLEA2D",
                "comments": "",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup = df_pqi_dxgroup.append(
        pd.DataFrame(
            {
                "primary_outcome": "EXCLUDE: TOE AMPUTATION PROCEDURE",
                "lst_dx": ",".join(["8411 "]),
                "var_name": "TOEAMIP",
                "comments": "",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_dxgroup["lst_dx"] = df_pqi_dxgroup["lst_dx"].str.replace(
        " ", "", regex=True
    )

    df_pqi_dxgroup.to_csv(
        os.path.join(data_folder, "dxgrp.csv"),
        index=False,
    )


def generate_prgrp_file():
    df_pqi_prgrp = pd.DataFrame(
        columns=["primary_outcome", "lst_pr", "var_name", "comments"]
    )

    df_pqi_prgrp = df_pqi_prgrp.append(
        pd.DataFrame(
            {
                "primary_outcome": "Haemodialysis",
                "lst_pr": ",".join(
                    ["3895", "3927", "3929", "3942", "3943", "3993", "3994"]
                ),
                "var_name": "ACSHYPP",
                "comments": "",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_prgrp = df_pqi_prgrp.append(
        pd.DataFrame(
            {
                "primary_outcome": "Exclude: Cardiac Procedures",
                "lst_pr": ",".join(
                    [
                        "0050",
                        "0051",
                        "0052",
                        "0053",
                        "0054",
                        "0056",
                        "0057",
                        "0066",
                        "1751",
                        "1752",
                        "1755",
                        "3500",
                        "3501",
                        "3502",
                        "3503",
                        "3504",
                        "3505",
                        "3506",
                        "3507",
                        "3508",
                        "3509",
                        "3510",
                        "3511",
                        "3512",
                        "3513",
                        "3514",
                        "3520",
                        "3521",
                        "3522",
                        "3523",
                        "3524",
                        "3525",
                        "3526",
                        "3527",
                        "3528",
                        "3531",
                        "3532",
                        "3533",
                        "3534",
                        "3535",
                        "3539",
                        "3541",
                        "3542",
                        "3550",
                        "3551",
                        "3552",
                        "3553",
                        "3554",
                        "3555",
                        "3560",
                        "3561",
                        "3562",
                        "3563",
                        "3570",
                        "3571",
                        "3572",
                        "3573",
                        "3581",
                        "3582",
                        "3583",
                        "3584",
                        "3591",
                        "3592",
                        "3593",
                        "3594",
                        "3595",
                        "3596",
                        "3597",
                        "3598",
                        "3599",
                        "3601",
                        "3602",
                        "3603",
                        "3604",
                        "3605",
                        "3606",
                        "3607",
                        "3609",
                        "3610",
                        "3611",
                        "3612",
                        "3613",
                        "3614",
                        "3615",
                        "3616",
                        "3617",
                        "3619",
                        "362 ",
                        "363 ",
                        "3631",
                        "3632",
                        "3633",
                        "3634",
                        "3639",
                        "3691",
                        "3699",
                        "3731",
                        "3732",
                        "3733",
                        "3734",
                        "3735",
                        "3736",
                        "3737",
                        "3741",
                        "375 ",
                        "3751",
                        "3752",
                        "3753",
                        "3754",
                        "3755",
                        "3760",
                        "3761",
                        "3762",
                        "3763",
                        "3764",
                        "3765",
                        "3766",
                        "3770",
                        "3771",
                        "3772",
                        "3773",
                        "3774",
                        "3775",
                        "3776",
                        "3777",
                        "3778",
                        "3779",
                        "3780",
                        "3781",
                        "3782",
                        "3783",
                        "3785",
                        "3786",
                        "3787",
                        "3789",
                        "3794",
                        "3795",
                        "3796",
                        "3797",
                        "3798",
                        "3826",
                    ]
                ),
                "var_name": "ACSCARP",
                "comments": "",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_prgrp = df_pqi_prgrp.append(
        pd.DataFrame(
            {
                "primary_outcome": "EXCLUDE: IMMUNOCOMPROMISED",
                "lst_pr": ",".join(
                    [
                        "0018",
                        "335 ",
                        "3350",
                        "3351",
                        "3352",
                        "336 ",
                        "375 ",
                        "3751",
                        "410 ",
                        "4100",
                        "4101",
                        "4102",
                        "4103",
                        "4104",
                        "4105",
                        "4106",
                        "4107",
                        "4108",
                        "4109",
                        "5051",
                        "5059",
                        "5280",
                        "5281",
                        "5282",
                        "5283",
                        "5285",
                        "5286",
                        "5569",
                    ]
                ),
                "var_name": "IMMUNIP",
                "comments": "",
            },
            index=[0],
        ),
        ignore_index=True,
    )

    df_pqi_prgrp = df_pqi_prgrp.append(
        pd.DataFrame(
            {
                "primary_outcome": "Lower extremity amputation",
                "lst_pr": ",".join(
                    [
                        "8410",
                        "8411",
                        "8412",
                        "8413",
                        "8414",
                        "8415",
                        "8416",
                        "8417",
                        "8418",
                        "8419",
                    ]
                ),
                "var_name": "ACSLEAP",
                "comments": (
                    "Lower extremity amputation rate(ACSCLEA) - ACSC #16"
                ),
            },
            index=[0],
        ),
        ignore_index=True,
    )
    df_pqi_prgrp["lst_pr"] = df_pqi_prgrp["lst_pr"].str.replace(
        " ", "", regex=True
    )
    df_pqi_prgrp.to_csv(
        os.path.join(data_folder, "prgrp.csv"),
        index=False,
    )


generate_dxgrp_file()
generate_prgrp_file()

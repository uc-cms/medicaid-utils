import os


def get_max_parquet_loc(root, claim_type, state, year) -> str:
    return os.path.join(
        root,
        "medicaid",
        str(year),
        state.upper(),
        "max",
        claim_type.lower(),
        "parquet",
    )


def get_taf_parquet_loc(root, claim_type, state, year):
    dct_fileloc = {}
    data_folder = os.path.join(
        root, "medicaid", str(year), state.upper(), "taf"
    )
    if claim_type in ["ip", "ot"]:
        dct_fileloc["base"] = os.path.join(
            data_folder, claim_type, f"{claim_type.lower()}h", "parquet"
        )
        dct_fileloc["line"] = os.path.join(
            data_folder, claim_type, f"{claim_type.lower()}l", "parquet"
        )
        dct_fileloc["occurrence_code"] = os.path.join(
            data_folder, claim_type, f"{claim_type.lower()}occr", "parquet"
        )
    if claim_type == "ps":
        dct_fileloc["dates"] = os.path.join(
            data_folder, "de", "dedts", "parquet"
        )
        dct_fileloc["base"] = os.path.join(
            data_folder, "de", "debse", "parquet"
        )
        dct_fileloc["managed_care"] = os.path.join(
            data_folder, "de", "demc", "parquet"
        )
        dct_fileloc["disability"] = os.path.join(
            data_folder, "de", "dedsb", "parquet"
        )
        dct_fileloc["mfp"] = os.path.join(
            data_folder, "de", "demfp", "parquet"
        )
        dct_fileloc["waiver"] = os.path.join(
            data_folder, "de", "dewvr", "parquet"
        )
        dct_fileloc["home_health"] = os.path.join(
            data_folder, "de", "dehsp", "parquet"
        )
    return dct_fileloc

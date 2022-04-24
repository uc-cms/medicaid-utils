import os


def get_max_parquet_loc(root, type, state, year):
    return os.path.join(
        root,
        "medicaid-max",
        "data",
        str(year),
        type.lower(),
        "parquet",
        state.upper(),
    )


def get_taf_parquet_loc(root, type, state, year):
    dct_fileloc = {}
    data_folder = os.path.join(root, "medicaid", str(year), "taf")
    if type in ["ip", "ot"]:
        dct_fileloc["header"] = os.path.join(
            data_folder, f"TAF{type.upper()}H", "parquet", state.upper()
        )
        dct_fileloc["line"] = os.path.join(
            data_folder, f"TAF{type.upper()}L", "parquet", state.upper()
        )
        dct_fileloc["occurrence_code"] = os.path.join(
            data_folder, f"TAF{type.upper()}OCCR", "parquet", state.upper()
        )
    if type == "ps":
        dct_fileloc["dates"] = os.path.join(
            data_folder, "TAFDEDTS", "parquet", state.upper()
        )
        dct_fileloc["base"] = os.path.join(
            data_folder, "TAFDEBSE", "parquet", state.upper()
        )
        dct_fileloc["managed_care"] = os.path.join(
            data_folder, "TAFDEMC", "parquet", state.upper()
        )
        dct_fileloc["disability"] = os.path.join(
            data_folder, "TAFDEDSB", "parquet", state.upper()
        )
        dct_fileloc["mfp"] = os.path.join(
            data_folder, "TAFDEMFP", "parquet", state.upper()
        )
        dct_fileloc["waiver"] = os.path.join(
            data_folder, "TAFDEWVR", "parquet", state.upper()
        )
        dct_fileloc["home_health"] = os.path.join(
            data_folder, "TAFDEHSP", "parquet", state.upper()
        )
    return dct_fileloc

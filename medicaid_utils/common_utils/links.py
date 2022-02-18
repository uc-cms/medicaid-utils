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
    data_folder = os.path.join(
        root,
        "medicaid",
        str(year),
        'taf')
    if type in ['ip', 'ot']:
        dct_fileloc['header'] = os.path.join(data_folder, f'TAF{type.upper()}H', 'parquet', state.upper())
        dct_fileloc['line'] = os.path.join(data_folder, f'TAF{type.upper()}L', 'parquet', state.upper())
        dct_fileloc['occurrence_code'] = os.path.join(data_folder, f'TAF{type.upper()}OCCR', 'parquet', state.upper())
    return dct_fileloc

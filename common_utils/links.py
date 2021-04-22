import os


def get_parquet_loc(root, type, state, year):
	return os.path.join(root, 'medicaid-max', 'data', str(year), type.lower(), 'parquet', state.upper())

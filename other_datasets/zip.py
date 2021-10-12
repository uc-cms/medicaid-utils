import os
import pandas as pd
import dask.dataframe as dd
from dask import delayed
import numpy as np
from math import ceil
import shutil
from typing import List

uds_lookup_folder = os.path.join(os.path.dirname(__file__), 'data', 'nppes')
zip_folder = os.path.join(os.path.dirname(__file__), 'data', 'zip')


def pool_zipcode_pcsa_datasets():
	"""Combine multiple sources of zipcode & PCSA datasets"""

	# Load PCSA x ZIP crosswalk from Dartmouth Atlas (https://data.world/dartmouthatlas/pcsa-data-crosswalk)
	df_pcsa = pd.read_csv(os.path.join(zip_folder, 'zip5_pcsav31.csv'), dtype=object)
	df_pcsa = df_pcsa.assign(zip5=df_pcsa['zip5'].str.replace(' ', '').str.zfill(5)).rename(columns={'zip5': 'zip'})

	# Load zipcode dataset from Simplemaps (https://simplemaps.com/data/us-zips)
	df_zips1 = pd.read_csv(os.path.join(zip_folder, 'uszips.csv'), dtype=object)
	df_zips1 = df_zips1.assign(zip=df_zips1['zip'].astype(str).str.replace(' ', '').str.zfill(5))
	df_zips1 = df_zips1[['zip', 'state_id']].rename(columns={'state_id': 'state_cd'})

	# Load zipcode dataset from geonames.org (http://download.geonames.org/export/zip/)
	df_zips2 = pd.read_csv(os.path.join(zip_folder, 'US.txt'),
	                       delimiter='\t',
	                       dtype=object, header=None,
	                       names=['countr_code', 'zip',
	                              'zip_name', 'state', 'state_cd',
	                              'county', 'fips_code', 'community_area',
	                              'community_area_code', 'lat', 'lng', 'accuracy'])
	df_zips2 = df_zips2.assign(zip=df_zips2['zip'].astype(str).str.replace(' ', '').str.zfill(5))

	df_zips_combined = pd.concat([df_zips2, df_zips1.loc[~df_zips1['zip'].isin(df_zips2['zip'])]])

	# Load zip x zcta crosswalk from UDS Mapper (https://udsmapper.org/zip-code-to-zcta-crosswalk/)
	df_zips3 = pd.read_excel(os.path.join(zip_folder, 'zip_to_zcta_2019.xlsx'), dtype=object)
	df_zips3 = df_zips3[['ZIP_CODE', 'STATE', 'ZCTA']].rename(columns={'ZIP_CODE': 'zip',
	                                                                   'STATE': 'state_cd'})
	df_zips3 = df_zips3.loc[~(df_zips3['state_cd'].isna() &
	                          (df_zips3['ZCTA'] == 'No ZCTA'))]

	df_zips3 = df_zips3.assign(zip=df_zips3['zip'].astype(str).str.replace(' ', '').str.zfill(5))

	df_zips_combined = df_zips_combined.merge(df_zips3.rename(columns={'state_cd': 'uds_state_cd'}),
	                                          on='zip', how='outer')
	df_zips_combined = df_zips_combined.assign(
		state_cd=df_zips_combined['state_cd'].where(df_zips_combined['state_cd'].notna(),
		                                            df_zips_combined['uds_state_cd']))

	df_zips_combined.to_csv(os.path.join(zip_folder, 'us_zips_multiple_sources.csv'),
	                        index=False)

	# Get PCSA x state crosswalk from HRR geographical boundaries file from Dartmouth Atlas
	# (https://data.dartmouthatlas.org/supplemental/)
	df_pcsa_ct = pd.read_csv(os.path.join(zip_folder, 'ct_pcsav31.csv'), dtype=object)
	df_pcsa_st = df_pcsa_ct[['PCSA', 'PCSA_ST']].drop_duplicates().merge(df_pcsa,
	                                                                     on='PCSA', how='left')
	df_pcsa_st = df_pcsa_st.loc[~df_pcsa_st['zip'].isna()]

	df_pcsa_st_zip = df_pcsa_st.merge(df_zips_combined, on='zip', how='outer')
	df_pcsa_st_zip = df_pcsa_st_zip.assign(state_cd=df_pcsa_st_zip['state_cd'].where(~df_pcsa_st_zip['state_cd'].isna(),
	                                                                                 df_pcsa_st_zip['PCSA_ST']))

	df_pcsa_st_zip = df_pcsa_st_zip.sort_values(['accuracy'])
	df_pcsa_st_zip = df_pcsa_st_zip.drop_duplicates('zip', keep='first')
	df_pcsa_st_zip = df_pcsa_st_zip[['PCSA', 'zip', 'state_cd', 'ZCTA', 'fips_code', 'lat', 'lng']].rename(columns={'PCSA': 'pcsa',
	                                                                                                  'ZCTA': 'zcta'})

	df_pcsa_st_zip.to_csv(os.path.join(zip_folder, "pcsa_st_zip_fips.csv"), index=False)


def generate_zip_pcsa_ruca_crosswalk():
	"""Generate zip x pcsa x ruca crosswalk"""
	df_pcsa_st_zip = pd.read_csv(os.path.join(zip_folder, "pcsa_st_zip_fips.csv"), dtype=object)
	# Load RUCA 3.1 dataset from https://www.ers.usda.gov/webdocs/DataFiles/53241/RUCA2010zipcode.xlsx?v=4790.9
	# or https://www.ers.usda.gov/webdocs/DataFiles/53241/RUCA2010zipcode.xlsx?v=8673)
	df_ruca = pd.read_excel(os.path.join(zip_folder, 'RUCA2010zipcode.xlsx'),
	                        sheet_name='Data', dtype='object')
	df_ruca = df_ruca.assign(ZIP_CODE=df_ruca['ZIP_CODE'].astype(str).str.replace(' ', '').str.zfill(5))
	df_ruca = df_ruca.rename(columns={'ZIP_CODE': 'zip',
	                                  'ZIP_TYPE': 'zip_type',
	                                  'RUCA2': 'ruca_code'})
	df_merged = df_pcsa_st_zip.merge(df_ruca[['zip', 'zip_type', 'ruca_code']], on='zip', how='outer')

	df_merged.loc[pd.to_numeric(df_merged['zip'], errors='coerce').between(1, 199, inclusive='both'),
	              'state_cd'] = 'AK'
	df_merged.loc[df_merged['zip'].str.startswith('569') & df_merged['state_cd'].isna(),
	              'state_cd'] = 'DC'
	df_merged.loc[df_merged['zip'].str.startswith('888') & df_merged['state_cd'].isna(),
	              'state_cd'] = 'DC'

	df_merged.to_csv(os.path.join(zip_folder, 'zip_state_pcsa_ruca_zcta.csv'), index=False)

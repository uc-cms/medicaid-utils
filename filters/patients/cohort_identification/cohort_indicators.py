import dask.dataframe as dd
import os
import pandas as pd
import numpy as np


def flag_rural(df_ps: dd.DataFrame, method='ruca') -> dd.DataFrame:
	"""
	Classified benes into rural/ non-rural on the basis of RUCA/ RUCC of their resident ZIP/ FIPS codes
		New Columns:
			resident_state_cd
			rural - 0/ 1/ -1, 1 when bene's residence is in a rural location, 0 when not and -1 when unknown.
			pcsa - resident PCSA code
			ruca_code - resident ruca_code
			when method='rucc':
					rucc_code - resident ruca_code
	:param df_ps:
	:param method:
	:return: dd.DataFrame
	"""
	year = df_ps.head()['MAX_YR_DT'].values[0]
	state = df_ps.head()['STATE_CD'].values[0]
	if ((year == 2012) & (state == 'RI')):
		df_ps['EL_RSDNC_ZIP_CD_LTST'] = df_ps['EL_RSDNC_ZIP_CD_LTST'].str[:-1]
	if os.path.isfile(os.path.join("data", 'zip_state_pcsa_ruca_zcta.csv')):
		df_pcsa_st_zip = pd.read_csv(os.path.join('data', 'pcsa_st_zip.csv'))
		df_ruca = pd.read_excel(os.path.join('data', 'RUCA2010zipcode.xlsx'), sheet_name='Data',
		                        dtype='object')
		df_ruca['ZIP_CODE'] = df_ruca['ZIP_CODE'].astype(str).str.replace(' ', '').str.zfill(5)
		df_ruca = df_ruca.rename(columns={'ZIP_CODE': 'zip',
		                                  'ZIP_TYPE': 'zip_type',
		                                  'RUCA2': 'ruca_code'})
		df_merged = df_pcsa_st_zip.merge(df_ruca[['zip', 'zip_type', 'ruca_code']], on='zip', how='outer')
		df_merged.rename(columns={'PCSA': 'pcsa',
		                          'ZCTA': 'zcta'},
		                 inplace=True)
		df_merged.loc[
			pd.to_numeric(df_merged['zip'], errors='coerce').between(1, 199, inclusive=True), 'state_cd'] = 'AK'
		df_merged.loc[df_merged['zip'] == '98189', 'state_cd'] = 'WA'
		df_merged.to_csv(os.path.join('data', 'zip_state_pcsa_ruca_zcta.csv'), index=False)

	df_zip_state_pcsa = pd.read_csv(os.path.join('data', 'zip_state_pcsa_ruca_zcta.csv'))
	df_zip_state_pcsa['zip'] = df_zip_state_pcsa['zip'].str.replace(' ', '').str.zfill(9)
	df_zip_state_pcsa = df_zip_state_pcsa.rename(columns={'zip': 'EL_RSDNC_ZIP_CD_LTST',
	                                                      'state_cd': 'resident_state_cd'})
	df_ps = df_ps[[col for col in df_ps.columns if col not in ['resident_state_cd',
	                                                           'pcsa',
	                                                           'ruca_code']]]
	df_ps['EL_RSDNC_ZIP_CD_LTST'] = df_ps['EL_RSDNC_ZIP_CD_LTST'].str.replace(' ', '')
	df_ps['EL_RSDNC_ZIP_CD_LTST'] = df_ps['EL_RSDNC_ZIP_CD_LTST'].str.zfill(9)  # ('^0+|\s+', '', regex=True)
	df_ps = df_ps.merge(df_zip_state_pcsa[['EL_RSDNC_ZIP_CD_LTST', 'resident_state_cd', 'pcsa',
	                                       'ruca_code']], how='left',
	                    on='EL_RSDNC_ZIP_CD_LTST')
	if method == 'ruca':
		df_ps = df_ps.map_partitions(
			lambda pdf: pdf.assign(urban=np.select(
				                       [pd.to_numeric(pdf['ruca_code'], errors='coerce').between(0, 4, inclusive=False),
				                        (pd.to_numeric(pdf['ruca_code'], errors='coerce') >= 4)],
				                       [0, 1],
				                       default=-1)))
	else:
		df_rucc = pd.read_excel(os.path.join('data', 'ruralurbancodes2013.xls'),
		                        sheet_name='Rural-urban Continuum Code 2013',
		                        dtype='object')
		df_rucc = df_rucc.rename(columns={'State': 'resident_state_cd',
		                                  'RUCC_2013': 'rucc_code',
		                                  'FIPS': 'EL_RSDNC_CNTY_CD_LTST'
		                                  })
		df_rucc['EL_RSDNC_CNTY_CD_LTST'] = df_rucc['EL_RSDNC_CNTY_CD_LTST'].str.strip().str[3:]
		df_rucc['resident_state_cd'] = df_rucc['resident_state_cd'].str.strip().str.upper()
		df_ps['EL_RSDNC_CNTY_CD_LTST'] = df_ps['EL_RSDNC_CNTY_CD_LTST'].str.strip()
		df_ps['resident_state_cd'] = df_ps['resident_state_cd'].where(~df_ps['resident_state_cd'].isna(),
		                                                              df_ps['STATE_CD'])
		df_ps = df_ps.merge(df_rucc[['EL_RSDNC_CNTY_CD_LTST', 'resident_state_cd', 'rucc_code']], how='left',
		                    on=['EL_RSDNC_CNTY_CD_LTST', 'resident_state_cd'])
		df_ps = df_ps.map_partitions(
			lambda pdf: pdf.assign(urban=np.select(
				[pd.to_numeric(pdf['rucc_code'], errors='coerce').between(1, 7, inclusive=True),
				 (pd.to_numeric(pdf['rucc_code'], errors='coerce') >= 8)],
				[0, 1],
				default=-1)))
	return df_ps



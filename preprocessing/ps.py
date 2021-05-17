import dask.dataframe as dd
import pandas as pd
import numpy as np
import os
import sys
from statistics import mode

sys.path.append('../')
from preprocessing import cms_file
from common_utils import dataframe_utils

data_folder = os.path.join(os.path.dirname(__file__), 'data')


class PS(cms_file.CMSFile):
	def __init__(self, year, st, data_root, index_col='BENE_MSIS', clean=True, preprocess=True, rural_method='ruca'):
		super(PS, self).__init__('ps', year, st, data_root, index_col, clean, preprocess)
		if clean:
			# Remove BENE_IDs that are duplicated
			self.df[f"_{self.index_col}"] = self.df.index
			self.df = self.df.drop_duplicates(subset=[index_col], keep=False)
			self.df = self.df.drop([f"_{self.index_col}"], axis=1)
		if preprocess:
			self.flag_rural(rural_method)
			self.add_eligibility_status_columns()

	def flag_rural(self, method='ruca') -> None:
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
		:return: None
		"""
		index_col = self.df.index.name
		self.df[index_col] = self.df.index
		self.df['EL_RSDNC_ZIP_CD_LTST'] = self.df['EL_RSDNC_ZIP_CD_LTST'].where(~((self.df['STATE_CD'] == 'RI') &
		                                                                      (self.df['year'] == 2012)),
		                                                                    self.df['EL_RSDNC_ZIP_CD_LTST'].str[:-1])
		if os.path.isfile(os.path.join(data_folder, 'zip_state_pcsa_ruca_zcta.csv')):
			df_pcsa_st_zip = pd.read_csv(os.path.join(data_folder, 'pcsa_st_zip.csv'))
			df_ruca = pd.read_excel(os.path.join(data_folder, 'RUCA2010zipcode.xlsx'), sheet_name='Data',
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
			df_merged.to_csv(os.path.join(data_folder, 'zip_state_pcsa_ruca_zcta.csv'), index=False)

		df_zip_state_pcsa = pd.read_csv(os.path.join(data_folder, 'zip_state_pcsa_ruca_zcta.csv'))
		df_zip_state_pcsa['zip'] = df_zip_state_pcsa['zip'].str.replace(' ', '').str.zfill(9)
		df_zip_state_pcsa = df_zip_state_pcsa.rename(columns={'zip': 'EL_RSDNC_ZIP_CD_LTST',
		                                                      'state_cd': 'resident_state_cd'})
		self.df = self.df[[col for col in self.df.columns if col not in ['resident_state_cd',
		                                                           'pcsa',
		                                                           'ruca_code']]]
		self.df['EL_RSDNC_ZIP_CD_LTST'] = self.df['EL_RSDNC_ZIP_CD_LTST'].str.replace(' ', '')
		self.df['EL_RSDNC_ZIP_CD_LTST'] = self.df['EL_RSDNC_ZIP_CD_LTST'].str.zfill(9)  # ('^0+|\s+', '', regex=True)
		self.df = self.df.merge(df_zip_state_pcsa[['EL_RSDNC_ZIP_CD_LTST', 'resident_state_cd', 'pcsa',
		                                       'ruca_code']], how='left',
		                    on='EL_RSDNC_ZIP_CD_LTST')
		df_rucc = pd.read_excel(os.path.join(data_folder, 'ruralurbancodes2013.xls'),
		                        sheet_name='Rural-urban Continuum Code 2013',
		                        dtype='object')
		df_rucc = df_rucc.rename(columns={'State': 'resident_state_cd',
		                                  'RUCC_2013': 'rucc_code',
		                                  'FIPS': 'EL_RSDNC_CNTY_CD_LTST'
		                                  })
		df_rucc['EL_RSDNC_CNTY_CD_LTST'] = df_rucc['EL_RSDNC_CNTY_CD_LTST'].str.strip().str[2:]
		df_rucc['resident_state_cd'] = df_rucc['resident_state_cd'].str.strip().str.upper()
		self.df['EL_RSDNC_CNTY_CD_LTST'] = self.df['EL_RSDNC_CNTY_CD_LTST'].str.strip()
		self.df['resident_state_cd'] = self.df['resident_state_cd'].where(~self.df['resident_state_cd'].isna(),
		                                                              self.df['STATE_CD'])
		self.df = self.df.merge(df_rucc[['EL_RSDNC_CNTY_CD_LTST', 'resident_state_cd', 'rucc_code']], how='left',
		                    on=['EL_RSDNC_CNTY_CD_LTST', 'resident_state_cd'])
		if method == 'ruca':
			self.df = self.df.map_partitions(
				lambda pdf: pdf.assign(rural=np.select(
					                       [pd.to_numeric(pdf['ruca_code'], errors='coerce').between(0, 4, inclusive=False),
					                        (pd.to_numeric(pdf['ruca_code'], errors='coerce') >= 4)],
					                       [0, 1],
					                       default=-1)))
		else:
			self.df = self.df.map_partitions(
				lambda pdf: pdf.assign(rural=np.select(
					[pd.to_numeric(pdf['rucc_code'], errors='coerce').between(1, 7, inclusive=True),
					 (pd.to_numeric(pdf['rucc_code'], errors='coerce') >= 8)],
					[0, 1],
					default=-1)))
		self.df = self.df.set_index(index_col)
		return None

	def add_eligibility_status_columns(self) -> None:
		"""
		Add eligibility columns based on MAX_ELG_CD_MO_{month} values for each month.
		MAX_ELG_CD_MO:00 = NOT ELIGIBLE, 99 = UNKNOWN ELIGIBILITY  => codes to denote ineligibility
		New Column(s):
		elg_mon_{month} - 0 or 1 value column, denoting eligibility for each month
		total_elg_mon - No. of eligible months
		elg_full_year - 0 or 1 value column, 1 if TotElMo = 12
		elg_over_9mon - 0 or 1 value column, 1 if TotElMo >= 9
		elg_over_6mon - 0 or 1 value column, 1 if TotElMo >= 6
		elg_cont_6mon - 0 or 1 value column, 1 if patient has 6 continuous eligible months
		mas_elg_change - 0 or 1 value column, 1 if patient had multiple mas group memberships during claim year
		mas_assignments -
		boe_assignments -
		dominant_boe_group -
		boe_elg_change - 0 or 1 value column, 1 if patient had multiple boe group memberships during claim year
		child_boe_elg_change - 0 or 1 value column, 1 if patient had multiple boe group memberships during claim year
		elg_change - 0 or 1 value column, 1 if patient had multiple eligibility group memberships during claim year
		eligibility_aged
		eligibility_child
		max_gap
		max_cont_enrollment
		:param dataframe df:
		:rtype: None
		"""
		self.df = self.df.map_partitions(
		lambda pdf: pdf.assign(**dict([('MAS_ELG_MON' + str(mon),
		                                pdf['MAX_ELG_CD_MO_' + str(mon)]
		                                .where(~(pdf['MAX_ELG_CD_MO_' + str(mon)].str.strip()
		                                         .isin(['00', '99', '', '.']) |
		                                pdf['MAX_ELG_CD_MO_' + str(mon)].isna()),
		                                       '99').astype(str).str.strip().str[:1]) for mon in range(1, 13)])))
		self.df = self.df.map_partitions(lambda pdf:
		            pdf.assign(mas_elg_change=(pdf[['MAS_ELG_MON' + str(mon) for mon in range(1, 13)]].replace('9', np.nan)\
		                                       .nunique(axis=1, dropna=True) > 1).astype(int),
		                       mas_assignments=pdf[['MAS_ELG_MON' + str(mon) for mon in range(1, 13)]].replace('9', '')\
		                                       .apply(lambda x: ",".join(set(','.join(x).split(","))), axis=1)))
		self.df = self.df.map_partitions(lambda pdf: pdf.assign(**dict([('BOE_ELG_MON' + str(mon),
		                                                   np.select(
		                                                       [pdf['MAX_ELG_CD_MO_' + str(mon)].astype(str).isin(
		                                                           ["11", "21", "31", "41"]),  # aged
		                                                        pdf['MAX_ELG_CD_MO_' + str(mon)].astype(str).isin(
		                                                            ["12", "22", "32", "42"]),  # blind / disabled
		                                                        pdf['MAX_ELG_CD_MO_' + str(mon)].astype(str).isin(
		                                                            ["14", "16", "24", "34", "44", "48"]),  # child
		                                                        pdf['MAX_ELG_CD_MO_' + str(mon)].astype(str).isin(
		                                                            ["15", "17", "25", "35", "3A", "45"]),  # adult
		                                                        pdf['MAX_ELG_CD_MO_' + str(mon)].astype(str).isin(
		                                                            ["51", "52", "54", "55"])],
		                                                       # state demonstration EXPANSION
		                                                       [1, 2, 3, 4, 5],
		                                                       default=6))
		                                                  for mon in range(1, 13)] +
		                                                 [('CHILD_BOE_ELG_MON' + str(mon),
		                                                   np.select(
		                                                       [pdf['MAX_ELG_CD_MO_' + str(mon)].astype(str).isin(
		                                                           ["11", "21", "31", "41"]),  # aged
		                                                           pdf['MAX_ELG_CD_MO_' + str(mon)].astype(str).isin(
		                                                               ["12", "22", "32", "42", "14", "16", "24", "34",
		                                                                "44", "48"]),  # blind / disabled OR CHILD
		                                                           pdf['MAX_ELG_CD_MO_' + str(mon)].astype(str).isin(
		                                                               ["15", "17", "25", "35", "3A", "45"]),  # adult
		                                                           pdf['MAX_ELG_CD_MO_' + str(mon)].astype(str).isin(
		                                                               ["51", "52", "54", "55"])],
		                                                       # state demonstration EXPANSION
		                                                       [1, 2, 3, 4],
		                                                       default=5))
		                                                  for mon in range(1, 13)]
		                                                 )))
		self.df = self.df.map_partitions(lambda pdf:
		                   pdf.assign(
		                       boe_elg_change=(pdf[['BOE_ELG_MON' + str(mon) for mon in range(1, 13)]]\
		                                       .replace(6, np.nan).nunique(axis=1, dropna=True) > 1).astype(int),
		                       child_boe_elg_change=(pdf[['CHILD_BOE_ELG_MON' + str(mon) for mon in range(1, 13)]]\
		                                       .replace(5, np.nan).nunique(axis=1, dropna=True) > 1).astype(int),
		                       boe_assignments=pdf[['BOE_ELG_MON' + str(mon) for mon in range(1, 13)]].astype(str)\
		                                       .apply(lambda x: ",".join(set(','.join(x).split(","))), axis=1),
		                       dominant_boe_grp=pdf[['BOE_ELG_MON' + str(mon) for mon in range(1, 13)]
		                       ].apply(lambda x: mode([y for y in x if y != 6] or [6]), axis=1).astype(int)
		                   ))
		self.df['elg_change'] = (self.df[['boe_elg_change', 'mas_elg_change']].sum(axis=1) > 0).astype(int)
		self.df['eligibility_aged'] = (self.df[['BOE_ELG_MON' + str(mon) for mon in range(1, 13)]] == 1).any(axis='columns')
		self.df['eligibility_child'] = (self.df[['BOE_ELG_MON' + str(mon) for mon in range(1, 13)]] == 3).any(axis='columns')
		self.df = self.df.map_partitions(lambda pdf: pdf.assign(**dict([('elg_mon_' + str(mon), (
		~(pdf['MAX_ELG_CD_MO_' + str(mon)].str.strip().isin(['00', '99', '', '.']) | pdf[
		    'MAX_ELG_CD_MO_' + str(mon)].isna())).astype(int))
		                                                  for mon in range(1, 13)])))
		self.df['total_elg_mon'] = self.df[['elg_mon_' + str(i) for i in range(1, 13)]].sum(axis=1)
		self.df['elg_full_year'] = (self.df['total_elg_mon'] == 12).astype(int)
		self.df['elg_over_9mon'] = (self.df['total_elg_mon'] >= 9).astype(int)
		self.df['elg_over_6mon'] = (self.df['total_elg_mon'] >= 6).astype(int)
		self.df = self.df.map_partitions(lambda pdf:
		                   pdf.assign(max_gap=pdf[['elg_mon_' + str(mon) for mon in range(1,13)]]\
		                                      .astype(int).astype(str).apply(lambda x: max(map(len,"".join(x).split('1'))),
		                                                         axis=1),
		                              max_cont_enrollment=pdf[['elg_mon_' + str(mon) for mon in range(1,13)]]\
		                                      .astype(int).astype(str).apply(lambda x: max(map(len,"".join(x).split('0'))),
		                                                         axis=1)))
		self.df['elg_cont_6mon'] = (self.df['max_cont_enrollment'] >= 6).astype(int)

		lst_cols_to_delete = ['MAS_ELG_MON' + str(mon) for mon in range(1,13)] + \
		                 ['BOE_ELG_MON' + str(mon) for mon in range(1,13)]
		self.df = self.df[[col for col in self.df.columns if col not in lst_cols_to_delete]]
		return None

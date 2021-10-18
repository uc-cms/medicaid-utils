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
other_data_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'other_datasets', 'data')


class PS(cms_file.CMSFile):
	"""Scripts to preprocess PS file"""
	def __init__(self, year: int, st: str, data_root: str, index_col: str = 'BENE_MSIS',
	             clean: bool = True, preprocess: bool = True, rural_method: str = 'ruca',
	             tmp_folder: str = None):
		"""
		Initializes PS file object by preloading and preprocessing(if opted in) the file
		:param year: Year
		:param st: State
		:param data_root: Root folder with cms data
		:param index_col: Column to use as index. Eg. BENE_MSIS or MSIS_ID. The raw file is expected to be already
		sorted with index column
		:param clean: Run cleaning routines if True
		:param preprocess: Add commonly used constructed variable columns, if True
		:param rural_method: Method to use for rural variable construction. Available options: 'ruca', 'rucc'
		:param tmp_folder: Folder to use to store temporary files
		"""
		super(PS, self).__init__('ps', year, st, data_root, index_col, False, False, tmp_folder)

		# Default filters to filter out benes that do not meet minimum standard of cleanliness criteria
		# duplicated_bene_id exclusion will remove benes with duplicated BENE_MSIS ids
		self.dct_default_filters = {'duplicated_bene_id': 0}
		if clean:
			self.clean()
		if preprocess:
			self.preprocess(rural_method)

	def clean(self):
		"""Runs cleaning routines and created common exclusion flags based on default filters"""
		super(PS, self).clean()
		self.flag_common_exclusions()
		self.df = self.cache_results()

	def preprocess(self, rural_method='ruca'):
		"""Adds rural and eligibility criteria indicator variables"""
		super(PS, self).preprocess()
		self.flag_rural(rural_method)
		self.add_eligibility_status_columns()
		self.df = self.cache_results()

	def flag_common_exclusions(self) -> None:
		"""
		Adds exclusion flags
		New Column(s):
			excl_duplicated_bene_id - 0 or 1, 1 when bene's index column is repeated
		:return:
		"""
		self.df = self.df.assign(**dict([(f"_{self.index_col}", self.df.index)]))
		# Some BENE_MSIS's are repeated in PS files. Some patients share the same BENE_ID and yet have different
		# MSIS_IDs. Some of them even have different 'dates of birth'. Since we cannot find any explanation for
		# such patterns, we decided on removing these BENE_MSIS's as per issue #29 in FARA project
		# (https://rcg.bsd.uchicago.edu/gitlab/mmurugesan/hrsa_max_feature_extraction/issues/29)
		self.df = self.df.map_partitions(
			lambda pdf: pdf.assign(
				excl_duplicated_bene_id=pdf.duplicated([f"_{self.index_col}"], keep=False).astype(int)))
		self.df = self.df.drop([f"_{self.index_col}"], axis=1)

	def flag_rural(self, method='ruca') -> None:
		"""
		Classifies benes into rural/ non-rural on the basis of RUCA/ RUCC of their resident ZIP/ FIPS codes
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
		zip_folder = os.path.join(other_data_folder, 'zip')
		self.df = self.df.assign(**dict([(index_col, self.df.index)]))

		# 2012 RI claims report zip codes have problems. They are all invalid unless the last character is dropped. So
		# dropping it as per email exchange with Alex Knitter & Dr. Laiteerapong (May 2020)
		self.df = self.df.assign(
			EL_RSDNC_ZIP_CD_LTST=self.df['EL_RSDNC_ZIP_CD_LTST'].where(~((self.df['STATE_CD'] == 'RI') &
			                                                             (self.df['year'] == 2012)),
			                                                           self.df['EL_RSDNC_ZIP_CD_LTST'].str[
			                                                           :-1]))

		# zip_state_pcsa_ruca_zcta.csv was constructed with RUCA 3.1
		# (from https://www.ers.usda.gov/webdocs/DataFiles/53241/RUCA2010zipcode.xlsx?v=8673),
		# ZCTAs x zipcode mappings from UDSMapper (https://udsmapper.org/zip-code-to-zcta-crosswalk/),
		# zipcodes from multiple sources, and distance between centroids of zipcodes using NBER data
		# (https://nber.org/distance/2016/gaz/zcta5/gaz2016zcta5centroid.csv)
		df_zip_state_pcsa = pd.read_csv(os.path.join(zip_folder, 'zip_state_pcsa_ruca_zcta.csv'),
		                                dtype=object)
		df_zip_state_pcsa = df_zip_state_pcsa.assign(zip=df_zip_state_pcsa['zip'].str.replace(' ', '').str.zfill(9))
		df_zip_state_pcsa = df_zip_state_pcsa.rename(columns={'zip': 'EL_RSDNC_ZIP_CD_LTST',
		                                                      'state_cd': 'resident_state_cd'})
		self.df = self.df[[col for col in self.df.columns if col not in ['resident_state_cd',
		                                                                 'pcsa',
		                                                                 'ruca_code']]]
		self.df = self.df.assign(EL_RSDNC_ZIP_CD_LTST=self.df['EL_RSDNC_ZIP_CD_LTST']
		                         .str.replace(' ', '').str.zfill(9))
		self.df = self.df.merge(df_zip_state_pcsa[['EL_RSDNC_ZIP_CD_LTST', 'resident_state_cd', 'pcsa',
		                                           'ruca_code']], how='left',
		                        on='EL_RSDNC_ZIP_CD_LTST')

		# RUCC codes were downloaded from https://www.ers.usda.gov/webdocs/DataFiles/53251/ruralurbancodes2013.xls?v=2372
		df_rucc = pd.read_excel(os.path.join(zip_folder, 'ruralurbancodes2013.xls'),
		                        sheet_name='Rural-urban Continuum Code 2013',
		                        dtype='object')
		df_rucc = df_rucc.rename(columns={'State': 'resident_state_cd',
		                                  'RUCC_2013': 'rucc_code',
		                                  'FIPS': 'EL_RSDNC_CNTY_CD_LTST'
		                                  })
		df_rucc = df_rucc.assign(EL_RSDNC_CNTY_CD_LTST=df_rucc['EL_RSDNC_CNTY_CD_LTST'].str.strip().str[2:],
		                         resident_state_cd=df_rucc['resident_state_cd'].str.strip().str.upper())
		self.df = self.df.assign(EL_RSDNC_CNTY_CD_LTST=self.df['EL_RSDNC_CNTY_CD_LTST'].str.strip(),
		                         resident_state_cd=self.df['resident_state_cd'].where(
			                         ~self.df['resident_state_cd'].isna(),
			                         self.df['STATE_CD']))

		self.df = self.df[[col for col in self.df.columns if col not in ['rucc_code']]]
		self.df = self.df.merge(df_rucc[['EL_RSDNC_CNTY_CD_LTST', 'resident_state_cd', 'rucc_code']], how='left',
		                        on=['EL_RSDNC_CNTY_CD_LTST', 'resident_state_cd'])
		self.df = self.df.assign(**dict([(col, dd.to_numeric(self.df[col], errors='coerce'))
		                                 for col in ['rucc_code', 'ruca_code']]))

		# RUCA codes >= 4 denote rural and the rest denote urban
		# as per https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6286055/#SD1
		# and as in FARA year 1 papers
		if method == 'ruca':
			self.df = self.df.map_partitions(
				lambda pdf: pdf.assign(rural=np.select(
					[pdf['ruca_code'].between(0, 4, inclusive='neither'),
					 (pdf['ruca_code'] >= 4)],
					[0, 1],
					default=-1)))
		else:
			# RUCC codes >= 8 denote rural and the rest denote urban
			self.df = self.df.map_partitions(
				lambda pdf: pdf.assign(rural=np.select(
					[pdf['rucc_code'].between(1, 7, inclusive='both'),
					 (pdf['rucc_code'] >= 8)],
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
			elg_full_year - 0 or 1 value column, 1 if total_elg_mon = 12
			elg_over_9mon - 0 or 1 value column, 1 if total_elg_mon >= 9
			elg_over_6mon - 0 or 1 value column, 1 if total_elg_mon >= 6
			elg_cont_6mon - 0 or 1 value column, 1 if patient has 6 continuous eligible months
			mas_elg_change - 0 or 1 value column, 1 if patient had multiple mas group memberships during claim year
			mas_assignments - comma separated list of MAS assignments
			boe_assignments - comma separated list of BOE assignments
			dominant_boe_group - BOE status held for the most number of months
			boe_elg_change - 0 or 1 value column, 1 if patient had multiple boe group memberships during claim year
			child_boe_elg_change - 0 or 1 value column, 1 if patient had multiple boe group memberships during claim year
			elg_change - 0 or 1 value column, 1 if patient had multiple eligibility group memberships during claim year
			eligibility_aged - Eligibility as aged anytime during the claim year
			eligibility_child - Eligibility as child anytime during the claim year
			max_gap - Maximum gap in enrollment in months
			max_cont_enrollment - Maximum duration of continuous enrollment
		:param dataframe df:
		:rtype: None
		"""
		# MAS & BOE groups are arrived at from MAX_ELG_CD variable
		# (https://resdac.org/sites/datadocumentation.resdac.org/files/MAX%20UNIFORM%20ELIGIBILITY%20CODE%20TABLE.txt)
		# FARA year 2 peds paper decided to collapse BOE assignments, combining disabled and child groups
		self.df = self.df.map_partitions(
			lambda pdf: pdf.assign(**dict([(f'MAS_ELG_MON_{mon}',
			                                pdf[f'MAX_ELG_CD_MO_{mon}']
			                                .where(~(pdf[f'MAX_ELG_CD_MO_{mon}'].str.strip()
			                                         .isin(['00', '99', '', '.']) |
			                                         pdf[f'MAX_ELG_CD_MO_{mon}'].isna()),
			                                       '99').astype(str).str.strip().str[:1]) for mon in range(1, 13)])))
		# TODO: Try to eliminate the use of apply
		self.df = self.df.map_partitions(lambda pdf:
		                                 pdf.assign(mas_elg_change=(
					                                 pdf[[f'MAS_ELG_MON_{mon}' for mon in range(1, 13)]].replace(
						                                 '9', np.nan).nunique(axis=1, dropna=True) > 1).astype(int),
		                                            mas_assignments=pdf[
			                                            [f'MAS_ELG_MON_{mon}' for mon in range(1, 13)]].replace('9', '') \
		                                            .apply(lambda x: ",".join(set(','.join(x).split(","))), axis=1)))
		self.df = self.df.map_partitions(lambda pdf: pdf.assign(**dict([(f'BOE_ELG_MON_{mon}',
		                                                                 np.select(
			                                                                 [pdf[f'MAX_ELG_CD_MO_{mon}']
				                                                                 .astype(str).isin(
				                                                                 ["11", "21", "31", "41"]),  # aged
				                                                                 pdf[f'MAX_ELG_CD_MO_{mon}']
					                                                                 .astype(str).isin(
					                                                                 ["12", "22", "32", "42"]),
				                                                                 # blind / disabled
				                                                                 pdf[f'MAX_ELG_CD_MO_{mon}']
					                                                                 .astype(str).isin(
					                                                                 ["14", "16", "24", "34", "44",
					                                                                  "48"]),  # child
				                                                                 pdf[f'MAX_ELG_CD_MO_{mon}']
					                                                                 .astype(str).isin(
					                                                                 ["15", "17", "25", "35", "3A",
					                                                                  "45"]),  # adult
				                                                                 pdf[f'MAX_ELG_CD_MO_{mon}']
					                                                                 .astype(str).isin(
					                                                                 ["51", "52", "54", "55"])],
			                                                                 # state demonstration EXPANSION
			                                                                 [1, 2, 3, 4, 5],
			                                                                 default=6))
		                                                                for mon in range(1, 13)] +
		                                                               [(f'CHILD_BOE_ELG_MON_{mon}',
		                                                                 np.select(
			                                                                 [pdf[f'MAX_ELG_CD_MO_{mon}']
				                                                                 .astype(str).isin(
				                                                                 ["11", "21", "31", "41"]),  # aged
				                                                                 pdf[f'MAX_ELG_CD_MO_{mon}']
					                                                                 .astype(str).isin(
					                                                                 ["12", "22", "32", "42", "14",
					                                                                  "16", "24", "34",
					                                                                  "44", "48"]),
				                                                                 # blind / disabled OR CHILD
				                                                                 pdf[f'MAX_ELG_CD_MO_{mon}']
					                                                                 .astype(str).isin(
					                                                                 ["15", "17", "25", "35", "3A",
					                                                                  "45"]),  # adult
				                                                                 pdf[f'MAX_ELG_CD_MO_{mon}']
					                                                                 .astype(str).isin(
					                                                                 ["51", "52", "54", "55"])],
			                                                                 # state demonstration EXPANSION
			                                                                 [1, 2, 3, 4],
			                                                                 default=5))
		                                                                for mon in range(1, 13)]
		                                                               )))
		self.df = self.df.map_partitions(lambda pdf:
		                                 pdf.assign(
			                                 boe_elg_change=(pdf[[f'BOE_ELG_MON_{mon}' for mon in range(1, 13)]]\
			                                                 .replace(6, np.nan).nunique(axis=1,
			                                                                             dropna=True) > 1).astype(int),
			                                 child_boe_elg_change=(
						                                 pdf[[f'CHILD_BOE_ELG_MON_{mon}' for mon in range(1, 13)]]\
						                                 .replace(5, np.nan).nunique(axis=1, dropna=True) > 1).astype(
				                                 int),
			                                 boe_assignments=pdf[
				                                 [f'BOE_ELG_MON_{mon}' for mon in range(1, 13)]].astype(str)\
				                                 .apply(lambda x: ",".join(set(','.join(x).split(","))), axis=1),
			                                 dominant_boe_grp=pdf[[f'BOE_ELG_MON_{mon}' for mon in range(1, 13)]]
				                                 .replace(6, np.nan).mode(axis=1, dropna=True)[0].fillna(6).astype(int)
		                                 ))

		self.df = self.df.assign(elg_change=(self.df[['boe_elg_change', 'mas_elg_change']].sum(axis=1) > 0).astype(int),
		                         eligibility_aged=(self.df[[f'BOE_ELG_MON_{mon}' for mon in range(1, 13)]] == 1).any(
			axis='columns'),
		                         eligibility_child=(self.df[[f'BOE_ELG_MON_{mon}' for mon in range(1, 13)]] == 3).any(
			axis='columns'))

		self.df = self.df.map_partitions(lambda pdf: pdf.assign(**dict([(f'elg_mon_{mon}', (
			~(pdf[f'MAX_ELG_CD_MO_{mon}'].str.strip().isin(['00', '99', '', '.']) | pdf[
				f'MAX_ELG_CD_MO_{mon}'].isna())).astype(int))
		                                                                for mon in range(1, 13)])))
		self.df = self.df.assign(total_elg_mon=self.df[[f'elg_mon_{i}' for i in range(1, 13)]].sum(axis=1),
		                         elg_full_year=(self.df['total_elg_mon'] == 12).astype(int),
		                         elg_over_9mon=(self.df['total_elg_mon'] >= 9).astype(int),
		                         elg_over_6mon=(self.df['total_elg_mon'] >= 6).astype(int))

		self.df = self.df.map_partitions(lambda pdf:
		                                 pdf.assign(max_gap=pdf[[f'elg_mon_{mon}' for mon in range(1, 13)]]\
		                                            .astype(int).astype(str).apply(
			                                 lambda x: max(map(len, "".join(x).split('1'))),
			                                 axis=1),
		                                            max_cont_enrollment=pdf[
			                                            ['elg_mon_' + str(mon) for mon in range(1, 13)]]\
		                                            .astype(int).astype(str).apply(
			                                            lambda x: max(map(len, "".join(x).split('0'))),
			                                            axis=1)))
		self.df = self.df.assign(elg_cont_6mon=(self.df['max_cont_enrollment'] >= 6).astype(int))

		lst_cols_to_delete = [f'MAS_ELG_MON_{mon}' for mon in range(1, 13)] + \
		                     [f'BOE_ELG_MON_{mon}' for mon in range(1, 13)]
		self.df = self.df.drop(lst_cols_to_delete, axis=1)
		return None

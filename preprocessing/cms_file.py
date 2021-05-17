import dask.dataframe as dd
import os
import errno
import sys
import numpy as np
import pandas as pd

sys.path.append('../../')
from common_utils import links, dataframe_utils


class CMSFile():
	def __init__(self, ftype, year, st, data_root, index_col='BENE_MSIS', clean=True, preprocess=True):
		self.fileloc = links.get_parquet_loc(data_root, ftype, st, year)
		self.ftype = ftype
		self.index_col = index_col
		self.year = year
		self.st = st
		if not os.path.exists(self.fileloc):
			raise FileNotFoundError(
				errno.ENOENT, os.strerror(errno.ENOENT), self.fileloc)
		self.df = dd.read_parquet(self.fileloc, index=False,
		                          engine='fastparquet').set_index(index_col)
		if clean:
			self.process_date_cols()
			self.add_gender()

	def export(self, dest_folder, format='csv'):
		self.df.to_csv(os.path.join(dest_folder, f'{self.ftype}_{self.year}_{self.st}.csv'), index=True, single_file=True)

	def add_gender(self) -> None:
		"""
		Adds integer 'female' column for PS file, based on 'EL_SEX_CD' column
		:param DataFrame df: Patient Summary
		:rtype: None
		"""
		self.df['female'] = (self.df.EL_SEX_CD == 'F').astype(int)
		self.df['female'] = self.df['female'].where((self.df.EL_SEX_CD == 'F') |
		                                  (self.df.EL_SEX_CD == 'M'), -1)
		return None

	def clean_diag_codes(self) -> None:
		self.df = self.df.map_partitions(
			lambda pdf: pdf.assign(**dict([(col,
			                                pdf[col].str.replace("[^a-zA-Z0-9]+", "", regex=True).str.upper())
			                               for col in pdf.columns if col.startswith('DIAG_CD_')])))
		return None

	def clean_proc_codes(self) -> None:
		self.df = self.df.map_partitions(
			lambda pdf: pdf.assign(**dict([(col,
			                                pdf[col].str.replace("[^a-zA-Z0-9]+", "", regex=True).str.upper())
			                               for col in pdf.columns
			                               if col.startswith('PRCDR_CD') and (not col.startswith('PRCDR_CD_SYS'))])))
		return None

	def process_date_cols(self) -> dd.DataFrame:
		"""Convert list of columns specified in a dataframe to datetime type
			New columns:
	            birth_year, birth_month, birth_day - date compoments of EL_DOB (date of birth)
	            death - 0 or 1
	            age - age in years, integer format
	            age_decimal - age in years, with decimals
	            age_day - age in days
	            adult - 0 or 1, 1 when patient's age is in [18,115]
	        if ftype == 'ip':
	            Clean/ impute admsn_date and add ip duration related columns
				New column(s):
				    admsn - 0 or 1, 1 when admsn_date is not null
				    flag_admsn_miss - 0 or 1, 1 when admsn_date was imputed
				    los - ip service duration in days
				    ageday_admsn - age in days as on admsn_date
				    age_admsn - age in years, with decimals, as on admsn_date
			if ftype == 'ot':
				Adds duration column, provided service end and begin dates are clean
				New Column(s):
		        diff & duration - duration of service in days
		        age_day_admsn - age in days as on admsn_date
		        age_admsn - age in years, with decimals, as on admsn_date
	        :param df: dd.DataFrame
	        :param str: CMS file type, allowed options: ['ip', 'lt', 'ot', 'ps', 'rx']
	        :rtype: DataFrame
	    """
		self.df = self.df.assign(**dict([('year', self.df[col].astype(int)) for col in ['MAX_YR_DT', 'YR_NUM']
		                                 if col in self.df.columns]))
		dct_date_col = {'EL_DOB': 'birth_date',
		                'ADMSN_DT': 'admsn_date',
		                'SRVC_BGN_DT': 'srvc_bgn_date',
		                'SRVC_END_DT': 'srvc_end_date',
		                'EL_DOD': 'date_of_death',
		                'MDCR_DOD': 'medicare_date_of_death'}
		for date_col in [col for col in dct_date_col if col not in self.df.columns]:
			dct_date_col.pop(date_col, None)
				
		df = self.df.assign(**dict([(dct_date_col[col],
		                        self.df[col]) for col in dct_date_col]))
		# converting lst_col columns to datetime type
		lst_col_to_convert = [dct_date_col[col] for col in dct_date_col.keys() if
		                      (df[[dct_date_col[col]]].select_dtypes(include=[np.datetime64]).shape[1] == 0)]
		df = dataframe_utils.convert_ddcols_to_datetime(df, lst_col_to_convert)
		df = df.assign(birth_year=df.birth_date.dt.year,
		               birth_month=df.birth_date.dt.month,
		               birth_day=df.birth_date.dt.day)
		df = df.assign(age=df.year - df.birth_year)
		df = df.assign(age=df['age'].where(df['age'].between(0, 115, inclusive=True), np.nan))
		df = df.assign(age_day=(dd.to_datetime((df.year * 10000 + 12 * 100 + 31).apply(str),
		                                       format='%Y%m%d') - df.birth_date).dt.days,
		               age_decimal=(dd.to_datetime((df.year * 10000 + 12 * 100 + 31).apply(str),
		                                           format='%Y%m%d') - df.birth_date) / np.timedelta64(1, 'Y'))
		df['adult'] = df['age'].between(18, 115, inclusive=True).astype(int)
		df['child'] = df['age'].between(0, 17, inclusive=True).astype(int)
		df['adult'] = df['adult'].where(~(df['age'].isna()), np.nan)
		df = df.map_partitions(lambda pdf: pdf.assign(adult=pdf.groupby(pdf.index)['adult'].transform(max),
		                                              age=pdf.groupby(pdf.index)['age'].transform(max),
		                                              age_day=pdf.groupby(pdf.index)['age_day'].transform(max),
		                                              age_decimal=pdf.groupby(pdf.index)['age_decimal'].transform(max)))
		if 'date_of_death' in df.columns:
			df['death'] = ((df.date_of_death.dt.year.fillna(df.year + 10).astype(int) <= df.year) |
			               (df.medicare_date_of_death.dt.year.fillna(df.year + 10).astype(int) <= df.year)).astype(int)

		if self.ftype == 'ip':
			df['admsn'] = (~df['admsn_date'].isnull()).astype(int)  # can be corrected to get more admissions
			df['flag_admsn_miss'] = 0
			df['flag_admsn_miss'] = df['flag_admsn_miss'].where(~df['admsn_date'].isnull(), 1)
			df['admsn_date'] = df['admsn_date'].where(~df['admsn_date'].isnull(), df['srvc_bgn_date'])

			df['los'] = np.nan
			mask_los = ((df['year'] >= df['admsn_date'].dt.year) &
			            # 	            (df_ip['srvc_end_date'].dt.year == df_ip['year']) &
			            (df['admsn_date'] <= df['srvc_end_date']))
			df['los'] = df['los'].where(~mask_los, (df['srvc_end_date'] - df['admsn_date']).dt.days + 1)

			df['age_day_admsn'] = (df['admsn_date'] - df['birth_date']).dt.days
			df['age_admsn'] = (df['age_day_admsn'].fillna(0) / 365.25).astype(int)

		if self.ftype == 'ot':
			df['diff'] = (df['srvc_end_date'] - df['srvc_bgn_date']).dt.days
			df['duration'] = np.nan
			duration_mask = (df['srvc_bgn_date'] <= df['srvc_end_date'])
			df['duration'] = df['duration'].where(~duration_mask,
			                                      (df['srvc_end_date'] - df['srvc_bgn_date']).dt.days)
			df['age_day_admsn'] = (df['srvc_bgn_date'] - df['birth_date']).dt.days
			df['age_admsn'] = (df['age_day_admsn'].fillna(0) / 365.25).astype(int)

		return df

	def calculate_payment(self) -> None:
		"""
	    Calculates payment amount (inplace)
	    New Column(s):
	        pymt_amt - name of payment amount column
	    :param DataFrame df: Claims data
	    :param str payment_col_name:
	    :rtype: None
	    """
		# cost
		# MDCD_PYMT_AMT=TOTAL AMOUNT OF MONEY PAID BY MEDICAID FOR THIS SERVICE
		# TP_PYMT_AMT=TOTAL AMOUNT OF MONEY PAID BY A THIRD PARTY
		# CHRG_AMT: we never use charge amount for cost analysis
		if self.ftype in ['ot', 'rx', 'ip']:
			self.df = self.df.map_partitions(lambda pdf: pdf.assign(
				pymt_amt=pdf[['MDCD_PYMT_AMT', 'TP_PYMT_AMT']].apply(pd.to_numeric, errors='coerce').sum(axis=1)))
		return None

	def flag_ed_use(self) -> None:
		"""
			Detects ed use in claims
			New Column(s):
			ed_cpt - 0 or 1, Claim has a procedure billed in ED code range (99281–99285)
						(PRCDR_CD_SYS_{1-6} == 01 & PRCDR_CD_{1-6} in (99281–99285))
			ed_ub92 - 0 or 1, Claim has a revenue center codes (0450 - 0459, 0981) - UB_92_REV_CD_GP_{1-23}
			ed_tos - 0 or 1, Claim has an outpatient type of service (MAX_TOS = 11) (if ftype == 'ip')
			ed_pos - 0 or 1, Claim has PLC_OF_SRVC_CD set to 23 (if ftype == 'ot')
			ed_use - any of ed_cpt, ed_ub92, ed_tos or ed_pos is 1
			any_ed - 0 or 1, 1 when any other claim from the same visit has ed_use set to 1 (if ftype == 'ot')
			:param df_ip:
			:rtype: None
			"""
		# reference: If the patient is a Medicare beneficiary, the general surgeon should bill the level of
		# ED code (99281–99285). http://bulletin.facs.org/2013/02/coding-for-hospital-admission
		self.df = self.df.map_partitions(
			lambda pdf: pdf.assign(**dict([('ed_cpt',
			                                np.column_stack(
				                                [pdf[col].str.startswith(('99281', '99282', '99283',
				                                                          '99284', '99285',), na=False)
				                                 for col in pdf.columns
				                                 if col.startswith((f'PRCDR_CD',)) and
				                                 (not col.startswith((f'PRCDR_CD_SYS',)))]).any(axis=1).astype(int))])))
		if self.ftype == 'ip':
			# Inpatient files:  Revenue Center Codes 0450-0459, 0981,
			# https://www.resdac.org/resconnect/articles/144
			# TOS - Type of Service
			# 11=outpatient hospital ???? not every IP which called outpatient hospital is called ED,
			# this may end up with too many ED
			self.df = self.df.map_partitions(
				lambda pdf: pdf.assign(**dict([('ed_ub92',
				                                np.column_stack([pd.to_numeric(pdf[col],
				                                                               errors='coerce').isin([450, 451, 452,
				                                                                                      453, 454, 455,
				                                                                                      456, 457, 458,
				                                                                                      459, 981])
				                                                 for col in pdf.columns
				                                                 if col.startswith('UB_92_REV_CD_GP_')])
				                                .any(axis=1).astype(int)),
				                               ('ed_tos',
				                                (pd.to_numeric(pdf['MAX_TOS'], errors='coerce') == 11).astype(int))])))

			self.df['ed_use'] = self.df[['ed_ub92', 'ed_cpt', 'ed_tos']].any(axis='columns').astype(int)
		if self.ftype == 'ot':
			# UB92: # ??????? 450,451,452,453,454,455,456,457,458,459,981 ????????
			self.df = self.df.map_partitions(
				lambda pdf: pdf.assign(**dict([('ed_ub92',
				                                pd.to_numeric(pdf['UB_92_REV_CD']).isin([450, 451, 452,
				                                                                         456, 459, 981]).astype(int)),
				                               ('ed_pos',
				                                (pd.to_numeric(pdf['PLC_OF_SRVC_CD'], errors='coerce') == 23).astype(
					                                int))])))
			self.df['ed_use'] = self.df[['ed_pos', 'ed_cpt', 'ed_ub92']].any(axis='columns').astype(int)
			# check ED use in other claims from the same visit
			self.df = self.df.map_partitions(lambda pdf: pdf.assign(any_ed=pdf.groupby([pdf.index,
			                                                                  'srvc_bgn_date'])['ed_use'].transform(
				'max')))
		return None









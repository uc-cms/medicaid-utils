import numpy as np
import pandas as pd
import sys

sys.path.append('../../')
from medicaid_utils.preprocessing import max_file
from medicaid_utils.common_utils import dataframe_utils


class MAXIP(max_file.MAXFile):
	def __init__(self, year, st, data_root, index_col='BENE_MSIS', clean=True, preprocess=True, tmp_folder=None):
		super(MAXIP, self).__init__('ip', year, st, data_root, index_col, False, False, tmp_folder)
		self.dct_default_filters = {'missing_dob': 0, 'duplicated': 0}
		if clean:
			self.clean()

		if preprocess:
			self.preprocess()

	def clean(self):
		super(MAXIP, self).clean()
		self.clean_diag_codes()
		self.clean_proc_codes()
		self.flag_common_exclusions()
		self.flag_duplicates()

	def preprocess(self):
		super(MAXIP, self).preprocess()
		self.calculate_payment()
		self.flag_ed_use()
		self.flag_ip_overlaps()

	def flag_common_exclusions(self) -> None:
		self.df = self.df.map_partitions(
			lambda pdf: pdf.assign(excl_missing_dob=pdf['birth_date'].isnull().astype(int),
			                       excl_missing_admsn_date=pdf['admsn_date'].isnull().astype(int),
			                       excl_missing_prncpl_proc_date=pdf['prncpl_proc_date'].isnull().astype(int),
			                       excl_encounter_claim=((pd.to_numeric(pdf['PHP_TYPE'], errors='coerce') == 77) |
			                                             ((pd.to_numeric(pdf['PHP_TYPE'], errors='coerce') == 88) &
			                                              (pd.to_numeric(pdf['TYPE_CLM_CD'], errors='coerce') == 3))
			                                             ).astype(int),
			                       excl_capitation_claim=((pd.to_numeric(pdf['PHP_TYPE'], errors='coerce') == 88) &
			                                              (pd.to_numeric(pdf['TYPE_CLM_CD'], errors='coerce') == 2)
			                                              ).astype(int),
			                       excl_ffs_claim=(~((pd.to_numeric(pdf['PHP_TYPE'], errors='coerce') == 77) |
			                                         ((pd.to_numeric(pdf['PHP_TYPE'], errors='coerce') == 88) &
			                                          pd.to_numeric(pdf['TYPE_CLM_CD'], errors='coerce').isin([2, 3]))
			                                         )).astype(int),
			                       excl_delivery=(pd.to_numeric(pdf['RCPNT_DLVRY_CD'], errors='coerce') == 1).astype(
				                       int),
			                       excl_female=(pdf['female'] == 1).astype(int)
			                       ))

	def flag_duplicates(self):
		self.df = dataframe_utils.fix_index(self.df, self.index_col, True)
		self.df = self.df.map_partitions(
			lambda pdf: pdf.assign(
				excl_duplicated=pdf.assign(_index_col=pdf.index)[[col for col in pdf.columns
				                                                  if col != 'excl_duplicated']]
					.duplicated(keep='first').astype(int)))

	def flag_ip_overlaps(self) -> None:
		"""
		Identifies duplicate/ overlapping claims.
		When several/ overlapping claims exist with the same MSIS_ID, claim with the largest payment amount is retained.
		New Column(s):
		    flag_ip_undup - 0 or 1, 1 when row is not a duplicate
		    flag_ip_dup_drop - 0 or 1, 1 when row is duplicate and must be dropped
		    flag_ip_overlap_drop - 0 or 1, 1 when row overlaps with another claim
		    ip_incl - 0 or 1, 1 when row is clean (flag_ip_dup_drop = 0 & flag_ip_overlap_drop = 0) and has los > 0
		:param df dd.DataFrame:
		:rtype: None
		"""

		df_flagged = self.df.copy()
		df_flagged[self.index_col] = df_flagged.index
		df_flagged['flag_ip_dup_drop'] = np.nan
		df_flagged['flag_ip_undup'] = np.nan
		df_flagged['admsntime'] = np.nan
		df_flagged['next_admsn_date'] = pd.to_datetime(np.nan, errors='coerce')
		df_flagged['last_admsn_date'] = pd.to_datetime(np.nan, errors='coerce')
		df_flagged['next_pymt_amt'] = np.nan
		df_flagged['last_pymt_amt'] = np.nan
		df_flagged['next_srvc_end_date'] = pd.to_datetime(np.nan, errors='coerce')
		df_flagged['last_srvc_end_date'] = pd.to_datetime(np.nan, errors='coerce')

		def _mptn_check_ip_overlaps(pdf_partition):
			pdf_partition.reset_index(drop=True, inplace=True)
			# check duplicate claims (same ID, admission date), flag the largest payment amount
			pdf_partition = pdf_partition.sort_values(by=[self.index_col, 'admsn_date', 'pymt_amt', 'los'],
			                                          ascending=[True, True, False, False])
			pdf_partition['flag_ip_dup_drop'] = (pdf_partition.groupby([self.index_col, 'admsn_date',
			                                                            'los'])['pymt_amt']
			                                     .rank(method='first', ascending=False) != 1).astype(int)
			pdf_partition['flag_ip_undup'] = (pdf_partition.groupby([self.index_col,
			                                                         'admsn_date'])[self.index_col].transform(
				'count') == 1).astype(int)
			overlap_mask = ((pd.to_numeric(pdf_partition['los'], errors='coerce') > 0) &
			                (pdf_partition['flag_ip_dup_drop'] != 1))
			pdf_partition.loc[overlap_mask,
			                  'admsntime'] = pdf_partition.loc[overlap_mask,
			].groupby(self.index_col)['admsn_date'].rank(method='dense')
			pdf_partition = pdf_partition.sort_values(by=[self.index_col, 'admsntime'])
			pdf_partition.loc[overlap_mask,
			                  'next_admsn_date'] = pdf_partition.loc[overlap_mask,
			].groupby(self.index_col)['admsn_date'].shift(-1)
			pdf_partition.loc[overlap_mask,
			                  'next_pymt_amt'] = pdf_partition.loc[overlap_mask,
			].groupby(self.index_col)['pymt_amt'].shift(-1)
			pdf_partition.loc[overlap_mask,
			                  'next_srvc_end_date'] = pdf_partition.loc[overlap_mask,
			].groupby(self.index_col)['srvc_end_date'].shift(-1)
			pdf_partition.loc[overlap_mask,
			                  'last_admsn_date'] = pdf_partition.loc[overlap_mask,
			].groupby(self.index_col)['admsn_date'].shift(1)
			pdf_partition.loc[overlap_mask,
			                  'last_pymt_amt'] = pdf_partition.loc[overlap_mask,
			].groupby(self.index_col)['pymt_amt'].shift(1)
			pdf_partition.loc[overlap_mask,
			                  'last_srvc_end_date'] = pdf_partition.loc[overlap_mask,
			].groupby(self.index_col)['srvc_end_date'].shift(1)
			pdf_partition = pdf_partition.sort_values(by=[self.index_col, 'admsn_date', 'pymt_amt'])
			pdf_partition.set_index(self.index_col, drop=False, inplace=True)
			return pdf_partition

		df_flagged = df_flagged.map_partitions(_mptn_check_ip_overlaps, meta=df_flagged.head(1))
		df_flagged['flag_overlap_next'] = (df_flagged['next_admsn_date'].notnull() & (
				df_flagged['srvc_end_date'] > df_flagged['next_admsn_date'])).astype(int)
		df_flagged['flag_overlap_last'] = (df_flagged['last_admsn_date'].notnull() & (
				df_flagged['admsn_date'] < df_flagged['last_srvc_end_date'])).astype(int)
		df_flagged['flag_overlap_drop'] = (((df_flagged['flag_overlap_next'] == 1) &
		                                    (df_flagged['pymt_amt'] < df_flagged['next_pymt_amt'])) |
		                                   ((df_flagged['flag_overlap_last'] == 1) &
		                                    (df_flagged['pymt_amt'] <= df_flagged['last_pymt_amt'])
		                                    )
		                                   ).astype(int)
		df_flagged['ip_incl'] = ((df_flagged['los'].astype(float) > 0) & (df_flagged['flag_ip_dup_drop'] != 1) & (
				df_flagged['flag_overlap_drop'] != 1)).astype(int)

		df_flagged = dataframe_utils.fix_index(df_flagged, self.index_col, True)
		self.df = df_flagged
		return None







import dask.dataframe as dd
import numpy as np
import pandas as pd
import sys

sys.path.append('../')
from preprocessing import all_claims
from common_utils import dataframe_utils


def ip_process_date_cols(df_ip: dd.DataFrame) -> dd.DataFrame:
	"""
	Clean/ impute admsn_date and add ip duration related columns
	New column(s):
	    admsn - 0 or 1, 1 when admsn_date is not null
	    flag_admsn_miss - 0 or 1, 1 when admsn_date was imputed
	    los - ip service duration in days
	    ageday_admsn - age in days as on admsn_date
	    age_admsn - age in years, with decimals, as on admsn_date
	:param df_ip:
	:rtype: DataFrame
	"""
	df_ip = all_claims.process_date_cols(df_ip)
	df_ip['admsn'] = (~df_ip['admsn_date'].isnull()).astype(int)  # can be corrected to get more admissions
	df_ip['flag_admsn_miss'] = 0
	df_ip['flag_admsn_miss'] = df_ip['flag_admsn_miss'].where(~df_ip['admsn_date'].isnull(), 1)
	df_ip['admsn_date'] = df_ip['admsn_date'].where(~df_ip['admsn_date'].isnull(), df_ip['srvc_bgn_date'])

	df_ip['los'] = np.nan
	mask_los = ((df_ip['year'] >= df_ip['admsn_date'].dt.year) &
# 	            (df_ip['srvc_end_date'].dt.year == df_ip['year']) & 
                (df_ip['admsn_date'] <= df_ip['srvc_end_date']))
	df_ip['los'] = df_ip['los'].where(~mask_los, (df_ip['srvc_end_date'] - df_ip['admsn_date']).dt.days + 1)

	df_ip['ageday_admsn'] = (df_ip['admsn_date'] - df_ip['birth_date']).dt.days
	df_ip['age_admsn'] = (df_ip['ageday_admsn'].fillna(0) / 365.25).astype(int)
	return df_ip


def ip_ed_use(df_ip: dd.DataFrame) -> dd.DataFrame:
	"""
	Detects ed use in ip claims
	New Column(s):
	ip_ed_cpt - 0 or 1, Claim has a procedure billed in ED code range (99281–99285)
				(PRCDR_CD_SYS_{1-6} == 01 & PRCDR_CD_{1-6} in (99281–99285))
	ip_ed_ub92 - 0 or 1, Claim has a revenue center codes (0450 - 0459, 0981) - UB_92_REV_CD_GP_{1-23}
	ip_ed_tos - 0 or 1, Claim has an outpatient type of service (MAX_TOS = 11)
	ip_ed - any of ip_ed_cpt, ip_ed_ub92 or ip_ed_tos is 1
	:param df_ip:
	:rtype: DataFrame
	"""
	# reference: If the patient is a Medicare beneficiary, the general surgeon should bill the level of
	# ED code (99281–99285). http://bulletin.facs.org/2013/02/coding-for-hospital-admission
	df_ip = df_ip.map_partitions(
		lambda pdf: pdf.assign(**dict([('ip_ed_cpt_' + str(i),
		                                ((pd.to_numeric(pdf['PRCDR_CD_SYS_' + str(i)],
									           errors='coerce') == 1) &
		                                 (pd.to_numeric(pdf['PRCDR_CD_' + str(i)],
									           errors='coerce').isin(
									[99281, 99282, 99283, 99284, 99285]))).astype(int)
									 ) for i in range(1, 7)])))
	df_ip['ip_ed_cpt'] = (df_ip[['ip_ed_cpt_' + str(i) for i in range(1, 7)]].sum(axis=1) > 0).astype(int)
	df_ip = df_ip[[col for col in df_ip.columns if col not in ['ip_ed_cpt_' + str(i) for i in range(1, 7)]]]
	# Inpatient files:  Revenue Center Codes 0450-0459, 0981,
	# https://www.resdac.org/resconnect/articles/144
	df_ip['ip_ed_ub92'] = df_ip.map_partitions(
		lambda pdf: pdf[['UB_92_REV_CD_GP_' + str(i) for i in range(1, 24)]]
			.apply(pd.to_numeric, errors='coerce').isin([450, 451, 452, 453, 454, 455,
		                                                 456, 457, 458, 459, 981]
		                                                ).astype(int).any(axis='columns').astype(int))
	# TOS - Type of Service
	# 11=outpatient hospital ???? not every IP which called outpatient hospital is called ED,
	# this may end up with too many ED
	df_ip['ip_ed_tos'] = df_ip.map_partitions(
			lambda pdf: (pd.to_numeric(pdf['MAX_TOS'], errors='coerce') == 11).astype(int))
	df_ip['ip_ed'] = df_ip[['ip_ed_ub92', 'ip_ed_cpt', 'ip_ed_tos']].any(axis='columns').astype(int)
	return df_ip


def flag_ip_duplicates(df: dd.DataFrame, index_col: str = 'MSIS_ID') -> dd.DataFrame:
	"""
	Identifies duplicate/ overlapping claims.
	When several/ overlapping claims exist with the same MSIS_ID, claim with the largest payment amount is retained.
	New Column(s):
	    flag_ip_undup - 0 or 1, 1 when row is not a duplicate
	    flag_ip_dup_drop - 0 or 1, 1 when row is duplicate and must be dropped
	    flag_ip_overlap_drop - 0 or 1, 1 when row overlaps with another claim
	    ip_incl - 0 or 1, 1 when row is clean (flag_ip_dup_drop = 0 & flag_ip_overlap_drop = 0) and has los > 0
	:param df dd.DataFrame:
	:rtype: dd.DataFrame
	"""

	df_flagged = df.copy()
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
		pdf_partition = pdf_partition.sort_values(by=[index_col, 'admsn_date', 'pymt_amt', 'los'],
	                                                        ascending=[True, True, False, False])
		pdf_partition['flag_ip_dup_drop'] = (pdf_partition.groupby([index_col, 'admsn_date'])['pymt_amt']
	                                                .rank(method='first', ascending=False) != 1).astype(int)
		pdf_partition['flag_ip_undup'] = (pdf_partition.groupby([index_col,
	                                                                        'admsn_date'])[index_col].transform(
	                                                                            'count') == 1).astype(int)
		overlap_mask = ((pd.to_numeric(pdf_partition['los'], errors='coerce') > 0) &
		                (pdf_partition['flag_ip_dup_drop'] != 1))
		pdf_partition.loc[overlap_mask,
		                  'admsntime'] = pdf_partition.loc[overlap_mask,
		                                                  ].groupby(index_col)['admsn_date'].rank(method='dense')
		pdf_partition = pdf_partition.sort_values(by=[index_col, 'admsntime'])
		pdf_partition.loc[overlap_mask,
		                  'next_admsn_date'] = pdf_partition.loc[overlap_mask,
		                                                        ].groupby(index_col)['admsn_date'].shift(-1)
		pdf_partition.loc[overlap_mask,
		                  'next_pymt_amt'] = pdf_partition.loc[overlap_mask,
		                                                      ].groupby(index_col)['pymt_amt'].shift(-1)
		pdf_partition.loc[overlap_mask,
		                  'next_srvc_end_date'] = pdf_partition.loc[overlap_mask,
		                                                           ].groupby(index_col)['srvc_end_date'].shift(-1)
		pdf_partition.loc[overlap_mask,
		                  'last_admsn_date'] = pdf_partition.loc[overlap_mask,
		                                                        ].groupby(index_col)['admsn_date'].shift(1)
		pdf_partition.loc[overlap_mask,
		                  'last_pymt_amt'] = pdf_partition.loc[overlap_mask,
		                                                      ].groupby(index_col)['pymt_amt'].shift(1)
		pdf_partition.loc[overlap_mask,
		                  'last_srvc_end_date'] = pdf_partition.loc[overlap_mask,
		                                                           ].groupby(index_col)['srvc_end_date'].shift(1)
		pdf_partition = pdf_partition.sort_values(by=[index_col, 'admsn_date', 'pymt_amt'])
		pdf_partition.set_index(index_col, drop=False, inplace=True)
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

	df_flagged = dataframe_utils.fix_index(df_flagged, index_col, False)
	return df_flagged

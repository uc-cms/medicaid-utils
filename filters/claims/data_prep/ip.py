import dask.dataframe as dd
import numpy as np
import pandas as pd


def ip_duration(df_ip: dd.DataFrame) -> dd.DataFrame:
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
	year = df_ip.head()['MAX_YR_DT'].values[0]
	df_ip['admsn'] = (~df_ip['admsn_date'].isnull()).astype(int)  # can be corrected to get more admissions
	df_ip['flag_admsn_miss'] = 0
	df_ip['flag_admsn_miss'] = df_ip['flag_admsn_miss'].where(~df_ip['admsn_date'].isnull(), 1)
	df_ip['admsn_date'] = df_ip['admsn_date'].where(~df_ip['admsn_date'].isnull(), df_ip['srvc_bgn_date'])

	df_ip['los'] = np.nan
	mask_los = ((df_ip['admsn_date'].dt.year - year).between(-1, 0, inclusive=True) &
	            (df_ip['srvc_end_date'].dt.year == year) & (df_ip['admsn_date'] <= df_ip['srvc_end_date']))
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
		lambda pdf: pdf[['UB_92_REV_CD_GP_' + str(i) for i in range(1, 24)]
							].apply(pd.to_numeric, errors='coerce').isin([450, 451, 452, 453, 454, 455,
		                                                                  456, 457, 458, 459, 981]
		                                                                 ).astype(int).any(axis='columns').astype(int))
	# TOS - Type of Service
	# 11=outpatient hospital ???? not every IP which called outpatient hospital is called ED,
	# this may end up with too many ED
	df_ip['ip_ed_tos'] = df_ip.map_partitions(
			lambda pdf: (pd.to_numeric(pdf['MAX_TOS'], errors='coerce') == 11).astype(int))
	df_ip['ip_ed'] = df_ip[['ip_ed_ub92', 'ip_ed_cpt', 'ip_ed_tos']].any(axis='columns').astype(int)
	return df_ip

import dask.dataframe as dd
import pandas as pd


def flag_chronic_conditions(df_cc: dd.DataFrame) -> dd.DataFrame:
	"""
	Adds boolean columns that denote presence of chronic conditions.
	 New Columns:
	    - any cardiac comorbidity
		diab_combined- diabetes
		hypten_combined- hypertension
		ckd_combined - chronic kidney disease
		depr_combined- depression
		copd_combined - COPD
		toba_combined - tobacco use

	:param df_cc:
	:return: dd.DataFrame
	"""
	lst_conditions = ['diab', 'hypten', 'depr', 'depsn', 'ckd', 'copd', 'obesity', 'toba']
	df_cc = df_cc.map_partitions(
		lambda pdf: pdf.assign(**dict([(condn + '_combined',
		                                pdf[[condn.upper() + '_MEDICAID',
	                                         condn.upper() + '_MEDICARE',
	                                         condn.upper() + '_COMBINED']].apply(pd.to_numeric, errors='coerce')
		                                .isin([1, 3])
		                                .any(axis='columns').astype(int)) for condn in lst_conditions])))
	df_cc = df_cc[[condn + '_combined' for condn in lst_conditions]]
	return df_cc

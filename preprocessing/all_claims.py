import dask.dataframe as dd
import sys
import numpy as np
import pandas as pd
from calendar import isleap
from datetime import datetime

sys.path.append('../')
from common_utils import dataframe_utils


def add_gender(df: dd.DataFrame) -> dd.DataFrame:
	"""
	Adds integer 'female' column for PS file, based on 'EL_SEX_CD' column
	:param DataFrame df: Patient Summary
	:rtype: None
	"""
	df['female'] = (df.EL_SEX_CD == 'F').astype(int)
	df['female'] = df['female'].where((df.EL_SEX_CD == 'F') |
	                                  (df.EL_SEX_CD == 'M'), -1)
	return df


def clean_diag_codes(df: dd.DataFrame) -> dd.DataFrame:
	return df.map_partitions(
		lambda pdf: pdf.assign(**dict([(col,
		                                pdf[col].str.replace("[^a-zA-Z0-9]+", "", regex=True).str.upper())
		                               for col in pdf.columns if col.startswith('DIAG_CD_')])))


def process_date_cols(df: dd.DataFrame) -> dd.DataFrame:
	"""Convert list of columns specified in a dataframe to datetime type,
       renames them
       w.r.t. claim years (before, current, after)
       New columns:
            EL_yr, EL_mon, EL_day - date compoments of EL_DOB (date of birth)
            death - year of death (from EL_DOD); -1 if has not died yet
            age - age in years, integer format
            age2 - age in years, with decimals
            ageday - age in days
            adult - 0 or 1, 1 when patient's age is in [18,115]
        :param df: dd.DataFrame
        :rtype: DataFrame
    """
	df = df.assign(**dict([('year', df[col].astype(int)) for col in ['MAX_YR_DT', 'YR_NUM'] if col in df.columns]))
	dct_date_col = {'EL_DOB': 'birth_date',
	                'ADMSN_DT': 'admsn_date',
	                'SRVC_BGN_DT': 'srvc_bgn_date',
	                'SRVC_END_DT': 'srvc_end_date',
	                'EL_DOD': 'date_of_death',
	                'MDCR_DOD': 'medicare_date_of_death'}
	dct_date_col = dict([(col, dct_date_col[col]) for col in dct_date_col.keys()
	                     if col in df.columns])
	df = df.assign(**dict([(dct_date_col[col],
	                        df[col]) for col in dct_date_col]))
	# converting lst_col columns to datetime type
	lst_col_to_convert = [dct_date_col[col] for col in dct_date_col.keys() if
	                      (df[[dct_date_col[col]]].select_dtypes(include=[np.datetime64]).shape[1] == 0)]
	df = dataframe_utils.convert_ddcols_to_datetime(df, lst_col_to_convert)
	df = df.assign(EL_yr = df.birth_date.dt.year,
	               EL_mon = df.birth_date.dt.month,
	               EL_day = df.birth_date.dt.day)
	df = df.assign(age = df.year - df.EL_yr)
	df = df.assign(age = df['age'].where(df['age'].between(0, 115, inclusive=True), np.nan))
	df = df.assign(ageday= (dd.to_datetime((df.year*10000+12*100+31).apply(str),format='%Y%m%d') - df.birth_date).dt.days,
	               age2 = (dd.to_datetime((df.year*10000+12*100+31).apply(str),format='%Y%m%d') - df.birth_date)/np.timedelta64(1, 'Y'))
	df['adult'] = df['age'].between(18, 115, inclusive=True).astype(int)
	df['child'] = df['age'].between(0, 17, inclusive=True).astype(int)
	df['adult'] = df['adult'].where(~(df['age'].isna()), np.nan)
	df = df.map_partitions(lambda pdf: pdf.assign(adult=pdf.groupby(pdf.index)['adult'].transform(max),
	                                              age=pdf.groupby(pdf.index)['age'].transform(max),
	                                              ageday=pdf.groupby(pdf.index)['ageday'].transform(max),
	                                              age2=pdf.groupby(pdf.index)['age2'].transform(max)))
	if 'date_of_death' in df.columns:
		df['death'] = ((df.date_of_death.dt.year.fillna(df.year + 10).astype(int) <= df.year) |
		               (df.medicare_date_of_death.dt.year.fillna(df.year + 10).astype(int) <= df.year)).astype(int)
	return df


def calculate_payment(df: dd.DataFrame) -> None:
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
	df = df.map_partitions(lambda pdf: pdf.assign(
		pymt_amt=pdf[['MDCD_PYMT_AMT', 'TP_PYMT_AMT']].apply(pd.to_numeric, errors='coerce').sum(axis=1)))
	return df

import dask.dataframe as dd
import sys
import numpy as np
from calendar import isleap
from datetime import datetime

sys.path.append('../../../')
from common_utils import dataframe_utils

def clean_diag_codes(df: dd.DataFrame) -> dd.DataFrame:
	return df.map_partitions(
		lambda pdf: pdf.assign(**dict([(col,
		                                pdf[col].str.replace("[^a-zA-Z0-9]+", "").str.upper())
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
	year = df.head()['MAX_YR_DT'].values[0]
	dct_date_col = {'EL_DOB': 'birth_date',
	                'ADMSN_DT': 'admsn_date',
	                'SRVC_BGN_DT': 'srvc_bgn_date',
	                'SRVC_END_DT': 'srvc_end_date',
	                'EL_DOD': 'date_of_death',
	                'MDCR_DOD': 'medicare_date_of_death'}
	df = df.assign(**dict([(dct_date_col[col],
	                        df[col]) for col in dct_date_col if col in df.columns]))
	# converting lst_col columns to datetime type
	lst_col_to_convert = [dct_date_col[col] for col in dct_date_col.keys() if (df[[dct_date_col[col]]].select_dtypes(include=[np.datetime64]).shape[1] == 0)]
	df = dataframe_utils.convert_ddcols_to_datetime(df, lst_col_to_convert)
	df['EL_yr'] = df.birth_date.dt.year
	df['EL_mon'] = df.birth_date.dt.month
	df['EL_day'] = df.birth_date.dt.day
	df['age'] = year - df.EL_yr
	df['age'] = df['age'].where(df['age'].between(0, 115, inclusive=True), np.nan)
	df['ageday'] = (datetime(year=year, month=12, day=31) - df.birth_date).dt.days
	series_datediff = datetime(year=year, month=12, day=31) - df.birth_date.map_partitions(
			lambda x: x.apply(lambda y: y.replace(year=year, day=28) if (isleap(y.year) and
			                                                             (y.day == 29) and
			                                                             (y.month == 2) and not
			                                                             isleap(year)) else y.replace(year=year)))
	df['age2'] = df['age'] + (series_datediff.dt.days + series_datediff.dt.seconds / 86400.0) / (
		(isleap(year) and 366) or 365)
	df['adult'] = df['age'].between(18, 115, inclusive=True).astype(int)
	df['child'] = df['age'].between(0, 17, inclusive=True).astype(int)
	df['adult'] = df['adult'].where(~(df['age'].isna()), np.nan)
	df = df.map_partitions(lambda pdf: pdf.assign(adult=pdf.groupby(pdf.index)['adult'].transform(max),
		                                          age=pdf.groupby(pdf.index)['age'].transform(max),
		                                          ageday=pdf.groupby(pdf.index)['ageday'].transform(max),
		                                          age2=pdf.groupby(pdf.index)['age2'].transform(max)))
	return df

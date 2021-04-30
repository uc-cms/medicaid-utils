#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import dask.dataframe as dd
import sys
import os
import csv
from typing import List
import logging
import pyarrow as pa

sys.path.append('../')
from common_utils import recipes


def toggle_datetime_string(df: dd.DataFrame, lst_datetime_col, to_string: bool = True) -> None:
	"""
	Toggles data columnes in the passed dataframe to string/ datatime types. Inplace updates.
	:param df: dask dataframe
	:param to_string bool: True to convert to string, False otherwise
	:return: None
	"""
	for col in df.columns:
		if col in lst_datetime_col:
			if to_string:
				df[col] = df[col].dt.strftime('%Y%m%d').replace('NaT', '').fillna('')
			else:
				df[col] = df[col].map_partitions(lambda x: pd.to_datetime(x, format='%Y%m%d', errors='coerce'))


def convert_ddcols_to_datetime(df: dd.DataFrame, lst_col) -> dd.DataFrame:
	"""Convert list of columns specified in a dataframe to datetime type
			:param pandas_df df: dataframe
			:param list(str) lst_col: list of column names
			:rtype: None
	"""
	df = df.map_partitions(lambda pdf: pdf.assign(**dict([(col,
	                                                       pd.to_datetime(pdf[col],
	                                                                      format='%Y%m%d',
	                                                                      errors='coerce')
	                                                       )
	                                                      for col in lst_col])))
	return df


def copy_ddcols(df: dd.DataFrame, lst_col: List[str], lst_new_names: List[str]):
	df = df.map_partitions(lambda pdf: pdf.assign(**dict([(lst_new_names[i],
	                                                       pdf[lst_col[i]]
	                                                       )
	                                                      for i in range(len(lst_col))])))
	return df


def get_reduced_column_names(multiidx_df_columns, combine_levels=False):
	return [(i[1] or i[0]) if not combine_levels else (i[1] + '_' + i[0]) for i in multiidx_df_columns]


def sas_to_pandas(filename: str):
	df = pd.read_sas(filename)
	lst_float_cols = df.select_dtypes(include=[np.float]).columns.tolist()
	for col in df.columns:
		if col not in lst_float_cols:
			df[col] = df[col].str.decode('utf-8')

	return df


def _value_counts_chunk(s):
	return s.value_counts()


def _value_counts_agg(s):
	s = s._selected_obj
	return s.groupby(level=list(range(s.index.nlevels))).sum()


def _value_counts_finalize(s):
	return s


dask_groupby_value_counts = dd.Aggregation('value_counts', _value_counts_chunk, _value_counts_agg,
                                           _value_counts_finalize)


def safe_convert_int_to_str(df: dd.DataFrame, lst_col) -> dd.DataFrame:
	df = df.map_partitions(lambda pdf: pdf.assign(**dict([(col,
	                                                       pdf[col].where(~pdf[col].isna(),
	                                                                      ''
	                                                                      ).astype(str) \
	                                                       .replace(['nan', 'None'], '')
	                                                       .apply(lambda x: str(int(float(x)))
	                                                       if (recipes.is_number(x) &
	                                                           (('.' in x) |
	                                                            ('e' in x) |
	                                                            ('E' in x)
	                                                            ) &
	                                                           (x != '')
	                                                           )
	                                                       else
	                                                       x
	                                                              )
	                                                       ) for col in lst_col]
	                                                     )
	                                              )
	                       )
	return df


def fix_index(df: dd.DataFrame, index_name: str, drop_column=True) -> dd.DataFrame:
	if (df.index.name != index_name) | (df.divisions[0] is None):
		if index_name not in df.columns:
			if df.index.name != index_name:
				raise ValueError('{0} not in dataframe'.format(index_name))
			df[index_name] = df.index
		df = df.set_index(index_name, drop=drop_column)
	else:
		if not drop_column:
			df[index_name] = df.index
		else:
			df = df[[col for col in df.columns if col != index_name]]
	return df


def prepare_dtypes_for_csv(df_temp: dd.DataFrame, df_schema: pd.DataFrame):
	df_temp = df_temp.map_partitions(
		lambda pdf: pdf.assign(**dict([(col, pdf[col].where(~pdf[col].isna(), '').astype(str)) \
		                               for col in df_schema.loc[df_schema['python_type']
		                              .isin(['int', 'str'])]['name'].tolist()
		                               ]
		                              )))
	df_temp = df_temp.map_partitions(lambda pdf: pdf.assign(**dict([(col, pdf[col].replace(['nan', 'None'], '')) \
	                                                                for col in df_schema.loc[df_schema['python_type']
	                                                               .isin(['int', 'str'])]['name'].tolist()
	                                                                ]
	                                                               )))

	df_temp = df_temp.map_partitions(lambda pdf: pdf.assign(**dict([(col,
	                                                                 pdf[col].apply(lambda x: str(int(float(x)))
	                                                                 if (recipes.is_number(x))
	                                                                 else
	                                                                 x
	                                                                                )
	                                                                 ) for col in df_schema.loc[df_schema['python_type']
	                                                               .isin(['int', 'str'])]['name'].tolist()
	                                                                ]
	                                                               )
	                                                        )
	                                 )

	df_temp = df_temp.map_partitions(lambda pdf: pdf.assign(**dict([(col,
	                                                                 pd.to_numeric(pdf[col], errors='coerce')) for col
	                                                                in
	                                                                df_schema.loc[
		                                                                df_schema['python_type'].isin(['float'])][
		                                                                'name'].tolist()])))
	return df_temp


def groupby_agg_list_string_cols(groupeddf, input_col_name, output_col_name):
	dct_col = {output_col_name: ",".join(set([i for sub_x in groupeddf[input_col_name] for i in sub_x if
	                                          ((i == i) & (i not in ["", "None", None, np.nan]))]))}
	return pd.Series(dct_col)


def groupby_agg_concat_string_col(groupeddf, input_col_name, output_col_name):
	dct_col = {output_col_name: ",".join(set([str(i) for i in groupeddf[input_col_name] if
	                                          ((i == i) & (i not in ["", "None", None, np.nan]))]))}
	return pd.Series(dct_col)


def export(df: dd.DataFrame, pq_engine: str, output_filename: str, pq_location: str, csv_location: str,
           lst_datetime_col: List[str],
           is_dask: bool = True, n_rows: int = -1, do_csv: bool = True, df_schema: pd.DataFrame = pd.DataFrame,
           logger_name: str = "Dataframe utils", rewrite: bool = False, do_parquet: bool = True) -> None:
	"""
	:param df:
	:param output_filename:
	:param pq_location:
	:param is_dask:
	:return:
	"""
	# df = df.persist()
	logger = logging.getLogger(logger_name)
	if n_rows < 0:
		n_rows = df.map_partitions(lambda pdf: pdf.shape[0]).compute().sum()
	if df.head().shape[0] < 1:
		df = df.repartition(npartitions=min(max(int(n_rows / 100000), 1), max(df.npartitions - 2, 1))).persist()
	else:
		df = df.repartition(partition_size="100MB").persist()#df.repartition(npartitions=max(int(n_rows / 100000), df.npartitions)).persist()
	toggle_datetime_string(df, lst_datetime_col, to_string=True)
	if is_dask:
		if do_parquet:
			os.umask(int("007", base=8))
			os.makedirs(pq_location, exist_ok=True, mode=int('2770', base=8))
			os.chmod(pq_location, mode=int('2770', base=8))
			if not rewrite:
				recipes.remove_ignore_if_not_exists(pq_location)
			if pq_engine == 'pyarrow':

				logger.info("NROWS: {0}({1})".format(str(n_rows), pq_location))
				df_sample = df.sample(frac=min(1, 250000 / max(n_rows, 250000)), replace=False,
				                      random_state=100).compute()
				lst_null_cols = df_sample.columns[df_sample.isnull().all(axis=0)] if (n_rows > 0) else []
				df_sample = df_sample.assign(**dict([(col, df_sample[col].astype('float'))
				                                     for col in lst_null_cols if (col not in lst_datetime_col)] +
				                                    [(col, df_sample[col].fillna('').astype(str))
				                                     for col in lst_null_cols if (col in lst_datetime_col)]
				                                    ))
				# df_sample.to_csv(os.path.join(csv_location, 'sample.csv'), index=False)
				schema_sample = pa.Schema.from_pandas(df_sample.reset_index(drop=True))
				df.to_parquet(pq_location + ('_temp' if (rewrite == True) else ''),
				              # compression='xz',
				              engine=pq_engine,
				              write_index=False,
				              schema=schema_sample
				              )
				if rewrite:
					recipes.remove_ignore_if_not_exists(pq_location)
					os.rename(pq_location + '_temp', pq_location)
			else:
				df.to_parquet(pq_location,
				              # compression='xz',
				              engine=pq_engine,
				              write_index=False,
				              )
		if do_csv:
			df_formatted = prepare_dtypes_for_csv(df, df_schema)[
				df_schema.sort_values(by='sr_no')['name'].tolist()]
			recipes.remove_ignore_if_not_exists(output_filename)
			# df_formatted.to_csv(output_filename, single_file=True, quoting=csv.QUOTE_NONNUMERIC, quotechar='"',
			#                     float_format='%f', index=False)
			df_formatted.to_csv(output_filename.replace('.csv', '.sas.csv'), single_file=True,  # header=False,
			                    quoting=csv.QUOTE_NONNUMERIC, quotechar='"',
			                    float_format='%f', index=False)

	else:
		df.to_parquet(
			os.path.join(pq_location, os.path.basename(os.path.normpath(output_filename)) + '.parquet.gzip'),
			compression='gzip', index=False)
		if do_csv:
			df.to_csv(output_filename, header=True, index=False)
	toggle_datetime_string(df, lst_datetime_col, to_string=False)


def get_first_day_gap(df, index_col, time_col, start_date_col, threshold):
	df = df.sort_values(by=[index_col, time_col])
	df = df.assign(gap_dur=df.groupby(index_col)[time_col].diff().dt.days.fillna(0),
	               prev_care_date=df.groupby(index_col)[time_col].shift())
	df = df.assign(gap_gt_threshold=(df['gap_dur'] >= threshold).astype(int))
	df = df.loc[df['prev_care_date'] >= df[start_date_col]]
	df = df.loc[df['gap_gt_threshold'] == 1]
	df = df.assign(gap_start_date=df.groupby(index_col)['prev_care_date'].transform('min'))
	df = df[[index_col, 'gap_start_date']].drop_duplicates().reset_index(drop=True)
	return df
import os
import pandas as pd
import dask.dataframe as dd
from dask import delayed
import numpy as np
from math import ceil
import shutil
from typing import List

nppes_lookup_folder = os.path.join(os.path.dirname(__file__), 'data', 'nppes')
fara_folder = os.path.join(os.path.dirname(__file__), 'data', 'fara')


def wide_to_long_nppes_taxonomy_file(year: int, pq_engine: str) -> None:
	"""
	Creates long format provider and taxonomy mapping datasets from NPI file. Two versions of provider
	mapping files are created, one of which has the provider ids cleaned by removing non alphanumeric characters
	:param year: year
	:param pq_engine: Parquet engine, fastparquet or pyarrow
	:return: None
	"""
	df_npi = dd.read_parquet(os.path.join(nppes_lookup_folder, str(year), 'npi_cleaned_parquet'),
	                         engine=pq_engine, index=False)
	df_npis = df_npi.to_delayed()

	def _mptn_wide_to_long(pdf, lst_id_col, lst_value_col, lst_nonnull_col):
		pdf = pdf.replace(to_replace=[None], value=np.nan)
		pdf = pdf.drop_duplicates(lst_id_col, keep='first')
		pdf = pd.wide_to_long(pdf,
		                      lst_value_col,
		                      lst_id_col,
		                      's_no', sep='_').reset_index() \
			.dropna(subset=lst_nonnull_col) \
			.reset_index(drop=True)
		pdf = pdf[['s_no'] + lst_id_col + lst_value_col]
		if pdf.shape[0] == 0:
			pdf = pd.DataFrame(columns=['s_no'] + lst_id_col + lst_value_col)
		return pdf

	lstlst_id_col = [['npi', 'replacement_npi', 'entity', 'p_loc_state', 'p_loc_zip'],
	                 ['npi', 'replacement_npi', 'entity', 'p_org_name', 'p_org_name_oth', 'p_mail_line_1',
	                  'p_mail_line_2',
	                  'p_mail_city', 'p_mail_state', 'p_mail_zip', 'p_loc_line_1', 'p_loc_line_2',
	                  'p_loc_city', 'p_loc_state', 'p_loc_zip'],
	                 ['npi', 'replacement_npi', 'entity', 'p_loc_state', 'p_loc_zip']]
	lstlst_value_col = [['p_tax_code', 'p_lic_state', 'p_prim_tax'],
	                    ['oth_pid', 'oth_pid_type', 'oth_pid_st', 'oth_pid_iss'],
	                    ['p_tax_group']]
	lstlst_nonnull_col = [['p_tax_code'], ['oth_pid'], ['p_tax_group']]

	lstlst_id_col = [[col for col in lst if col in df_npi.columns] for lst in lstlst_id_col]

	lsttpl_df = [(delayed(_mptn_wide_to_long)(df, lstlst_id_col[0], lstlst_value_col[0], lstlst_nonnull_col[0]),
	              delayed(_mptn_wide_to_long)(df, lstlst_id_col[1], lstlst_value_col[1], lstlst_nonnull_col[1]),
	              delayed(_mptn_wide_to_long)(df, lstlst_id_col[2], lstlst_value_col[2], lstlst_nonnull_col[2]))
	             for df in df_npis]

	lsttpl_df = dd.compute(*lsttpl_df)

	df_taxonomy = pd.concat([tpl[0] for tpl in lsttpl_df])
	df_taxonomy['export_year'] = year
	df_taxonomy = df_taxonomy.rename(columns={'p_tax_code': 'taxonomy_code',
	                                          'p_lic_state': 'taxonomy_state',
	                                          'p_prim_tax': 'is_primary_taxonomy'})

	df_taxonomy = df_taxonomy.set_index('npi')
	n_rows = df_taxonomy.shape[0]
	shutil.rmtree(os.path.join(nppes_lookup_folder, str(year), 'npi_taxonomy_parquet'),
	              ignore_errors=True)
	dd.from_pandas(df_taxonomy, npartitions=max(ceil(n_rows / 50000), 1)).to_parquet(
		os.path.join(nppes_lookup_folder, str(year), 'npi_taxonomy_parquet'),
		engine='fastparquet',
		write_index=True)
	del df_taxonomy

	df_pid = pd.concat([tpl[1] for tpl in lsttpl_df])
	df_pid = df_pid.rename(columns={'oth_pid': 'provider_id',
	                                'oth_pid_type': 'provider_id_type',
	                                'oth_pid_st': 'provider_id_state',
	                                'oth_pid_iss': 'provider_id_issuer'})
	df_pid['export_year'] = year
	n_rows = df_pid.shape[0]
	df_pid = df_pid.set_index('npi')
	df_pid = dd.from_pandas(df_pid, npartitions=max(ceil(n_rows / 50000), 1))
	shutil.rmtree(os.path.join(nppes_lookup_folder, str(year), 'npi_provider_parquet'),
	              ignore_errors=True)
	df_pid.to_parquet(os.path.join(nppes_lookup_folder, str(year), 'npi_provider_parquet'),
	                  engine='fastparquet',
	                  write_index=True)

	df_pid['provider_id'] = df_pid['provider_id'].str.replace("[^a-zA-Z0-9]+", "", regex=True).str.upper()
	df_pid['provider_id_type'] = df_pid['provider_id_type'].astype(int)
	shutil.rmtree(os.path.join(nppes_lookup_folder, str(year), 'npi_provider_parquet_cleaned'),
	              ignore_errors=True)
	df_pid.to_parquet(os.path.join(nppes_lookup_folder, str(year), 'npi_provider_parquet_cleaned'),
	                  engine='fastparquet',
	                  write_index=True)
	del df_pid

	df_tax_group = df_pid = pd.concat([tpl[2] for tpl in lsttpl_df])
	del lsttpl_df
	df_tax_group = df_tax_group.rename(columns={'p_tax_group': 'taxonomy_group'})
	df_tax_group['export_year'] = year
	n_rows = df_tax_group.shape[0]
	df_tax_group = df_tax_group.set_index('npi')
	shutil.rmtree(os.path.join(nppes_lookup_folder, str(year), 'npi_taxonomy_group_parquet'),
	              ignore_errors=True)
	dd.from_pandas(df_tax_group, npartitions=max(ceil(n_rows / 50000), 1)).to_parquet(
		os.path.join(nppes_lookup_folder, str(year), 'npi_taxonomy_group_parquet'),
		engine='fastparquet',
		write_index=True)


def generate_npi_taxonomy_mappings(year: int, pq_engine: str = 'fastparquet') -> None:
	"""
	Generates taxonomy type x NPI mappings for the set of taxonomy types in taxonomy codes lookup file
	(taxonomy_codes.csv)
	:param year:
	:param pq_engine: Parquet engine, fastparquet or pyarrow
	:return:
	"""
	pdf_taxonomy = pd.read_csv(os.path.join(fara_folder, 'taxonomy_codes.csv'))
	pdf_taxonomy = pdf_taxonomy.assign(lst_cd=pdf_taxonomy['lst_cd'].str.split(","))
	dct_taxonomy = pdf_taxonomy.set_index('taxonomy').T.to_dict('records')[0]

	df_npi_taxonomy = dd.read_parquet(os.path.join(nppes_lookup_folder, str(year), 'npi_taxonomy_parquet'),
	                                  engine=pq_engine, index=False)

	df_npi_taxonomy = df_npi_taxonomy.assign(**dict([(f'taxonomy_{taxonomy}',
	                                                  df_npi_taxonomy['taxonomy_code'].isin(
		                                                  dct_taxonomy[taxonomy]).astype(int))
	                                                 for taxonomy in pdf_taxonomy['taxonomy']]))
	lst_id_col = ['npi', 'replacement_npi', 'entity', 'p_loc_state', 'taxonomy_state', 'is_primary_taxonomy']
	lst_id_col = [col for col in lst_id_col if col in df_npi_taxonomy.columns]
	lst_taxonomy_col = [f'taxonomy_{taxonomy}' for taxonomy in dct_taxonomy]
	df_npi_taxonomy = df_npi_taxonomy[lst_id_col + lst_taxonomy_col]
	df_npi_taxonomy = df_npi_taxonomy.rename(columns={'taxonomy_state': 'licensing_state'})
	df_npi_taxonomy = df_npi_taxonomy.loc[df_npi_taxonomy[lst_taxonomy_col].sum(axis=1) > 0]

	df_npi_primary_taxonomy = df_npi_taxonomy.loc[df_npi_taxonomy['is_primary_taxonomy'].str.strip() == 'Y'].compute()
	df_npi_primary_taxonomy.to_pickle(os.path.join(nppes_lookup_folder, str(year),
	                                               'npi_primary_taxonomies.pickle'))
	del df_npi_primary_taxonomy

	df_npi_taxonomy = df_npi_taxonomy.loc[df_npi_taxonomy['is_primary_taxonomy'].str.strip() != 'Y'].compute()
	df_npi_taxonomy.to_pickle(os.path.join(nppes_lookup_folder, str(year),
	                                       'npi_secondary_taxonomies.pickle'))
	df_npi_taxonomy_group = dd.read_parquet(
		os.path.join(nppes_lookup_folder, str(year), 'npi_taxonomy_group_parquet'),
		engine=pq_engine, index=False)
	df_npi_taxonomy_group = df_npi_taxonomy_group.assign(**dict([(f'taxonomy_{taxonomy}',
	                                                              df_npi_taxonomy_group['taxonomy_group'].str.contains(
		                                                              "|".join(
			                                                              dct_taxonomy[taxonomy])).astype(int))
	                                                             for taxonomy in pdf_taxonomy['taxonomy']]))

	lst_id_col = ['npi', 'replacement_npi', 'entity', 'p_loc_state']
	lst_id_col = [col for col in lst_id_col if col in df_npi_taxonomy_group.columns]
	df_npi_taxonomy_group = df_npi_taxonomy_group[lst_id_col + lst_taxonomy_col]
	df_npi_taxonomy_group = df_npi_taxonomy_group.loc[df_npi_taxonomy_group[lst_taxonomy_col].sum(axis=1) > 0].compute()
	df_npi_taxonomy_group.to_pickle(os.path.join(nppes_lookup_folder, str(year),
	                                             'npi_taxonomy_groups.pickle'))


def cleanup_raw_npi_files(lst_year: List[int], pq_engine: str = 'fastparquet') -> None:
	"""
	Cleans up raw NPPES files for a list of years by standardizing column names, generating long format mappings
	from the flat raw file and creating NPI mappings for a known set of taxonomies
	:param lst_year:
	:param pq_engine: Parquet engine, fastparquet or pyarrow
	:return:
	"""
	for year in lst_year:
		df_npi = dd.read_csv(os.path.join(nppes_lookup_folder, str(year), 'npi-raw.csv'), dtype=object,
		                     blocksize="20MB")
		df_npi = df_npi.replace(to_replace=[None], value=np.nan)
		df_npi = df_npi.rename(columns={**{'NPI': 'npi',
		                                   'Replacement NPI': 'replacement_npi',
		                                   "Entity Type Code": 'entity',
		                                   'Provider Organization Name (Legal Business Name)': 'p_org_name',
		                                   'Provider Other Organization Name': 'p_org_name_oth',
		                                   'Provider Other Organization Name Type Code': 'p_org_name_oth_type',
		                                   'Provider First Line Business Mailing Address': 'p_mail_line_1',
		                                   'Provider Second Line Business Mailing Address': 'p_mail_line_2',
		                                   'Provider Business Mailing Address City Name': 'p_mail_city',
		                                   'Provider Business Mailing Address State Name': 'p_mail_state',
		                                   'Provider Business Mailing Address Postal Code': 'p_mail_zip',
		                                   'Provider First Line Business Practice Location Address': 'p_loc_line_1',
		                                   'Provider Second Line Business Practice Location Address': 'p_loc_line_2',
		                                   'Provider Business Practice Location Address City Name': 'p_loc_city',
		                                   'Provider Business Practice Location Address State Name': 'p_loc_state',
		                                   'Provider Business Practice Location Address Postal Code': 'p_loc_zip',
		                                   },
		                                **{'porgname': 'p_org_name',
		                                   'porgnameoth': 'p_org_name_oth',
		                                   'pmailline1': 'p_mail_line_1',
		                                   'pmailline2': 'p_mail_line_2',
		                                   'pmailcityname': 'p_mail_city',
		                                   'pmailstatename': 'p_mail_state',
		                                   'pmailzip': 'p_mail_zip',
		                                   'plocline1': 'p_loc_line_1',
		                                   'plocline2': 'p_loc_line_2',
		                                   'ploccityname': 'p_loc_city',
		                                   'plocstatename': 'p_loc_state',
		                                   'ploczip': 'p_loc_zip'},
		                                **dict([(col, col.replace('Healthcare Provider Taxonomy Code_', 'p_tax_code_'))
		                                        for col in df_npi.columns if
		                                        col.startswith('Healthcare Provider Taxonomy Code_')] +
		                                       [(col, col.replace('Healthcare Provider Primary Taxonomy Switch_',
		                                                          'p_prim_tax_'))
		                                        for col in df_npi.columns if
		                                        col.startswith('Healthcare Provider Primary Taxonomy Switch_')] +
		                                       [(
		                                        col, col.replace('Provider License Number State Code_', 'p_lic_state_'))
		                                        for col in df_npi.columns if
		                                        col.startswith('Provider License Number State Code_')] +
		                                       [(col, col.replace('Other Provider Identifier_', 'oth_pid_'))
		                                        for col in df_npi.columns if
		                                        col.startswith('Other Provider Identifier_')] +
		                                       [(col,
		                                         col.replace('Other Provider Identifier Type Code_', 'oth_pid_type_'))
		                                        for col in df_npi.columns if
		                                        col.startswith('Other Provider Identifier Type Code_')] +
		                                       [(col, col.replace('Other Provider Identifier State_', 'oth_pid_st_'))
		                                        for col in df_npi.columns if
		                                        col.startswith('Other Provider Identifier State_')] +
		                                       [(col, col.replace('Other Provider Identifier Issuer_', 'oth_pid_iss_'))
		                                        for col in df_npi.columns if
		                                        col.startswith('Other Provider Identifier Issuer_')] +
		                                       [(
		                                        col, col.replace('Healthcare Provider Taxonomy Group_', 'p_tax_group_'))
		                                        for col in df_npi.columns if
		                                        col.startswith('Healthcare Provider Taxonomy Group_')] +
		                                       [(col, col.replace('ptaxcode', 'p_tax_code_'))
		                                        for col in df_npi.columns if col.startswith('ptaxcode')] +
		                                       [(col, col.replace('pprimtax', 'p_prim_tax_'))
		                                        for col in df_npi.columns if col.startswith('pprimtax')] +
		                                       [(col, col.replace('plicstate', 'p_lic_state_'))
		                                        for col in df_npi.columns if col.startswith('plicstate')] +
		                                       [(col, col.replace('othpid', 'oth_pid_'))
		                                        for col in df_npi.columns if (col.startswith('othpid') &
		                                                                      (not col.startswith('othpidty')) &
		                                                                      (not col.startswith('othpidiss')))] +
		                                       [(col, col.replace('othpidty', 'oth_pid_type_'))
		                                        for col in df_npi.columns if col.startswith('othpidty')] +
		                                       [(col, col.replace('othpidst', 'oth_pid_st_'))
		                                        for col in df_npi.columns if col.startswith('othpidst')] +
		                                       [(col, col.replace('othpidiss', 'oth_pid_iss_'))
		                                        for col in df_npi.columns if col.startswith('othpidiss')] +
		                                       [(col, col.replace('ptaxgroup', 'p_tax_group_'))
		                                        for col in df_npi.columns if col.startswith('ptaxgroup')]
		                                       )
		                                }
		                       )
		df_npi = df_npi.set_index('npi')
		shutil.rmtree(os.path.join(nppes_lookup_folder, str(year), 'npi_cleaned_parquet'),
		              ignore_errors=True)
		df_npi.to_parquet(os.path.join(nppes_lookup_folder, str(year), 'npi_cleaned_parquet'),
		                  engine=pq_engine,
		                  write_index=True)

		if year > 2019:
			df_othname = dd.read_csv(os.path.join(nppes_lookup_folder, str(year),
			                                      'othname-raw.csv'), dtype=object)
			df_othname = df_othname.rename(columns={**{'NPI': 'npi',
			                                           'Provider Other Organization Name': 'p_org_name_oth',
			                                           'Provider Other Organization Name Type Code': 'p_org_name_oth_type'}
			                                        }
			                               )
			df_othname = df_othname.set_index('npi')
			shutil.rmtree(os.path.join(nppes_lookup_folder, str(year), 'othname_cleaned_parquet'),
			              ignore_errors=True)
			df_othname.to_parquet(os.path.join(nppes_lookup_folder, str(year), 'othname_cleaned_parquet'),
			                      engine=pq_engine,
			                      write_index=True)

			df_ploc = dd.read_csv(os.path.join(nppes_lookup_folder, str(year),
			                                   'plocation-raw.csv'), dtype=object)
			df_ploc = df_ploc.rename(columns={**{"NPI": 'npi',
			                                     "Provider Secondary Practice Location Address- Address Line 1": 'p_sec_loc_line_1',
			                                     "Provider Secondary Practice Location Address-  Address Line 2": 'p_sec_loc_line_2',
			                                     "Provider Secondary Practice Location Address - City Name": 'p_sec_loc_city',
			                                     "Provider Secondary Practice Location Address - State Name": 'p_sec_loc_state',
			                                     "Provider Secondary Practice Location Address - Postal Code": 'p_sec_loc_zip',
			                                     "Provider Secondary Practice Location Address - Country Code (If outside U.S.)": 'p_sec_loc_country',
			                                     "Provider Secondary Practice Location Address - Telephone Number": 'p_sec_loc_phone',
			                                     "Provider Secondary Practice Location Address - Telephone Extension": 'p_sec_loc_phone_ext',
			                                     "Provider Practice Location Address - Fax Number": 'p_sec_loc_fax'}
			                                  }
			                         )
			df_ploc = df_ploc.set_index('npi')
			shutil.rmtree(os.path.join(nppes_lookup_folder, str(year), 'plocation_cleaned_parquet'),
			              ignore_errors=True)
			df_ploc.to_parquet(os.path.join(nppes_lookup_folder, str(year), 'plocation_cleaned_parquet'),
			                   engine=pq_engine,
			                   write_index=True)
		wide_to_long_nppes_taxonomy_file(year, pq_engine)
		generate_npi_taxonomy_mappings(year, pq_engine)



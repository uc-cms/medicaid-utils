import sys
import os
import logging
import pandas as pd
import gc
import numpy as np
import dask.dataframe as dd
import shutil

sys.path.append('../../')
from preprocessing import cms_file, ip, ot, ps
from filters.claims import dx_and_proc
from common_utils import dataframe_utils

data_folder = os.path.join(os.path.dirname(__file__), 'data')


def apply_range_filter(tpl_range, df, filter_name, col_name, data_type, f_type, tmp_folder,
                       logger_name=__file__):
	logger = logging.getLogger(logger_name)
	index_name = df.index.name
	start = pd.to_datetime(tpl_range[0], format='%Y%m%d', errors='coerce') \
		if (data_type == 'date') else pd.to_numeric(tpl_range[0], errors='coerce')
	end = pd.to_datetime(tpl_range[1], format='%Y%m%d', errors='coerce') \
		if (data_type == 'date') else pd.to_numeric(tpl_range[1], errors='coerce')
	if pd.notnull(start) | pd.notnull(end):
		if ~(pd.isnull(start) | pd.isnull(end)):
			df = df.loc[df[col_name].between(start, end, inclusive='both')]
		elif pd.isnull(start):
			df = df.loc[df[col_name] <= end]
		else:
			df = df.loc[df[col_name] >= start]
	else:
		logger.info(f"{' '.join(filter_name.split('_')[2:])} range {tpl_range} is invalid.")
	return df


def filter_claim_files(claim, dct_claim_filters, tmp_folder, logger_name=__file__):
	logger = logging.getLogger(logger_name)
	logger.info(f"{claim.st} ({claim.year}) has {claim.df.shape[0].compute()} {claim.ftype} claims")
	dct_filter = claim.dct_default_filters.copy()
	if claim.ftype in dct_claim_filters:
		dct_filter.update(dct_claim_filters[claim.ftype])
	for filter_name in dct_filter:
		filtered = 1
		if filter_name.startswith('range_') and ("_".join(filter_name.split('_')[2:]) in claim.df.columns):
			claim.df = apply_range_filter(dct_filter[filter_name], claim.df, filter_name,
			                              "_".join(filter_name.split('_')[2:]),
			                              filter_name.split('_')[1], claim.ftype, tmp_folder,
			                              logger_name=logger_name)

		elif f'excl_{filter_name}' in claim.df.columns:
				claim.df = claim.df.loc[claim.df[f'excl_{filter_name}'] == int(dct_filter[filter_name])]
		else:
			filtered = 0
			logger.info(f"Filter {filter_name} is currently not supported for {claim.ftype} files")
		if filtered:
			if claim.tmp_folder is None:
				claim.tmp_folder = tmp_folder
			claim.df = claim.cache_results()
			logger.info(
				f"Applying {filter_name} = {dct_filter[filter_name]} filter reduces {claim.ftype} claim count to "
				f"{claim.df.shape[0].compute()}")
	return claim


def extract_cohort(st, year, dct_diag_codes, dct_proc_codes, dct_cohort_filters, dct_export_filters,
                   lst_types_to_export, data_root, dest_folder, clean_exports=True, preprocess_exports=True,
                   logger_name=__file__):
	logger = logging.getLogger(logger_name)
	tmp_folder = os.path.join(dest_folder, 'tmp_files')
	dct_claims = dict()
	try:
		dct_claims['ip'] = ip.IP(year, st, data_root, clean=True, preprocess=True)
		dct_claims['ot'] = ot.OT(year, st, data_root, clean=True, preprocess=True, tmp_folder=os.path.join(tmp_folder, 'ot'))
		dct_claims['rx'] = cms_file.CMSFile('rx', year, st, data_root, clean=False, preprocess=False)
		dct_claims['ps'] = ps.PS(year, st, data_root, clean=True,
		                         preprocess=True if 'ps' in dct_cohort_filters else False,
		                         tmp_folder=os.path.join(tmp_folder, 'ps'))
		logger.info(f"{st} ({year}) has {dct_claims['ps'].df.shape[0].compute()} benes")
	except Exception as ex:
		logger.warning(f"{year} data is missing for {st}")
		logger.exception(ex)
		return 1
	os.makedirs(dest_folder, exist_ok=True)
	os.makedirs(tmp_folder, exist_ok=True)
	
	for f_type in dct_cohort_filters:
		dct_claims[f_type] = filter_claim_files(dct_claims[f_type],
		                                        dct_cohort_filters,
		                                        os.path.join(tmp_folder, f_type),
		                                        logger_name)
	pdf_patients = None
	if bool(dct_diag_codes) | bool(dct_proc_codes):
		pdf_patients = dx_and_proc.get_patient_ids_with_conditions(dct_diag_codes,
		                                                           dct_proc_codes,
		                                                           logger_name=logger_name,
		                                                           ip=dct_claims['ip'].df.rename(
			                                                           columns={'prncpl_proc_date': 'service_date'})[
			                                                           [col for col in dct_claims['ip'].df.columns if
			                                                            col.startswith(('PRCDR', 'DIAG',))] + [
				                                                           'service_date']],
		                                                           ot=dct_claims['ot'].df.rename(
			                                                           columns={'srvc_bgn_date': 'service_date'})[
			                                                           [col for col in dct_claims['ot'].df.columns if
			                                                            col.startswith(('PRCDR', 'DIAG',))] + [
				                                                           'service_date']]
		                                                           )
		pdf_patients['include'] = 1
	else:
		pdf_patients = pd.concat([dct_claims[f_type].df.assign(**dict([(dct_claims[f_type].index_col,
		                                                                dct_claims[f_type].df.index),
		                                                               ('include', 1)]))[
			                          [dct_claims[f_type].index_col, 'include']].compute() for f_type in
		                          dct_claims]).drop_duplicates().set_index(dct_claims['ps'].index_col)
	pdf_patients['YEAR'] = year
	pdf_patients['STATE_CD'] = st
		
	logger.info(f"{st} ({year}) has {pdf_patients.shape[0]} benes with specified conditions/ procedures")
	
	if 'ps' not in dct_cohort_filters:
		dct_claims['ps'].df = dct_claims['ps'].df.loc[
			dct_claims['ps'].df.index.isin(pdf_patients.loc[pdf_patients['include'] == 1].index.tolist())].persist()
		dct_claims['ps'].df = dct_claims['ps'].cache_results()
		dct_claims['ps'] = filter_claim_files(dct_claims['ps'],
		                                      {},
		                                      os.path.join(tmp_folder, 'ps'),
		                                      logger_name)
		logger.info(f"{st} ({year}) has {pdf_patients.loc[pdf_patients['include'] == 1].shape[0]} benes with specified "
		            f"conditions who also meet the cohort inclusion criteria")
		pdf_patients['include'] = pdf_patients['include'].where(
			pdf_patients.index.isin(dct_claims['ps'].df.index.compute().tolist()), 0)
		logger.info(f"For {st} ({year}), {pdf_patients.loc[pdf_patients['include'] == 1].shape[0]} benes remain after "
		            f"cleaning PS")
	pdf_dob = dct_claims['ps'].df[['birth_date']].compute()
	del dct_claims
	gc.collect()
	
	pdf_patients = pdf_patients.merge(pdf_dob, left_index=True, right_index=True, how='inner')
	pdf_patients.to_csv(os.path.join(dest_folder, f'cohort_{st}_{year}.csv'), index=True)
	
	shutil.rmtree(tmp_folder)
	export_cohort_datasets(pdf_patients, year, st, data_root, lst_types_to_export, dest_folder, dct_export_filters,
	                       clean_exports, preprocess_exports, logger_name)
	return 0


def export_cohort_datasets(pdf_cohort, year, st, data_root, lst_types_to_export, dest_folder, dct_export_filters,
                           clean_exports=False, preprocess_exports=False, logger_name=__file__):

	#TODO: Concat exported files
	logger = logging.getLogger(logger_name)
	tmp_folder = os.path.join(dest_folder, 'tmp_files')
	os.makedirs(dest_folder, exist_ok=True)
	os.makedirs(tmp_folder, exist_ok=True)
	dct_claims = dict()
	for f_type in sorted(lst_types_to_export):
		try:
			if f_type == 'ip':
				dct_claims[f_type] = ip.IP(year, st, data_root, clean=False, preprocess=False)
			elif f_type == 'ot':
				dct_claims[f_type] = ot.OT(year, st, data_root, clean=False, preprocess=False,
				                           tmp_folder=os.path.join(tmp_folder, f_type))
			elif f_type == 'ps':
				dct_claims[f_type] = ps.PS(year, st, data_root, clean=False, preprocess=False,
				                           tmp_folder=os.path.join(tmp_folder, f_type))
			else:
				dct_claims[f_type] = cms_file.CMSFile(f_type, year, st, data_root, clean=False, preprocess=False)
		except Exception as ex:
			logger.warning(f"{year} {f_type} data is missing for {st}")
			logger.exception(ex)
			return 1
	for f_type in sorted(lst_types_to_export):
		logger.info(f"Exporting {f_type} for {st} ({year})")
		dct_claims[f_type].df = dct_claims[f_type].df.loc[
			dct_claims[f_type].df.index.isin(pdf_cohort.loc[pdf_cohort['include'] == 1].index.tolist())]
		dct_claims[f_type].df = dct_claims[f_type].cache_results()

		if clean_exports or preprocess_exports:
			if clean_exports:
				dct_claims[f_type].clean()
			if preprocess_exports:
				dct_claims[f_type].preprocess()
			dct_claims[f_type].df = dct_claims[f_type].cache_results()
		if bool(dct_export_filters):
			dct_claims[f_type] = filter_claim_files(dct_claims[f_type], dct_export_filters,
			                                        os.path.join(tmp_folder, f'{f_type}'),
			                                        logger_name)
		dct_claims[f_type].export(dest_folder)
	shutil.rmtree(tmp_folder)

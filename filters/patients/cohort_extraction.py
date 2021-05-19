import sys
import os
import logging
import pandas as pd
import numpy as np
import dask.dataframe as dd

sys.path.append('../../')
from preprocessing import cms_file, ip, ot, ps
from filters.claims import dx_and_proc

data_folder = os.path.join(os.path.dirname(__file__), 'data')


def apply_range_filter(tpl_range, df, filter_name, col_name, data_type, f_type, logger_name=__file__):
	logger = logging.getLogger(logger_name)
	start = pd.to_datetime(tpl_range[0], format='%Y%m%d', errors='coerce') \
		if data_type is 'date' else pd.to_numeric(tpl_range[0], errors='coerce')
	end = pd.to_datetime(tpl_range[1], format='%Y%m%d', errors='coerce') \
		if data_type is 'date' else pd.to_numeric(tpl_range[1], errors='coerce')
	if (~np.isnan(start)) | (~np.isnan(end)):
		if ~(np.isnan(start) | np.isnan(end)):
			df = df.loc[df[col_name].between(start, end, inclusive=True)]
		elif np.isnan(start):
			df = df.loc[df[col_name] <= end]
		else:
			df = df.loc[df[col_name] >= start]
		logger.info(f"Restricting the {filter_name} to  {tpl_range} reduces "
		            f"{f_type} claim count to {df.shape[0].compute()}")
	else:
		logger.info(f"{filter_name} range {tpl_range} is invalid.")
	return df


def filter_claim_files(claim, dct_claim_filters, f_type, st, year, logger_name=__file__):
	logger = logging.getLogger(logger_name)
	logger.info(f"{st} ({year}) has {claim.df.shape[0].compute()} {f_type} claims")
	dct_filter = claim.dct_default_filters.copy()
	if f_type in dct_claim_filters:
		dct_filter.update(dct_claim_filters[f_type])
	for filter in dct_filter:
		if filter not in ['observation_period', 'age_range']:
			if f'excl_{filter}' in claim.df.columns:
				claim.df = claim.df.loc[claim.df[f'excl_{filter}'] == int(dct_filter[f'excl_{filter}'] != 1)]
				logger.info(f"Applying {filter} = {dct_filter[filter]} exclusion reduces {f_type} claim count to "
				            f"{claim.df.shape[0].compute()}")
			else:
				logger.info(f"Filter {filter} is currently not supported")
		if 'observation_period' in dct_filter:
			if f_type in ['ip', 'ot']:
				claim.df = apply_range_filter(dct_filter['observation_period'], claim.df, 'observation_period',
				                              'admsn_date' if f_type == 'ip' else 'srvc_bgn_date',
				                              'date', f_type, logger_name=logger_name)
		if 'age_range' in dct_filter:
			if f_type in ['ip', 'ot', 'ps']:
				claim.df = apply_range_filter(dct_filter['age_range'], claim.df, 'age_range',
				                              'age_admsn' if f_type in ['ip', 'ot'] else 'age_decimal',
				                              'numeric', f_type, logger_name=logger_name)
	return claim


def extract_cohort(st, year, dct_diag_codes, dct_proc_codes, dct_cohort_filters, dct_export_filters,
                   lst_types, data_root, dest_folder, clean_exports=True, preprocess_exports=True,
                   logger_name=__file__):
	logger = logging.getLogger(logger_name)
	dct_claims = dict()
	try:
		dct_claims['ip'] = ip.IP(year, st, data_root, clean=True, preprocess=False)
		dct_claims['ot'] = ot.OT(year, st, data_root, clean=True, preprocess=False)
		dct_claims['rx'] = cms_file.CMSFile('rx', year, st, data_root, clean=False, preprocess=False)
		dct_claims['ps'] = ps.PS(year, st, data_root, clean=clean_exports, preprocess=preprocess_exports)
		logger.info(f"{st} ({year}) has {dct_claims['ps'].df.shape[0].compute()} benes")
	except Exception as ex:
		logger.warning(f"{year} data is missing for {st}")
		logger.exception(ex)
		return 1
	os.makedirs(dest_folder, exist_ok=True)

	for f_type in ['ip', 'ot', 'ps']:
		dct_claims[f_type] = filter_claim_files(dct_claims[f_type], dct_cohort_filters, f_type, st, year,
		                                        logger_name)

	pdf_patients = dx_and_proc.get_patient_ids_with_conditions(dct_diag_codes,
	                                                           dct_proc_codes,
	                                                           logger_name=logger_name,
	                                                           ip=dct_claims['ip'].df.copy(),
	                                                           ot=dct_claims['ot'].df.copy()
	                                                           )
	pdf_patients['YEAR'] = year
	pdf_patients['STATE_CD'] = st
	pdf_patients.to_csv(os.path.join(dest_folder, f'cohort_{year}_{st}.csv'), index=True)
	logger.info(f"{st} ({year}) has {pdf_patients.shape[0]} benes with specified conditions/ procedures")
	dct_claims['ps'].df = dct_claims['ps'].df.loc[dct_claims['ps'].df.index.isin(pdf_patients.index.tolist())]
	logger.info(f"{st} ({year}) has {pdf_patients.shape[0]} cleaned benes with specified conditions/ procedures")
	for f_type in lst_types:
		cms_data = dct_claims[f_type]
		if f_type == 'ip':
			cms_data = ip.IP(year, st, data_root, clean=clean_exports, preprocess=preprocess_exports)
		if f_type == 'ot':
			cms_data = ot.OT(year, st, data_root, clean=clean_exports, preprocess=preprocess_exports)
		if f_type != 'ps':
			cms_data = filter_claim_files(cms_data, dct_export_filters, f_type, st, year, logger_name)
			cms_data.df = cms_data.df.loc[cms_data.df.index.isin(pdf_patients.index.tolist())]
		cms_data.export(dest_folder)
	return 0

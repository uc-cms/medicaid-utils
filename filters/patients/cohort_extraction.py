import sys
import os
import logging
import dask.dataframe as dd

sys.path.append('../../')
from preprocessing import cms_file, ip, ot, ps
from filters.claims import dx_and_proc

data_folder = os.path.join(os.path.dirname(__file__), 'data')


def extract_cohort(st, year, dct_diag_codes, dct_proc_codes, data_root, dest_folder, lst_types,
                   logger_name=__file__):
	logger = logging.getLogger(logger_name)
	dct_claims = dict()
	try:
		dct_claims['ip'] = ip.IP(year, st, data_root, clean=True, preprocess=False)
		dct_claims['ot'] = ot.OT(year, st, data_root, clean=True, preprocess=False)
		dct_claims['rx'] = cms_file.CMSFile('rx', year, st, data_root, clean=False, preprocess=False)
		dct_claims['ps'] = ps.PS(year, st, data_root, clean=False, preprocess=False)
	except Exception as ex:
		logger.warning(f"{year} data is missing for {st}")
		logger.exception(ex)
		return 1
	os.makedirs(dest_folder, exist_ok=True)

	pdf_patients = dx_and_proc.get_patient_ids_with_conditions(dct_diag_codes,
	                                                           dct_proc_codes,
	                                                           logger_name=logger_name,
	                                                           ip=dct_claims['ip'].df.copy(),
	                                                           ot=dct_claims['ot'].df.copy()
	                                                           )
	pdf_patients['YEAR'] = year
	pdf_patients['STATE_CD'] = st
	pdf_patients.to_csv(os.path.join(dest_folder, f'cohort_{year}_{st}.csv'), index=True)
	for f_type in lst_types:
		if f_type in ['ip', 'ot']:
			dct_claims[f_type] = cms_file.CMSFile(f_type, year, st, data_root, clean=False, preprocess=False)
		cms_data = dct_claims[f_type]
		cms_data.df = cms_data.df.loc[cms_data.df.index.isin(pdf_patients.index.tolist())]
		cms_data.export(dest_folder)

	return 0

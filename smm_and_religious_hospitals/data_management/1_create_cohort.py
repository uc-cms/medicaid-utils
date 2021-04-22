import dask.dataframe as dd
import pandas as pd
import sys
import logging
import os

sys.path.append('../')
from common_utils import recipes, links, dataframe_utils
from preprocessing import all_claims, ip
from topics.obgyn import hospitalization

package_folder = os.path.dirname(__file__)
log_folder = os.path.join(package_folder, 'logs')

# Setup weekly logging and log entry formats
recipes.setup_logger(__name__, os.path.join(log_folder, 'cohort_creation.log'),
                     logging.DEBUG)
logger = logging.getLogger(__name__)


def create_cohort(state: str = 'IL', server_root: str = '/mnt/data', index_name: str = 'BENE_MSIS'):
	df_ip_2010 = dd.read_parquet(links.get_parquet_loc(server_root, 'ip', state, 2010))
	df_ip_2011 = dd.read_parquet(links.get_parquet_loc(server_root, 'ip', state, 2011))

	logger.info(f"Processing ip from {state} for the years 2010 and 2011")

	df_ip = dd.concat([df_ip_2010, df_ip_2011])
	df_ip = dataframe_utils.fix_index(df_ip, index_name, False)

	# clean up diagnosis code columns
	df_ip = all_claims.clean_diag_codes(df_ip)
	df_ip = ip.ip_process_date_cols(df_ip)
	df_ip = all_claims.add_gender(df_ip)

	df_ip = df_ip.drop_duplicates()

	# TODO: Impute admission date from service begin date, like FARA?
	df_ip = df_ip.assign(excl_missing_dob_or_admsn_date=(df_ip['birth_date'].isnull() |
	                                                     df_ip['admsn_date'].isnull()).astype(int),
	                     excl_outside_observation_period=(~((df_ip["admsn_date"] >= pd.Timestamp(year=2010,
	                                                                                             month=3,
	                                                                                             day=1)) &
	                                                        (df_ip["admsn_date"] <= pd.Timestamp(year=2011,
	                                                                                             month=10,
	                                                                                             day=31)))).astype(int),
	                     excl_age_outside_target_range=(~df_ip['age_admsn'].between(15, 50, inclusive=True)
	                                                    ).astype(int))

	logger.info(f"Starting with {df_ip.shape[0].compute()} claims")

	logger.info(f"Removing {df_ip.loc[df_ip['ip_incl'] == 0]} ip claims that have duplicate overlaps")
	df_ip = df_ip.loc[df_ip['ip_incl'] == 1]
	logger.info(f'Left with {df_ip.shape[0].compute()} claims')

	logger.info(f'Removing {df_ip.loc[df_ip.excl_outside_observation_period == 1].shape[0].compute()} claims that '
	            f'are outside the observation period')
	df_ip = df_ip.loc[df_ip['excl_outside_observation_period'] == 0]
	logger.info(f'Left with {df_ip.shape[0].compute()} claims')

	logger.info(f"Removing {df_ip.loc[df_ip.excl_missing_dob_or_admsn_date == 1].shape[0].compute()} claims that have "
	            f"missing birth or admission dates")
	df_ip = df_ip.loc[df_ip['excl_missing_dob_or_admsn_date'] == 0]
	logger.info(f'Left with {df_ip.shape[0].compute()} claims')

	logger.info(f"Removing {df_ip.loc[df_ip.female != 1].shape[0].compute()} claims that are from benes that are "
	            f"not female")
	df_ip = df_ip.loc[df_ip['female'] == 1]
	logger.info(f'Left with {df_ip.shape[0].compute()} claims')

	logger.info(f"Removing {df_ip.loc[df_ip.excl_age_outside_target_range == 1].compute()} claims that are from benes "
	            f"that are not in the target age range")
	df_ip = df_ip.loc[df_ip['excl_age_outside_target_range'] == 0]
	logger.info(f'Left with {df_ip.shape[0].compute()} claims')

	df_ip = hospitalization.flag_birth(df_ip)
	df_ip = hospitalization.flag_abnormal_pregnancy(df_ip)
	df_ip = hospitalization.flag_preterm(df_ip)

	df_ip = df_ip.assign(hosp_pregnancy_associated=df_ip[[col for col in df_ip.columns if col.startswith('')]
														 ].any(axis=1).astype(int))
	logger.info(f"Removing {df_ip.loc[df_ip['hosp_pregnancy_associated'] == 0].shape[0].compute()} "
	            f"non-pregnancy related hospitalization claims")
	df_ip = df_ip.loc[df_ip['hosp_pregnancy_associated'] == 1].compute()
	logger.info(f'Left with {df_ip.shape[0].compute()} claims')

	df_ip = df_ip.sort_values(['BENE_MSIS', 'admsn_date'], ascending=True)
	df_ip = df_ip.drop_duplicates(subset=['BENE_MSIS'], keep='last')
	logger.info(f"Removing all claims except index claims")
	logger.info(f'Left with {df_ip.shape[0].compute()} claims')








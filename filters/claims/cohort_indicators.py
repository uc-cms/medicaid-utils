import dask.dataframe as dd
import os
import pandas as pd
import numpy as np
from itertools import product
from typing import List

data_folder = os.path.join(os.path.dirname(__file__), 'data')


def get_patient_ids_with_conditions(dct_diag_codes: dict, dct_procedure_codes: dict,
                                    df_ip_claims: dd.DataFrame, df_ot_claims: dd.DataFrame) -> List[str]:
	"""
	Gets patient ids with conditions denoted by provided diagnosis codes or procedure codes
	:param lst_diag_codes:
	:param dct_procedure_codes:
	:param df_ip_claims:
	:param df_ot:
	:return:
	"""
	lstdf_patient = []
	pdf_patients = pd.DataFrame()
	for df in [df_ip_claims, df_ot_claims]:
		if df is not None:
			df['diag_condn'] = 0
			df['proc_condn'] = 0

			if bool(dct_diag_codes):
				df = df.map_partitions(
					lambda pdf: pdf.assign(**dict([(f'diag_condn_{condn}',
					                                pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
					                                .apply(
						                                lambda x: x.str.upper().str.startswith(
							                                tuple([str(dx_code) for dx_code in dct_diag_codes[condn]]))
					                                ).any(axis='columns').astype(int)
					                                ) for condn in dct_diag_codes])
					                       ))
				df = df.assign(diag_condn=df[[col for col in df.columns
				                              if col.startswith('diag_condn_')]].any(axis=1).astype(int))

			if bool(dct_procedure_codes):
				n_prcdr_cd_col = [col for col in df.columns if col.startswith('PRCDR_CD_') and
				                  (not col.startswith('PRCDR_CD_SYS_'))]
				df = df.map_partitions(
					lambda pdf: pdf.assign(**dict([(f"VALID_PRCDR_1_CD_{i}",
					                                pdf[f'PRCDR_CD_{i}']
					                                ) for i in range(1, n_prcdr_cd_col)] +
					                              [(f"VALID_PRCDR_{sys_code}_CD_{i}",
					                                pdf[f'PRCDR_CD_{i}']
					                                .where((pd.to_numeric(pdf[f'PRCDR_CD_SYS_{i}'],
					                                                      errors='coerce') == sys_code), "")
					                                ) for sys_code, i in product([int(sys_code) for sys_code in
					                                                              list(dct_procedure_codes.keys())
					                                                              if int(sys_code) != 1],
					                                                             range(1, n_prcdr_cd_col))]
					                              )))
				df = df.map_partitions(
					lambda pdf: pdf.assign(**dict([(f'proc_condn_{sys_code}',
					                                pdf[[col for col in pdf.columns
					                                     if col.startswith(f"VALID_PRCDR_{sys_code}_CD_")]
					                                    ].apply(lambda x: x.str.upper().str.strip()
					                                            .str.startswith(tuple(dct_procedure_codes[sys_code]))
					                                            .any(axis='columns')).astype(int))
					                               for sys_code in list(dct_procedure_codes.keys())])
					                       ))
				df = df.assign(proc_condn=df[[col for col in df.columns
				                              if col.startswith('proc_condn_')]].any(axis=1).astype(int))
			df[df.index.name] = df.index
			lstdf_patient.append(df.loc[df[['proc_condn',
			                                'diag_condn']].any(axis=1)][[df.index.name] +
			                                                            [col for col in df.columns
			                                                             if col.startswith(('proc_condn',
			                                                                                'diag_condn'))]
			                                                            ].drop_duplicates())
	pdf_patients = pd.concat(lstdf_patient, ignore_index=True).drop_duplicates()
	if pdf_patients.shape[0] > 0:
		index_col = [col for col in pdf_patients.columns if not col.startswith(('proc_condn', 'diag_condn'))][0]
		pdf_patients = pdf_patients.set_index(index_col)
	return pdf_patients

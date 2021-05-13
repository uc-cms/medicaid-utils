import os
import pandas as pd
from itertools import product

data_folder = os.path.join(os.path.dirname(__file__), 'data')


def get_patient_ids_with_conditions(dct_diag_codes: dict, dct_procedure_codes: dict,
                                    **dct_claims) -> pd.DataFrame():
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
	index_col = None
	for claim_type in dct_claims:
		df = dct_claims[claim_type]
		if df is not None:
			df['diag_condn'] = 0
			df['proc_condn'] = 0
			if (index_col is not None) and (index_col != df.index.name):
				raise Exception("Passed claims files do not have the same index")
			index_col = df.index.name
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
				n_prcdr_cd_col = len([col for col in df.columns if col.startswith('PRCDR_CD_') and
				                      (not col.startswith('PRCDR_CD_SYS_'))])
				lst_sys_code = list(set([int(sys_code) for lst_sys_code in
				                         [list(dct_procedure_codes[proc].keys()) for proc in dct_procedure_codes.keys()]
				                         for sys_code in lst_sys_code if int(sys_code) != 1]))
				df = df.map_partitions(
					lambda pdf: pdf.assign(**dict([(f"VALID_PRCDR_1_CD_{i}",
					                                pdf[f'PRCDR_CD_{i}']
					                                ) for i in range(1, n_prcdr_cd_col)] +
					                              [(f"VALID_PRCDR_{sys_code}_CD_{i}",
					                                pdf[f'PRCDR_CD_{i}']
					                                .where((pd.to_numeric(pdf[f'PRCDR_CD_SYS_{i}'],
					                                                      errors='coerce') == sys_code), "")
					                                ) for sys_code, i in product(lst_sys_code,
					                                                             range(1, n_prcdr_cd_col))]
					                              )))
				df = df.map_partitions(
					lambda pdf: pdf.assign(**dict([(f'proc_condn_{proc}_{sys_code}',
					                                pdf[[col for col in pdf.columns if
					                                     col.startswith((f'VALID_PRCDR_{sys_code}_CD_',))]]
					                                .apply(lambda x: x.str.upper().str.strip()
					                                       .str.startswith(tuple(dct_procedure_codes[proc][sys_code])))
					                                .any(axis='columns').astype(int)) for sublist in
					                               [product([proc], list(dct_procedure_codes[proc].keys()))
					                                for proc in dct_procedure_codes] for proc, sys_code in sublist])))

				df = df.assign(**dict([(f'proc_condn_{proc}',
				                        df[[col for col in df.columns if col.startswith(f'proc_condn_{proc}_')]
				                        ].any(axis=1).astype(int)) for proc in dct_procedure_codes.keys()]))
				df = df.drop([item for subitem in
				              [[col for col in df.columns if col.startswith(f'proc_condn_{proc}_')] for proc in
				               dct_procedure_codes.keys()] for item in subitem], axis=1)
				df = df.assign(proc_condn=df[[col for col in df.columns
				                              if col.startswith('proc_condn_')]].any(axis=1).astype(int))
			df[index_col] = df.index
			df = df.loc[df[['proc_condn', 'diag_condn']].any(axis=1)][[index_col] +
			                                                          [col for col in df.columns
			                                                           if col.startswith(('proc_condn',
			                                                                              'diag_condn'))]
																	  ].drop_duplicates()
			df = df.rename(columns=dict([(col, f'{claim_type}_{col}') for col in df.columns if col != index_col]))
			lstdf_patient.append(df.compute())
	pdf_patients = pd.concat(lstdf_patient, ignore_index=True)
	if pdf_patients.shape[0] > 0:
		pdf_patients = pdf_patients.fillna(0).groupby(index_col).max().astype(int)
	return pdf_patients


def flag_diagnoses_and_procedures(dct_diag_codes: dict, dct_procedure_codes: dict,
                                  **dct_claims) -> dict:
	"""
	Create flags for claims containing provided diagnosis or procedure codes
	:param lst_diag_codes:
	:param dct_procedure_codes:
	:param df_ip_claims:
	:param df_ot:
	:return:
	"""
	for claim_type in dct_claims:
		df = dct_claims[claim_type]
		if df is not None:
			if bool(dct_diag_codes):
				df = df.map_partitions(
					lambda pdf: pdf.assign(**dict([(f'diag_{condn}',
					                                pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
					                                .apply(
						                                lambda x: x.str.upper().str.startswith(
							                                tuple([str(dx_code) for dx_code in dct_diag_codes[condn]]))
					                                ).any(axis='columns').astype(int)
					                                ) for condn in dct_diag_codes])
					                       ))

			if bool(dct_procedure_codes):
				n_prcdr_cd_col = len([col for col in df.columns if col.startswith('PRCDR_CD_') and
				                      (not col.startswith('PRCDR_CD_SYS_'))])
				lst_sys_code = list(set([int(sys_code) for lst_sys_code in
				                         [list(dct_procedure_codes[proc].keys()) for proc in dct_procedure_codes.keys()]
				                         for sys_code in lst_sys_code if int(sys_code) != 1]))
				df = df.map_partitions(
					lambda pdf: pdf.assign(**dict([(f"VALID_PRCDR_1_CD_{i}",
					                                pdf[f'PRCDR_CD_{i}']
					                                ) for i in range(1, n_prcdr_cd_col)] +
					                              [(f"VALID_PRCDR_{sys_code}_CD_{i}",
					                                pdf[f'PRCDR_CD_{i}']
					                                .where((pd.to_numeric(pdf[f'PRCDR_CD_SYS_{i}'],
					                                                      errors='coerce') == sys_code), "")
					                                ) for sys_code, i in product(lst_sys_code,
					                                                             range(1, n_prcdr_cd_col))]
					                              )))
				df = df.map_partitions(
					lambda pdf: pdf.assign(**dict([(f'proc_{proc}_{sys_code}',
					                                pdf[[col for col in pdf.columns if
					                                     col.startswith((f'VALID_PRCDR_{sys_code}_CD_',))]]
					                                .apply(lambda x: x.str.upper().str.strip()
					                                       .str.startswith(tuple(dct_procedure_codes[proc][sys_code])))
					                                .any(axis='columns').astype(int)) for sublist in
					                               [product([proc], list(dct_procedure_codes[proc].keys()))
					                                for proc in dct_procedure_codes] for proc, sys_code in sublist])))

				df = df.assign(**dict([(f'proc_{proc}',
				                        df[[col for col in df.columns if col.startswith(f'proc_{proc}_')]
				                        ].any(axis=1).astype(int)) for proc in dct_procedure_codes.keys()]))
				df = df.drop([item for subitem in
				              [[col for col in df.columns if col.startswith(f'proc_{proc}_')] for proc in
				               dct_procedure_codes.keys()] for item in subitem] +
				             [col for col in df.columns if col.startswith('VALID_PRCDR_')], axis=1)

			dct_claims[claim_type] = df
	return dct_claims

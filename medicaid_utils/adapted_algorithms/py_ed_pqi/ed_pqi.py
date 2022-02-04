#!/usr/bin/env python

"""pmca.py: This program creates claim level Potentially Preventable ED visit indicators (Sheryl Davies, Ellen Schultz
 et al 2017)."""
__author__ = "Manoradhan Murugesan"
__email__  = "manorathan@uchicago.edu"


import os
from itertools import product
import pandas as pd
import numpy as np
import dask.dataframe as dd

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

class EDPreventionQualityIndicators():
	package_folder, filename = os.path.split(__file__)
	data_folder = os.path.join(package_folder, 'data')

	@classmethod
	def get_patient_exclusion_indicators(cls, df_ip, df_ot, df_ps):
		pdf_icd9_diseases = pd.read_excel(os.path.join(cls.data_folder, 'icd9_codes.xlsx'), sheet_name='disease_diagnoses')
		pdf_icd9_edvsts = pd.read_excel(os.path.join(cls.data_folder, 'icd9_codes.xlsx'), sheet_name='edvst_types')
		dct_conditions = pdf_icd9_diseases.groupby('condition')['include_icd9'].apply(
			lambda x: tuple([str(cd) for cd in x])).to_dict()
		dct_edvst_cat = pdf_icd9_edvsts.groupby('condition')['include_icd9'].apply(
			lambda x: tuple([str(cd) for cd in x])).to_dict()
		def flag_disease_diagnoses(df, dct_conditions, dct_edvst_cat):
			df = df.map_partitions(
				lambda pdf: pdf.assign(
					**dict([('diag_' + condn,
					         pdf[pdf.columns[pd.Series(pdf.columns).str.startswith('DIAG_CD')].tolist()]
					         .apply(lambda x: x.str.upper().str.startswith(dct_conditions[condn]))
					         .any(axis='columns').astype(int)
					         ) for condn in ['facial_trauma', 'diabetes', 'immunocompromised', 'cystic_fibrosis',
					                         'pneumonia', 'fever', 'uti', 'spinal', 'trauma', 'cancer']] +
					       [('diag_uti_with_ut_malformation',
					         (pdf['DIAG_CD_1'].str.upper().str.startswith(dct_conditions['uti']) &
					          pdf[pdf.columns[pd.Series(pdf.columns).str.startswith('DIAG_CD')].tolist()]
					          .apply(lambda x: x.str.upper().str.startswith(dct_conditions['ut_malformation']))
					          .any(axis='columns')).astype(int)
					         )] +
					       [('diag_cellulitis_with_diabetes',
					         (pdf['DIAG_CD_1'].str.upper().str.startswith(dct_edvst_cat['acute_acsc_cellulitis']) &
					          pdf[pdf.columns[pd.Series(pdf.columns).str.startswith('DIAG_CD')].tolist()]
					          .apply(lambda x: x.str.upper().str.startswith(dct_conditions['diabetes']))
					          .any(axis='columns')).astype(int)
					         )]
					       )
				)
			)
			return df

		lst_conditions = ['facial_trauma', 'diabetes', 'immunocompromised', 'cystic_fibrosis', 'pneumonia', 'fever',
		                  'uti', 'spinal', 'trauma', 'cancer', 'uti_with_ut_malformation', 'cellulitis_with_diabetes']
		def agg_conditions_to_patient_level(pdf_claims, lst_conditions):
			return pdf_claims.groupby('MSIS_ID').agg(**dict([('diag_' + condn, ('diag_' + condn, 'max'))
					                                                for condn in lst_conditions]))
		df_ip_with_indicators = flag_disease_diagnoses(df_ip, dct_conditions, dct_edvst_cat)
		fix_index(df_ip_with_indicators, index_name='MSIS_ID')
		df_ip_with_indicators = df_ip_with_indicators.map_partitions(
			lambda pdf: agg_conditions_to_patient_level(pdf, lst_conditions)).compute()
		if df_ip_with_indicators.index.name != 'MSIS_ID':
			df_ip_with_indicators = df_ip_with_indicators.set_index('MSIS_ID')
		df_ip_with_indicators['MSIS_ID'] = df_ip_with_indicators.index

		df_ot_with_indicators = flag_disease_diagnoses(df_ot, dct_conditions, dct_edvst_cat)
		fix_index(df_ot_with_indicators, index_name='MSIS_ID')
		df_ot_with_indicators = df_ot_with_indicators.map_partitions(
			lambda pdf: agg_conditions_to_patient_level(pdf, lst_conditions)).compute()
		if df_ot_with_indicators.index.name != 'MSIS_ID':
			df_ot_with_indicators = df_ot_with_indicators.set_index('MSIS_ID')
		df_ot_with_indicators['MSIS_ID'] = df_ot_with_indicators.index

		pdf_ps = pd.concat([df_ip_with_indicators, df_ot_with_indicators], ignore_index=True)
		pdf_ps = agg_conditions_to_patient_level(pdf_ps, lst_conditions)
		if pdf_ps.index.name != 'MSIS_ID':
			pdf_ps = pdf_ps.set_index('MSIS_ID')
		pdf_ps = pdf_ps[[col for col in pdf_ps.columns if col!= 'MSIS_ID']]
		df_ps = df_ps[['Female', 'age', 'ageday']].merge(pdf_ps, left_index=True, right_index=True, how='left')
		df_ps = df_ps.map_partitions(
			lambda pdf: pdf.assign(**dict([(col,
			                                pdf[col].fillna(0)
			                                ) for col in pdf.columns if col not in ['Female', 'age', 'ageday',
			                                                                        'MSIS_ID']
			                               ])))
		fix_index(df_ps, 'MSIS_ID')
		return df_ps

	@classmethod
	def flag_potentially_preventable_ed_visits(cls, df_ed, df_ps, months_restricted=False):
		pdf_icd9_diseases = pd.read_excel(os.path.join(cls.data_folder, 'icd9_codes.xlsx'),
		                                  sheet_name='disease_diagnoses')
		pdf_icd9_edvsts = pd.read_excel(os.path.join(cls.data_folder, 'icd9_codes.xlsx'), sheet_name='edvst_types')
		dct_conditions = pdf_icd9_diseases.groupby('condition')['include_icd9'].apply(
			lambda x: tuple([str(cd) for cd in x])).to_dict()
		dct_edvst_cat = pdf_icd9_edvsts.groupby('condition')['include_icd9'].apply(
			lambda x: tuple([str(cd) for cd in x])).to_dict()

		if months_restricted == False:
			df_ed['valid_month'] = 1

		lst_claimtype = ['']
		if months_restricted:
			lst_claimtype.append('valid_')

		fix_index(df_ed, 'MSIS_ID')
		def mptn_categorise_ed_visits(pdf, dct_edvst_cat, dct_conditions):

			### Adding indicator and cost columns for each of PQI ED covered disease conditions
			pdf = pdf.assign(**dict([('edvst_' + condn ,
									  pdf['DIAG_CD_1'].str.startswith(dct_edvst_cat[condn]).astype(int))
										for condn in dct_edvst_cat.keys()] +
									[('edcost_' + condn,
									 pdf['pymt_amt']
											.where(pdf['DIAG_CD_1'].str.startswith(dct_edvst_cat[condn]),
												   0))
										for condn in dct_edvst_cat.keys()]
									))
			### Lower respiratory infection related ED visits: first-listed diagnosis for Lower respiratory infection
			### and 2) a second-listed diagnosis of COPD or asthma.
			pdf = pdf.assign(**dict([('edvst_' + condn,
									 (pdf['DIAG_CD_1'].str.startswith(dct_edvst_cat[condn]) &
										pdf[pdf.columns[pd.Series(pdf.columns).str.startswith('DIAG_CD')].tolist()]
											.apply(lambda x: x.str.upper()
																.str.startswith(
																tuple(  list(dct_edvst_cat['chronic_acsc_asthma']) +
																		list(dct_edvst_cat['chronic_acsc_copd'])))
													).any(axis='columns')).astype(int))
										for condn in ['chronic_acsc_lresp']] +
										[('edcost_' + condn,
										pdf['pymt_amt'].where(
										        (pdf['DIAG_CD_1'].str.startswith(dct_edvst_cat[condn]) &
										         pdf[pdf.columns[pd.Series(pdf.columns).str.startswith('DIAG_CD')].tolist()]
										                .apply(lambda x: x.str.upper().str.startswith(
										                    tuple(list(dct_edvst_cat['chronic_acsc_asthma']) +
										                          list(dct_edvst_cat['chronic_acsc_copd'])))
										                       ).any(axis='columns')),
										                    0
										                    )
										) for condn in ['chronic_acsc_lresp']]))

			### Subsetting claims that meet months restriction
			pdf_months_restricted = pdf.loc[pdf['valid_month'] == 1]

			### Aggregating claims to day level
			pdf = pdf.groupby(['MSIS_ID', 'DATE']).agg(**dict([('edvst_' + condn,
			                                              ('edvst_' + condn, 'max'))
			                                             for condn in dct_edvst_cat.keys()] +
			                                            [('edcost_' + condn,
			                                              ('edcost_' + condn, 'sum'))
			                                             for condn in dct_edvst_cat.keys()]
			                                            )).reset_index(drop=False).set_index('MSIS_ID')

			### Aggregating claims to patient level
			pdf = pdf.groupby('MSIS_ID').agg(**dict([('edvst_' + condn + '_count',
		                                                            ('edvst_' + condn, 'sum'))
		                                                            for condn in dct_edvst_cat.keys()] +
			                                                      [('edvst_' + condn + '_cost',
			                                                        ('edcost_' + condn, 'sum'))
			                                                       for condn in dct_edvst_cat.keys()]))
			pdf_months_restricted = pdf_months_restricted.groupby(['MSIS_ID', 'DATE']).agg(**dict([('valid_edvst_' + condn,
			                                                        ('edvst_' + condn, 'max'))
			                                                        for condn in dct_edvst_cat.keys()] +
					                                                [('valid_edcost_' + condn,
					                                                    ('edcost_' + condn, 'sum'))
					                                                   for condn in dct_edvst_cat.keys()]
			                                                  )).reset_index(drop=False).set_index('MSIS_ID')
			pdf_months_restricted = pdf_months_restricted.groupby('MSIS_ID').agg(**dict([('valid_edvst_' + condn + '_count',
		                                                            ('valid_edvst_' + condn, 'sum'))
		                                                            for condn in dct_edvst_cat.keys()] +
			                                                      [('valid_edvst_' + condn + '_cost',
			                                                        ('valid_edcost_' + condn, 'sum'))
			                                                       for condn in dct_edvst_cat.keys()]))

			pdf_combined = pdf.merge(pdf_months_restricted, left_index=True, right_index=True, how='left')
			if pdf_combined.index.name != 'MSIS_ID':
				pdf_combined = pdf_combined.set_index('MSIS_ID')

			return pdf_combined

		df_ed = df_ed.map_partitions(lambda pdf: mptn_categorise_ed_visits(pdf, dct_edvst_cat, dct_conditions))
		fix_index(df_ed, 'MSIS_ID', drop_column=False)
		df_ed = df_ed.compute()

		df_ed = df_ps.merge(df_ed, left_index=True, right_index=True, how='inner')
		df_ed = df_ed.map_partitions(
			lambda pdf: pdf.assign(**dict([(col, pdf[col].fillna(0))
			                               for col in pdf.columns if (col.startswith('edvst') |
			                                                          col.startswith('valid_edvst')) ])))
		df_ed = df_ed.map_partitions(
			lambda pdf: pdf.assign(**dict([(claim_type + 'pqi_ed_dental_' + agg_type,
		                                    pdf[claim_type + 'edvst_dental_' + agg_type].where((pdf['age']>=5) &
		                                                                          (pdf['diag_facial_trauma'] == 0),
		                                                                          np.nan))
			                               for claim_type, agg_type in product(lst_claimtype,
			                                                                   ['count', 'cost'])] +
			                              [(claim_type + 'pqi_ed_' + condn + '_' + agg_type,
			                                pdf[claim_type + 'edvst_' + condn + '_' + agg_type].where((pdf['age'] >= 40),
			                                                                      np.nan))
			                               for claim_type, agg_type, condn in product(lst_claimtype,
			                                                                          ['count', 'cost'],
			                                                              [condn_abbr for condn_abbr in dct_edvst_cat.keys()
			                                                               if condn_abbr.startswith('chronic_acsc')])] +
			                              [(claim_type + 'pqi_ed_' + condn + '_' + agg_type,
			                                pdf[claim_type + 'edvst_' + condn + '_' + agg_type]
			                                                .where((pdf['ageday'] >= 90) &
			                                                       (pdf['age'] < 65) &
			                                                       (pdf['diag_immunocompromised'] == 0) &
			                                                       (~((pdf['Female'] == 1) &
			                                                          (pdf['age'].between(18, 34, inclusive=True)) &
			                                                          (pdf['diag_uti_with_ut_malformation'] == 1))) &
			                                                       (pdf['diag_cellulitis_with_diabetes'] == 0),
			                                                        np.nan))
			                               for claim_type, agg_type, condn in product(lst_claimtype,
			                                                                          ['count', 'cost'],
			                                                              [condn_abbr for condn_abbr in
			                                                               dct_edvst_cat.keys()
			                                                               if condn_abbr.startswith('acute_acsc')])] +
			                              [(claim_type + 'pqi_ed_asthma_' + agg_type,
			                                pdf[claim_type + 'edvst_' + condn + '_' + agg_type]
			                                .where(pdf['age'].between(5,39, inclusive=True) &
			                                       (pdf['diag_cystic_fibrosis'] == 0) &
			                                       (pdf['diag_pneumonia'] == 0),
			                                       np.nan))
			                               for claim_type, agg_type, condn in product(lst_claimtype,
			                                                                          ['count', 'cost'],
			                                                              ['chronic_acsc_asthma'])] +
			                              [(claim_type + 'pqi_ed_back_pain_' + agg_type,
			                                pdf[claim_type + 'edvst_' + condn + '_' + agg_type]
			                                .where((pdf['age']>= 18) &
			                                       (pdf['diag_trauma'] == 0) &
			                                       (pdf['diag_uti'] == 0) &
			                                       (pdf['diag_fever'] == 0) &
			                                       (pdf['diag_cancer'] == 0) &
			                                       (pdf['diag_spinal'] == 0),
			                                       np.nan))
			                               for claim_type, agg_type, condn in product(lst_claimtype,
			                                                                          ['count', 'cost'],
			                                                              ['back_pain'])]
			                              )))
		df_ed = df_ed.map_partitions(
			lambda pdf: pdf.assign(**dict([(claim_type + 'pqi_ed_' + condn_cat + '_acsc_' + agg_type,
			                                pdf[[claim_type + 'pqi_ed_' + condn + '_' + agg_type
			                                            for condn in dct_edvst_cat.keys()
			                                                if condn.startswith(condn_cat + '_acsc')]].sum(axis='columns',
			                                                                                               min_count=1))
			                               for claim_type, agg_type, condn_cat in product(lst_claimtype,
			                                                                              ['count', 'cost'],
			                                                                  ['chronic', 'acute']
			                                                                  )])))
		fix_index(df_ed, 'MSIS_ID', drop_column=True)

		df_ed = df_ed[['MSIS_ID'] +
		              [claim_type + 'pqi_ed_' + condn+ '_' + agg_type
		                    for claim_type, condn, agg_type in product(lst_claimtype,
		                                                               ['dental', 'chronic_acsc',
		                                                                'acute_acsc', 'asthma',
		                                                                'back_pain'],
		                                                                ['count', 'cost'])]]
		return df_ed

def get_ed_pqis(df_ip, df_ot, df_ps, df_ed, restrict_months=False):
	df_ps = EDPreventionQualityIndicators.get_patient_exclusion_indicators(df_ip, df_ot, df_ps)
	df_ed = EDPreventionQualityIndicators.flag_potentially_preventable_ed_visits(df_ed, df_ps, restrict_months)
	return df_ed


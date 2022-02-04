#!/usr/bin/env python

"""betos_proc_codes.py"""
__author__ = "Manoradhan Murugesan"
__email__ = "manorathan@uchicago.edu"

import sys
import os
from itertools import product
import numpy as np
import pandas as pd
import dask.dataframe as dd
from typing import List


class BetosProcCodes():
	package_folder, filename = os.path.split(__file__)
	data_folder = os.path.join(package_folder, 'data')

	@classmethod
	def get_betos_cpt_crosswalk(cls, year: int) -> pd.DataFrame:
		"""
		Get CPT x Betos code crosswalk, with betos code and betos category information
		:param int year: Public use file year
		:rtype pd.DataFrame: CPT x Betos code crosswalk with the below columns:
								cpt_code - HCPCS codes A0010-V9999 AMA/CPT-4 codes 00100-99999
								betos_code - BETOS codes D1A-Z2
								betos_code_name - BETOS code description
								betos_cat - BETOS category abbrevation, with the below 7 values:
												EVALUATION AND MANAGEMENT - betos_eval
												PROCEDURES - betos_proc
												IMAGING	- betos_img
												TESTS -	betos_test
												DURABLE MEDICAL EQUIPMENT - betos_dme
												OTHER - betos_oth
												EXCEPTIONS/UNCLASSIFIED - betos_uncla
		"""
		pdf_crosswalk = pd.read_csv(os.path.join(cls.data_folder, 'betpuf{0}.txt'.format(str(year)[-2:])),
		                            header=None, names=['tmp'])
		if year != 2020:
			pdf_crosswalk['cpt_code'] = pdf_crosswalk['tmp'].str.split(" ").str[0]
			pdf_crosswalk['betos_code'] = pdf_crosswalk['tmp'].str.split(" ").str[1].str[:3].replace('', np.nan)
			pdf_crosswalk['termination_date'] = pd.to_datetime(pdf_crosswalk['tmp'].str.split(" ").str[2]
			                                                   .combine_first(pdf_crosswalk['tmp'].str.split(" ")
			                                                                  .str[1].str[3:]),
			                                                   errors='coerce')
			pdf_crosswalk = pdf_crosswalk.loc[pdf_crosswalk['betos_code'].notna()][['cpt_code', 'betos_code']]
			pdf_code_lookup = pd.read_csv(os.path.join(cls.data_folder,
			                                           'r-me-bet{0}.txt'.format(str(year)[-2:])),
			                              header=None, sep='=', error_bad_lines=True, skiprows=54,
			                              names=['betos_code', 'betos_code_name'])
			pdf_code_lookup = pdf_code_lookup.loc[~pdf_code_lookup['betos_code_name'].isna()] \
				.astype(str).apply(lambda x: x.str.strip(), axis=1)
			pdf_code_lookup = pdf_code_lookup.replace("", np.nan)
			pdf_code_lookup = pdf_code_lookup.dropna()

			pdf_cat = pd.read_csv(os.path.join(cls.data_folder,
			                                   'betos_cat.csv'))
			dct_betos_cat = dict(pdf_cat[['betos_code_start', 'betos_code_abbr']].values.tolist())
			pdf_crosswalk['betos_cat'] = pdf_crosswalk['betos_code'].apply(lambda x: dct_betos_cat[x[0]])
			pdf_crosswalk = pdf_crosswalk.merge(pdf_code_lookup, on='betos_code', how='left')
		else:
			pdf_crosswalk = pd.read_csv(os.path.join(cls.data_folder, 'betpuf{0}.txt'.format(str(year)[-2:])),
			                            header=None, names=['cpt_code', 'betos_code', 'betos_code_name'], sep=" ")
			pdf_crosswalk = pdf_crosswalk.loc[pdf_crosswalk['betos_code'].notna()]
			pdf_cat = pd.read_csv(os.path.join(cls.data_folder,
			                                   'betos2_cat.csv'))
			pdf_cat_lvl2 = pd.read_csv(os.path.join(cls.data_folder,
			                                   'betos2_cat_lvl2.csv'), na_filter=False)
			pdf_fam = pd.read_csv(os.path.join(cls.data_folder,
			                                   'betfam20.txt'), dtype='object')
			pdf_fam['betos_fam_abbr'] = 'betos_fam_' + pdf_fam['family']
			dct_betos_cat = dict(pdf_cat[['betos_code_start', 'betos_code_abbr']].values.tolist())
			dct_betos_cat_lvl2 = dict(pdf_cat_lvl2[['betos_code_start', 'betos_code_abbr']].values.tolist())
			dct_betos_fam = dict(pdf_fam[['family', 'betos_fam_abbr']].values.tolist())
			pdf_crosswalk['betos_cat'] = pdf_crosswalk['betos_code'].apply(lambda x: dct_betos_cat[x[0]])
			pdf_crosswalk['betos_cat_lvl2'] = pdf_crosswalk['betos_code'].apply(lambda x: dct_betos_cat_lvl2[x[:2]])
			pdf_crosswalk['betos_fam'] = pdf_crosswalk['betos_code'].apply(lambda x: dct_betos_fam[x[2:4]])
		return pdf_crosswalk

	@classmethod
	def get_betos_cat(cls, df: dd.DataFrame, pdf_crosswalk: pd.DataFrame, claim_type:str='medicaid',
	                  proc_code_prefix:str='PRCDR_CD', concat_codes_to_list:bool=True) -> dd.DataFrame:
		"""
		Get claimwise Betos codes & categories related to CPT procedure codes in claim
		:param dd.DataFrame df: Claim dask dataframe
		:param pd.DataFrame pdf_crosswalk: CPT x Betos code crosswalk, with betos code and betos category information
		:param str claim_type: Medicaid or Medicare claim type
		:param str proc_code_prefix: Column name prefix for procedure code columns
		:param bool concat_codes_to_list: When true, output betos code & cats as comma separated string columns
		"""
		dct_code_lookup = pdf_crosswalk.groupby(['betos_code'])['cpt_code'].apply(lambda x: tuple(x)).to_dict()
		dct_cat_lookup = pdf_crosswalk.groupby(['betos_cat'])['cpt_code'].apply(lambda x: tuple(x)).to_dict()
		# pdf_lookup = pdf_crosswalk.groupby('betos_code').agg({'cpt_code': list,
		#                                             'betos_cat': pd.Series.mode,
		#                                             'betos_code_name': pd.Series.mode})
		# dct_code_lookup = pdf_lookup.groupby(['betos_code'])['cpt_code'].apply(lambda x: tuple(x)).to_dict()
		# dct_cat_lookup = pdf_lookup.groupby(['betos_cat'])['cpt_code'].apply(lambda x: tuple(x)).to_dict()
		lst_col_to_delete = []
		if claim_type == 'medicaid':
			proc_code_col_prefix = str(proc_code_prefix)
			## Filtering procedure codes with sys code = 1 (CPT) & 6 (HCPCS)
			df = df.map_partitions(
				lambda pdf: pdf.assign(**dict([("VALID_" + (col.replace("_SYS", "")),
				                                pdf[col.replace("_SYS", "")]
				                                .where((pd.to_numeric(pdf[col], errors='coerce').isin([1,6])),
				                                       "").str.strip().str.upper().str.replace(".", "")
				                                ) for col in pdf.columns if col.startswith(proc_code_col_prefix + '_SYS')
				                               ])))
			lst_col_to_delete.extend(["VALID_" + (col.replace("_SYS", "")) for col in df.columns
				                                              if col.startswith(proc_code_col_prefix + '_SYS')])
			proc_code_prefix = "VALID_" + proc_code_prefix
		df = df.map_partitions(
			lambda pdf: pdf.assign(
				**dict([("lst_betos_code",
		                 pdf[[col for col in pdf.columns if col.startswith(proc_code_prefix)]]
		                        .apply(lambda x: ",".join(list(set([betos_code for betos_code in list(dct_code_lookup.keys())
		                                                    if any(cpt_code.strip().startswith(dct_code_lookup[betos_code])
		                                                           for cpt_code in ",".join(x).split(","))]))),
		                                axis=1)
				         ),
				        ("lst_betos_cat",
				         pdf[[col for col in pdf.columns if col.startswith(proc_code_prefix)]]
				         .apply(lambda x: ",".join(list(set([betos_cat for betos_cat in list(dct_cat_lookup.keys())
				                                             if any(cpt_code.strip().startswith(dct_cat_lookup[betos_cat])
				                                                    for cpt_code in ",".join(x).split(","))]))),
				                axis=1)
				         )
				        ])))
		# if concat_codes_to_list == True:
		# 	lst_betos_code_col = ["betos_code_" + betos_code for betos_code in dct_code_lookup.keys()]
		# 	lst_betos_cat_col = list(dct_cat_lookup.keys())
		# 	lst_col_to_delete.extend(lst_betos_cat_col + lst_betos_code_col)
		# 	df = df.map_partitions(
		# 		lambda pdf: pdf.assign(
		# 			lst_betos_code=pd.Series([''.join(x).strip(', ')
		# 			                          for x in np.where(pdf[lst_betos_code_col],
		# 			                                            ['{},'.format(colname)
		# 			                                                    for colname in lst_betos_code_col],
		# 			                                            '')], index=pdf.index),
		# 			lst_betos_cat=pd.Series([''.join(x).strip(', ') for x in np.where(pdf[lst_betos_cat_col],
		# 			                                            ['{},'.format(colname)
		# 			                                                    for colname in lst_betos_cat_col],
		# 			                                            '')], index=pdf.index)
		# 		)
		# 	)
			# df = df.assign(lst_betos_code = df['lst_betos_code'].replace('', np.nan),
			#                lst_betos_cat= df['lst_betos_cat'].replace('', np.nan))


		df = df[[col for col in df.columns if col not in lst_col_to_delete]]
		return df


def assign_betos_cat(df: dd.DataFrame, year:int, claim_type:str='medicaid',
	                  proc_code_prefix:str='PRCDR_CD') -> dd.DataFrame:
		"""
		Get claimwise BETOS codes & categories related to CPT procedure codes in claim
		:param dd.DataFrame df: Claim dask dataframe
		:param int year: Public use file year
		:param str claim_type: Medicaid or Medicare claim type
		:param str proc_code_prefix: Column name prefix for procedure code columns
		:param bool concat_codes_to_list: When true, output betos code & cats as comma separated string columns
		:rtype dd.DataFrame: Added columns in output dataframe:
								If concat_codes_to_list=True,
									* lst_betos_code - Comma separated BETOS codes
									* lst_betos_cat - Comma separated BETOS cat
								Else,
									One boolean column each for all BETOS codes & BETOS categories

		"""
		pdf_crosswalk = BetosProcCodes.get_betos_cpt_crosswalk(year)
		df = BetosProcCodes.get_betos_cat(df, pdf_crosswalk, claim_type, proc_code_prefix)
		return df


import pandas as pd
import sys

sys.path.append('../')
from preprocessing import cms_file


class CC(cms_file.CMSFile):
	def __init__(self, year, st, data_root, index_col='BENE_MSIS', clean=True, preprocess=True):
		super(CC, self).__init__('cc', year, '', data_root, index_col, clean, preprocess)
		self.df = self.df.loc[self.df['STATE_CD'] == st]
		if clean:
			pass
		if preprocess:
			lst_conditions = [col.removesuffix('_COMBINED').lower() for col in self.df.columns
			                  if col.endswith('_COMBINED')]
			self.agg_conditions(lst_conditions)

	def agg_conditions(self, lst_conditions):
		self.df = self.df.map_partitions(
			lambda pdf: pdf.assign(**dict([(condn + '_combined',
			                                pdf[[condn.upper() + '_MEDICAID',
			                                     condn.upper() + '_MEDICARE',
			                                     condn.upper() + '_COMBINED']].apply(pd.to_numeric, errors='coerce')
			                                .isin([1, 3])
			                                .any(axis='columns').astype(int)) for condn in lst_conditions])))

	def get_chronic_conditions(self, lst_conditions=None):
		if lst_conditions is None:
			lst_conditions = [col.removesuffix('_COMBINED').lower() for col in self.df.columns
			                  if col.endswith('_COMBINED')]
		if not set([condn + '_combined' for condn in lst_conditions]).issubset(set(list(self.df.columns))):
			self.agg_conditions(lst_conditions)
		return self.df[[condn + '_combined' for condn in lst_conditions]]



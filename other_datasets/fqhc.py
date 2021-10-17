import sys
import os
import requests
import copy
import re
from time import sleep
from datetime import datetime
from fuzzywuzzy import process, fuzz
import ast
from collections import Counter
import dask.dataframe as dd
import pandas as pd
import glob
import numpy as np
import usaddress

sys.path.append('../../')
from other_datasets import hcris
from common_utils.usps_address import USPSAddress


hcris_lookup_folder = os.path.join(os.path.dirname(__file__), 'data', 'hcris')
nppes_lookup_folder = os.path.join(os.path.dirname(__file__), 'data', 'nppes')
fqhc_lookup_folder = os.path.join(os.path.dirname(__file__), 'data', 'fqhc')
pq_engine = 'fastparquet'

# NPI Lookup params
npi_api = 'https://npiregistry.cms.hhs.gov/api/?'
dct_params = {'enumeration_type': 'NPI-2', 'version': 2.1, 'limit': 100}


def clean_hclinic_name(hclinic_name):
	return re.sub(' +', ' ', re.sub("[^a-zA-Z0-9 ]+", '', hclinic_name))


def clean_zip(zipcode):
	return str(zipcode).replace('-', '')


def get_taxonomies(lstdct_tax):
	return ",".join([dct_tax['code'] for dct_tax in lstdct_tax])


def flatten_nppes_query_result(pdf):
	def json_to_list(lstdct_match, target_name, target_name2, target_address, target_city, target_state,
	                 lst_priority_taxonomies=['261QF0400X', '261QR1300X']):
		lst_res = []
		lst_res = [expand_query_result(dct_res, target_name,
		                               target_name2, target_address, target_city, target_state)
		           for dct_res in lstdct_match]
		return lst_res

	def expand_query_result(dct_res, target_name, target_name2, target_address, target_city, target_state):
		fqhc = 0
		rural = 0
		npi = dct_res['number']
		lst_name = [dct_res['basic']['name']] if 'name' in dct_res['basic'] else []
		lst_name = lst_name + [dct_res['basic']['organization_name']] if ('organization_name' in dct_res['basic']) else []
		if ('other_names' in dct_res):
			for dct_oth in dct_res['other_names']:
				lst_name = lst_name + [dct_oth['organization_name']] if ('organization_name' in dct_oth) else []
		orgname = process.extractOne(target_name,
		                             lst_name,
		                             scorer=fuzz.ratio) if (len(lst_name) > 0) else None
		orgnameoth = ''
		if orgname is not None:
			orgname = orgname[0]
			lst_othname = [n for n in lst_name if n != orgname]
			orgnameoth = process.extractOne(target_name2 if pd.notnull(target_name2) else target_name,
			                                lst_othname,
			                                scorer=fuzz.ratio) if (len(lst_othname) > 0) else None
			if orgnameoth is not None:
				orgnameoth = orgnameoth[0]

		lstdct_address = dct_res['addresses'] if ('addresses' in dct_res) else []
		lstdct_address.extend(dct_res['practiceLocations'] if ('practiceLocations' in dct_res) else [])
		lst_addresses = [
			dct_adrs['address_1'] + ' ' + dct_adrs['address_2'] + ' ' + dct_adrs['city'] + ' ' + dct_adrs['state'] for \
			dct_adrs in lstdct_address]
		target_address_full = (target_address if pd.notnull(target_address) else '') + ' ' + \
		                      (target_city if pd.notnull(target_city) else '') + ' ' + \
		                      (target_state if pd.notnull(target_state) else '')
		address = process.extractOne(target_address_full,
		                             lst_addresses,
		                             scorer=fuzz.ratio) if (len(lst_addresses) > 0) else None
		plocline1 = plocline2 = pmailline1 = pmailline2 = ploccityname = pmailcityname = plocstatename = \
			pmailstatename = ploczip = pmailzip = ''
		if address is not None:
			address = address[0]
			adr_id = lst_addresses.index(address)
			plocline1 = lstdct_address[adr_id]['address_1']
			plocline2 = lstdct_address[adr_id]['address_2']
			ploccityname = lstdct_address[adr_id]['city']
			plocstatename = lstdct_address[adr_id]['state']
			ploczip = lstdct_address[adr_id]['postal_code']
			lst_other_address = [adr for adr in lst_addresses if adr != address]
			othaddress = process.extractOne(target_address_full,
			                                lst_other_address,
			                                scorer=fuzz.ratio) if (len(lst_other_address) > 0) else \
				None
			if othaddress is not None:
				othaddress = othaddress[0]
				othadr_id = lst_addresses.index(othaddress)
				pmailline1 = lstdct_address[othadr_id]['address_1']
				pmailline2 = lstdct_address[othadr_id]['address_2']
				pmailcityname = lstdct_address[othadr_id]['city']
				pmailstatename = lstdct_address[othadr_id]['state']
				pmailzip = lstdct_address[othadr_id]['postal_code']

		lst_tax = [dcttax['code'] for dcttax in dct_res['taxonomies']]
		if '261QF0400X' in lst_tax:
			fqhc = 1
		if '261QR1300X' in lst_tax:
			rural = 1
		return npi, orgname, orgnameoth, plocline1, plocline2, ploccityname, plocstatename, ploczip, pmailline1, \
		       pmailline2, pmailcityname, pmailstatename, pmailzip, fqhc, rural

	pdf['lst_matches'] = pdf.apply(lambda row: json_to_list(row['results'],
	                                                        row['hclinic_name'],
	                                                        np.nan,
	                                                        row['hclinic_street_address'],
	                                                        row['hclinic_city'],
	                                                        row['hclinic_state']
	                                                        ),
	                               axis=1, )
	pdf = pdf.explode('lst_matches')
	pdf[['npi', 'p_org_name', 'p_org_name_oth', 'p_loc_line_1', 'p_loc_line_2',
	     'p_loc_city', 'p_loc_state', 'p_loc_zip', 'p_mail_line_1',
	     'p_mail_line_2', 'p_mail_city', 'p_mail_state', 'p_mail_zip', 'fqhc',
	     'rural']] = pdf['lst_matches'].apply(
		pd.Series)
	pdf = pdf.drop(['lst_matches'], axis=1)
	return pdf


def compute_match_purity(pdf):
	def match_purity(p_org_name, p_org_name_oth, provider_id_state,
	                 p_loc_line_1, p_loc_line_2, p_loc_city, p_loc_state,
	                 p_mail_line_1, p_mail_line_2,
	                 p_mail_city, p_mail_state,
	                 hclinic_name,  hclinic_street_address,
	                 hclinic_city, hclinic_state):
		name_score = 0 if (hclinic_name == '') else max(fuzz.ratio(p_org_name, hclinic_name),
		                                                fuzz.ratio(p_org_name_oth, hclinic_name),
		                                                fuzz.ratio(p_loc_line_2, hclinic_name),
		                                                fuzz.ratio(p_mail_line_2, hclinic_name))
		address_score = 0 if (hclinic_street_address == '') else max(
			fuzz.ratio((p_loc_line_1 or '') + ' ' + (p_loc_line_2 or ''), hclinic_street_address),
			fuzz.ratio((p_mail_line_1 or '') + ' ' + (p_mail_line_2 or ''), hclinic_street_address),
			fuzz.ratio((p_loc_line_1 or ''), hclinic_street_address),
			fuzz.ratio((p_mail_line_1 or ''), hclinic_street_address), )
		city_score = 0 if (hclinic_city == '') else max(fuzz.ratio(p_loc_city, hclinic_city),
		                                                fuzz.ratio(p_mail_city, hclinic_city))
		state_match = int((hclinic_state == provider_id_state) or
		                  (hclinic_state == p_loc_state) or
		                  (hclinic_state == p_mail_state))

		return [name_score, address_score, city_score, state_match]

	if 'provider_id_state' not in pdf.columns:
		pdf['provider_id_state'] = ''
	pdf = pdf.replace(to_replace=[None, 'None'], value=np.nan).fillna("")
	pdf = pdf.assign(
		**dict([(col, pdf[col].astype(str))
		        for col in ['provider_id_state', 'p_loc_line_1', 'p_loc_line_2', 'p_loc_city',
		                    'p_loc_state', 'p_mail_line_1', 'p_mail_line_2', 'p_mail_city', 'p_mail_state',
		                    'hclinic_name', 'hclinic_street_address', 'hclinic_city', 'hclinic_state']]))

	pdf[['name_score', 'address_score', 'city_score', 'state_match']] = pdf.apply(
		lambda row: match_purity(row['p_org_name'],
		                         row['p_org_name_oth'],
		                         row['provider_id_state'],
		                         row['p_loc_line_1'],
		                         row['p_loc_line_2'],
		                         row['p_loc_city'],
		                         row['p_loc_state'],
		                         row['p_mail_line_1'],
		                         row['p_mail_line_2'],
		                         row['p_mail_city'],
		                         row['p_mail_state'],
		                         row['hclinic_name'],
		                         row['hclinic_street_address'],
		                         row['hclinic_city'],
		                         row['hclinic_state'],
		                         ),
		axis=1, result_type='expand')
	return pdf


def compute_door_no_matches(pdf):
	pdf = pdf.assign(hclinic_address_digits=pdf['hclinic_street_address']
	                 .apply(lambda x: next(iter([int(s) for s in x.split() if s.isdigit()]), np.nan)).fillna(''),
	                 p_loc_address_digits=pdf['p_loc_line_1']
	                 .apply(lambda x: next(iter([int(s) for s in x.split() if s.isdigit()]), np.nan)
	                 if pd.notnull(x) else np.nan).fillna(''),
	                 p_mail_address_digits=pdf['p_mail_line_1']
	                 .apply(lambda x: next(iter([int(s) for s in x.split() if s.isdigit()]), np.nan)
	                 if pd.notnull(x) else np.nan).fillna(''))
	sleep_time = 0 if (datetime.today().weekday() in [5, 6]) else 0.12

	def parse_address(hclinic_street_address, hclinic_city, hclinic_state, hclinic_zipcode,
	                  p_loc_line_1, p_loc_line_2, p_loc_city, p_loc_state, p_loc_zipcode,
	                  p_mail_line_1, p_mail_line_2, p_mail_city, p_mail_state, p_mail_zipcode,):
		p_loc_address = (p_loc_line_1 or '') + ' ' + (p_loc_line_2 or '')
		p_mail_address = (p_mail_line_1 or '') + ' ' + (p_mail_line_2 or '')

		hclinic_usps_address = USPSAddress(name='',
		                                   street=hclinic_street_address,
		                                   city=hclinic_city,
		                                   state=hclinic_state,
		                                   zip4=hclinic_zipcode)
		hclinic_standardized_address = ''
		hclinic_address_error = ''
		try:
			hclinic_standardized_address = hclinic_usps_address.standardized()
			hclinic_address_error = hclinic_usps_address._error
		except Exception as ex:
			hclinic_address_error = 'API call error'
		hclinic_standardized = int(bool(hclinic_standardized_address))
		sleep(sleep_time)

		dct_hclinic_address = dict(usaddress.parse(hclinic_standardized_address if hclinic_standardized
		                                           else hclinic_street_address))

		p_loc_usps_address = USPSAddress(name='',
		                                 street=p_loc_address,
		                                 city=p_loc_city,
		                                 state=p_loc_state,
		                                 zip4=p_loc_zipcode)
		p_loc_address_error = ''
		p_loc_standardized_address = ''
		try:
			p_loc_standardized_address = p_loc_usps_address.standardized()
			p_loc_address_error = p_loc_usps_address._error
		except Exception as ex:
			p_loc_address_error = 'API call error'
		p_loc_standardized = int(bool(p_loc_standardized_address))
		sleep(sleep_time)

		dct_p_loc_address = dict(usaddress.parse(p_loc_standardized_address if p_loc_standardized
		                                         else p_loc_address))

		p_mail_usps_address = USPSAddress(name='',
		                                  street=p_mail_address,
		                                  city=p_mail_city,
		                                  state=p_mail_state,
		                                  zip4=p_mail_zipcode)
		p_mail_standardized_address = ''
		p_mail_address_error = ''
		try:
			p_mail_standardized_address = p_mail_usps_address.standardized()
			p_mail_address_error = p_mail_usps_address._error
		except Exception as ex:
			p_mail_address_error = 'API call error'
		p_mail_standardized = int(bool(p_mail_standardized_address))
		sleep(sleep_time)

		dct_p_mail_address = dict(usaddress.parse(p_mail_standardized_address if p_mail_standardized
		                                          else p_mail_address))

		dct_hclinic_address = {v: k for k, v in dct_hclinic_address.items()}
		dct_p_loc_address = {v: k for k, v in dct_p_loc_address.items()}
		dct_p_mail_address = {v: k for k, v in dct_p_mail_address.items()}

		hclinic_post_box_id = dct_hclinic_address.get('USPSBoxID', np.nan)
		hclinic_post_box_group_id = dct_hclinic_address.get('USPSBoxGroupID', np.nan)
		hclinic_address_no = dct_hclinic_address.get('AddressNumber', np.nan)
		hclinic_occupancy_no = dct_hclinic_address.get('OccupancyIdentifier', np.nan)

		p_loc_post_box_id = dct_p_loc_address.get('USPSBoxID', np.nan)
		p_loc_post_box_group_id = dct_p_loc_address.get('USPSBoxGroupID', np.nan)
		p_loc_address_no = dct_p_loc_address.get('AddressNumber', np.nan)
		p_loc_occupancy_no = dct_p_loc_address.get('OccupancyIdentifier', np.nan)

		p_mail_post_box_id = dct_p_mail_address.get('USPSBoxID', np.nan)
		p_mail_post_box_group_id = dct_p_mail_address.get('USPSBoxGroupID', np.nan)
		p_mail_address_no = dct_p_mail_address.get('AddressNumber', np.nan)
		p_mail_occupancy_no = dct_p_mail_address.get('OccupancyIdentifier', np.nan)

		return [hclinic_post_box_id, hclinic_post_box_group_id, hclinic_address_no,
		        hclinic_occupancy_no, p_loc_post_box_id, p_loc_post_box_group_id,
		        p_loc_address_no, p_loc_occupancy_no, p_mail_post_box_id,
		        p_mail_post_box_group_id, p_mail_address_no, p_mail_occupancy_no,
		        hclinic_standardized_address, p_loc_standardized_address, p_mail_standardized_address,
		        hclinic_standardized, hclinic_address_error,
		        p_loc_standardized, p_loc_address_error,
		        p_mail_standardized, p_mail_address_error]

	pdf[["hclinic_post_box_id", "hclinic_post_box_group_id", "hclinic_address_no",
	     "hclinic_occupancy_no", "p_loc_post_box_id", "p_loc_post_box_group_id",
	     "p_loc_address_no", "p_loc_occupancy_no", "p_mail_post_box_id",
	     "p_mail_post_box_group_id", "p_mail_address_no", "p_mail_occupancy_no",
	     "hclinic_standardized_address", "p_loc_standardized_address", "p_mail_standardized_address",
	     "hclinic_standardized", "hclinic_address_error",
	     "p_loc_standardized", "p_loc_address_error",
	     "p_mail_standardized", "p_mail_address_error"
	     ]] = pdf.apply(lambda row: parse_address(row['hclinic_street_address'],
	                                              row['hclinic_city'],
	                                              row['hclinic_state'],
	                                              row['hclinic_zipcode'],
	                                              row['p_loc_line_1'],
	                                              row['p_loc_line_2'],
	                                              row['p_loc_city'],
	                                              row['p_loc_state'],
	                                              row['p_loc_zip'],
	                                              row['p_mail_line_1'],
	                                              row['p_mail_line_2'],
	                                              row['p_mail_city'],
	                                              row['p_mail_state'],
	                                              row['p_mail_zip']
	                                              ),
	                    axis=1, result_type='expand')
	pdf = pdf.replace(to_replace=[None, 'None'], value=np.nan).fillna("")

	def match_purity(p_loc_address_digits, p_loc_post_box_id, p_loc_post_box_group_id,
	                 p_loc_address_no, p_loc_occupancy_no, p_mail_address_digits, p_mail_post_box_id,
	                 p_mail_post_box_group_id, p_mail_address_no, p_mail_occupancy_no,
	                 hclinic_address_digits, hclinic_post_box_id, hclinic_post_box_group_id,
	                 hclinic_address_no, hclinic_occupancy_no, hclinic_standardized_address,
	                 p_loc_standardized_address, p_mail_standardized_address, hclinic_standardized):
		digits_match = 0 if (hclinic_address_digits == '') else int((hclinic_address_digits == p_loc_address_digits) or
		                                                            (hclinic_address_digits == p_mail_address_digits))
		door_no_match = 0 if not bool(hclinic_post_box_id or hclinic_post_box_group_id or
		                              hclinic_address_no or hclinic_occupancy_no
		                              ) else int(((bool(hclinic_address_no) &
		                                           ((hclinic_address_no == p_loc_address_no) |
		                                            (hclinic_address_no == p_mail_address_no))) |
		                                          (not bool(hclinic_address_no))) &
		                                         ((bool(hclinic_occupancy_no) &
		                                           ((hclinic_occupancy_no == p_loc_occupancy_no) |
		                                            (hclinic_occupancy_no == p_mail_occupancy_no))) | (not bool(
		                                             hclinic_occupancy_no))) &
		                                         ((bool(hclinic_post_box_id) &
		                                           ((hclinic_post_box_id == p_loc_post_box_id) |
		                                            (hclinic_post_box_id == p_mail_post_box_id))) | (not bool(
		                                             hclinic_post_box_id))) &
		                                         ((bool(hclinic_post_box_group_id) &
		                                           ((hclinic_post_box_group_id == p_loc_post_box_group_id) |
		                                            (hclinic_post_box_group_id == p_mail_post_box_group_id))) | (
		                                              not bool(
		                                                  hclinic_post_box_group_id))
		                                          ))

		standardized_address_score = 0 if ((hclinic_standardized_address == '') |
		                                   (not hclinic_standardized)
		                                   ) else max(fuzz.ratio(p_loc_standardized_address,
		                                                         hclinic_standardized_address),
		                                              fuzz.ratio(p_mail_standardized_address,
		                                                         hclinic_standardized_address))

		return [standardized_address_score, digits_match, door_no_match]

	pdf[['standardized_address_score', 'digits_match', 'door_no_match']] = pdf.apply(
	    lambda row: match_purity(row['p_loc_address_digits'],
	                             row["p_loc_post_box_id"],
	                             row["p_loc_post_box_group_id"],
	                             row["p_loc_address_no"],
	                             row["p_loc_occupancy_no"],
	                             row['p_mail_address_digits'],
	                             row["p_mail_post_box_id"],
	                             row["p_mail_post_box_group_id"],
	                             row["p_mail_address_no"],
	                             row["p_mail_occupancy_no"],
	                             row['hclinic_address_digits'],
	                             row["hclinic_post_box_id"],
	                             row["hclinic_post_box_group_id"],
	                             row["hclinic_address_no"],
	                             row["hclinic_occupancy_no"],
	                             row['hclinic_standardized_address'],
	                             row['p_loc_standardized_address'],
	                             row['p_mail_standardized_address'],
	                             row['hclinic_standardized']
	                             ),
	    axis=1, result_type='expand')
	return pdf


def match_hcris_fqhcs():
	df_hcris = pd.read_pickle(os.path.join(hcris_lookup_folder, 'hcris_all_years.pickle'))

	df_npi_provider = dd.concat([dd.read_parquet(os.path.join(nppes_lookup_folder,
	                                                          str(yr),
	                                                          'npi_provider_parquet_cleaned'),
	                                             engine=pq_engine,
	                                             index=False) for yr in range(2009, 2022)])
	df_npi_provider_cleaned = df_npi_provider.assign(
		provider_id=df_npi_provider['provider_id'].str.rstrip('0'))

	df_npi = dd.concat([dd.read_parquet(os.path.join(nppes_lookup_folder,
	                                                 str(yr),
	                                                 'npi_cleaned_parquet'),
	                                    engine=pq_engine,
	                                    index=False) for yr in range(2009, 2022)])

	df_hcris_matched = df_npi_provider.merge(df_hcris,
	                                                     left_on=['provider_id', 'provider_id_state'],
	                                                     right_on=['hclinic_provider_id_cleaned', 'hclinic_state'],
	                                                     how='inner'
	                                                     ).compute()

	df_hcris_matched_ploc = df_npi_provider.merge(df_hcris,
	                                              left_on=['provider_id', 'p_loc_state'],
	                                              right_on=['hclinic_provider_id_cleaned', 'hclinic_state'],
	                                              how='inner'
	                                              ).compute()

	df_hcris_matched_mloc = df_npi_provider.merge(df_hcris,
	                                              left_on=['provider_id', 'p_mail_state'],
	                                              right_on=['hclinic_provider_id', 'hclinic_state'],
	                                              how='inner'
	                                              ).compute()

	df_hcris_matched = pd.concat([df_hcris_matched,
	                              df_hcris_matched_ploc,
	                              df_hcris_matched_mloc],
	                             ignore_index=True).reset_index() \
		.drop_duplicates(keep='first') \
		.reset_index(drop=True)

	df_hcris_matched.to_pickle(os.path.join(fqhc_lookup_folder, 'hcris_nppes_based_matches.pkl'))

	df_hcris_api_based = df_hcris.loc[~df_hcris['hclinic_provider_id'].isin(df_hcris_matched['hclinic_provider_id'])]

	df_hcris_api_based = dd.from_pandas(df_hcris_api_based, npartitions=30)
	df_hcris_api_based = df_hcris_api_based.map_partitions(
		lambda pdf: pdf.assign(
			npi_json=pdf.apply(
				lambda row: requests.get(
					npi_api,
					params={**dct_params,
					        **{'organization_name': clean_hclinic_name(row['hclinic_name']),
					           'city': row['hclinic_city'],
					           'state': row['hclinic_state'],
					           'postcode': clean_zip(row['hclinic_zipcode'])
					           }
					        }, ).json(),
				axis=1
			)
		)
	).compute()
	df_hcris_api_based[['result_count', 'results']] = df_hcris_api_based['npi_json'].apply(pd.Series)

	df_hcris_api_based_relaxed_city = df_hcris_api_based.loc[df_hcris_api_based['result_count'] < 1]
	df_hcris_api_based_matched = df_hcris_api_based.loc[df_hcris_api_based['result_count'] >= 1]

	df_hcris_api_based_relaxed_city = dd.from_pandas(df_hcris_api_based_relaxed_city, npartitions=30)
	df_hcris_api_based_relaxed_city = df_hcris_api_based_relaxed_city.map_partitions(
		lambda pdf: pdf.assign(
			npi_json=pdf.apply(
				lambda row: requests.get(
					npi_api,
					params={**dct_params,
					        **{'organization_name': clean_hclinic_name(row['hclinic_name']),
					           'state': row['hclinic_state'],
					           'postcode': clean_zip(row['hclinic_zipcode'])
					           }
					        }).json(),
				axis=1
			)
		)
	).compute()
	df_hcris_api_based_relaxed_city[['result_count', 'results']] = df_hcris_api_based_relaxed_city['npi_json'].apply(
		pd.Series)

	df_hcris_api_based_relaxed_name = df_hcris_api_based_relaxed_city.loc[
		df_hcris_api_based_relaxed_city['result_count'] < 1]
	df_hcris_api_based_matched = pd.concat([df_hcris_api_based_matched,
	                                        df_hcris_api_based_relaxed_city.loc[
		                                        df_hcris_api_based_relaxed_city['result_count'] >= 1]],
	                                       ignore_index=True).reset_index(drop=True)

	df_hcris_api_based_relaxed_name = dd.from_pandas(df_hcris_api_based_relaxed_name, npartitions=30)
	df_hcris_api_based_relaxed_name = df_hcris_api_based_relaxed_name.map_partitions(
		lambda pdf: pdf.assign(
			npi_json=pdf.apply(
				lambda row: requests.get(
					npi_api,
					params={**dct_params,
					        **{'organization_name': clean_hclinic_name(row['hclinic_name']) + '*',
					           'state': row['hclinic_state'],
					           'zip': clean_zip(row['hclinic_zipcode'])
					           }
					        }).json(),
				axis=1
			)
		)
	).compute()
	df_hcris_api_based_relaxed_name[['result_count', 'results']] = df_hcris_api_based_relaxed_name['npi_json'].apply(
		pd.Series)

	df_hcris_api_based_relaxed_more_relaxed_name = df_hcris_api_based_relaxed_name.loc[
		df_hcris_api_based_relaxed_name['result_count'] < 1]

	df_hcris_api_based_matched = pd.concat([df_hcris_api_based_matched,
	                                        df_hcris_api_based_relaxed_name.loc[
		                                        df_hcris_api_based_relaxed_name['result_count'] >= 1]],
	                                       ignore_index=True).reset_index(drop=True)

	df_hcris_api_based_matched_flattened = flatten_nppes_query_result(df_hcris_api_based_matched)

	df_hcris_api_based_matched_flattened = dd.from_pandas(df_hcris_api_based_matched_flattened
	                                                      .replace(to_replace=[None, 'None'],
	                                                               value=np.nan).fillna(""),
	                                                      npartitions=30)
	df_hcris_api_based_matched_flattened = df_hcris_api_based_matched_flattened \
		.map_partitions(lambda pdf: compute_match_purity(pdf)).compute()

	df_hcris_api_based_matched_flattened.to_pickle(os.path.join(fqhc_lookup_folder,
	                                                            'hcris_api_perfect_matches.pkl'))
	df_hcris_perfect_matches = pd.concat(
		[df_hcris_api_based_matched_flattened, df_hcris_matched],
	    ignore_index=True).sort_values(by=['hclinic_provider_id', 'npi']).reset_index(drop=True)

	df_hcris_perfect_matches.to_pickle(os.path.join(fqhc_lookup_folder, 'hcris_perfect_matches.pkl'))

	df_hcris_unmatched = df_hcris.loc[
		~df_hcris['hclinic_provider_id'].isin(df_hcris_perfect_matches['hclinic_provider_id'])]
	df_hcris_state_relaxed = df_npi_provider.merge(df_hcris_unmatched,
	                                               left_on=['provider_id'],
	                                               right_on=['hclinic_provider_id'],
	                                               how='inner'
	                                               ).compute()

	df_hcris_state_relaxed = dd.from_pandas(df_hcris_state_relaxed
	                                                      .replace(to_replace=[None, 'None'],
	                                                               value=np.nan).fillna(""),
	                                                      npartitions=30)
	df_hcris_state_relaxed = df_hcris_state_relaxed \
		.map_partitions(lambda pdf: compute_match_purity(pdf)).compute()

	df_hcris_state_relaxed = df_hcris_state_relaxed.drop_duplicates(['hclinic_provider_id', 'npi'], keep='first'). \
		sort_values(by=['hclinic_provider_id', 'npi']).reset_index(drop=True)
	df_hcris_state_relaxed_matched = df_hcris_state_relaxed.loc[
		(df_hcris_state_relaxed[['name_score', 'address_score']] > 80) \
			.any(axis='columns')]

	df_hcris_matched = pd.concat([df_hcris_perfect_matches,
	                              df_hcris_state_relaxed_matched],
	                             ignore_index=True).sort_values(by=['hclinic_provider_id', 'npi']).reset_index(
		drop=True)

	df_hcris_matched.to_pickle(os.path.join(fqhc_lookup_folder, 'hcris_perfect_matches_with_state_relaxed.pkl'))


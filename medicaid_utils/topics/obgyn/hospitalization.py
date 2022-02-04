import dask.dataframe as dd
import pandas as pd
import numpy as np
from medicaid_utils.filters.claims import dx_and_proc


def flag_preterm(df_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Detects preterm birth related hospitalization in claims
    New Column(s):
        hosp_preterm - integer column, 1 when claim denotes preterm birth and 0 otherwise
    :param df_claims:
    :rtype: dd.DataFrame
    """
    dct_diag_codes = {'preterm': {'incl': ['6440', '6442', '7651', '7650', '7652'],
                                  'excl': ['76520', '76529']}}
    df_claims = dx_and_proc.flag_diagnoses_and_procedures(dct_diag_codes, {}, df_claims)
    df_claims = df_claims.rename(columns={'diag_preterm': 'hosp_preterm'})
    return df_claims


def flag_multiple_births(df_claims: dd.DataFrame) -> dd.DataFrame:
    """
            Identifies multiple births
            New Column(s):
                hosp_multiple_births -
            :param df_claims:
            :rtype: dd.DataFrame
            """
    dct_diag_codes = {'multiple_births': {'incl': ['V272', 'V273', 'V274',
                                                  'V275', 'V276', 'V277']}}
    df_claims = dx_and_proc.flag_diagnoses_and_procedures(dct_diag_codes, {}, df_claims)
    df_claims = df_claims.rename(columns={'diag_multiple_births': 'hosp_multiple_births'})
    return df_claims


def flag_delivery_mode(df_claims: dd.DataFrame) -> dd.DataFrame:
    """
        Identifies mode of birth
        New Column(s):
            hosp_vag_dlvry - vaginal delivery, int, 0 or 1
            hosp_csrn_dlvry - caesarian delivery, int, 0 or 1
        :param df_claims:
        :rtype: dd.DataFrame
        """
    dct_proc_codes = {'vag_dlvry': {1: [str(cd) for cd in range(59400, 59411)],
                                    6: [str(cd) for cd in range(59400, 59411)]},
                      'csrn_dlvry': {1: [str(cd) for cd in range(59510, 59516)] +
                                        [str(cd) for cd in range(59618, 59623)],
                                     2: ['74'],
                                     6: [str(cd) for cd in range(59510, 59516)] +
                                        [str(cd) for cd in range(59618, 59623)]
                                     }}
    df_claims = dx_and_proc.flag_diagnoses_and_procedures({}, dct_proc_codes, df_claims)
    df_claims = df_claims.rename(columns={'proc_vag_dlvry': 'hosp_vag_dlvry',
                                          'proc_csrn_dlvry': 'hosp_csrn_dlvry',})
    return df_claims


def flag_delivery(df_ip_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Detects normal and stillbirths related hospitalization in claims
    New Column(s):
        hosp_birth - integer column, 1 when claim denotes live or still birth and 0 otherwise
    :param df_ip_claims:
    :rtype: dd.DataFrame
    """
    df_ip_claims = df_ip_claims.assign(hosp_birth=(dd.to_numeric(df_ip_claims['RCPNT_DLVRY_CD'],
                                                                 errors='coerce') == 1).astype(int))
    return df_ip_claims


def flag_abnormal_pregnancy(df_claims: dd.DataFrame) -> dd.DataFrame:
    """
        Detects ectopic, molar, or abnormal pregnancy, spontaneous or induced abortion related hospitalization
        New Column(s):
            hosp_abnormal_pregnancy - integer column, 1 when claim denotes ectopic, molar, or abnormal pregnancy,
            spontaneous or induced abortion and 0 otherwise
        :param df_claims:
        :rtype: dd.DataFrame
        """
    dct_diag_codes = {'abnormal_pregnancy': {'incl': ['63']}}
    df_claims = dx_and_proc.flag_diagnoses_and_procedures(dct_diag_codes, {}, df_claims)
    df_claims = df_claims.rename(columns={'diag_abnormal_pregnancy': 'hosp_abnormal_pregnancy'})
    return df_claims


def flag_prenatal(df_claims: dd.DataFrame) -> dd.DataFrame:
    dct_diag_codes = {'prenatal': {'incl': ["V22", "V23"]}}
    df_claims = dx_and_proc.flag_diagnoses_and_procedures(dct_diag_codes, {}, df_claims)
    df_claims = df_claims.rename(columns={'diag_prenatal': 'hosp_prenatal'})
    return df_claims


def flag_smm_events(df_ip_claims: dd.DataFrame, index_admsn_date_col='index_admsn_date') -> dd.DataFrame:

    """
    Adds flags for SMM related hospitaliztions
    New Columns:
        hosp_smm_myo - ,
        hosp_smm_aneurysm - ,
        hosp_smm_renal - ,
        hosp_smm_respiratory - ,
        hosp_smm_embolism= - ,
        hosp_smm_cardiac - ,
        hosp_smm_coagulation - ,
        hosp_smm_eclampsia - ,
        hosp_smm_heart - ,
        hosp_smm_cerebrovascular - ,
        hosp_smm_edema - ,
        hosp_smm_anesthesia - ,
        hosp_smm_sepsis - ,
        hosp_smm_shock - ,
        hosp_smm_sickle - ,
        hosp_smm_thrombotic - ,
        hosp_smm_cardiac_rhythm - ,
        hosp_smm_transfusion - ,
        hosp_smm_hysterectomy - ,
        hosp_smm_tracheostomy - ,
        hosp_smm_ventilation -
        hosp_smm - Any SMM related hospitalization
        hosp_smm_no_blood - Any SMM related hospitalization, with transfusion not as the sole cause
    :param df_ip_claims:
    :return:
    """
    dct_diag_codes = {'smm_myo': {'incl': ['410']},
                      'smm_aneurysm': {'incl': ['441']},
                      'smm_renal': {'incl': ["5845", "5846", "5847", "5848", "5849", '6693']},
                      'smm_respiratory': {'incl': ['5185', "51881", "51882", "51884", "7991"]},
                      'smm_embolism': {'incl': ['6731']},
                      'smm_cardiac': {'incl': ["42741", "42742", "4275"]},
                      'smm_coagulation': {'incl': ["2866", "2869", '6663']},
                      'smm_eclampsia': {'incl': ['6426']},
                      'smm_heart': {'incl': ["9971"]},
                      'smm_cerebrovascular': {'incl': ['430', '431', '432', '433', '434', '436', '437', '6715',
                                                       '6740', "99702"]},
                      'smm_edema': {'incl': ["5184", "4281", "4280", "42821", "42823", "42831", "42833", "42841",
                                             "42843"]},
                      'smm_anesthesia': {'incl': ['6680', '6681', '6682']},
                      'smm_sepsis': {'incl': ['038', '6702', "99591", "99592"]},
                      'smm_shock': {'incl': ['6691', '7855', '9980', "9950", "9954"]},
                      'smm_sickle': {'incl': ["28242", "28262", "28264", "28269"]},
                      'smm_thrombotic': {'incl': ['4151', '6730', '6732', '6733', '6738']}
                      }
    dct_proc_codes = {'smm_cardiac_rhythm': {2: ['996']},
                      'smm_transfusion': {2: ['990']},
                      'smm_hysterectomy': {2: ['683', '684', '685', '686', '687', '688', '689']},
                      'smm_tracheostomy': {2: ["311"]},
                      'smm_ventilation': {2: ["9390", "9601", "9602", "9603", "9605"]}}
    df_ip_claims = dx_and_proc.flag_diagnoses_and_procedures(dct_diag_codes, dct_proc_codes, df_ip_claims)
    df_ip_claims = df_ip_claims.rename(columns=dict([(f"diag_{condn}", f"hosp_{condn}") for condn in dct_diag_codes] +
                                                    [(f"proc_{condn}", f"hosp_{condn}") for condn in dct_proc_codes]))
    df_ip_claims['hosp_smm'] = df_ip_claims[df_ip_claims.columns[df_ip_claims.columns.str.startswith('hosp_smm_')]
                                            ].max(axis=1)

    df_ip_claims["hosp_smm_no_blood"] = df_ip_claims['hosp_smm'].where((df_ip_claims['hosp_smm_transfusion'] == 0) |
                                                                       (df_ip_claims[df_ip_claims.columns[df_ip_claims
                                                                        .columns.str.startswith('hosp_smm_')]
                                                                        ].sum(axis=1) > 1),
                                                                       0)
    if index_admsn_date_col in df_ip_claims.columns:
        admsn_col_name = 'admsn_date' if 'admsn_date' in df_ip_claims.columns else 'srvc_bgn_date'
        lst_smm_col = [col for col in df_ip_claims.columns if col.startswith('hosp_smm')]
        df_ip_claims = df_ip_claims.assign(**dict([(col + '_on_index_hosp',
                                                    ((df_ip_claims[col] == 1) &
                                                     (df_ip_claims[admsn_col_name] == df_ip_claims[index_admsn_date_col])
                                                     ).astype(int))
                                                   for col in lst_smm_col]
                                                  ))
        df_ip_claims = df_ip_claims.map_partitions(lambda pdf: pdf.assign(**dict([(col + '_12weeks_after_index_hosp',
                                                                                   ((pdf[col] == 1) &
                                                                                    pdf[admsn_col_name].between(
                                                                                        pdf[index_admsn_date_col],
                                                                                        pdf[index_admsn_date_col] +
                                                                                        pd.Timedelta(days=83),
                                                                                        inclusive=True)).astype(int)) for
                                                                                  col in lst_smm_col]
                                                                                 )))
        df_ip_claims = df_ip_claims.map_partitions(lambda pdf: pdf.assign(**dict([(col + '_6weeks_after_index_hosp',
                                                                                   ((pdf[col] == 1) &
                                                                                    pdf[admsn_col_name].between(
                                                                                        pdf[index_admsn_date_col],
                                                                                        pdf[index_admsn_date_col] +
                                                                                        pd.Timedelta(days=41),
                                                                                        inclusive=True)).astype(int)) for
                                                                                  col in lst_smm_col]
                                                                                 )))
        df_ip_claims = df_ip_claims.map_partitions(lambda pdf: pdf.assign(**dict([(col + '_6weeks_prior_index_hosp',
                                                                                   ((pdf[col] == 1) &
                                                                                    pdf[admsn_col_name].between(
                                                                                        pdf[index_admsn_date_col] -
                                                                                        pd.Timedelta(days=41),
                                                                                        pdf[index_admsn_date_col],
                                                                                        inclusive=True)).astype(int)) for
                                                                                  col in lst_smm_col]
                                                                                 )))
    return df_ip_claims


def calculate_conception(df_claims: dd.DataFrame) -> (dd.DataFrame, dd.DataFrame):
    lst_hosp_pregnancy_col = ["hosp_abnormal_pregnancy", "hosp_birth", "hosp_preterm", "hosp_prenatal"]
    lst_missing_col = [col for col in lst_hosp_pregnancy_col if col not in df_claims.columns]
    if len(lst_missing_col) > 0:
        df_claims = df_claims.assign(**dict([(col, 0) for col in lst_missing_col]))

    lst_conception_col = ['abortive', 'full_term_delivery', 'preterm_delivery', 'prenatal']

    df_claims = df_claims.assign(abortive=(df_claims["hosp_abnormal_pregnancy"] == 1).astype(int),
                                 full_term_delivery=((df_claims["hosp_abnormal_pregnancy"] == 0) &
                                                     (df_claims["hosp_birth"] == 1) &
                                                     (df_claims["hosp_preterm"] == 0)).astype(int),
                                 preterm_delivery=((df_claims["hosp_abnormal_pregnancy"] == 0) &
                                                   (df_claims["hosp_birth"] == 1) &
                                                   (df_claims["hosp_preterm"] == 1)).astype(int),
                                 prenatal=((df_claims["hosp_abnormal_pregnancy"] == 0) &
                                           (df_claims["hosp_birth"] == 0) &
                                           (df_claims["hosp_prenatal"] == 1)).astype(int)
                                 )
    df_conception = df_claims.loc[df_claims[lst_conception_col].sum(axis=1) == 1]
    df_check = df_claims.loc[df_claims[lst_conception_col].sum(axis=1) > 1].compute()

    df_conception = df_conception.map_partitions(
        lambda pdf: pdf.assign(
            conception=pdf['service_date'] - np.select([(pdf['abortive'] == 1),
                                                        (pdf['full_term_delivery'] == 1),
                                                        (pdf['preterm_delivery'] == 1),
                                                        (pdf['prenatal'] == 1)],
                                                       [pd.Timedelta(days=75),
                                                        pd.Timedelta(days=270) - pd.Timedelta(days=15),
                                                        pd.Timedelta(days=245) - pd.Timedelta(days=15),
                                                        pd.Timedelta(days=45)],
                                                       default=np.nan
                                                       )
        )
    )
    df_conception = df_conception.drop(lst_conception_col, axis=1)

    return df_conception, df_check



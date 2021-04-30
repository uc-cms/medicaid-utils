import dask.dataframe as dd
import pandas as pd


def flag_preterm(df_ip_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Detects preterm birth related hospitalization in claims
    New Column(s):
        hosp_preterm - integer column, 1 when claim denotes preterm birth and 0 otherwise
    :param df_ip_claims:
    :rtype: dd.DataFrame
    """
    df_ip_claims = df_ip_claims.map_partitions(
        lambda pdf: pdf.assign(
            hosp_preterm=((~pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                          .apply(lambda x: x.str.startswith(('76529', '76529',))).any(axis='columns')) &
                          pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                           .apply(lambda x: x.str.startswith(('6440', '6442',
                                                                          '7651', '7650',
                                                                         '7652',))).any(axis='columns')
                          ).astype(int)))
    return df_ip_claims


def flag_delivery(df_ip_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Detects normal and stillbirths related hospitalization in claims
    New Column(s):
        hosp_birth - integer column, 1 when claim denotes live or still birth and 0 otherwise
    :param df_ip_claims:
    :rtype: dd.DataFrame
    """
    df_ip_claims = df_ip_claims.assign(hosp_birth = (dd.to_numeric(df_ip_claims['RCPNT_DLVRY_CD'], errors='coerce') == 1).astype(int))
    return df_ip_claims


def flag_abnormal_pregnancy(df_ip_claims: dd.DataFrame) -> dd.DataFrame:
    """
        Detects ectopic, molar, or abnormal pregnancy, spontaneous or induced abortion related hospitalization
        New Column(s):
            hosp_abnormal_pregnancy - integer column, 1 when claim denotes ectopic, molar, or abnormal pregnancy,
            spontaneous or induced abortion and 0 otherwise
        :param df_ip_claims:
        :rtype: dd.DataFrame
        """
    df_ip_claims = df_ip_claims.map_partitions(
        lambda pdf: pdf.assign(
            hosp_abnormal_pregnancy=pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                .apply(lambda x: x.str.upper().str.startswith(('63'))).any(axis='columns').astype(int)))
    return df_ip_claims


def flag_smm_events(df_ip_claims: dd.DataFrame) -> dd.DataFrame:

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
    df_ip_claims = df_ip_claims.map_partitions(
        lambda pdf: pdf.assign(
            hosp_smm_myo=pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                          .apply(lambda x: x.str.startswith(('410'))).any(axis='columns').astype(int),
            hosp_smm_aneurysm=pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                              .apply(lambda x: x.str.startswith(('441'))).any(axis='columns').astype(int),
            hosp_smm_renal=(pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                            .apply(lambda x: x.str.startswith(('6693'))).any(axis='columns') |
                            pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                            .isin(["5845", "5846", "5847", "5848", "5849"]).any(axis='columns')
                            ).astype(int),
            hosp_smm_respiratory=(pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                            .apply(lambda x: x.str.startswith(('5185'))).any(axis='columns') |
                            pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                            .isin(["51881", "51882", "51884", "7991"]).any(axis='columns')
                            ).astype(int),
            hosp_smm_embolism=pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                              .apply(lambda x: x.str.startswith(('6731'))).any(axis='columns').astype(int),
            hosp_smm_cardiac=pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                            .isin(["42741", "42742", "4275"]).any(axis='columns').astype(int),
            hosp_smm_coagulation=(pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                            .apply(lambda x: x.str.startswith(('6663'))).any(axis='columns') |
                            pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                            .isin(["2866", "2869"]).any(axis='columns')
                            ).astype(int),
            hosp_smm_eclampsia=pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                              .apply(lambda x: x.str.startswith(('6426'))).any(axis='columns').astype(int),
            hosp_smm_heart=pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                            .isin(["9971"]).any(axis='columns').astype(int),
            hosp_smm_cerebrovascular=(pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                                      .apply(lambda x: x.str.startswith(('430', '431', '432', '433', '434', '436', '437',
                                                                         '6715', '6740',))).any(axis='columns') |
                                      pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                                        .isin(["99702"]).any(axis='columns')).astype(int),
            hosp_smm_edema=pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                            .isin(["5184", "4281", "4280", "42821", "42823", "42831", "42833", "42841", "42843"]).any(axis='columns').astype(int),
            hosp_smm_anesthesia=pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                              .apply(lambda x: x.str.startswith(('6680', '6681', '6682',))
                                     ).any(axis='columns').astype(int),
            hosp_smm_sepsis=(pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                                      .apply(lambda x: x.str.startswith(('038', '6702',))).any(axis='columns') |
                                      pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                                        .isin(["99591", "99592"]).any(axis='columns')).astype(int),
            hosp_smm_shock=(pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                                      .apply(lambda x: x.str.startswith(('6691', '7855', '9980', ))).any(axis='columns') |
                                      pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                                        .isin(["9950", "9954"]).any(axis='columns')).astype(int),
            hosp_smm_sickle=pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                            .isin(["28242", "28262", "28264", "28269"]).any(axis='columns').astype(int),
            hosp_smm_thrombotic=pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                              .apply(lambda x: x.str.startswith(('4151', '6730', '6732', '6733', '6738'))
                                     ).any(axis='columns').astype(int),
            hosp_smm_cardiac_rhythm=pdf[[col for col in pdf.columns if col.startswith('PRCDR_CD') and
                                         not col.startswith('PRCDR_CD_SYS')]]
                              .apply(lambda x: x.str.startswith(('996',))
                                     ).any(axis='columns').astype(int),
            hosp_smm_transfusion=pdf[[col for col in pdf.columns if col.startswith('PRCDR_CD') and
                                         not col.startswith('PRCDR_CD_SYS')]]
                              .apply(lambda x: x.str.startswith(('990',))
                                     ).any(axis='columns').astype(int),
            hosp_smm_hysterectomy=pdf[[col for col in pdf.columns if col.startswith('PRCDR_CD') and
                                         not col.startswith('PRCDR_CD_SYS')]]
                              .apply(lambda x: x.str.startswith(('683', '684', '685', '686', '687', '688', '689',))
                                     ).any(axis='columns').astype(int),
            hosp_smm_tracheostomy=pdf[[col for col in pdf.columns if col.startswith('PRCDR_CD') and
                                       not col.startswith('PRCDR_CD_SYS')]]
                            .isin(["311"]).any(axis='columns').astype(int),
            hosp_smm_ventilation=pdf[[col for col in pdf.columns if col.startswith('PRCDR_CD') and
                                      not col.startswith('PRCDR_CD_SYS')]]
                            .isin(["9390", "9601", "9602", "9603", "9605"]).any(axis='columns').astype(int),
        ))
    df_ip_claims['hosp_smm'] = df_ip_claims[df_ip_claims.columns[df_ip_claims.columns.str.startswith('hosp_smm_')]
                                            ].max(axis=1)

    df_ip_claims["hosp_smm_no_blood"] = df_ip_claims['hosp_smm'].where((df_ip_claims['hosp_smm_transfusion'] == 0) |
                                                                       (df_ip_claims[df_ip_claims.columns[df_ip_claims
                                                                        .columns.str.startswith('hosp_smm_')]
                                                                        ].sum(axis=1) > 1),
                                                                       0)

    lst_smm_col = [col for col in df_ip_claims.columns if col.startswith('hosp_smm')]
    df_ip_claims = df_ip_claims.assign(**dict([(col + '_on_index_hosp',
                                                ((df_ip_claims[col] == 1) &
                                                 (df_ip_claims['admsn_date'] == df_ip_claims[
                                                     'index_admsn_date'])).astype(int))
                                               for col in lst_smm_col]
                                              ))
    df_ip_claims = df_ip_claims.map_partitions(lambda pdf: pdf.assign(**dict([(col + '_12weeks_after_index_hosp',
                                                                               ((pdf[col] == 1) &
                                                                                pdf['admsn_date'].between(
                                                                                    pdf['index_admsn_date'],
                                                                                    pdf[
                                                                                        "index_admsn_date"] + pd.Timedelta(
                                                                                        days=83),
                                                                                    inclusive=True)).astype(int)) for
                                                                              col in lst_smm_col]
                                                                             )))
    return df_ip_claims


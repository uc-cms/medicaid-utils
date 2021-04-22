import dask.dataframe as dd
import pandas as pd


def flag_preterm(df_ip_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Detects preterm birth related hospitalization in claims
    New Column(s):
        diag_preterm - integer column, 1 when claim denotes preterm birth and 0 otherwise
    :param df_ip_claims:
    :rtype: dd.DataFrame
    """
    df_ip_claims = df_ip_claims.map_partitions(
        lambda pdf: pdf.assign(
            hosp_preterm=(pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                          .apply(lambda x: x.str.upper().str.startswith(('V27'))).any(axis='columns') &
                          (pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                           .apply(lambda x: x.str.upper().str.startswith(('6442', '6444',
                                                                          '7650', '7651'))).any(axis='columns'))
                          ).astype(int)))
    return df_ip_claims


def flag_birth(df_ip_claims: dd.DataFrame) -> dd.DataFrame:
    """
    Detects normal and stillbirths related hospitalization in claims
    New Column(s):
        diag_birth - integer column, 1 when claim denotes live or still birth and 0 otherwise
    :param df_ip_claims:
    :rtype: dd.DataFrame
    """
    df_ip_claims = df_ip_claims.map_partitions(
        lambda pdf: pdf.assign(
            hosp_birth=pdf[pdf.columns[pdf.columns.str.startswith('DIAG_CD_')]]
                .apply(lambda x: x.str.upper().str.startswith(('V27'))).any(axis='columns').astype(int)))
    return df_ip_claims


def flag_abnormal_pregnancy(df_ip_claims: dd.DataFrame) -> dd.DataFrame:
    """
        Detects ectopic, molar, or abnormal pregnancy, spontaneous or induced abortion related hospitalization
        New Column(s):
            diag_abnormal_pregnancy - integer column, 1 when claim denotes ectopic, molar, or abnormal pregnancy,
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
    """
    ip["SMM_MYO"] = ip[["SMM_MYO_DIAG_CD_{}".format(x) for x in range(1,10)]].max(axis=1)
            ip["SMM_ANEURYSM"] = ip[["SMM_ANEURYSM_DIAG_CD_{}".format(x) for x in range(1,10)]].max(axis=1)
            ip["SMM_RENAL"] = ip[["SMM_RENAL_DIAG_CD_{}".format(x) for x in range(1,10)]].max(axis=1)
            ip["SMM_RESPIRATORY"] = ip[["SMM_RESPIRATORY_DIAG_CD_{}".format(x) for x in range(1,10)]].max(axis=1)
            ip["SMM_EMBOLISM"] = ip[["SMM_EMBOLISM_DIAG_CD_{}".format(x) for x in range(1,10)]].max(axis=1)
            ip["SMM_CARDIAC"] = ip[["SMM_CARDIAC_DIAG_CD_{}".format(x) for x in range(1,10)]].max(axis=1)
            ip["SMM_COAGULATION"] = ip[["SMM_COAGULATION_DIAG_CD_{}".format(x) for x in range(1,10)]].max(axis=1)
            ip["SMM_ECLAMPSIA"] = ip[["SMM_ECLAMPSIA_DIAG_CD_{}".format(x) for x in range(1,10)]].max(axis=1)
            ip["SMM_HEART"] = ip[["SMM_HEART_DIAG_CD_{}".format(x) for x in range(1,10)]].max(axis=1)
            ip["SMM_CEREBROVASCULAR"] = ip[["SMM_CEREBROVASCULAR_DIAG_CD_{}".format(x) for x in range(1,10)]].max(axis=1)
            ip["SMM_EDEMA"] = ip[["SMM_EDEMA_DIAG_CD_{}".format(x) for x in range(1,10)]].max(axis=1)
            ip["SMM_ANESTHESIA"] = ip[["SMM_ANESTHESIA_DIAG_CD_{}".format(x) for x in range(1,10)]].max(axis=1)
            ip["SMM_SEPSIS"] = ip[["SMM_SEPSIS_DIAG_CD_{}".format(x) for x in range(1,10)]].max(axis=1)
            ip["SMM_SHOCK"] = ip[["SMM_SHOCK_DIAG_CD_{}".format(x) for x in range(1,10)]].max(axis=1)
            ip["SMM_SICKLE"] = ip[["SMM_SICKLE_DIAG_CD_{}".format(x) for x in range(1,10)]].max(axis=1)
            ip["SMM_THROMBOTIC"] = ip[["SMM_THROMBOTIC_DIAG_CD_{}".format(x) for x in range(1,10)]].max(axis=1)
            ip["SMM_CARDIAC_RHYTHM"] = ip[["SMM_CARDIAC_RHYTHM_PRCDR_CD_{}".format(x) for x in range(1,7)]].max(axis=1)
            ip["SMM_TRANSFUSION"] = ip[["SMM_TRANSFUSION_PRCDR_CD_{}".format(x) for x in range(1,7)]].max(axis=1)
            ip["SMM_HYSTERECTOMY"] = ip[["SMM_HYSTERECTOMY_PRCDR_CD_{}".format(x) for x in range(1,7)]].max(axis=1)
            ip["SMM_TRACHEOSTOMY"] = ip[["SMM_TRACHEOSTOMY_PRCDR_CD_{}".format(x) for x in range(1,7)]].max(axis=1)
            ip["SMM_VENTILATION"] = ip[["SMM_VENTILATION_PRCDR_CD_{}".format(x) for x in range(1,7)]].max(axis=1)

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
                                      .apply(lambda x: x.str.startswith(('431', '432', '433', '434', '436', '437',
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

    df_ip_claims["hosp_smm_no_blood"] = (((df_ip_claims['hosp_smm_transfusion'] == 0) & (df_ip_claims['smm'] == 1)) |
                                         ((df_ip_claims['hosp_smm_transfusion'] == 1) &
                                          (df_ip_claims[df_ip_claims.columns[df_ip_claims
                                                                             .columns.str.startswith('hosp_smm_')]
                                                        ].sum(axis=1) > 1))).astype(int)
    return df_ip_claims


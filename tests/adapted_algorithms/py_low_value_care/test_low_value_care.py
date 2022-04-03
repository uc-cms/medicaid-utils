from datetime import datetime, timedelta
from medicaid_utils.adapted_algorithms.py_low_value_care import low_value_care


def test_low_value_care(claims_folder):
    st = "WY"
    year = 2012
    index_col = "MSIS_ID"
    (
        dct_measures,
        dct_denom,
        lst_condn,
        pdf_denom_spec,
        pdf_measure_spec,
    ) = low_value_care.LowValueCare.get_denom_measure_spec()
    pdf_dates = low_value_care.LowValueCare.combine_dates_in_claims(
        st, year, lst_condn, index_col, claims_folder
    )

    print("Testing combine_dates_in_claims")
    assert (
        pdf_dates.shape[0] == pdf_dates.index.nunique()
    ), "Grouping by index was not successful"

    pdf_tmp_dates = pdf_dates.copy(deep=True)
    pdf_tmp_dates = pdf_tmp_dates.head(10)
    first_bene = pdf_tmp_dates.index[0]
    pdf_tmp_dates.loc[
        first_bene,
        "denom_low_or_intermediate_risk_non_cardiothoracic_surgery_ot_dates",
    ] = [datetime.now()]
    pdf_tmp_dates.loc[
        first_bene,
        "msr_stress_electrocardiogram_echocardiogram_nuclear_medicine_imaging_cardiac_mri_or_ct_angiography_occurring_within_30_days_prior_to_surgery_ot_dates",
    ] = [datetime.now() + timedelta(days=-12)]
    pdf_tmp_dates = (
        low_value_care.LowValueCare.construct_low_value_care_measures(
            pdf_tmp_dates, year, dct_measures, dct_denom
        )
    )
    print("pre_operative_stress_testing test 1")
    assert (
        pdf_tmp_dates.at[
            first_bene,
            "service_stress_electrocardiogram_echocardiogram_nuclear_medicine_imaging_cardiac_mri_or_ct_angiography_occurring_within_30_days_prior_to_surgery_with_"
            + "denom_pre_operative_stress_testing",
        ]
        == 1
    ), "pre_operative_stress_testing test 1 failed"

    pdf_tmp_dates = pdf_dates.copy(deep=True)
    pdf_tmp_dates = pdf_tmp_dates.head(10)
    first_bene = pdf_tmp_dates.index[0]
    pdf_tmp_dates.loc[
        first_bene,
        "denom_low_or_intermediate_risk_non_cardiothoracic_surgery_all_dates",
    ] = [datetime.now()]
    pdf_tmp_dates.loc[
        first_bene,
        "msr_stress_electrocardiogram_echocardiogram_nuclear_medicine_imaging_cardiac_mri_or_ct_angiography_occurring_within_30_days_prior_to_surgery_ot_dates",
    ] = [datetime.now() + timedelta(days=-12)]
    pdf_tmp_dates = (
        low_value_care.LowValueCare.construct_low_value_care_measures(
            pdf_tmp_dates, year, dct_measures, dct_denom
        )
    )
    print("pre_operative_stress_testing test 2")
    assert (
        pdf_tmp_dates.at[
            first_bene,
            "service_stress_electrocardiogram_echocardiogram_nuclear_medicine_imaging_cardiac_mri_or_ct_angiography_occurring_within_30_days_prior_to_surgery_with_"
            + "denom_pre_operative_stress_testing",
        ]
        == 0
    ), "pre_operative_stress_testing test 2 failed"

    pdf_tmp_dates = pdf_dates.copy(deep=True)
    pdf_tmp_dates = pdf_tmp_dates.head(10)
    first_bene = pdf_tmp_dates.index[0]
    pdf_tmp_dates.loc[
        first_bene,
        "denom_low_or_intermediate_risk_non_cardiothoracic_surgery_ot_dates",
    ] = [datetime.now()]
    pdf_tmp_dates.loc[
        first_bene,
        "msr_stress_electrocardiogram_echocardiogram_nuclear_medicine_imaging_cardiac_mri_or_ct_angiography_occurring_within_30_days_prior_to_surgery_ot_dates",
    ] = [datetime.now() + timedelta(days=12)]
    pdf_tmp_dates = (
        low_value_care.LowValueCare.construct_low_value_care_measures(
            pdf_tmp_dates, year, dct_measures, dct_denom
        )
    )
    print("pre_operative_stress_testing test 3")
    assert (
        pdf_tmp_dates.at[
            first_bene,
            "service_stress_electrocardiogram_echocardiogram_nuclear_medicine_imaging_cardiac_mri_or_ct_angiography_occurring_within_30_days_prior_to_surgery_with_"
            + "denom_pre_operative_stress_testing",
        ]
        == 0
    ), "pre_operative_stress_testing test 3 failed"

    pdf_tmp_dates = pdf_dates.copy(deep=True)
    pdf_tmp_dates = pdf_tmp_dates.head(10)
    pdf_tmp_dates.loc[
        first_bene, "denom_acquired_hypothyroidism_all_dates"
    ] = [
        datetime.now().replace(year=2011, month=12),
        datetime.now().replace(year=2012, month=1),
    ]
    pdf_tmp_dates.loc[
        first_bene, "denom_excl_chronic_acquired_hypothyroidism_all_dates"
    ] = [
        datetime.now().replace(year=2011, month=12),
        datetime.now().replace(year=2012, month=1),
    ]
    pdf_tmp_dates.loc[first_bene, "msr_total_or_free_t3_all_dates"] = [
        datetime.now().replace(year=2012, month=1) + timedelta(days=12)
    ]
    pdf_tmp_dates = (
        low_value_care.LowValueCare.construct_low_value_care_measures(
            pdf_tmp_dates, year, dct_measures, dct_denom
        )
    )
    print("total_or_free_t3_tests_for_patients_with_hypothyroidism test 1")

    assert (
        pdf_tmp_dates.at[
            first_bene,
            "service_total_or_free_t3_with_"
            + "denom_total_or_free_t3_tests_for_patients_with_hypothyroidism",
        ]
        == 0
    ), "total_or_free_t3_tests_for_patients_with_hypothyroidism test 1 failed"

    pdf_tmp_dates = pdf_dates.copy(deep=True)
    pdf_tmp_dates = pdf_tmp_dates.head(10)
    pdf_tmp_dates.loc[
        first_bene, "denom_acquired_hypothyroidism_all_dates"
    ] = [
        datetime.now().replace(year=2011, month=12),
        datetime.now().replace(year=2012, month=1),
    ]
    pdf_tmp_dates.loc[first_bene, "msr_total_or_free_t3_all_dates"] = [
        datetime.now().replace(year=2012, month=1) + timedelta(days=12)
    ]
    pdf_tmp_dates = (
        low_value_care.LowValueCare.construct_low_value_care_measures(
            pdf_tmp_dates, year, dct_measures, dct_denom
        )
    )
    print("total_or_free_t3_tests_for_patients_with_hypothyroidism test 2")
    assert (
        pdf_tmp_dates.at[
            first_bene,
            "service_total_or_free_t3_with_"
            + "denom_total_or_free_t3_tests_for_patients_with_hypothyroidism",
        ]
        == 1
    ), "total_or_free_t3_tests_for_patients_with_hypothyroidism test 2 failed"

    pdf_tmp_dates = pdf_dates.copy(deep=True)
    pdf_tmp_dates = pdf_tmp_dates.head(10)
    pdf_tmp_dates.loc[first_bene, "denom_sinusitis_all_dates"] = [
        datetime.now().replace(year=2011, month=5),
        datetime.now().replace(year=2012, month=2),
    ]
    pdf_tmp_dates.loc[first_bene, "denom_excl_chronic_sinusitis_all_dates"] = [
        datetime.now().replace(year=2011, month=5),
        datetime.now().replace(year=2012, month=2),
    ]
    pdf_tmp_dates.loc[first_bene, "msr_ct_of_maxillofacial_area_all_dates"] = [
        datetime.now().replace(year=2012, month=2) + timedelta(days=12)
    ]
    pdf_tmp_dates = (
        low_value_care.LowValueCare.construct_low_value_care_measures(
            pdf_tmp_dates, year, dct_measures, dct_denom
        )
    )
    print("ct_for_acute_uncomplicated_rhinosinusitis test 1")
    assert (
        pdf_tmp_dates.at[
            first_bene,
            "service_ct_of_maxillofacial_area_with_"
            + "denom_ct_for_acute_uncomplicated_rhinosinusitis",
        ]
        == 0
    ), "ct_for_acute_uncomplicated_rhinosinusitis test 1 failed"

    pdf_tmp_dates = pdf_dates.copy(deep=True)
    pdf_tmp_dates = pdf_tmp_dates.head(10)
    pdf_tmp_dates.loc[first_bene, "denom_sinusitis_all_dates"] = [
        datetime.now().replace(year=2011, month=1),
        datetime.now().replace(year=2012, month=2),
    ]
    pdf_tmp_dates.loc[first_bene, "denom_excl_chronic_sinusitis_all_dates"] = [
        datetime.now().replace(year=2011, month=1),
        datetime.now().replace(year=2012, month=2),
    ]
    pdf_tmp_dates.loc[first_bene, "msr_ct_of_maxillofacial_area_all_dates"] = [
        datetime.now().replace(year=2012, month=2) + timedelta(days=12)
    ]
    pdf_tmp_dates = (
        low_value_care.LowValueCare.construct_low_value_care_measures(
            pdf_tmp_dates, year, dct_measures, dct_denom
        )
    )
    print("ct_for_acute_uncomplicated_rhinosinusitis test 2")
    assert (
        pdf_tmp_dates.at[
            first_bene,
            "service_ct_of_maxillofacial_area_with_"
            + "denom_ct_for_acute_uncomplicated_rhinosinusitis",
        ]
        == 1
    ), "ct_for_acute_uncomplicated_rhinosinusitis test 2 failed"

    pdf_tmp_dates = pdf_dates.copy(deep=True)
    pdf_tmp_dates = pdf_tmp_dates.head(10)
    pdf_tmp_dates.loc[first_bene, "denom_low_back_pain_all_dates"] = [
        datetime.now().replace(year=2012, month=2)
    ]

    pdf_tmp_dates.loc[
        first_bene, "msr_radiologic_ct_and_mri_imaging_of_spine_all_dates"
    ] = [datetime.now().replace(year=2012, month=2) + timedelta(days=12)]
    pdf_tmp_dates = (
        low_value_care.LowValueCare.construct_low_value_care_measures(
            pdf_tmp_dates, year, dct_measures, dct_denom
        )
    )
    print("imaging_for_nonspecific_low_back_pain test 1")
    assert (
        pdf_tmp_dates.at[
            first_bene,
            "service_radiologic_ct_and_mri_imaging_of_spine_with_"
            + "denom_imaging_for_nonspecific_low_back_pain",
        ]
        == 1
    ), "imaging_for_nonspecific_low_back_pain test 1 failed"

    pdf_tmp_dates = pdf_dates.copy(deep=True)
    pdf_tmp_dates = pdf_tmp_dates.head(10)
    pdf_tmp_dates.loc[first_bene, "denom_low_back_pain_all_dates"] = [
        datetime.now().replace(year=2012, month=2)
    ]
    pdf_tmp_dates.loc[
        "000CAD150864EB04BA0C63DDFDD8E8F0", "denom_excl_cancers_all_dates"
    ] = [datetime.now().replace(year=2011, month=5)]
    pdf_tmp_dates.loc[
        first_bene, "msr_radiologic_ct_and_mri_imaging_of_spine_all_dates"
    ] = [datetime.now().replace(year=2012, month=2) + timedelta(days=12)]
    pdf_tmp_dates = (
        low_value_care.LowValueCare.construct_low_value_care_measures(
            pdf_tmp_dates, year, dct_measures, dct_denom
        )
    )
    print("imaging_for_nonspecific_low_back_pain test 2")
    assert (
        pdf_tmp_dates.at[
            first_bene,
            "service_radiologic_ct_and_mri_imaging_of_spine_with_"
            + "denom_imaging_for_nonspecific_low_back_pain",
        ]
        == 0
    ), "imaging_for_nonspecific_low_back_pain test 2 failed"

    pdf_tmp_dates = pdf_dates.copy(deep=True)
    pdf_tmp_dates = pdf_tmp_dates.head(10)
    pdf_tmp_dates.loc[first_bene, "denom_low_back_pain_all_dates"] = [
        datetime.now().replace(year=2012, month=2)
    ]
    # pdf_tmp_dates.loc['000CAD150864EB04BA0C63DDFDD8E8F0', 'denom_excl_cancers_all_dates'] =
    # [datetime.now().replace(year=2011, month=5)]
    pdf_tmp_dates.loc[
        first_bene, "msr_radiologic_ct_and_mri_imaging_of_spine_all_dates"
    ] = [datetime.now().replace(year=2012, month=2) + timedelta(days=60)]
    pdf_tmp_dates = (
        low_value_care.LowValueCare.construct_low_value_care_measures(
            pdf_tmp_dates, year, dct_measures, dct_denom
        )
    )
    print("imaging_for_nonspecific_low_back_pain test 3")
    assert (
        pdf_tmp_dates.at[
            first_bene,
            "service_radiologic_ct_and_mri_imaging_of_spine_with_"
            + "denom_imaging_for_nonspecific_low_back_pain",
        ]
        == 0
    ), "imaging_for_nonspecific_low_back_pain test 3 failed"

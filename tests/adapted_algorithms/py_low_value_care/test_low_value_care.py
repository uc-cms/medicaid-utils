from datetime import datetime, timedelta

import pandas as pd
import pytest
from medicaid_utils.adapted_algorithms.py_low_value_care import low_value_care


@pytest.fixture(scope="module")
def lvc_spec():
    """Load measure and denominator specs from bundled data."""
    (
        dct_measures,
        dct_denom,
        lst_condn,
        _,
        _,
    ) = low_value_care.LowValueCare.get_denom_measure_spec()
    return dct_measures, dct_denom, lst_condn


@pytest.fixture()
def pdf_dates(lvc_spec):
    """Create a synthetic pdf_dates DataFrame with the expected structure."""
    _, _, lst_condn = lvc_spec
    suffixes = ["_ip_dates", "_ot_dates", "_ed_dates", "_all_dates"]
    cols = [c + s for c in lst_condn for s in suffixes]
    cols.append("eligibility_pattern")

    index = pd.Index(
        ["BID0001", "BID0002", "BID0003", "BID0004", "BID0005",
         "BID0006", "BID0007", "BID0008", "BID0009", "BID0010"],
        name="MSIS_ID",
    )
    pdf = pd.DataFrame(index=index, columns=cols)
    # Fill date columns with empty lists, eligibility with full-year enrollment
    for col in cols:
        if col == "eligibility_pattern":
            pdf[col] = "1" * 24
        else:
            pdf[col] = pdf[col].apply(lambda x: [])
    return pdf


class TestConstructLowValueCareMeasures:
    def test_pre_operative_stress_testing_1(self, pdf_dates, lvc_spec):
        """Service within 30 days prior to surgery should flag measure."""
        dct_measures, dct_denom, _ = lvc_spec
        first_bene = pdf_dates.index[0]
        pdf_dates.loc[
            first_bene,
            "denom_low_or_intermediate_risk_non_cardiothoracic_surgery_ot_dates",
        ] = [datetime.now()]
        pdf_dates.loc[
            first_bene,
            "msr_stress_electrocardiogram_echocardiogram_nuclear_medicine_imaging"
            "_cardiac_mri_or_ct_angiography_occurring_within_30_days_prior_to"
            "_surgery_ot_dates",
        ] = [datetime.now() + timedelta(days=-12)]
        result = low_value_care.LowValueCare.construct_low_value_care_measures(
            pdf_dates, 2012, dct_measures, dct_denom
        )
        assert (
            result.at[
                first_bene,
                "service_stress_electrocardiogram_echocardiogram_nuclear_medicine"
                "_imaging_cardiac_mri_or_ct_angiography_occurring_within_30_days"
                "_prior_to_surgery_with_"
                "denom_pre_operative_stress_testing",
            ]
            == 1
        ), "pre_operative_stress_testing test 1 failed"

    def test_pre_operative_stress_testing_2(self, pdf_dates, lvc_spec):
        """Using all_dates instead of ot_dates should not flag measure."""
        dct_measures, dct_denom, _ = lvc_spec
        first_bene = pdf_dates.index[0]
        pdf_dates.loc[
            first_bene,
            "denom_low_or_intermediate_risk_non_cardiothoracic_surgery_all_dates",
        ] = [datetime.now()]
        pdf_dates.loc[
            first_bene,
            "msr_stress_electrocardiogram_echocardiogram_nuclear_medicine_imaging"
            "_cardiac_mri_or_ct_angiography_occurring_within_30_days_prior_to"
            "_surgery_ot_dates",
        ] = [datetime.now() + timedelta(days=-12)]
        result = low_value_care.LowValueCare.construct_low_value_care_measures(
            pdf_dates, 2012, dct_measures, dct_denom
        )
        assert (
            result.at[
                first_bene,
                "service_stress_electrocardiogram_echocardiogram_nuclear_medicine"
                "_imaging_cardiac_mri_or_ct_angiography_occurring_within_30_days"
                "_prior_to_surgery_with_"
                "denom_pre_operative_stress_testing",
            ]
            == 0
        ), "pre_operative_stress_testing test 2 failed"

    def test_pre_operative_stress_testing_3(self, pdf_dates, lvc_spec):
        """Service AFTER surgery (positive days) should not flag measure."""
        dct_measures, dct_denom, _ = lvc_spec
        first_bene = pdf_dates.index[0]
        pdf_dates.loc[
            first_bene,
            "denom_low_or_intermediate_risk_non_cardiothoracic_surgery_ot_dates",
        ] = [datetime.now()]
        pdf_dates.loc[
            first_bene,
            "msr_stress_electrocardiogram_echocardiogram_nuclear_medicine_imaging"
            "_cardiac_mri_or_ct_angiography_occurring_within_30_days_prior_to"
            "_surgery_ot_dates",
        ] = [datetime.now() + timedelta(days=12)]
        result = low_value_care.LowValueCare.construct_low_value_care_measures(
            pdf_dates, 2012, dct_measures, dct_denom
        )
        assert (
            result.at[
                first_bene,
                "service_stress_electrocardiogram_echocardiogram_nuclear_medicine"
                "_imaging_cardiac_mri_or_ct_angiography_occurring_within_30_days"
                "_prior_to_surgery_with_"
                "denom_pre_operative_stress_testing",
            ]
            == 0
        ), "pre_operative_stress_testing test 3 failed"

    def test_hypothyroidism_with_exclusion(self, pdf_dates, lvc_spec):
        """Chronic exclusion present should result in 0."""
        dct_measures, dct_denom, _ = lvc_spec
        first_bene = pdf_dates.index[0]
        pdf_dates.at[first_bene, "denom_acquired_hypothyroidism_all_dates"] = [
            datetime.now().replace(year=2011, month=12),
            datetime.now().replace(year=2012, month=1),
        ]
        pdf_dates.at[
            first_bene, "denom_excl_chronic_acquired_hypothyroidism_all_dates"
        ] = [
            datetime.now().replace(year=2011, month=12),
            datetime.now().replace(year=2012, month=1),
        ]
        pdf_dates.loc[first_bene, "msr_total_or_free_t3_all_dates"] = [
            datetime.now().replace(year=2012, month=1) + timedelta(days=12)
        ]
        result = low_value_care.LowValueCare.construct_low_value_care_measures(
            pdf_dates, 2012, dct_measures, dct_denom
        )
        assert (
            result.at[
                first_bene,
                "service_total_or_free_t3_with_"
                "denom_total_or_free_t3_tests_for_patients_with_hypothyroidism",
            ]
            == 0
        )

    def test_hypothyroidism_without_exclusion(self, pdf_dates, lvc_spec):
        """Without chronic exclusion, measure should flag."""
        dct_measures, dct_denom, _ = lvc_spec
        first_bene = pdf_dates.index[0]
        pdf_dates.at[first_bene, "denom_acquired_hypothyroidism_all_dates"] = [
            datetime.now().replace(year=2011, month=12),
            datetime.now().replace(year=2012, month=1),
        ]
        pdf_dates.loc[first_bene, "msr_total_or_free_t3_all_dates"] = [
            datetime.now().replace(year=2012, month=1) + timedelta(days=12)
        ]
        result = low_value_care.LowValueCare.construct_low_value_care_measures(
            pdf_dates, 2012, dct_measures, dct_denom
        )
        assert (
            result.at[
                first_bene,
                "service_total_or_free_t3_with_"
                "denom_total_or_free_t3_tests_for_patients_with_hypothyroidism",
            ]
            == 1
        )

    def test_sinusitis_with_chronic_exclusion(self, pdf_dates, lvc_spec):
        """Chronic sinusitis exclusion should result in 0."""
        dct_measures, dct_denom, _ = lvc_spec
        first_bene = pdf_dates.index[0]
        pdf_dates.at[first_bene, "denom_sinusitis_all_dates"] = [
            datetime.now().replace(year=2011, month=5),
            datetime.now().replace(year=2012, month=2),
        ]
        pdf_dates.at[
            first_bene, "denom_excl_chronic_sinusitis_all_dates"
        ] = [
            datetime.now().replace(year=2011, month=5),
            datetime.now().replace(year=2012, month=2),
        ]
        pdf_dates.loc[
            first_bene, "msr_ct_of_maxillofacial_area_all_dates"
        ] = [
            datetime.now().replace(year=2012, month=2) + timedelta(days=12)
        ]
        result = low_value_care.LowValueCare.construct_low_value_care_measures(
            pdf_dates, 2012, dct_measures, dct_denom
        )
        assert (
            result.at[
                first_bene,
                "service_ct_of_maxillofacial_area_with_"
                "denom_ct_for_acute_uncomplicated_rhinosinusitis",
            ]
            == 0
        )

    def test_sinusitis_without_chronic_exclusion(self, pdf_dates, lvc_spec):
        """Without chronic exclusion, sinusitis measure should flag."""
        dct_measures, dct_denom, _ = lvc_spec
        first_bene = pdf_dates.index[0]
        pdf_dates.at[first_bene, "denom_sinusitis_all_dates"] = [
            datetime.now().replace(year=2011, month=1),
            datetime.now().replace(year=2012, month=2),
        ]
        pdf_dates.at[
            first_bene, "denom_excl_chronic_sinusitis_all_dates"
        ] = [
            datetime.now().replace(year=2011, month=1),
            datetime.now().replace(year=2012, month=2),
        ]
        pdf_dates.loc[
            first_bene, "msr_ct_of_maxillofacial_area_all_dates"
        ] = [
            datetime.now().replace(year=2012, month=2) + timedelta(days=12)
        ]
        result = low_value_care.LowValueCare.construct_low_value_care_measures(
            pdf_dates, 2012, dct_measures, dct_denom
        )
        assert (
            result.at[
                first_bene,
                "service_ct_of_maxillofacial_area_with_"
                "denom_ct_for_acute_uncomplicated_rhinosinusitis",
            ]
            == 1
        )

    def test_low_back_pain_imaging_flags(self, pdf_dates, lvc_spec):
        """Low back pain with imaging within followup should flag."""
        dct_measures, dct_denom, _ = lvc_spec
        first_bene = pdf_dates.index[0]
        pdf_dates.loc[first_bene, "denom_low_back_pain_all_dates"] = [
            datetime.now().replace(year=2012, month=2)
        ]
        pdf_dates.loc[
            first_bene,
            "msr_radiologic_ct_and_mri_imaging_of_spine_all_dates",
        ] = [
            datetime.now().replace(year=2012, month=2) + timedelta(days=12)
        ]
        result = low_value_care.LowValueCare.construct_low_value_care_measures(
            pdf_dates, 2012, dct_measures, dct_denom
        )
        assert (
            result.at[
                first_bene,
                "service_radiologic_ct_and_mri_imaging_of_spine_with_"
                "denom_imaging_for_nonspecific_low_back_pain",
            ]
            == 1
        )

    def test_low_back_pain_with_cancer_exclusion(self, pdf_dates, lvc_spec):
        """Cancer exclusion should suppress low back pain imaging flag."""
        dct_measures, dct_denom, _ = lvc_spec
        first_bene = pdf_dates.index[0]
        pdf_dates.loc[first_bene, "denom_low_back_pain_all_dates"] = [
            datetime.now().replace(year=2012, month=2)
        ]
        pdf_dates.loc[first_bene, "denom_excl_cancers_all_dates"] = [
            datetime.now().replace(year=2011, month=5)
        ]
        pdf_dates.loc[
            first_bene,
            "msr_radiologic_ct_and_mri_imaging_of_spine_all_dates",
        ] = [
            datetime.now().replace(year=2012, month=2) + timedelta(days=12)
        ]
        result = low_value_care.LowValueCare.construct_low_value_care_measures(
            pdf_dates, 2012, dct_measures, dct_denom
        )
        assert (
            result.at[
                first_bene,
                "service_radiologic_ct_and_mri_imaging_of_spine_with_"
                "denom_imaging_for_nonspecific_low_back_pain",
            ]
            == 0
        )

    def test_low_back_pain_imaging_outside_window(self, pdf_dates, lvc_spec):
        """Imaging outside followup window should not flag."""
        dct_measures, dct_denom, _ = lvc_spec
        first_bene = pdf_dates.index[0]
        pdf_dates.loc[first_bene, "denom_low_back_pain_all_dates"] = [
            datetime.now().replace(year=2012, month=2)
        ]
        pdf_dates.loc[
            first_bene,
            "msr_radiologic_ct_and_mri_imaging_of_spine_all_dates",
        ] = [
            datetime.now().replace(year=2012, month=2) + timedelta(days=60)
        ]
        result = low_value_care.LowValueCare.construct_low_value_care_measures(
            pdf_dates, 2012, dct_measures, dct_denom
        )
        assert (
            result.at[
                first_bene,
                "service_radiologic_ct_and_mri_imaging_of_spine_with_"
                "denom_imaging_for_nonspecific_low_back_pain",
            ]
            == 0
        )

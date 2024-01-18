with smart_data_indicators as (
    select
        smart_data_element_all.pat_key,
        smart_data_element_all.patient_name,
        smart_data_element_all.mrn,
        smart_data_element_all.dob,
        max(case
            when smart_data_element_all.concept_id = 'CHOP#7604'
            then cast('1840-12-31' as date) + cast(smart_data_element_all.element_value as int)
        end) as microalbuminuria_last_screened_date,
        max(case
            when smart_data_element_all.concept_id = 'CHOP#7544'
            then cast('1840-12-31' as date) + cast(smart_data_element_all.element_value as int)
        end) as retinopathy_last_screened_date,
        max(case
            when smart_data_element_all.concept_id = 'CHOP#7601'
            then cast('1840-12-31' as date) + cast(smart_data_element_all.element_value as int)
        end) as labs_last_screened_date,
        max(case
            when smart_data_element_all.concept_id = 'EPIC#OPH451'
            then smart_data_element_all.encounter_date
        end) as opthalmology_eye_exam_date,
        row_number() over(
            partition by
                smart_data_element_all.pat_key
            order by
                microalbuminuria_last_screened_date desc, --noqa: L028
                retinopathy_last_screened_date desc, --noqa: L028
                labs_last_screened_date desc, --noqa: L028
                opthalmology_eye_exam_date desc --noqa: L028
        ) as row_num
    from
        {{ref('smart_data_element_all') }} as smart_data_element_all
    where
        smart_data_element_all.concept_id in (
            'CHOP#7604', --LAST MICROALBUMINURIA DATE
            'CHOP#7544', --LAST RETINOPATHY SCREENING
            'CHOP#7601', --LAST ANNUAL LABS
            'EPIC#OPH451' --OPHTH DILATION EYES 
        )
    group by
        smart_data_element_all.pat_key,
        smart_data_element_all.patient_name,
        smart_data_element_all.mrn,
        smart_data_element_all.dob
)

select distinct
    stg_usnwr_diabetes_primary_pop.submission_year,
    stg_usnwr_diabetes_primary_pop.primary_key as patient_key,
    stg_usnwr_diabetes_primary_pop.mrn,
    stg_usnwr_diabetes_primary_pop.patient_name,
    stg_usnwr_diabetes_primary_pop.dob,
    stg_usnwr_diabetes_primary_pop.current_age,
    stg_usnwr_diabetes_primary_pop.insurance_status,
    stg_usnwr_diabetes_primary_pop.diabetes_type_12,
    coalesce(stg_usnwr_diabetes_procedure_labs.microa_date,
        smart_data_indicators.microalbuminuria_last_screened_date) as microalbuminuria_last_screened_date,
    case
        when smart_data_indicators.microalbuminuria_last_screened_date
            between (current_date - interval('1year')) and current_date
        then 1 else stg_usnwr_diabetes_procedure_labs.microa_1_yr
    end as microalbuminuria_1yr_ind,
    coalesce(smart_data_indicators.retinopathy_last_screened_date,
        smart_data_indicators.opthalmology_eye_exam_date) as retinopathy_last_screened_date,
    case
        when smart_data_indicators.retinopathy_last_screened_date
            between (current_date - interval('2 year')) and current_date
        then 1
        when smart_data_indicators.opthalmology_eye_exam_date
            between (current_date - interval('2 year')) and current_date
        then 1 else 0
    end as retinopathy_2yr_ind,
    smart_data_indicators.labs_last_screened_date,
    case
        when smart_data_indicators.labs_last_screened_date
            between (current_date - interval('1 year'))  and current_date
        then 1 else 0
    end as annual_labs_1yr_ind,
    case
        when smart_data_indicators.labs_last_screened_date
            between (current_date - interval('2 year'))  and current_date
        then 1 else 0
    end as annual_labs_2yr_ind,
    case
        when smart_data_indicators.labs_last_screened_date
            between (current_date - interval('3 year'))  and current_date
        then 1 else 0
    end as annual_labs_3yr_ind,
    stg_usnwr_diabetes_lipid_panel.result_date as lipid_result_date,
    stg_usnwr_diabetes_lipid_panel.result_value as lipid_result,
    stg_usnwr_diabetes_lipid_panel.abnormal_result_ind,
    coalesce(annual_labs_1yr_ind, stg_usnwr_diabetes_lipid_panel.lipid_1yr_ind) as lipid_1yr_ind,
    coalesce(annual_labs_3yr_ind, stg_usnwr_diabetes_lipid_panel.lipid_3yr_ind) as lipid_3yr_ind,
    coalesce(stg_usnwr_diabetes_tsh.tsh_2_yr, annual_labs_2yr_ind) as tsh_2_yr_ind,
    coalesce(stg_usnwr_diabetes_tsh.tsh_date, smart_data_indicators.labs_last_screened_date) as tsh_date,
    stg_usnwr_diabetes_primary_pop.start_date,
    stg_usnwr_diabetes_primary_pop.end_date,
    stg_usnwr_diabetes_primary_pop.pat_key
from
    {{ref('stg_usnwr_diabetes_primary_pop')}} as stg_usnwr_diabetes_primary_pop
    inner join {{ref('diabetes_patient_all')}} as diabetes_patient_all
        on stg_usnwr_diabetes_primary_pop.primary_key = diabetes_patient_all.patient_key
    left join smart_data_indicators
        on stg_usnwr_diabetes_primary_pop.pat_key = smart_data_indicators.pat_key
            and smart_data_indicators.row_num = '1'
    left join {{ref('stg_usnwr_diabetes_lipid_panel')}} as stg_usnwr_diabetes_lipid_panel
        on stg_usnwr_diabetes_primary_pop.primary_key = stg_usnwr_diabetes_lipid_panel.patient_key
            and stg_usnwr_diabetes_lipid_panel.ldl_num = '1'
    left join {{ref('stg_usnwr_diabetes_tsh')}} as stg_usnwr_diabetes_tsh
        on stg_usnwr_diabetes_tsh.patient_key = stg_usnwr_diabetes_primary_pop.primary_key
    left join {{ref('stg_usnwr_diabetes_procedure_labs')}} as stg_usnwr_diabetes_procedure_labs
        on stg_usnwr_diabetes_procedure_labs.pat_key = stg_usnwr_diabetes_primary_pop.pat_key

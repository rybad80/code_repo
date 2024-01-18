select
    'operational' as domain, --noqa: L029
    'finance' as subdomain,
    stg_usnwr_diabetes_labs_final.patient_key as primary_key,
    stg_usnwr_diabetes_labs_final.metric_date,
    case
        when usnews_metadata_calendar.metric_id = 'c31a'
            and stg_usnwr_diabetes_labs_final.tsh_2_yr = '1'
        then stg_usnwr_diabetes_labs_final.patient_key
        when usnews_metadata_calendar.metric_id = 'c31b'
            and stg_usnwr_diabetes_labs_final.over_11 = '1'
            and stg_usnwr_diabetes_labs_final.lipid_3yr_ind = '1'
        then stg_usnwr_diabetes_labs_final.patient_key
        when usnews_metadata_calendar.metric_id = 'c31c'
            and stg_usnwr_diabetes_labs_final.over_11 = '1'
            and stg_usnwr_diabetes_labs_final.diabetes_5_years = '1'
            and stg_usnwr_diabetes_labs_final.microa_1_yr = '1'
        then stg_usnwr_diabetes_labs_final.patient_key
        when usnews_metadata_calendar.metric_id = 'c31d'
            and stg_usnwr_diabetes_labs_final.over_11 = '1'
            and stg_usnwr_diabetes_labs_final.diabetes_5_years = '1'
            and stg_usnwr_diabetes_labs_final.retinopathy_screen_2_yr = '1'
        then stg_usnwr_diabetes_labs_final.patient_key
    end as num,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.metric_id,
    stg_usnwr_diabetes_labs_final.submission_year,
    stg_usnwr_diabetes_labs_final.patient_name,
    stg_usnwr_diabetes_labs_final.mrn,
    stg_usnwr_diabetes_labs_final.dob,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.division,
    case
        when usnews_metadata_calendar.metric_id = 'c31a'
        then stg_usnwr_diabetes_labs_final.patient_key
        when usnews_metadata_calendar.metric_id = 'c31b'
            and stg_usnwr_diabetes_labs_final.over_11 = '1'
        then stg_usnwr_diabetes_labs_final.patient_key
        when usnews_metadata_calendar.metric_id = 'c31c'
            and stg_usnwr_diabetes_labs_final.over_11 = '1'
            and stg_usnwr_diabetes_labs_final.diabetes_5_years = '1'
        then stg_usnwr_diabetes_labs_final.patient_key
        when usnews_metadata_calendar.metric_id = 'c31d'
            and stg_usnwr_diabetes_labs_final.over_11 = '1'
            and stg_usnwr_diabetes_labs_final.diabetes_5_years = '1'
        then stg_usnwr_diabetes_labs_final.patient_key
    end as denom
from
    {{ref('stg_usnwr_diabetes_labs_final')}} as stg_usnwr_diabetes_labs_final
    inner join {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
        on usnews_metadata_calendar.metric_id in ('c31a', 'c31b', 'c31c', 'c31d')
where
    stg_usnwr_diabetes_labs_final.diabetes_type_12 = 'Type 1'
    and (num is not null or denom is not null) -- filter out rows with both num and denom null
union all
select
    'operational' as domain, --noqa: L029
    'finance' as subdomain,
    stg_usnwr_diabetes_labs_final.patient_key as primary_key,
    stg_usnwr_diabetes_labs_final.metric_date,
    case
        when usnews_metadata_calendar.metric_id = 'c31e'
            and stg_usnwr_diabetes_labs_final.lipid_1yr_ind = '1'
        then stg_usnwr_diabetes_labs_final.patient_key
        when usnews_metadata_calendar.metric_id = 'c31f'
            and stg_usnwr_diabetes_labs_final.microa_1_yr = '1'
        then stg_usnwr_diabetes_labs_final.patient_key
        when usnews_metadata_calendar.metric_id = 'c31g'
            and stg_usnwr_diabetes_labs_final.retinopathy_screen_2_yr = '1'
        then stg_usnwr_diabetes_labs_final.patient_key
    end as num,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.metric_id,
    stg_usnwr_diabetes_labs_final.submission_year,
    stg_usnwr_diabetes_labs_final.patient_name,
    stg_usnwr_diabetes_labs_final.mrn,
    stg_usnwr_diabetes_labs_final.dob,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.division,
    case
        when usnews_metadata_calendar.metric_id in ('c31e', 'c31f', 'c31g')
        then stg_usnwr_diabetes_labs_final.patient_key
    end as denom
from
    {{ref('stg_usnwr_diabetes_labs_final')}} as stg_usnwr_diabetes_labs_final
    inner join {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
        on usnews_metadata_calendar.metric_id in ('c31e', 'c31f', 'c31g')
where
    stg_usnwr_diabetes_labs_final.diabetes_type_12 = 'Type 2'
    and (num is not null or denom is not null) -- filter out rows with both num and denom null

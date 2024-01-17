select distinct
    stg_usnwr_diabetes_primary_pop.primary_key,
    stg_usnwr_diabetes_primary_pop.patient_name,
    stg_usnwr_diabetes_primary_pop.mrn,
    stg_usnwr_diabetes_primary_pop.dob,
    case
        when stg_usnwr_diabetes_primary_pop.current_age < 8 then '0-7'
        when stg_usnwr_diabetes_primary_pop.current_age < 14 then '8-13'
        when stg_usnwr_diabetes_primary_pop.current_age < 19 then '14-18'
    end as age_group,
    stg_usnwr_diabetes_primary_pop.insurance_status,
    diabetes_lab_result.most_recent_a1c_result,
    diabetes_lab_result.most_recent_a1c_date,
    case
        when diabetes_lab_result.most_recent_a1c_result <= 7.5
        then 1 else 0
    end as optimal_a1c_ind,
    case
        -- these are unique patients for just the counts in c35.1a to c35.1c
        when lower(stg_usnwr_diabetes_primary_pop.insurance_status) = 'private'
            and ((age_group = '0-7' and usnews_metadata_calendar.metric_id = 'c35.1a1')
                or (age_group = '8-13' and usnews_metadata_calendar.metric_id = 'c35.1b1')
                or (age_group = '14-18' and usnews_metadata_calendar.metric_id = 'c35.1c1'))
        then stg_usnwr_diabetes_primary_pop.primary_key
        when lower(stg_usnwr_diabetes_primary_pop.insurance_status) = 'non-private'
            and ((age_group = '0-7' and usnews_metadata_calendar.metric_id = 'c35.1a2')
                or (age_group = '8-13' and usnews_metadata_calendar.metric_id = 'c35.1b2')
                or (age_group = '14-18' and usnews_metadata_calendar.metric_id = 'c35.1c2'))
        then stg_usnwr_diabetes_primary_pop.primary_key
        -- these are unique patients with optimal a1c for just the counts in c35.1d to c35.1f
        when lower(stg_usnwr_diabetes_primary_pop.insurance_status) = 'private'
            and optimal_a1c_ind = '1'
            and ((age_group = '0-7' and usnews_metadata_calendar.metric_id = 'c35.1d1')
                or (age_group = '8-13' and usnews_metadata_calendar.metric_id = 'c35.1e1')
                or (age_group = '14-18' and usnews_metadata_calendar.metric_id = 'c35.1f1'))
        then stg_usnwr_diabetes_primary_pop.primary_key
        when lower(stg_usnwr_diabetes_primary_pop.insurance_status) = 'non-private'
            and optimal_a1c_ind = '1'
            and ((age_group = '0-7' and usnews_metadata_calendar.metric_id = 'c35.1d2')
                or (age_group = '8-13' and usnews_metadata_calendar.metric_id = 'c35.1e2')
                or (age_group = '14-18' and usnews_metadata_calendar.metric_id = 'c35.1f2'))
        then stg_usnwr_diabetes_primary_pop.primary_key
        --numberator value for c35.1 questions related to percentages
        when lower(stg_usnwr_diabetes_primary_pop.insurance_status) = 'private'
            and optimal_a1c_ind = '1'
            and ((age_group = '0-7' and usnews_metadata_calendar.metric_id = 'c35.1a')
                or (age_group = '8-13' and usnews_metadata_calendar.metric_id = 'c35.1c')
                or (age_group = '14-18' and usnews_metadata_calendar.metric_id = 'c35.1e'))
        then stg_usnwr_diabetes_primary_pop.primary_key
        when lower(stg_usnwr_diabetes_primary_pop.insurance_status) = 'non-private'
            and optimal_a1c_ind = '1'
            and ((age_group = '0-7' and usnews_metadata_calendar.metric_id = 'c35.1b')
                or (age_group = '8-13' and usnews_metadata_calendar.metric_id = 'c35.1d')
                or (age_group = '14-18' and usnews_metadata_calendar.metric_id = 'c35.1f'))
        then stg_usnwr_diabetes_primary_pop.primary_key
        else null
    end as num,
    case
        when lower(stg_usnwr_diabetes_primary_pop.insurance_status) = 'private'
            and ((age_group = '0-7' and usnews_metadata_calendar.metric_id = 'c35.1a')
                or (age_group = '8-13' and usnews_metadata_calendar.metric_id = 'c35.1c')
                or (age_group = '14-18' and usnews_metadata_calendar.metric_id = 'c35.1e'))
        then stg_usnwr_diabetes_primary_pop.primary_key
        when lower(stg_usnwr_diabetes_primary_pop.insurance_status) = 'non-private'
            and ((age_group = '0-7' and usnews_metadata_calendar.metric_id = 'c35.1b')
                or (age_group = '8-13' and usnews_metadata_calendar.metric_id = 'c35.1d')
                or (age_group = '14-18' and usnews_metadata_calendar.metric_id = 'c35.1f'))
        then stg_usnwr_diabetes_primary_pop.primary_key
        else null
    end as denom,
    stg_usnwr_diabetes_primary_pop.most_recent_endo_encounter as last_endo_visit_date,
    round(months_between(last_endo_visit_date, stg_usnwr_diabetes_primary_pop.dob) / 12, 2) as age_at_last_visit,
    stg_usnwr_diabetes_primary_pop.submission_year,
    stg_usnwr_diabetes_primary_pop.start_date,
    stg_usnwr_diabetes_primary_pop.end_date,
    usnews_metadata_calendar.metric_name,
    usnews_metadata_calendar.metric_id,
    usnews_metadata_calendar.question_number,
    usnews_metadata_calendar.division,
    stg_usnwr_diabetes_primary_pop.encounter_key
from
    {{ref('stg_usnwr_diabetes_primary_pop')}} as stg_usnwr_diabetes_primary_pop
    inner join {{ref('diabetes_lab_result')}} as diabetes_lab_result
        on diabetes_lab_result.patient_key = stg_usnwr_diabetes_primary_pop.primary_key
            and (diabetes_lab_result.most_recent_a1c_date between stg_usnwr_diabetes_primary_pop.start_date
                and stg_usnwr_diabetes_primary_pop.end_date)
    inner join {{ref('usnews_metadata_calendar')}} as usnews_metadata_calendar
        on lower(usnews_metadata_calendar.question_number) like 'c35.1%'
where
    stg_usnwr_diabetes_primary_pop.diabetes_type_12 = 'Type 1'
    and (lower(stg_usnwr_diabetes_primary_pop.insurance_status) = 'private'
            and ((age_group = '0-7' and usnews_metadata_calendar.metric_id = 'c35.1a1')
                or (age_group = '8-13' and usnews_metadata_calendar.metric_id = 'c35.1b1')
                or (age_group = '14-18' and usnews_metadata_calendar.metric_id = 'c35.1c1')))
        or (lower(stg_usnwr_diabetes_primary_pop.insurance_status) = 'non-private'
            and ((age_group = '0-7' and usnews_metadata_calendar.metric_id = 'c35.1a2')
                or (age_group = '8-13' and usnews_metadata_calendar.metric_id = 'c35.1b2')
                or (age_group = '14-18' and usnews_metadata_calendar.metric_id = 'c35.1c2')))
        -- these are unique patients with optimal a1c for just the counts in c35.1d to c35.1f
        or (lower(stg_usnwr_diabetes_primary_pop.insurance_status) = 'private'
            and optimal_a1c_ind = '1'
            and ((age_group = '0-7' and usnews_metadata_calendar.metric_id = 'c35.1d1')
                or (age_group = '8-13' and usnews_metadata_calendar.metric_id = 'c35.1e1')
                or (age_group = '14-18' and usnews_metadata_calendar.metric_id = 'c35.1f1')))
        or (lower(stg_usnwr_diabetes_primary_pop.insurance_status) = 'non-private'
            and optimal_a1c_ind = '1'
            and ((age_group = '0-7' and usnews_metadata_calendar.metric_id = 'c35.1d2')
                or (age_group = '8-13' and usnews_metadata_calendar.metric_id = 'c35.1e2')
                or (age_group = '14-18' and usnews_metadata_calendar.metric_id = 'c35.1f2')))
        or (lower(stg_usnwr_diabetes_primary_pop.insurance_status) = 'private'
            and optimal_a1c_ind = '1'
            and ((age_group = '0-7' and usnews_metadata_calendar.metric_id = 'c35.1a')
                or (age_group = '8-13' and usnews_metadata_calendar.metric_id = 'c35.1c')
                or (age_group = '14-18' and usnews_metadata_calendar.metric_id = 'c35.1e')))
        or (lower(stg_usnwr_diabetes_primary_pop.insurance_status) = 'non-private'
            and optimal_a1c_ind = '1'
            and ((age_group = '0-7' and usnews_metadata_calendar.metric_id = 'c35.1b')
                or (age_group = '8-13' and usnews_metadata_calendar.metric_id = 'c35.1d')
                or (age_group = '14-18' and usnews_metadata_calendar.metric_id = 'c35.1f')))
    -- filter out rows with both num and denom null
    and (num is not null or denom is not null)
